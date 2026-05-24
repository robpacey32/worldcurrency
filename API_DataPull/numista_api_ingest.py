import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Optional

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account


# =========================
# CONFIG
# =========================
PROJECT_ID = "currency-pacey32-github"
DATASET_ID = "numistascrape"
DATASET_ID_VIEW = "numistaviews"

COUNTRIES_SOURCE = f"{PROJECT_ID}.{DATASET_ID_VIEW}.1_Countries_Latest"
SEARCH_RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_numista_search_results"
DETAIL_RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_numista_type_detail"
PROGRESS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.numista_api_progress"
STATE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.numista_country_refresh_state"

NUMISTA_BASE_URL = "https://api.numista.com/v3"
NUMISTA_CATEGORY = "banknote"
NUMISTA_LANG = "en"

SEARCH_PAGE_SIZE = 50
SLEEP_SECONDS = 2.0
REQUEST_TIMEOUT = 60

MAX_SEARCH_REQUESTS_PER_RUN = 500
MAX_DETAIL_REQUESTS_PER_RUN = 2500

PROGRESS_BATCH_SIZE = 50

TEST_ISSUERS = []   # set to [] to run all issuers
MAX_NEW_TYPE_IDS_FOR_TEST = None  # None for no limit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =========================
# AUTH / CLIENTS
# =========================
class APIKeyManager:
    def __init__(self, keys: List[str]):
        cleaned_keys = [k.strip() for k in keys if k and k.strip()]
        if not cleaned_keys:
            raise ValueError("No API keys provided.")

        self.keys = cleaned_keys
        self.index = 0
        self.exhausted_keys: Set[str] = set()

    def get_current_key(self) -> str:
        return self.keys[self.index]

    def get_current_headers(self) -> Dict[str, str]:
        return {
            "Numista-API-Key": self.get_current_key(),
            "Accept": "application/json",
            "User-Agent": "pacey32-numista-loader/1.0",
        }

    def mark_current_key_failed(self, reason: str) -> None:
        failed_key = self.get_current_key()
        self.exhausted_keys.add(failed_key)

        masked_key = f"{failed_key[:4]}..." if len(failed_key) >= 4 else "***"
        logging.warning("Marking current API key as failed (%s): %s", reason, masked_key)

        remaining_indices = [
            i for i, key in enumerate(self.keys) if key not in self.exhausted_keys
        ]

        if not remaining_indices:
            raise RuntimeError("All Numista API keys exhausted.")

        self.index = remaining_indices[0]
        next_key = self.get_current_key()
        masked_next = f"{next_key[:4]}..." if len(next_key) >= 4 else "***"
        logging.info(
            "Switched to next Numista API key: %s (%s remaining)",
            masked_next,
            len(remaining_indices),
        )

    def get_active_key_count(self) -> int:
        return len([k for k in self.keys if k not in self.exhausted_keys])


def get_api_keys() -> List[str]:
    keys_str = os.environ.get("NUMISTA_API_KEYS")
    if not keys_str:
        raise ValueError("NUMISTA_API_KEYS environment variable is missing.")
    return [k.strip() for k in keys_str.split(",") if k.strip()]


def get_bq_client() -> bigquery.Client:
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str:
        info = json.loads(json_str)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=info["project_id"], credentials=credentials)

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        return bigquery.Client.from_service_account_json(creds_path)

    return bigquery.Client(project=PROJECT_ID)


# =========================
# BIGQUERY SETUP
# =========================
def ensure_tables_exist(client: bigquery.Client) -> None:
    search_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("issuer_name", "STRING"),
        bigquery.SchemaField("search_query", "STRING"),
        bigquery.SchemaField("page_number", "INT64"),
        bigquery.SchemaField("result_position", "INT64"),
        bigquery.SchemaField("type_id", "INT64"),
        bigquery.SchemaField("type_url", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("api_total", "INT64"),
        bigquery.SchemaField("api_pages", "INT64"),
        bigquery.SchemaField("matched_on_issuer_name", "BOOL"),
        bigquery.SchemaField("raw_result_json", "JSON"),
    ]

    detail_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("type_id", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("detail_url", "STRING"),
        bigquery.SchemaField("raw_detail_json", "JSON"),
    ]

    progress_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("issuer_name", "STRING"),
        bigquery.SchemaField("stage", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("search_rows_written", "INT64"),
        bigquery.SchemaField("detail_rows_written", "INT64"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    ]

    state_schema = [
        bigquery.SchemaField("issuer_name", "STRING"),
        bigquery.SchemaField("last_successful_run_ts", "TIMESTAMP"),
        bigquery.SchemaField("last_attempted_run_ts", "TIMESTAMP"),
        bigquery.SchemaField("last_run_id", "STRING"),
        bigquery.SchemaField("last_status", "STRING"),
        bigquery.SchemaField("last_message", "STRING"),
        bigquery.SchemaField("search_rows_written", "INT64"),
        bigquery.SchemaField("detail_rows_written", "INT64"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    for table_id, schema in [
        (SEARCH_RAW_TABLE, search_schema),
        (DETAIL_RAW_TABLE, detail_schema),
        (PROGRESS_TABLE, progress_schema),
        (STATE_TABLE, state_schema),
    ]:
        try:
            client.get_table(table_id)
            logging.info("Table exists: %s", table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            logging.info("Created table: %s", table_id)


# =========================
# NUMISTA API
# =========================
def numista_get(
    path: str,
    params: Dict[str, Any],
    key_manager: APIKeyManager,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    url = f"{NUMISTA_BASE_URL}{path}"
    requester = session or requests

    while True:
        headers = key_manager.get_current_headers()

        try:
            response = requester.get(
                url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 60
                )

                logging.warning(
                    "HTTP 429 rate/quota limit hit for %s with params=%s. "
                    "Sleeping for %s seconds before retrying same key.",
                    path,
                    params,
                    wait_seconds,
                )

                time.sleep(wait_seconds)
                continue

            if response.status_code in (401, 403):
                key_manager.mark_current_key_failed(
                    f"HTTP {response.status_code} auth/quota error"
                )
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code in (401, 403):
                key_manager.mark_current_key_failed(f"HTTP {status_code}")
                continue

            if status_code == 429:
                retry_after = (
                    exc.response.headers.get("Retry-After")
                    if exc.response is not None
                    else None
                )
                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 60
                )

                logging.warning(
                    "HTTP 429 raised as HTTPError for %s with params=%s. "
                    "Sleeping for %s seconds before retrying same key.",
                    path,
                    params,
                    wait_seconds,
                )

                time.sleep(wait_seconds)
                continue

            raise

        except requests.exceptions.RequestException as exc:
            logging.warning("Request failed for %s with params=%s: %s", path, params, exc)
            raise


def build_search_query(issuer_name: str) -> str:
    return issuer_name.strip()


def extract_possible_issuer_strings(item: Dict[str, Any]) -> List[str]:
    values = []

    for key in ["issuer", "country", "authority"]:
        val = item.get(key)
        if isinstance(val, str):
            values.append(val)
        elif isinstance(val, dict):
            for subkey in ["name", "title", "text"]:
                subval = val.get(subkey)
                if isinstance(subval, str):
                    values.append(subval)

    issuers = item.get("issuers")
    if isinstance(issuers, list):
        for x in issuers:
            if isinstance(x, str):
                values.append(x)
            elif isinstance(x, dict):
                for subkey in ["name", "title", "text"]:
                    subval = x.get(subkey)
                    if isinstance(subval, str):
                        values.append(subval)

    return [v.strip() for v in values if isinstance(v, str) and v.strip()]


def result_matches_issuer(issuer_name: str, item: Dict[str, Any]) -> bool:
    issuer_lower = issuer_name.strip().lower()
    possible_values = extract_possible_issuer_strings(item)

    if any(v.lower() == issuer_lower for v in possible_values):
        return True

    title = str(item.get("title") or "").strip().lower()
    if issuer_lower and issuer_lower in title:
        return True

    return False


def search_type_ids_for_issuer(
    issuer_name: str,
    key_manager: APIKeyManager,
    max_search_requests_remaining: int,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    page = 1
    search_requests_used = 0
    search_query = build_search_query(issuer_name)

    while True:
        if search_requests_used >= max_search_requests_remaining:
            logging.warning(
                "Stopping search for %s because request guardrail was reached.",
                issuer_name
            )
            break

        params = {
            "category": NUMISTA_CATEGORY,
            "q": search_query,
            "page": page,
            "count": SEARCH_PAGE_SIZE,
            "lang": NUMISTA_LANG,
        }

        data = numista_get(
            path="/types",
            params=params,
            key_manager=key_manager,
            session=session,
        )
        search_requests_used += 1

        results = data.get("types") or data.get("results") or []
        api_total = data.get("total")
        api_pages = data.get("pages")

        if not results:
            break

        kept = 0
        for pos, item in enumerate(results, start=1):
            type_id = item.get("id")
            if type_id is None:
                continue

            matched = result_matches_issuer(issuer_name, item)

            all_rows.append({
                "issuer_name": issuer_name,
                "search_query": search_query,
                "page_number": page,
                "result_position": pos,
                "type_id": int(type_id),
                "type_url": f"{NUMISTA_BASE_URL}/types/{type_id}",
                "title": item.get("title"),
                "category": item.get("category", NUMISTA_CATEGORY),
                "api_total": int(api_total) if api_total is not None else None,
                "api_pages": int(api_pages) if api_pages is not None else None,
                "matched_on_issuer_name": matched,
                "raw_result_json": item,
            })
            kept += 1

        logging.info(
            "Search issuer=%s page=%s returned %s rows",
            issuer_name,
            page,
            kept,
        )

        if len(results) < SEARCH_PAGE_SIZE:
            break

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_rows


def fetch_type_detail(
    type_id: int,
    key_manager: APIKeyManager,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    data = numista_get(
        path=f"/types/{type_id}",
        params={"lang": NUMISTA_LANG},
        key_manager=key_manager,
        session=session,
    )

    return {
        "type_id": int(type_id),
        "category": NUMISTA_CATEGORY,
        "detail_url": f"{NUMISTA_BASE_URL}/types/{type_id}",
        "raw_detail_json": data,
    }


# =========================
# BIGQUERY HELPERS
# =========================
def run_query_df(client: bigquery.Client, sql: str) -> pd.DataFrame:
    try:
        job = client.query(sql)
        return job.to_dataframe()
    except Exception:
        logging.exception("BigQuery query failed.\nSQL:\n%s", sql)
        raise

def get_issuers_to_search(client: bigquery.Client) -> List[str]:
    sql = f"""
    WITH issuers AS (
      SELECT DISTINCT
        TRIM(CAST(name AS STRING)) AS issuer_name,
        SAFE_CAST(priority_bucket AS INT64) AS priority_bucket
      FROM `{COUNTRIES_SOURCE}`
      WHERE name IS NOT NULL
        AND TRIM(CAST(name AS STRING)) != ''
    ),
    search_loads AS (
      SELECT
        TRIM(CAST(issuer_name AS STRING)) AS issuer_name,
        MAX(load_timestamp) AS last_search_load_timestamp
      FROM `{SEARCH_RAW_TABLE}`
      WHERE issuer_name IS NOT NULL
        AND TRIM(CAST(issuer_name AS STRING)) != ''
      GROUP BY 1
    )
    SELECT
      i.issuer_name
    FROM issuers i
    LEFT JOIN search_loads s
      ON LOWER(i.issuer_name) = LOWER(s.issuer_name)
    ORDER BY
      CASE WHEN s.last_search_load_timestamp IS NULL THEN 0 ELSE 1 END,
      COALESCE(i.priority_bucket, 50) ASC,
      s.last_search_load_timestamp ASC,
      i.issuer_name ASC
    """

    df = run_query_df(client, sql)
    issuers = df["issuer_name"].tolist()

    if TEST_ISSUERS:
        test_set = {x.strip().lower() for x in TEST_ISSUERS}
        issuers = [x for x in issuers if x.strip().lower() in test_set]
        logging.info("TEST MODE: restricting issuers to %s", issuers)

    logging.info(
        "Loaded %s issuers ordered by missing search data, priority bucket, then oldest search timestamp",
        len(issuers),
    )
    return issuers


def get_existing_detail_ids(client: bigquery.Client) -> Set[int]:
    try:
        sql = f"""
        SELECT DISTINCT type_id
        FROM `{DETAIL_RAW_TABLE}`
        WHERE type_id IS NOT NULL
        """
        df = run_query_df(client, sql)
        if df.empty:
            return set()
        return set(df["type_id"].astype(int).tolist())
    except Exception:
        return set()


def append_json_rows(
    client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
    run_id: str,
) -> None:
    if not rows:
        logging.info("No rows to insert into %s", table_id)
        return

    load_ts = datetime.now(timezone.utc).isoformat()
    final_rows = []

    for row in rows:
        row_copy = dict(row)
        row_copy["run_id"] = run_id
        row_copy["load_timestamp"] = load_ts
        final_rows.append(row_copy)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_json(final_rows, table_id, job_config=job_config)
    job.result()

    logging.info("Inserted %s rows into %s", len(final_rows), table_id)


def build_progress_row(
    run_id: str,
    issuer_name: str,
    stage: str,
    status: str,
    search_rows_written: Optional[int] = None,
    detail_rows_written: Optional[int] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "issuer_name": issuer_name,
        "stage": stage,
        "status": status,
        "search_rows_written": search_rows_written,
        "detail_rows_written": detail_rows_written,
        "message": message,
        "load_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def flush_progress_rows(
    client: bigquery.Client,
    progress_buffer: List[Dict[str, Any]],
) -> None:
    if not progress_buffer:
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = client.load_table_from_json(
        progress_buffer,
        PROGRESS_TABLE,
        job_config=job_config,
    )
    job.result()

    logging.info(
        "Inserted %s progress rows into %s",
        len(progress_buffer),
        PROGRESS_TABLE,
    )

    progress_buffer.clear()


def add_progress_row(
    client: bigquery.Client,
    progress_buffer: List[Dict[str, Any]],
    run_id: str,
    issuer_name: str,
    stage: str,
    status: str,
    search_rows_written: Optional[int] = None,
    detail_rows_written: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    progress_buffer.append(build_progress_row(
        run_id=run_id,
        issuer_name=issuer_name,
        stage=stage,
        status=status,
        search_rows_written=search_rows_written,
        detail_rows_written=detail_rows_written,
        message=message,
    ))

    logging.info(
        "Progress buffered | issuer=%s | stage=%s | status=%s | buffer_size=%s",
        issuer_name,
        stage,
        status,
        len(progress_buffer),
    )

    if len(progress_buffer) >= PROGRESS_BATCH_SIZE:
        flush_progress_rows(client, progress_buffer)


def upsert_country_state(
    client: bigquery.Client,
    issuer_name: str,
    run_id: str,
    status: str,
    message: Optional[str],
    search_rows_written: int,
    detail_rows_written: int,
) -> None:
    now_ts = datetime.now(timezone.utc).isoformat()

    issuer_name_sql = json.dumps(issuer_name)
    run_id_sql = json.dumps(run_id)
    status_sql = json.dumps(status)
    message_sql = "NULL" if message is None else json.dumps(message)

    sql = f"""
    MERGE `{STATE_TABLE}` AS target
    USING (
      SELECT
        {issuer_name_sql} AS issuer_name,
        TIMESTAMP('{now_ts}') AS last_attempted_run_ts,
        {run_id_sql} AS last_run_id,
        {status_sql} AS last_status,
        {message_sql} AS last_message,
        {search_rows_written} AS search_rows_written,
        {detail_rows_written} AS detail_rows_written,
        TIMESTAMP('{now_ts}') AS updated_at
    ) AS source
    ON LOWER(target.issuer_name) = LOWER(source.issuer_name)

    WHEN MATCHED THEN
      UPDATE SET
        last_attempted_run_ts = source.last_attempted_run_ts,
        last_run_id = source.last_run_id,
        last_status = source.last_status,
        last_message = source.last_message,
        search_rows_written = source.search_rows_written,
        detail_rows_written = source.detail_rows_written,
        updated_at = source.updated_at,
        last_successful_run_ts = CASE
          WHEN source.last_status = 'SUCCESS' THEN source.last_attempted_run_ts
          ELSE target.last_successful_run_ts
        END

    WHEN NOT MATCHED THEN
      INSERT (
        issuer_name,
        last_successful_run_ts,
        last_attempted_run_ts,
        last_run_id,
        last_status,
        last_message,
        search_rows_written,
        detail_rows_written,
        updated_at
      )
      VALUES (
        source.issuer_name,
        CASE
          WHEN source.last_status = 'SUCCESS' THEN source.last_attempted_run_ts
          ELSE NULL
        END,
        source.last_attempted_run_ts,
        source.last_run_id,
        source.last_status,
        source.last_message,
        source.search_rows_written,
        source.detail_rows_written,
        source.updated_at
      )
    """
    client.query(sql).result()
    logging.info(
        "Updated state | issuer=%s | status=%s | search_rows=%s | detail_rows=%s",
        issuer_name,
        status,
        search_rows_written,
        detail_rows_written,
    )


# =========================
# MAIN
# =========================
def main() -> None:
    run_id = str(uuid.uuid4())

    client = get_bq_client()
    api_keys = get_api_keys()
    key_manager = APIKeyManager(api_keys)

    logging.info("Run ID: %s", run_id)
    logging.info("Loaded %s Numista API keys", len(api_keys))

    ensure_tables_exist(client)

    issuers = get_issuers_to_search(client)
    existing_detail_ids = get_existing_detail_ids(client)

    logging.info("Issuers available to process this run: %s", len(issuers))
    logging.info("Already have %s type details stored", len(existing_detail_ids))

    search_requests_used = 0
    detail_requests_used = 0
    total_search_rows_written = 0
    total_detail_rows_written = 0
    progress_buffer: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for issuer_name in issuers:
            remaining_search = MAX_SEARCH_REQUESTS_PER_RUN - search_requests_used
            if remaining_search <= 0:
                logging.warning("Search guardrail reached for this run.")
                break

            issuer_rows: List[Dict[str, Any]] = []
            issuer_type_ids: Set[int] = set()
            issuer_detail_rows: List[Dict[str, Any]] = []
            current_stage = "issuer_search_started"

            try:
                logging.info("Starting issuer: %s", issuer_name)

                issuer_rows = search_type_ids_for_issuer(
                    issuer_name=issuer_name,
                    key_manager=key_manager,
                    max_search_requests_remaining=remaining_search,
                    session=session,
                )
                current_stage = "issuer_search_complete"

                issuer_pages_used = len({
                    (r["issuer_name"], r["page_number"]) for r in issuer_rows
                })
                search_requests_used += issuer_pages_used

                append_json_rows(client, SEARCH_RAW_TABLE, issuer_rows, run_id=run_id)
                total_search_rows_written += len(issuer_rows)

                add_progress_row(
                    client=client,
                    progress_buffer=progress_buffer,
                    run_id=run_id,
                    issuer_name=issuer_name,
                    stage="issuer_search_complete",
                    status="SUCCESS",
                    search_rows_written=len(issuer_rows),
                    detail_rows_written=0,
                    message="Search rows written successfully",
                )

                for row in issuer_rows:
                    type_id = row["type_id"]
                    if type_id not in existing_detail_ids:
                        issuer_type_ids.add(type_id)

                if MAX_NEW_TYPE_IDS_FOR_TEST is not None:
                    issuer_type_ids = set(sorted(issuer_type_ids)[:MAX_NEW_TYPE_IDS_FOR_TEST])
                    logging.info(
                        "TEST MODE: limiting issuer=%s new type IDs to first %s",
                        issuer_name,
                        MAX_NEW_TYPE_IDS_FOR_TEST,
                    )

                logging.info(
                    "Issuer=%s has %s new type IDs to fetch",
                    issuer_name,
                    len(issuer_type_ids),
                )

                current_stage = "issuer_detail_started"

                for idx, type_id in enumerate(sorted(issuer_type_ids), start=1):
                    if detail_requests_used >= MAX_DETAIL_REQUESTS_PER_RUN:
                        logging.warning("Detail guardrail reached for this run.")
                        break

                    logging.info(
                        "[%s | %s/%s] Fetching detail for type_id=%s",
                        issuer_name,
                        idx,
                        len(issuer_type_ids),
                        type_id,
                    )

                    detail_row = fetch_type_detail(
                        type_id=type_id,
                        key_manager=key_manager,
                        session=session,
                    )
                    issuer_detail_rows.append(detail_row)
                    detail_requests_used += 1
                    time.sleep(SLEEP_SECONDS)

                append_json_rows(client, DETAIL_RAW_TABLE, issuer_detail_rows, run_id=run_id)
                total_detail_rows_written += len(issuer_detail_rows)

                existing_detail_ids.update({row["type_id"] for row in issuer_detail_rows})

                remaining_unfetched = len(issuer_type_ids) - len(issuer_detail_rows)
                if detail_requests_used >= MAX_DETAIL_REQUESTS_PER_RUN and remaining_unfetched > 0:
                    current_stage = "issuer_detail_partial"

                    partial_message = (
                        f"Detail guardrail reached before issuer completed. "
                        f"{remaining_unfetched} type IDs remaining."
                    )

                    add_progress_row(
                        client=client,
                        progress_buffer=progress_buffer,
                        run_id=run_id,
                        issuer_name=issuer_name,
                        stage="issuer_detail_complete",
                        status="FAILED",
                        search_rows_written=len(issuer_rows),
                        detail_rows_written=len(issuer_detail_rows),
                        message=partial_message,
                    )

                    upsert_country_state(
                        client=client,
                        issuer_name=issuer_name,
                        run_id=run_id,
                        status="FAILED",
                        message=partial_message,
                        search_rows_written=len(issuer_rows),
                        detail_rows_written=len(issuer_detail_rows),
                    )

                    logging.warning(
                        "Issuer=%s not fully completed due to detail guardrail",
                        issuer_name,
                    )
                    break

                current_stage = "issuer_detail_complete"

                add_progress_row(
                    client=client,
                    progress_buffer=progress_buffer,
                    run_id=run_id,
                    issuer_name=issuer_name,
                    stage="issuer_detail_complete",
                    status="SUCCESS",
                    search_rows_written=len(issuer_rows),
                    detail_rows_written=len(issuer_detail_rows),
                    message="Issuer completed successfully",
                )

                upsert_country_state(
                    client=client,
                    issuer_name=issuer_name,
                    run_id=run_id,
                    status="SUCCESS",
                    message="Issuer completed successfully",
                    search_rows_written=len(issuer_rows),
                    detail_rows_written=len(issuer_detail_rows),
                )

                logging.info(
                    "Completed issuer=%s | search rows=%s | detail rows=%s",
                    issuer_name,
                    len(issuer_rows),
                    len(issuer_detail_rows),
                )

            except Exception as exc:
                logging.exception(
                    "Failed issuer %s at stage %s: %s",
                    issuer_name,
                    current_stage,
                    exc,
                )

                try:
                    add_progress_row(
                        client=client,
                        progress_buffer=progress_buffer,
                        run_id=run_id,
                        issuer_name=issuer_name,
                        stage=current_stage,
                        status="FAILED",
                        search_rows_written=len(issuer_rows),
                        detail_rows_written=len(issuer_detail_rows),
                        message=str(exc),
                    )
                except Exception:
                    logging.exception(
                        "Failed to buffer/write progress row for issuer=%s after error",
                        issuer_name,
                    )

                try:
                    upsert_country_state(
                        client=client,
                        issuer_name=issuer_name,
                        run_id=run_id,
                        status="FAILED",
                        message=f"{current_stage}: {str(exc)}",
                        search_rows_written=len(issuer_rows),
                        detail_rows_written=len(issuer_detail_rows),
                    )
                except Exception:
                    logging.exception(
                        "Failed to update state table for issuer=%s after error",
                        issuer_name,
                    )

                continue

            time.sleep(SLEEP_SECONDS)

    flush_progress_rows(client, progress_buffer)

    logging.info("Run complete.")
    logging.info("Search requests used: %s", search_requests_used)
    logging.info("Detail requests used: %s", detail_requests_used)
    logging.info("Search rows written: %s", total_search_rows_written)
    logging.info("Detail rows written: %s", total_detail_rows_written)
    logging.info(
        "Active API keys remaining at end of run: %s",
        key_manager.get_active_key_count(),
    )


if __name__ == "__main__":
    main()