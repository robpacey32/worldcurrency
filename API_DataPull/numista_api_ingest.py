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

NUMISTA_BASE_URL = "https://api.numista.com/v3"
NUMISTA_CATEGORY = "banknote"
NUMISTA_LANG = "en"
SEARCH_PAGE_SIZE = 50
SLEEP_SECONDS = 0.4
REQUEST_TIMEOUT = 60

MAX_SEARCH_REQUESTS_PER_RUN = 250
MAX_DETAIL_REQUESTS_PER_RUN = 1000

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
def wait_for_table_ready(
    client: bigquery.Client,
    table_id: str,
    max_wait_seconds: int = 30,
    poll_interval: float = 2.0,
) -> None:
    start = time.time()

    while True:
        try:
            client.get_table(table_id)
            logging.info("Confirmed table is visible: %s", table_id)
            return
        except Exception:
            elapsed = time.time() - start
            if elapsed >= max_wait_seconds:
                raise RuntimeError(
                    f"Table {table_id} was created but was not visible after "
                    f"{max_wait_seconds} seconds."
                )
            logging.info(
                "Waiting for table metadata to propagate: %s (%.1fs elapsed)",
                table_id,
                elapsed,
            )
            time.sleep(poll_interval)


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

    for table_id, schema in [
        (SEARCH_RAW_TABLE, search_schema),
        (DETAIL_RAW_TABLE, detail_schema),
        (PROGRESS_TABLE, progress_schema),
    ]:
        try:
            client.get_table(table_id)
            logging.info("Table exists: %s", table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            logging.info("Created table: %s", table_id)
            wait_for_table_ready(client, table_id)

    time.sleep(5)


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
                key_manager.mark_current_key_failed("HTTP 429 rate/quota limit")
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

            if status_code in (401, 403, 429):
                key_manager.mark_current_key_failed(f"HTTP {status_code}")
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
    SELECT DISTINCT CAST(name AS STRING) AS issuer_name
    FROM `{COUNTRIES_SOURCE}`
    WHERE name IS NOT NULL
      AND TRIM(CAST(name AS STRING)) != ''
    ORDER BY issuer_name
    """

    df = run_query_df(client, sql)
    issuers = df["issuer_name"].tolist()

    if TEST_ISSUERS:
        test_set = {x.strip().lower() for x in TEST_ISSUERS}
        issuers = [x for x in issuers if x.strip().lower() in test_set]
        logging.info("TEST MODE: restricting issuers to %s", issuers)

    logging.info("Loaded %s issuers from %s", len(issuers), COUNTRIES_SOURCE)
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


def get_completed_issuers(client: bigquery.Client) -> Set[str]:
    try:
        sql = f"""
        SELECT DISTINCT issuer_name
        FROM `{PROGRESS_TABLE}`
        WHERE stage = 'issuer_detail_complete'
          AND status = 'SUCCESS'
          AND issuer_name IS NOT NULL
        """
        df = run_query_df(client, sql)
        if df.empty:
            return set()
        return set(df["issuer_name"].astype(str).tolist())
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


def append_progress_row(
    client: bigquery.Client,
    run_id: str,
    issuer_name: str,
    stage: str,
    status: str,
    search_rows_written: Optional[int] = None,
    detail_rows_written: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    row = [{
        "run_id": run_id,
        "issuer_name": issuer_name,
        "stage": stage,
        "status": status,
        "search_rows_written": search_rows_written,
        "detail_rows_written": detail_rows_written,
        "message": message,
        "load_timestamp": datetime.now(timezone.utc).isoformat(),
    }]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_json(row, PROGRESS_TABLE, job_config=job_config)
    job.result()

    logging.info(
        "Progress logged | issuer=%s | stage=%s | status=%s",
        issuer_name,
        stage,
        status,
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
    completed_issuers = get_completed_issuers(client)
    existing_detail_ids = get_existing_detail_ids(client)

    if completed_issuers:
        issuers = [x for x in issuers if x not in completed_issuers]
        logging.info(
            "Skipping %s issuers already completed in prior runs",
            len(completed_issuers),
        )

    logging.info("Issuers remaining to process this run: %s", len(issuers))
    logging.info("Already have %s type details stored", len(existing_detail_ids))

    search_requests_used = 0
    detail_requests_used = 0
    total_search_rows_written = 0
    total_detail_rows_written = 0

    with requests.Session() as session:
        for issuer_name in issuers:
            remaining_search = MAX_SEARCH_REQUESTS_PER_RUN - search_requests_used
            if remaining_search <= 0:
                logging.warning("Search guardrail reached for this run.")
                break

            issuer_rows: List[Dict[str, Any]] = []
            issuer_type_ids: Set[int] = set()
            issuer_detail_rows: List[Dict[str, Any]] = []

            try:
                logging.info("Starting issuer: %s", issuer_name)

                issuer_rows = search_type_ids_for_issuer(
                    issuer_name=issuer_name,
                    key_manager=key_manager,
                    max_search_requests_remaining=remaining_search,
                    session=session,
                )

                issuer_pages_used = len({
                    (r["issuer_name"], r["page_number"]) for r in issuer_rows
                })
                search_requests_used += issuer_pages_used

                append_json_rows(client, SEARCH_RAW_TABLE, issuer_rows, run_id=run_id)
                total_search_rows_written += len(issuer_rows)

                append_progress_row(
                    client=client,
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
                    matched = row.get("matched_on_issuer_name", False)
                    if matched and type_id not in existing_detail_ids:
                        issuer_type_ids.add(type_id)

                if MAX_NEW_TYPE_IDS_FOR_TEST is not None:
                    issuer_type_ids = set(sorted(issuer_type_ids)[:MAX_NEW_TYPE_IDS_FOR_TEST])
                    logging.info(
                        "TEST MODE: limiting issuer=%s new type IDs to first %s",
                        issuer_name,
                        MAX_NEW_TYPE_IDS_FOR_TEST,
                    )

                logging.info(
                    "Issuer=%s has %s new matched type IDs to fetch",
                    issuer_name,
                    len(issuer_type_ids),
                )

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

                existing_detail_ids.update(
                    {row["type_id"] for row in issuer_detail_rows}
                )

                if detail_requests_used >= MAX_DETAIL_REQUESTS_PER_RUN and issuer_type_ids:
                    remaining_ids = len(issuer_type_ids) - len(issuer_detail_rows)
                    if remaining_ids > 0:
                        append_progress_row(
                            client=client,
                            run_id=run_id,
                            issuer_name=issuer_name,
                            stage="issuer_detail_complete",
                            status="FAILED",
                            search_rows_written=len(issuer_rows),
                            detail_rows_written=len(issuer_detail_rows),
                            message=(
                                f"Detail guardrail reached before issuer completed. "
                                f"{remaining_ids} type IDs remaining."
                            ),
                        )
                        logging.warning(
                            "Issuer=%s not fully completed due to detail guardrail",
                            issuer_name,
                        )
                        break

                append_progress_row(
                    client=client,
                    run_id=run_id,
                    issuer_name=issuer_name,
                    stage="issuer_detail_complete",
                    status="SUCCESS",
                    search_rows_written=len(issuer_rows),
                    detail_rows_written=len(issuer_detail_rows),
                    message="Issuer completed successfully",
                )

                logging.info(
                    "Completed issuer=%s | search rows=%s | detail rows=%s",
                    issuer_name,
                    len(issuer_rows),
                    len(issuer_detail_rows),
                )

            except Exception as exc:
                logging.exception("Failed issuer %s: %s", issuer_name, exc)

                try:
                    append_progress_row(
                        client=client,
                        run_id=run_id,
                        issuer_name=issuer_name,
                        stage="issuer_detail_complete",
                        status="FAILED",
                        search_rows_written=len(issuer_rows),
                        detail_rows_written=len(issuer_detail_rows),
                        message=str(exc),
                    )
                except Exception:
                    logging.exception(
                        "Failed to write progress row for issuer=%s after error",
                        issuer_name,
                    )

                continue

            time.sleep(SLEEP_SECONDS)

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
