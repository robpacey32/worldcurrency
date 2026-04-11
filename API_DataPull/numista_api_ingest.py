import os
import json
import time
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

COUNTRIES_SOURCE = f"{PROJECT_ID}.{DATASET_ID}.1_Countries"
SEARCH_RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_numista_search_results"
DETAIL_RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_numista_type_detail"

NUMISTA_BASE_URL = "https://api.numista.com/v3"
NUMISTA_CATEGORY = "banknote"
NUMISTA_LANG = "en"
SEARCH_PAGE_SIZE = 50
SLEEP_SECONDS = 0.4
REQUEST_TIMEOUT = 60

# Hard stops to protect quota usage per run
MAX_SEARCH_REQUESTS_PER_RUN = 250
MAX_DETAIL_REQUESTS_PER_RUN = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

TEST_ISSUERS = ["Slovakia"]   # set to [] to run all issuers
MAX_NEW_TYPE_IDS_FOR_TEST = 10  # None for no limit

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
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("issuer_name", "STRING"),
        bigquery.SchemaField("search_query", "STRING"),
        bigquery.SchemaField("page_number", "INT64"),
        bigquery.SchemaField("type_id", "INT64"),
        bigquery.SchemaField("type_url", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("raw_result_json", "JSON"),
    ]

    detail_schema = [
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("type_id", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("detail_url", "STRING"),
        bigquery.SchemaField("raw_detail_json", "JSON"),
    ]

    for table_id, schema in [
        (SEARCH_RAW_TABLE, search_schema),
        (DETAIL_RAW_TABLE, detail_schema),
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
                key_manager.mark_current_key_failed("HTTP 429 rate/quota limit")
                continue

            if response.status_code in (401, 403):
                key_manager.mark_current_key_failed(f"HTTP {response.status_code} auth/quota error")
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
            logging.warning("Transient request error: %s", exc)
            raise


def build_search_query(issuer_name: str) -> str:
    return issuer_name.strip()


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
        if not results:
            break

        for item in results:
            type_id = item.get("id")
            if type_id is None:
                continue

            all_rows.append({
                "issuer_name": issuer_name,
                "search_query": search_query,
                "page_number": page,
                "type_id": int(type_id),
                "type_url": f"{NUMISTA_BASE_URL}/types/{type_id}",
                "title": item.get("title"),
                "category": item.get("category", NUMISTA_CATEGORY),
                "raw_result_json": item,
            })

        logging.info(
            "Search issuer=%s page=%s returned %s rows",
            issuer_name,
            page,
            len(results),
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
        params={},
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
    return client.query(sql).to_dataframe()


def get_issuers_to_search(client: bigquery.Client) -> List[str]:
    sql = f"""
    SELECT DISTINCT Country AS issuer_name
    FROM `{COUNTRIES_SOURCE}`
    WHERE Country IS NOT NULL
      AND TRIM(Country) != ''
    ORDER BY Country
    """
    df = run_query_df(client, sql)
    issuers = df["issuer_name"].tolist()

    if TEST_ISSUERS:
        test_set = {x.strip().lower() for x in TEST_ISSUERS}
        issuers = [x for x in issuers if x.strip().lower() in test_set]
        logging.info("TEST MODE: restricting issuers to %s", issuers)

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
) -> None:
    if not rows:
        logging.info("No rows to insert into %s", table_id)
        return

    load_ts = datetime.now(timezone.utc).isoformat()
    final_rows = []

    for row in rows:
        row_copy = dict(row)
        row_copy["load_timestamp"] = load_ts
        final_rows.append(row_copy)

    errors = client.insert_rows_json(table_id, final_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for {table_id}: {errors}")

    logging.info("Inserted %s rows into %s", len(final_rows), table_id)


# =========================
# MAIN
# =========================
def main() -> None:
    client = get_bq_client()
    api_keys = get_api_keys()
    key_manager = APIKeyManager(api_keys)

    logging.info("Loaded %s Numista API keys", len(api_keys))

    ensure_tables_exist(client)

    issuers = get_issuers_to_search(client)
    logging.info("Loaded %s issuers from %s", len(issuers), COUNTRIES_SOURCE)

    existing_detail_ids = get_existing_detail_ids(client)
    logging.info("Already have %s type details stored", len(existing_detail_ids))

    search_requests_used = 0
    detail_requests_used = 0

    all_search_rows: List[Dict[str, Any]] = []
    new_type_ids: Set[int] = set()

    with requests.Session() as session:
        for issuer_name in issuers:
            remaining_search = MAX_SEARCH_REQUESTS_PER_RUN - search_requests_used
            if remaining_search <= 0:
                logging.warning("Search guardrail reached for this run.")
                break

            try:
                issuer_rows = search_type_ids_for_issuer(
                    issuer_name=issuer_name,
                    key_manager=key_manager,
                    max_search_requests_remaining=remaining_search,
                    session=session,
                )
            except Exception as exc:
                logging.exception("Failed search for issuer %s: %s", issuer_name, exc)
                continue

            issuer_pages_used = len({
                (r["issuer_name"], r["page_number"]) for r in issuer_rows
            })
            search_requests_used += issuer_pages_used

            all_search_rows.extend(issuer_rows)

            for row in issuer_rows:
                type_id = row["type_id"]
                if type_id not in existing_detail_ids:
                    new_type_ids.add(type_id)

            time.sleep(SLEEP_SECONDS)

        append_json_rows(client, SEARCH_RAW_TABLE, all_search_rows)

        if MAX_NEW_TYPE_IDS_FOR_TEST is not None:
            new_type_ids = set(sorted(new_type_ids)[:MAX_NEW_TYPE_IDS_FOR_TEST])
            logging.info(
                "TEST MODE: limiting new type IDs to first %s",
                MAX_NEW_TYPE_IDS_FOR_TEST
            )

        logging.info("Discovered %s new type IDs to fetch", len(new_type_ids))

        detail_rows: List[Dict[str, Any]] = []

        for idx, type_id in enumerate(sorted(new_type_ids), start=1):
            if detail_requests_used >= MAX_DETAIL_REQUESTS_PER_RUN:
                logging.warning("Detail guardrail reached for this run.")
                break

            logging.info(
                "[%s/%s] Fetching detail for type_id=%s",
                idx,
                len(new_type_ids),
                type_id,
            )

            try:
                detail_row = fetch_type_detail(
                    type_id=type_id,
                    key_manager=key_manager,
                    session=session,
                )
                detail_rows.append(detail_row)
                detail_requests_used += 1
            except Exception as exc:
                logging.exception("Failed detail fetch for %s: %s", type_id, exc)

            time.sleep(SLEEP_SECONDS)

        append_json_rows(client, DETAIL_RAW_TABLE, detail_rows)

    logging.info("Run complete.")
    logging.info("Search requests used: %s", search_requests_used)
    logging.info("Detail requests used: %s", detail_requests_used)
    logging.info("Search rows written: %s", len(all_search_rows))
    logging.info("Detail rows written: %s", len(detail_rows))
    logging.info("Active API keys remaining at end of run: %s", key_manager.get_active_key_count())


if __name__ == "__main__":
    main()