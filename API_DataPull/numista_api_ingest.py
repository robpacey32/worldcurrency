import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

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
SEARCH_PAGE_SIZE = 50   # verified in official example
SLEEP_SECONDS = 0.4     # small pause between calls
REQUEST_TIMEOUT = 60

# Hard stop to protect your free quota
MAX_SEARCH_REQUESTS_PER_RUN = 250
MAX_DETAIL_REQUESTS_PER_RUN = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =========================
# AUTH / CLIENTS
# =========================
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


def get_numista_headers() -> Dict[str, str]:
    api_key = os.environ.get("NUMISTA_API_KEY")
    if not api_key:
        raise ValueError("NUMISTA_API_KEY environment variable is missing.")
    return {
        "Numista-API-Key": api_key,
        "Accept": "application/json",
        "User-Agent": "pacey32-numista-loader/1.0"
    }


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
    headers: Dict[str, str],
) -> Dict[str, Any]:
    url = f"{NUMISTA_BASE_URL}{path}"
    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 429:
        raise RuntimeError("Numista API rate/quota limit hit (HTTP 429).")
    response.raise_for_status()
    return response.json()


def build_search_query(issuer_name: str) -> str:
    """
    Conservative version:
    Use issuer/country name as the text query because that pattern is verified
    from the official example using q=... on /types.
    If you later confirm a dedicated issuer filter in the docs, replace this.
    """
    return issuer_name.strip()


def search_type_ids_for_issuer(
    issuer_name: str,
    headers: Dict[str, str],
    max_search_requests_remaining: int,
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

        data = numista_get("/types", params=params, headers=headers)
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
            issuer_name, page, len(results)
        )

        # stop when final page is reached
        if len(results) < SEARCH_PAGE_SIZE:
            break

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_rows


def fetch_type_detail(
    type_id: int,
    headers: Dict[str, str],
) -> Dict[str, Any]:
    data = numista_get(
        path=f"/types/{type_id}",
        params={},
        headers=headers,
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
    return df["issuer_name"].tolist()


def get_existing_detail_ids(client: bigquery.Client) -> Set[int]:
    sql = f"""
    SELECT DISTINCT type_id
    FROM `{DETAIL_RAW_TABLE}`
    WHERE type_id IS NOT NULL
    """
    df = run_query_df(client, sql)
    if df.empty:
        return set()
    return set(df["type_id"].astype(int).tolist())


def append_json_rows(
    client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
) -> None:
    if not rows:
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
    headers = get_numista_headers()

    ensure_tables_exist(client)

    issuers = get_issuers_to_search(client)
    logging.info("Loaded %s issuers from %s", len(issuers), COUNTRIES_SOURCE)

    existing_detail_ids = get_existing_detail_ids(client)
    logging.info("Already have %s type details stored", len(existing_detail_ids))

    search_requests_used = 0
    detail_requests_used = 0

    all_search_rows: List[Dict[str, Any]] = []
    new_type_ids: Set[int] = set()

    for issuer_name in issuers:
        remaining_search = MAX_SEARCH_REQUESTS_PER_RUN - search_requests_used
        if remaining_search <= 0:
            logging.warning("Search guardrail reached for this run.")
            break

        issuer_rows = search_type_ids_for_issuer(
            issuer_name=issuer_name,
            headers=headers,
            max_search_requests_remaining=remaining_search,
        )

        # approximate usage by distinct pages found
        issuer_pages_used = len(set(
            (r["issuer_name"], r["page_number"]) for r in issuer_rows
        ))
        search_requests_used += issuer_pages_used

        all_search_rows.extend(issuer_rows)

        for row in issuer_rows:
            type_id = row["type_id"]
            if type_id not in existing_detail_ids:
                new_type_ids.add(type_id)

        time.sleep(SLEEP_SECONDS)

    append_json_rows(client, SEARCH_RAW_TABLE, all_search_rows)

    logging.info("Discovered %s new type IDs to fetch", len(new_type_ids))

    detail_rows: List[Dict[str, Any]] = []
    for idx, type_id in enumerate(sorted(new_type_ids), start=1):
        if detail_requests_used >= MAX_DETAIL_REQUESTS_PER_RUN:
            logging.warning("Detail guardrail reached for this run.")
            break

        logging.info("[%s/%s] Fetching detail for type_id=%s", idx, len(new_type_ids), type_id)

        try:
            detail_row = fetch_type_detail(type_id=type_id, headers=headers)
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


if __name__ == "__main__":
    main()