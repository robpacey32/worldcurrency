import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT_ID = "currency-pacey32-github"
DATASET_ID = "numistascrape"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.reference_countries"

REST_COUNTRIES_URL = (
    "https://restcountries.com/v3.1/all"
    "?fields=name,cca2,cca3,ccn3,region,subregion,population,area,flags,currencies"
)

REQUEST_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


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


def ensure_table_exists(client: bigquery.Client) -> None:
    schema = [
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("country_name", "STRING"),
        bigquery.SchemaField("official_name", "STRING"),
        bigquery.SchemaField("iso_alpha2", "STRING"),
        bigquery.SchemaField("iso_alpha3", "STRING"),
        bigquery.SchemaField("numeric_code", "STRING"),
        bigquery.SchemaField("cioc", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("subregion", "STRING"),
        bigquery.SchemaField("population", "INT64"),
        bigquery.SchemaField("area", "FLOAT64"),
        bigquery.SchemaField("flag_png", "STRING"),
        bigquery.SchemaField("flag_svg", "STRING"),
        bigquery.SchemaField("flag_alt", "STRING"),
        bigquery.SchemaField("currency_codes", "STRING", mode="REPEATED"),
        bigquery.SchemaField("raw_country_json", "JSON"),
    ]

    try:
        client.get_table(TABLE_ID)
        logging.info("Table exists: %s", TABLE_ID)
    except Exception:
        table = bigquery.Table(TABLE_ID, schema=schema)
        client.create_table(table)
        logging.info("Created table: %s", TABLE_ID)


def fetch_countries() -> List[Dict[str, Any]]:
    response = requests.get(REST_COUNTRIES_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("REST Countries response was not a list")

    logging.info("Fetched %s countries from REST Countries", len(data))
    return data


def get_nested(data: Dict[str, Any], *keys: str) -> Optional[Any]:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def transform_country(item: Dict[str, Any], load_ts: str) -> Dict[str, Any]:
    currencies = item.get("currencies")
    currency_codes = sorted(currencies.keys()) if isinstance(currencies, dict) else []

    return {
        "load_timestamp": load_ts,
        "country_name": get_nested(item, "name", "common"),
        "official_name": get_nested(item, "name", "official"),
        "iso_alpha2": item.get("cca2"),
        "iso_alpha3": item.get("cca3"),
        "numeric_code": item.get("ccn3"),
        "region": item.get("region"),
        "subregion": item.get("subregion"),
        "population": item.get("population"),
        "area": item.get("area"),
        "flag_png": get_nested(item, "flags", "png"),
        "flag_svg": get_nested(item, "flags", "svg"),
        "flag_alt": get_nested(item, "flags", "alt"),
        "currency_codes": currency_codes,
        "raw_country_json": item,
    }


def load_to_bigquery(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job = client.load_table_from_json(rows, TABLE_ID, job_config=job_config)
    job.result()

    logging.info("Loaded %s rows to %s", len(rows), TABLE_ID)


def main() -> None:
    client = get_bq_client()
    ensure_table_exists(client)

    load_ts = datetime.now(timezone.utc).isoformat()
    countries = fetch_countries()
    rows = [transform_country(item, load_ts) for item in countries]

    rows = [
        row for row in rows
        if row["country_name"] and row["iso_alpha3"]
    ]

    load_to_bigquery(client, rows)


if __name__ == "__main__":
    main()