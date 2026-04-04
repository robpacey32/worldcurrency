from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas as pd
import os
import json
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT_ID = "currency-pacey32-github"
DATASET = "numistascrape"
SOURCE_TABLE = "1_Countries"
TARGET_TABLE = "2_NotesPerCountry"
BASE_URL = "https://en.numista.com"


def get_bq_client() -> bigquery.Client:
    json_str = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
    info = json.loads(json_str)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def read_countries_from_bigquery() -> pd.DataFrame:
    client = get_bq_client()
    query = f"""
    SELECT
      name,
      url,
      relative_url,
      alt_names,
      path,
      depth,
      li_classes
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    WHERE name = path
    AND name = 'Slovakia'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY url
      ORDER BY load_timestamp DESC
    ) = 1
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def upload_to_bigquery(df: pd.DataFrame) -> None:
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows to {table_id}")


def get_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1600,1400")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    return webdriver.Chrome(options=options)


def get_page_html(url: str) -> str:
    driver = get_driver()
    try:
        driver.get(url)

        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        time.sleep(5)

        html = driver.page_source

        print("PAGE TITLE:", driver.title)
        print("CURRENT URL:", driver.current_url)

        return html
    finally:
        driver.quit()


def scrape_note_links_all_pages(first_page_url: str, issuer_name: str) -> list:
    page = 1
    rows = []

    while True:
        url = first_page_url.replace("-1.html", f"-{page}.html")
        print(f"Scraping {issuer_name} - page {page}")

        html = get_page_html(url)
        soup = BeautifulSoup(html, "html.parser")

        blocks = soup.select("div.resultat_recherche div.description_piece")

        if not blocks:
            break

        for block in blocks:
            a = block.select_one("strong a")
            if not a:
                continue

            href = a.get("href")
            if not href:
                continue

            note_url = BASE_URL + href
            title = a.get_text(" ", strip=True)

            rows.append({
                "issuer_name": issuer_name,
                "issuer_page": url,
                "note_title": title,
                "note_url": note_url
            })

        page += 1

    return rows


if __name__ == "__main__":
    df_issuers = read_countries_from_bigquery()

    df_issuers = df_issuers[df_issuers["url"].str.contains("-1.html", na=False)].copy()

    all_rows = []

    for _, row in df_issuers.iterrows():
        issuer_name = row["name"]
        issuer_url = row["url"]

        rows = scrape_note_links_all_pages(issuer_url, issuer_name)
        all_rows.extend(rows)

    df_notes = pd.DataFrame(all_rows).drop_duplicates(subset=["issuer_name", "note_url"]).reset_index(drop=True)
    df_notes["load_timestamp"] = datetime.now(timezone.utc)

    print(df_notes.head())
    print("Total notes scraped:", len(df_notes))

    upload_to_bigquery(df_notes)
