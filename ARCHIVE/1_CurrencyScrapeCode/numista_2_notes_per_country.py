from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas as pd
import os
import json
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "currency-pacey32-github"
SOURCE_DATASET = "numistaviews"
TARGET_DATASET = "numistascrape"
SOURCE_TABLE = "1_Countries_Latest"
TARGET_TABLE = "2_NotesPerCountry"
BASE_URL = "https://en.numista.com"


def get_bq_client() -> bigquery.Client:
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str:
        info = json.loads(json_str)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)

    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)

    raise ValueError("Set credentials env var")


def read_countries_from_bigquery() -> pd.DataFrame:
    client = get_bq_client()
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
    WHERE name = path
    AND name = 'Afghanistan'
    """
    rows = list(client.query(query).result())
    return pd.DataFrame([dict(row.items()) for row in rows])


def upload_to_bigquery(df: pd.DataFrame) -> None:
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
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
    options.add_argument("--disable-extensions")
    options.add_argument("--lang=en-GB")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=options)


def wait_for_real_content(driver):
    try:
        WebDriverWait(driver, 20).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, "div.resultat_recherche div.description_piece")) > 0
        )
    except:
        print("Content not found (likely blocked)")


def get_page_html(driver, url):
    for attempt in range(3):
        driver.get(url)

        wait_for_real_content(driver)
        time.sleep(2)

        if "Just a moment" not in driver.title:
            break

        print(f"Blocked - retry {attempt+1}")
        time.sleep(5)

    print("PAGE TITLE:", driver.title)
    print("CURRENT URL:", driver.current_url)

    return driver.page_source


def build_banknote_url(country_url: str) -> str:
    slug = country_url.rstrip("/").split("/")[-1].replace("-1.html", "")

    if not slug.endswith("_section"):
        slug = f"{slug}_section"

    return (
        f"{BASE_URL}/catalogue/index.php"
        f"?e={slug}"
        f"&r=&st=148&cat=y"
        f"&im1=&im2=&ru=&ie=&no=&v=&cu=&a=&dg=&i=&b=&m=&f=&t=&t2=&w=&mt=&u=&g="
        f"&ps=200"
        f"&p=1"
        f"&q=200"
    )


def scrape_note_links_one_page(url: str, issuer_name: str) -> list:
    driver = get_driver()

    try:
        print(f"Scraping {issuer_name}")
        print(f"REQUEST URL: {url}")

        html = get_page_html(driver, url)

        if "Just a moment" in driver.title:
            print(f"Still blocked for {issuer_name}")
            return []

        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.select("div.resultat_recherche div.description_piece")

        if not blocks:
            print(f"No note blocks found for {issuer_name}")
            return []

        rows = []

        for block in blocks:
            a = block.select_one("strong a")
            if not a:
                continue

            href = a.get("href")
            if not href:
                continue

            note_url = BASE_URL + href if href.startswith("/") else href
            title = a.get_text(" ", strip=True)

            rows.append({
                "issuer_name": issuer_name,
                "issuer_page": driver.current_url,
                "note_title": title,
                "note_url": note_url
            })

        print(f"Found {len(rows)} banknotes for {issuer_name}")
        return rows

    finally:
        driver.quit()


if __name__ == "__main__":
    df_issuers = read_countries_from_bigquery()

    all_rows = []

    for _, row in df_issuers.iterrows():
        time.sleep(5)  # slow down between countries

        issuer_name = row["name"]
        issuer_url = row["url"]

        banknote_url = build_banknote_url(issuer_url)
        rows = scrape_note_links_one_page(banknote_url, issuer_name)
        all_rows.extend(rows)

    if not all_rows:
        print("No notes scraped.")
    else:
        df_notes = (
            pd.DataFrame(all_rows)
            .drop_duplicates(subset=["issuer_name", "note_url"])
            .reset_index(drop=True)
        )
        df_notes["load_timestamp"] = datetime.now(timezone.utc)

        print(df_notes.head())
        print("Total notes scraped:", len(df_notes))

        upload_to_bigquery(df_notes)
