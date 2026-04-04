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
SOURCE_TABLE = "2_NotesPerCountry"
TARGET_TABLE = "3_NoteDetail"


# -------------------------
# BIGQUERY
# -------------------------
def get_bq_client() -> bigquery.Client:
    json_str = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
    info = json.loads(json_str)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def read_note_links_from_bigquery() -> pd.DataFrame:
    client = get_bq_client()
    query = f"""
    SELECT
      issuer_name,
      note_title,
      note_url
    FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY note_url
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


# -------------------------
# SELENIUM
# -------------------------
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

        time.sleep(3)

        return driver.page_source
    finally:
        driver.quit()


# -------------------------
# SCRAPING
# -------------------------
def extract_features(soup):
    data = {}

    rows = soup.select("#fiche_caracteristiques table tr")
    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue

        key = th.get_text(" ", strip=True)
        value = td.get_text(" ", strip=True)

        data[key] = value

    return data


def extract_descriptions(soup):
    data = {}

    section = soup.select_one("#fiche_descriptions")
    if not section:
        return data

    current_side = None

    for tag in section.find_all(["h3", "p", "span", "a"]):
        if tag.name == "h3":
            current_side = tag.get_text(strip=True)

        elif current_side == "Obverse":
            if tag.name == "p" and "Script:" not in tag.text and "Lettering:" not in tag.text:
                data["Obverse Description"] = tag.get_text(" ", strip=True)

            if "Script:" in tag.text:
                data["Obverse Script"] = tag.get_text(" ", strip=True).replace("Script:", "").strip()

            if tag.get("id") == "obverse_lettering":
                data["Obverse Lettering"] = tag.get_text(" ", strip=True)

        elif current_side == "Reverse":
            if tag.name == "p" and "Script:" not in tag.text and "Lettering:" not in tag.text:
                data["Reverse Description"] = tag.get_text(" ", strip=True)

            if "Script:" in tag.text:
                data["Reverse Script"] = tag.get_text(" ", strip=True).replace("Script:", "").strip()

            if tag.get("id") == "reverse_lettering":
                data["Reverse Lettering"] = tag.get_text(" ", strip=True)

        elif current_side == "Printer" and tag.name == "a":
            data["Printer"] = tag.get_text(" ", strip=True)

    return data


def extract_images_and_credit(soup):
    data = {}

    images = soup.select("#fiche_photo a.coin_pic")
    if len(images) > 0:
        data["Image Front"] = images[0].get("href")
    if len(images) > 1:
        data["Image Back"] = images[1].get("href")

    credit = soup.select_one("#fiche_photo p.mentions")
    if credit:
        data["Image Credit"] = credit.get_text(" ", strip=True)

    return data


def extract_note_title(soup):
    title = soup.select_one("h1")
    if title:
        return title.get_text(" ", strip=True)
    return None


def scrape_note(note_url: str) -> dict:
    html = get_page_html(note_url)
    soup = BeautifulSoup(html, "html.parser")

    features = extract_features(soup)
    descriptions = extract_descriptions(soup)
    images = extract_images_and_credit(soup)
    title = extract_note_title(soup)

    data = {
        "Note Title": title,
        **features,
        **descriptions,
        **images,
        "Note URL": note_url
    }

    return data


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    df_links = read_note_links_from_bigquery()

    # TEMP TEST FILTER
    df_links = df_links[df_links["note_url"] == "https://en.numista.com/247092"]

    all_rows = []

    for _, row in df_links.iterrows():
        print("Scraping:", row["note_url"])
        note_data = scrape_note(row["note_url"])
        all_rows.append(note_data)

    df_notes = pd.DataFrame(all_rows)
    df_notes["load_timestamp"] = datetime.now(timezone.utc)

    print(df_notes.T)

    upload_to_bigquery(df_notes)