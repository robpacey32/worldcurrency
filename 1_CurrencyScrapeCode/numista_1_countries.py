from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from datetime import datetime, timezone
import pandas as pd
import os
import json

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT_ID = "currency-pacey32-github"
DATASET = "numistascrape"
TABLE = "1_Countries"
BASE_URL = "https://en.numista.com"
URL = "https://en.numista.com/catalogue/pays.php"


def get_bq_client() -> bigquery.Client:
    json_str = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
    info = json.loads(json_str)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def upload_to_bigquery(df: pd.DataFrame) -> None:
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows to {table_id}")


def get_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1400")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
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
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.liste_pays"))
        )
        return driver.page_source
    finally:
        driver.quit()


def get_direct_link(li: Tag):
    for child in li.children:
        if isinstance(child, Tag) and child.name == "a" and "name" in (child.get("class") or []):
            return child
    return None


def get_direct_alt_names(li: Tag):
    for child in li.children:
        if isinstance(child, Tag) and child.name == "span" and "alt_names" in (child.get("class") or []):
            return child.get_text(" ", strip=True)
    return None


def get_child_uls(li: Tag):
    uls = []
    for child in li.children:
        if isinstance(child, Tag):
            if child.name == "ul":
                uls.append(child)
            elif child.name == "details":
                for dchild in child.children:
                    if isinstance(dchild, Tag) and dchild.name == "ul":
                        uls.append(dchild)
    return uls


def walk_tree(li: Tag, path=None):
    if path is None:
        path = []

    rows = []

    a = get_direct_link(li)
    if not a:
        return rows

    name = a.get_text(" ", strip=True)
    href = a.get("href")
    alt_names = get_direct_alt_names(li)

    current_path = path + [name]

    rows.append({
        "name": name,
        "url": urljoin(BASE_URL, href),
        "relative_url": href,
        "alt_names": alt_names,
        "path": " > ".join(current_path),
        "depth": len(current_path) - 1,
        "li_classes": " ".join(li.get("class", [])),
    })

    for ul in get_child_uls(li):
        for child_li in ul.find_all("li", recursive=False):
            rows.extend(walk_tree(child_li, current_path))

    return rows


def scrape_liste_pays(page_url: str) -> pd.DataFrame:
    html = get_page_html(page_url)
    soup = BeautifulSoup(html, "html.parser")

    rows = []

    root_ul = soup.find("ul", class_="liste_pays")
    if root_ul is None:
        raise ValueError("Could not find ul.liste_pays on page")

    for li in root_ul.find_all("li", recursive=False):
        rows.extend(walk_tree(li))

    df = pd.DataFrame(rows).drop_duplicates(subset=["url", "path"]).reset_index(drop=True)
    df["load_timestamp"] = datetime.now(timezone.utc)
    return df


if __name__ == "__main__":
    df = scrape_liste_pays(URL)

    print(df.head(20))
    print("Total rows:", len(df))

    upload_to_bigquery(df)