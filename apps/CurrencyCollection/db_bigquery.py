import os
import json
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

from config import PROJECT_ID, BANKNOTE_MASTER_VIEW


@st.cache_resource
def get_bq_client():
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

    if json_str:
        info = json.loads(json_str)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=info["project_id"], credentials=credentials)

    if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in st.secrets:
        info = dict(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=info["project_id"], credentials=credentials)

    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=3600)
def get_country_summary():
    sql = f"""
    SELECT
      country_name,
      iso_alpha3,
      COUNT(DISTINCT id) AS total_notes
    FROM `{BANKNOTE_MASTER_VIEW}`
    WHERE iso_alpha3 IS NOT NULL
      AND country_name IS NOT NULL
    GROUP BY country_name, iso_alpha3
    ORDER BY country_name
    """

    return get_bq_client().query(sql).to_dataframe()


@st.cache_data(ttl=3600)
def get_notes_for_country(iso_alpha3: str):
    sql = f"""
    SELECT
      id AS type_id,
      country_name,
      iso_alpha3,
      title,
      CAST(min_year AS STRING) AS year,
      face_value_text AS value,
      currency_full_name AS currency,
      obverse_image AS image_front_url,
      reverse_image AS image_back_url,
      url AS numista_url
    FROM `{BANKNOTE_MASTER_VIEW}`
    WHERE iso_alpha3 = @iso_alpha3
      AND include_on_map = TRUE
    ORDER BY
      min_year,
      title
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("iso_alpha3", "STRING", iso_alpha3)
        ]
    )

    return get_bq_client().query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_all_note_country_lookup():
    sql = f"""
    SELECT
      id AS type_id,
      country_name,
      iso_alpha3
    FROM `{BANKNOTE_MASTER_VIEW}`
    WHERE iso_alpha3 IS NOT NULL
      AND country_name IS NOT NULL
      AND include_on_map = TRUE
    """

    return get_bq_client().query(sql).to_dataframe()