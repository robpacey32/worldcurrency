import os
import streamlit as st


def get_config_value(name: str, default=None):
    if name in st.secrets:
        return st.secrets[name]
    return os.environ.get(name, default)


PROJECT_ID = get_config_value("GCP_PROJECT_ID", "currency-pacey32-github")
BANKNOTE_MASTER_VIEW = get_config_value(
    "BANKNOTE_MASTER_VIEW",
    "currency-pacey32-github.numistaviews.5_Numista_BanknoteMaster_Enriched",
)

MONGO_URI = get_config_value("MONGO_URI")
MONGO_DB_NAME = get_config_value("MONGO_DB_NAME", "currencycollection")
MONGO_COLLECTION_NAME = get_config_value("MONGO_COLLECTION_NAME", "user_banknotes")