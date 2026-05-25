from pathlib import Path
import sys
import streamlit as st

from styles import apply_currency_theme

CURRENT_FILE = Path(__file__).resolve()
for parent in CURRENT_FILE.parents:
    if (parent / "shared" / "auth").exists():
        sys.path.insert(0, str(parent))
        break

from shared.auth.ui_auth import render_login_portal, restore_login_from_storage

st.set_page_config(page_title="Currency Collection", page_icon="🪙", layout="wide")
apply_currency_theme()

restore_login_from_storage()

if "user" not in st.session_state or st.session_state.user is None:
    render_login_portal(show_title=True)
    st.stop()

st.title("Currency Collection")
st.write("Track your world banknote collection.")

#st.page_link("apps/CurrencyCollection/pages/1_World_Map.py", label="Open world map", icon="🌍")
st.success("Login successful.")
st.write("Use the Streamlit sidebar to open **1 World Map**.")