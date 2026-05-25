from pathlib import Path
import sys
import pandas as pd
import plotly.express as px
import streamlit as st

from db_bigquery import (
    get_country_summary,
    get_notes_for_country,
    get_all_note_country_lookup,
)
from db_mongo import get_owned_type_ids, set_owned_note
from styles import apply_currency_theme

CURRENT_FILE = Path(__file__).resolve()
for parent in CURRENT_FILE.parents:
    if (parent / "shared" / "auth").exists():
        sys.path.insert(0, str(parent))
        break

from shared.auth.ui_auth import render_login_portal, restore_login_from_storage

st.set_page_config(page_title="Currency Collection Map", page_icon="🌍", layout="wide")
apply_currency_theme()

restore_login_from_storage()

if "user" not in st.session_state or st.session_state.user is None:
    render_login_portal(show_title=True)
    st.stop()

user = st.session_state.user
user_id = str(user.get("user_id") or user.get("_id") or user.get("email"))

st.title("World Currency Collection")

country_df = get_country_summary()
owned_type_ids = get_owned_type_ids(user_id)

if country_df.empty:
    st.warning("No country data found. Check the BigQuery banknote master view.")
    st.stop()

all_notes_df = get_all_note_country_lookup()

if not all_notes_df.empty and owned_type_ids:
    all_notes_df["type_id"] = all_notes_df["type_id"].astype(int)

    owned_lookup_df = all_notes_df[
        all_notes_df["type_id"].isin(owned_type_ids)
    ]

    owned_counts = (
        owned_lookup_df
        .groupby("iso_alpha3")["type_id"]
        .nunique()
        .reset_index(name="owned_notes")
    )

    country_df = country_df.merge(
        owned_counts,
        on="iso_alpha3",
        how="left",
    )
else:
    country_df["owned_notes"] = 0

country_df["owned_notes"] = country_df["owned_notes"].fillna(0).astype(int)
country_df["completion_pct"] = (
    country_df["owned_notes"] / country_df["total_notes"] * 100
).fillna(0)

st.subheader("Collection map")

fig = px.choropleth(
    country_df,
    locations="iso_alpha3",
    color="completion_pct",
    hover_name="country_name",
    hover_data={
        "iso_alpha3": False,
        "total_notes": True,
        "owned_notes": True,
        "completion_pct": ":.1f",
    },
    color_continuous_scale="Greens",
    range_color=(0, 100),
)

fig.update_layout(
    margin=dict(l=0, r=0, t=0, b=0),
    height=550,
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

selected_country = st.selectbox(
    "Select a country",
    country_df["country_name"].sort_values().tolist(),
)

selected_row = country_df[country_df["country_name"] == selected_country].iloc[0]
iso_alpha3 = selected_row["iso_alpha3"]

notes_df = get_notes_for_country(iso_alpha3)

if notes_df.empty:
    st.warning("No notes found for this country.")
    st.stop()

notes_df["type_id"] = notes_df["type_id"].astype(int)
notes_df["owned"] = notes_df["type_id"].isin(owned_type_ids)

owned_count = int(notes_df["owned"].sum())
total_count = len(notes_df)
completion_pct = round((owned_count / total_count) * 100, 1) if total_count else 0

col1, col2, col3 = st.columns(3)
col1.metric("Country", selected_country)
col2.metric("Owned", f"{owned_count} / {total_count}")
col3.metric("Completion", f"{completion_pct}%")

st.subheader(f"Notes from {selected_country}")

st.markdown(
    """
    <style>
    .note-img img {
        max-width: 110px;
        max-height: 70px;
        object-fit: contain;
        border-radius: 4px;
        border: 1px solid #e5e7eb;
        background: #fafafa;
    }
    .note-title {
        font-size: 0.95rem;
        font-weight: 600;
        line-height: 1.25rem;
    }
    .note-meta {
        font-size: 0.8rem;
        color: #6b7280;
        margin-top: 0.15rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

for _, row in notes_df.iterrows():
    type_id = int(row["type_id"])
    current_owned = bool(row["owned"])

    title = str(row.get("title") or "Untitled note")
    year = "" if pd.isna(row.get("year")) else str(row.get("year"))
    value = "" if pd.isna(row.get("value")) else str(row.get("value"))
    currency = "" if pd.isna(row.get("currency")) else str(row.get("currency"))

    meta_parts = [x for x in [year, value, currency] if x]
    meta_text = " — ".join(meta_parts)

    img_url = row.get("image_front_url") or row.get("image_back_url")

    img_html = (
        f'<img src="{img_url}" />'
        if img_url and not pd.isna(img_url)
        else "<div style='width:110px;height:70px;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:4px;'></div>"
    )

    col_img, col_note, col_owned = st.columns(
        [1.2, 6, 1.2],
        vertical_alignment="center",
    )

    with col_img:
        st.markdown(
            f'<div class="note-img">{img_html}</div>',
            unsafe_allow_html=True,
        )

    with col_note:
        st.markdown(
            f"""
            <div class="note-title">{title}</div>
            <div class="note-meta">{meta_text}</div>
            """,
            unsafe_allow_html=True,
        )

    with col_owned:
        new_owned = st.checkbox(
            "Got",
            value=current_owned,
            key=f"owned_{type_id}",
            label_visibility="collapsed",
        )

    if new_owned != current_owned:
        set_owned_note(
            user_id=user_id,
            type_id=type_id,
            country_name=str(row["country_name"]),
            iso_alpha3=str(row["iso_alpha3"]),
            owned=new_owned,
        )

        try:
            get_owned_type_ids.clear()
        except Exception:
            pass

        st.rerun()