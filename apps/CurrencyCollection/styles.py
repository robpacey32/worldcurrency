import streamlit as st


def apply_currency_theme():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.5rem;
        }

        div[data-testid="stMetric"] {
            background: #f8f9fb;
            padding: 1rem;
            border-radius: 0.75rem;
            border: 1px solid #e5e7eb;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )