from pathlib import Path
import streamlit as st


def load_app_css(css_filename="styles.css"):
    # Find repo root
    current = Path(__file__).resolve()
    for parent in current.parents:
        # BetTracker CSS
        css_path = parent / "Apps" / "BetTracker" / css_filename
        if css_path.exists():
            with open(css_path) as f:
                st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
            return