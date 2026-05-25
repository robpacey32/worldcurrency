from __future__ import annotations

import time
from typing import Callable, Optional

import streamlit as st
from streamlit_local_storage import LocalStorage

from .auth import register_user, login_user, request_password_reset
from .db_mongo import (
    create_user_session,
    get_user_session_by_token,
    delete_user_session_by_token,
    extend_user_session,
    get_user_by_username,
)
from .email_utils import send_verification_email, send_password_reset_email

SESSION_DAYS = 30
SESSION_STORAGE_KEY = "session_token"
HELP_EMAIL = "info@pacey32.com"
THEME_CALLBACK: Optional[Callable[[], None]] = None
SEND_VERIFICATION_EMAIL_CALLBACK: Callable[[str, str], None] = send_verification_email
SEND_PASSWORD_RESET_EMAIL_CALLBACK: Callable[[str, str], None] = send_password_reset_email


def configure_ui_auth(
    *,
    session_days: int = 30,
    session_storage_key: str = "session_token",
    help_email: str = "info@pacey32.com",
    theme_callback: Optional[Callable[[], None]] = None,
    send_verification_email_callback: Optional[Callable[[str, str], None]] = None,
    send_password_reset_email_callback: Optional[Callable[[str, str], None]] = None,
) -> None:
    global SESSION_DAYS
    global SESSION_STORAGE_KEY
    global HELP_EMAIL
    global THEME_CALLBACK
    global SEND_VERIFICATION_EMAIL_CALLBACK
    global SEND_PASSWORD_RESET_EMAIL_CALLBACK

    SESSION_DAYS = session_days
    SESSION_STORAGE_KEY = session_storage_key
    HELP_EMAIL = help_email
    THEME_CALLBACK = theme_callback

    if send_verification_email_callback is not None:
        SEND_VERIFICATION_EMAIL_CALLBACK = send_verification_email_callback

    if send_password_reset_email_callback is not None:
        SEND_PASSWORD_RESET_EMAIL_CALLBACK = send_password_reset_email_callback


def get_local_storage():
    return LocalStorage()


def _ensure_session_defaults():
    if "user" not in st.session_state:
        st.session_state.user = None

    if "display_currency" not in st.session_state:
        st.session_state.display_currency = "GBP"

    if "show_reset" not in st.session_state:
        st.session_state.show_reset = False


def _build_session_user(user: dict) -> dict:
    return {
        "username": user["username"],
        "email": user.get("email"),
        "user_id": user["username"],
        "display_currency": user.get("display_currency", "GBP"),
    }


def restore_login_from_storage() -> bool:
    _ensure_session_defaults()

    localS = get_local_storage()
    token = localS.getItem(SESSION_STORAGE_KEY)

    if not token:
        return False

    session_doc = get_user_session_by_token(token)
    if not session_doc:
        return False

    username = session_doc.get("username")
    if not username:
        return False

    user = get_user_by_username(username)
    if not user:
        return False

    extend_user_session(token, days=SESSION_DAYS)

    st.session_state.user = _build_session_user(user)
    st.session_state.display_currency = user.get("display_currency", "GBP")
    return True


def get_current_user():
    _ensure_session_defaults()

    if st.session_state.user is None:
        restore_login_from_storage()

    return st.session_state.user


def save_session_to_storage(token: str):
    localS = get_local_storage()
    localS.setItem(SESSION_STORAGE_KEY, token)
    time.sleep(2)


def clear_session_from_storage():
    localS = get_local_storage()
    localS.deleteItem(SESSION_STORAGE_KEY)


def logout():
    _ensure_session_defaults()

    localS = get_local_storage()
    token = localS.getItem(SESSION_STORAGE_KEY)

    if token:
        delete_user_session_by_token(token)

    clear_session_from_storage()
    st.session_state.user = None
    st.session_state.display_currency = "GBP"


def render_login_portal(show_title: bool = True):
    _ensure_session_defaults()

    if THEME_CALLBACK is not None:
        THEME_CALLBACK()

    if show_title:
        st.title("Account")

    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    st.markdown("### Sign in")
    tab1, tab2 = st.tabs(["Login", "Register"])

    with tab1:
        with st.form("login_form"):
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            keep_signed_in = st.checkbox("Keep me signed in", value=True)
            login_submitted = st.form_submit_button("Login")

        if login_submitted:
            user, error = login_user(username, password)

            if user:
                session_user = _build_session_user(user)

                st.session_state.user = session_user
                st.session_state.display_currency = session_user["display_currency"]

                if keep_signed_in:
                    token = create_user_session(user["username"], days=SESSION_DAYS)
                    save_session_to_storage(token)

                st.rerun()
            else:
                st.error(error)

        if st.button("Forgot password?"):
            st.session_state.show_reset = True

        if st.session_state.get("show_reset"):
            reset_email = st.text_input("Enter your email to reset password", key="reset_email")

            if st.button("Send reset link"):
                ok, token = request_password_reset(reset_email)

                if ok and token:
                    SEND_PASSWORD_RESET_EMAIL_CALLBACK(reset_email.strip().lower(), token)

                st.success("If that email exists, a reset link has been sent.")

    with tab2:
        with st.form("register_form"):
            username = st.text_input("Choose a username", key="reg_username")
            email = st.text_input("Email", key="reg_email")
            password = st.text_input("Choose a password", type="password", key="reg_password")
            register_submitted = st.form_submit_button("Register")

        if register_submitted:
            ok, msg, token = register_user(username, email, password)

            if ok:
                SEND_VERIFICATION_EMAIL_CALLBACK(email.strip().lower(), token)
                st.success(msg)
            else:
                st.error(msg)

    st.markdown(
        f"""
        <div class="help-box">
            Need help? Contact
            <a href="mailto:{HELP_EMAIL}" style="color:#F4D995; text-decoration:none; font-weight:700;">
                {HELP_EMAIL}
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)