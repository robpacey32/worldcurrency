from __future__ import annotations

from typing import Optional

import resend

from .config import get_config

APP_BASE_URL: str = "http://localhost:8501"
EMAIL_FROM: str = "info@pacey32.com"
APP_NAME: str = "Pacey32"
RESEND_API_KEY: Optional[str] = None


def configure_email_utils(
    *,
    app_base_url: str,
    resend_api_key: str,
    email_from: str = "info@pacey32.com",
    app_name: str = "Pacey32",
) -> None:
    global APP_BASE_URL
    global EMAIL_FROM
    global APP_NAME
    global RESEND_API_KEY

    APP_BASE_URL = app_base_url.rstrip("/")
    EMAIL_FROM = email_from
    APP_NAME = app_name
    RESEND_API_KEY = resend_api_key

    resend.api_key = RESEND_API_KEY


def configure_email_utils_from_config() -> None:
    cfg = get_config()

    resend_api_key = cfg.get("RESEND_API_KEY")
    app_base_url = cfg.get("APP_BASE_URL", "http://localhost:8501")
    email_from = cfg.get("EMAIL_FROM", "info@pacey32.com")
    app_name = cfg.get("APP_NAME", "Pacey32")

    if not resend_api_key:
        raise ValueError("RESEND_API_KEY is missing")

    configure_email_utils(
        app_base_url=app_base_url,
        resend_api_key=resend_api_key,
        email_from=email_from,
        app_name=app_name,
    )


def _ensure_email_configured():
    if not RESEND_API_KEY:
        configure_email_utils_from_config()


def send_verification_email(email: str, token: str):
    _ensure_email_configured()

    verify_url = f"{APP_BASE_URL}?verify_token={token}"

    html = f"""
    <html>
      <body>
        <h2>Verify your email</h2>
        <p>Thanks for signing up to {APP_NAME}.</p>
        <p>Please click the link below to verify your email address:</p>
        <p><a href="{verify_url}">Verify my email</a></p>
        <p>If you did not create this account, you can ignore this email.</p>
      </body>
    </html>
    """

    resend.Emails.send({
        "from": EMAIL_FROM,
        "to": [email],
        "subject": f"Verify your {APP_NAME} account",
        "html": html,
    })


def send_password_reset_email(email: str, token: str):
    _ensure_email_configured()

    reset_url = f"{APP_BASE_URL}?reset_token={token}"

    html = f"""
    <html>
      <body>
        <h2>Reset your password</h2>
        <p>We received a request to reset your {APP_NAME} password.</p>
        <p>Click the link below to choose a new password:</p>
        <p><a href="{reset_url}">Reset my password</a></p>
        <p>If you did not request this, you can ignore this email.</p>
      </body>
    </html>
    """

    resend.Emails.send({
        "from": EMAIL_FROM,
        "to": [email],
        "subject": f"Reset your {APP_NAME} password",
        "html": html,
    })