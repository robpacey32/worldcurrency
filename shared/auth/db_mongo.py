import hashlib
import secrets
from datetime import datetime, timezone, timedelta

from pymongo import MongoClient

from .config import get_config

cfg = get_config()

MONGO_URI = cfg["MONGO_URI"]
AUTH_DB_NAME = cfg.get("AUTH_DB_NAME", "pacey32_auth")
AUTH_USERS_COLLECTION = cfg.get("AUTH_USERS_COLLECTION", "users")
AUTH_SESSIONS_COLLECTION = cfg.get("AUTH_SESSIONS_COLLECTION", "user_sessions")
AUTH_EMAIL_VERIFICATIONS_COLLECTION = cfg.get("AUTH_EMAIL_VERIFICATIONS_COLLECTION", "email_verifications")
AUTH_PASSWORD_RESETS_COLLECTION = cfg.get("AUTH_PASSWORD_RESETS_COLLECTION", "password_resets")

client = MongoClient(MONGO_URI)
db = client[AUTH_DB_NAME]

users_col = db[AUTH_USERS_COLLECTION]
user_sessions_col = db[AUTH_SESSIONS_COLLECTION]
email_verifications_col = db[AUTH_EMAIL_VERIFICATIONS_COLLECTION]
password_resets_col = db[AUTH_PASSWORD_RESETS_COLLECTION]

# Ensure indexes (safe to run multiple times)
users_col.create_index("username", unique=True)
users_col.create_index("email", unique=True)

# Session indexes
user_sessions_col.create_index("token_hash", unique=True)
user_sessions_col.create_index("expires_at", expireAfterSeconds=0)

# Email verification indexes
email_verifications_col.create_index("token_hash", unique=True)
email_verifications_col.create_index("expires_at", expireAfterSeconds=0)

# Password reset indexes
password_resets_col.create_index("token_hash", unique=True)
password_resets_col.create_index("expires_at", expireAfterSeconds=0)


def utc_now():
    return datetime.now(timezone.utc)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# -------------------------
# USER SESSIONS
# -------------------------
def create_user_session(username: str, days: int = 30) -> str:
    username = username.strip().lower()
    token = secrets.token_urlsafe(32)
    token_hash = hash_token(token)
    now = utc_now()
    expires_at = now + timedelta(days=days)

    user_sessions_col.insert_one(
        {
            "token_hash": token_hash,
            "username": username,
            "created_at": now,
            "expires_at": expires_at,
        }
    )

    return token


def get_user_session_by_token(token: str):
    token_hash = hash_token(token)
    return user_sessions_col.find_one({"token_hash": token_hash})


def delete_user_session_by_token(token: str):
    token_hash = hash_token(token)
    user_sessions_col.delete_one({"token_hash": token_hash})


def extend_user_session(token: str, days: int = 30):
    token_hash = hash_token(token)
    user_sessions_col.update_one(
        {"token_hash": token_hash},
        {
            "$set": {
                "expires_at": utc_now() + timedelta(days=days),
            }
        }
    )


def delete_all_user_sessions(username: str):
    user_sessions_col.delete_many({"username": username.strip().lower()})


# -------------------------
# EMAIL VERIFICATION
# -------------------------
def get_user_by_email(email: str):
    return users_col.find_one({"email": email.strip().lower()})


def get_user_by_username(username: str):
    return users_col.find_one({"username": username.strip().lower()})


def mark_user_email_verified(username: str):
    users_col.update_one(
        {"username": username.strip().lower()},
        {
            "$set": {
                "email_verified": True,
                "email_verified_at": utc_now(),
            }
        }
    )


def create_email_verification_token(username: str, email: str, hours: int = 24) -> str:
    username = username.strip().lower()
    email = email.strip().lower()
    token = secrets.token_urlsafe(32)
    token_hash = hash_token(token)
    now = utc_now()
    expires_at = now + timedelta(hours=hours)

    email_verifications_col.insert_one(
        {
            "token_hash": token_hash,
            "username": username,
            "email": email,
            "created_at": now,
            "expires_at": expires_at,
        }
    )

    return token


def get_email_verification_by_token(token: str):
    token_hash = hash_token(token)
    return email_verifications_col.find_one({"token_hash": token_hash})


def delete_email_verification_by_token(token: str):
    token_hash = hash_token(token)
    email_verifications_col.delete_one({"token_hash": token_hash})


def delete_email_verifications_for_user(username: str):
    email_verifications_col.delete_many({"username": username.strip().lower()})


def create_email_verification(username: str, email: str, hours: int = 24) -> str:
    return create_email_verification_token(username, email, hours=hours)


def verify_email_token(token: str):
    token_hash = hash_token(token)
    record = email_verifications_col.find_one({"token_hash": token_hash})

    if not record:
        return False, "Invalid or expired token"

    users_col.update_one(
        {"username": record["username"]},
        {
            "$set": {
                "email_verified": True,
                "email_verified_at": utc_now(),
            }
        }
    )

    email_verifications_col.delete_one({"token_hash": token_hash})

    return True, "Email verified successfully"


# -------------------------
# PASSWORD RESETS
# -------------------------
def create_password_reset_token(username: str, hours: int = 1) -> str:
    username = username.strip().lower()
    token = secrets.token_urlsafe(32)
    token_hash = hash_token(token)
    now = utc_now()
    expires_at = now + timedelta(hours=hours)

    password_resets_col.insert_one(
        {
            "token_hash": token_hash,
            "username": username,
            "created_at": now,
            "expires_at": expires_at,
        }
    )

    return token


def get_password_reset_by_token(token: str):
    token_hash = hash_token(token)
    return password_resets_col.find_one({"token_hash": token_hash})


def delete_password_reset_by_token(token: str):
    token_hash = hash_token(token)
    password_resets_col.delete_one({"token_hash": token_hash})


def delete_password_resets_for_user(username: str):
    password_resets_col.delete_many({"username": username.strip().lower()})


def create_password_reset(email: str, hours: int = 2):
    user = users_col.find_one({"email": email.strip().lower()})
    if not user:
        return False, None

    token = secrets.token_urlsafe(32)
    token_hash = hash_token(token)

    password_resets_col.insert_one(
        {
            "token_hash": token_hash,
            "username": user["username"],
            "created_at": utc_now(),
            "expires_at": utc_now() + timedelta(hours=hours),
        }
    )

    return True, token


def reset_password_with_token(token: str, new_password_hash: bytes):
    token_hash = hash_token(token)
    record = password_resets_col.find_one({"token_hash": token_hash})

    if not record:
        return False, "Invalid or expired token"

    users_col.update_one(
        {"username": record["username"]},
        {"$set": {"password_hash": new_password_hash}}
    )

    password_resets_col.delete_one({"token_hash": token_hash})
    user_sessions_col.delete_many({"username": record["username"]})

    return True, "Password reset successfully"


# -------------------------
# USERS
# -------------------------
def update_last_login(username: str):
    users_col.update_one(
        {"username": username.strip().lower()},
        {"$set": {"last_login_at": utc_now()}}
    )


def update_user_password(username: str, password_hash: bytes):
    users_col.update_one(
        {"username": username.strip().lower()},
        {"$set": {"password_hash": password_hash}}
    )


def update_user_display_currency(username: str, display_currency: str):
    users_col.update_one(
        {"username": username.strip().lower()},
        {"$set": {"display_currency": display_currency}}
    )