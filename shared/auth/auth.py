import bcrypt
from datetime import datetime, timezone

from .db_mongo import (
    users_col,
    update_last_login,
    update_user_password,
    delete_all_user_sessions,
    create_email_verification_token,
    create_password_reset_token,
    get_user_by_email,
    get_user_by_username,
    mark_user_email_verified,
    get_email_verification_by_token,
    delete_email_verification_by_token,
    get_password_reset_by_token,
    delete_password_reset_by_token,
)


def hash_password(password: str) -> bytes:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())


def check_password(password: str, password_hash) -> bool:
    if isinstance(password_hash, str):
        password_hash = password_hash.encode("utf-8")
    return bcrypt.checkpw(password.encode("utf-8"), password_hash)


def register_user(username: str, email: str, password: str):
    clean_username = username.strip().lower()
    clean_email = email.strip().lower()

    existing = users_col.find_one({
        "$or": [
            {"username": clean_username},
            {"email": clean_email}
        ]
    })

    if existing:
        return False, "Username or email already exists", None

    users_col.insert_one({
        "username": clean_username,
        "email": clean_email,
        "password_hash": hash_password(password),
        "created_at": datetime.now(timezone.utc),
        "last_login_at": None,
        "display_currency": "GBP",
        "email_verified": False,
        "email_verified_at": None,
        "role": "user",
        "is_active": True,
    })

    token = create_email_verification_token(clean_username, clean_email)

    return True, "User created. Please verify your email.", token


def login_user(username: str, password: str):
    clean_username = username.strip().lower()
    user = get_user_by_username(clean_username)

    if not user:
        return None, "User not found"

    if not check_password(password, user["password_hash"]):
        return None, "Incorrect password"

    if not user.get("is_active", True):
        return None, "This account is inactive."

    if not user.get("email_verified", False):
        return None, "Please verify your email before logging in."

    update_last_login(clean_username)
    return get_user_by_username(clean_username), None


def change_password(username: str, current_password: str, new_password: str):
    user = get_user_by_username(username.strip().lower())
    if not user:
        return False, "User not found"

    if not check_password(current_password, user["password_hash"]):
        return False, "Current password is incorrect"

    update_user_password(username, hash_password(new_password))
    delete_all_user_sessions(username)

    return True, "Password updated successfully"


def request_password_reset(email: str):
    clean_email = email.strip().lower()
    user = get_user_by_email(clean_email)

    if not user:
        return False, None

    if not user.get("is_active", True):
        return False, None

    token = create_password_reset_token(user["username"])
    return True, token


def reset_password_with_token(token: str, new_password: str):
    reset_doc = get_password_reset_by_token(token)
    if not reset_doc:
        return False, "Invalid or expired reset link"

    username = reset_doc["username"]

    update_user_password(username, hash_password(new_password))
    delete_password_reset_by_token(token)
    delete_all_user_sessions(username)

    return True, "Password has been reset"


def verify_email_token(token: str):
    doc = get_email_verification_by_token(token)

    if not doc:
        return False, "Invalid or expired verification link"

    username = doc["username"]

    mark_user_email_verified(username)
    delete_email_verification_by_token(token)

    return True, "Email verified successfully"