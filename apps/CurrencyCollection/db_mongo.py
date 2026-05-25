from datetime import datetime, timezone
from typing import List
import streamlit as st
from pymongo import MongoClient, UpdateOne

from config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME


@st.cache_resource
def get_mongo_collection():
    if not MONGO_URI:
        raise ValueError("MONGO_URI is missing")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    collection.create_index(
        [("user_id", 1), ("type_id", 1)],
        unique=True,
        name="user_type_unique",
    )

    return collection


def get_owned_type_ids(user_id: str) -> set[int]:
    collection = get_mongo_collection()

    rows = collection.find(
        {"user_id": user_id, "owned": True},
        {"type_id": 1, "_id": 0},
    )

    return {int(row["type_id"]) for row in rows if row.get("type_id") is not None}


def set_owned_note(user_id: str, type_id: int, country_name: str, iso_alpha3: str, owned: bool):
    collection = get_mongo_collection()
    now = datetime.now(timezone.utc)

    collection.update_one(
        {
            "user_id": user_id,
            "type_id": int(type_id),
        },
        {
            "$set": {
                "user_id": user_id,
                "type_id": int(type_id),
                "country_name": country_name,
                "iso_alpha3": iso_alpha3,
                "owned": bool(owned),
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            },
        },
        upsert=True,
    )