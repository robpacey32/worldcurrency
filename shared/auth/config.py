from __future__ import annotations

import os
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # fallback for older Python


_cached_config = None


def get_config() -> dict:
    global _cached_config

    if _cached_config is not None:
        return _cached_config

    candidate_paths = [
        Path("/etc/secrets/secrets.toml"),
        Path("/opt/render/project/src/Apps/BetTracker/.streamlit/secrets.toml"),
        Path("/opt/render/.streamlit/secrets.toml"),
        Path.cwd() / ".streamlit" / "secrets.toml",
    ]

    for path in candidate_paths:
        if path.exists():
            with open(path, "rb") as f:
                _cached_config = tomllib.load(f)
                return _cached_config

    try:
        import streamlit as st
        _cached_config = dict(st.secrets)
        return _cached_config
    except Exception:
        pass

    raise RuntimeError("No secrets configuration could be loaded.")