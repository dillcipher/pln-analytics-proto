from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from app.core.constants import PROCESSED, RAW, RAW_UPLOAD

BACKEND_ROOT = Path(__file__).resolve().parents[2]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_list(name: str, defaults: list[str]) -> list[str]:
    value = os.getenv(name)
    configured = (
        [item.strip().rstrip("/") for item in value.split(",") if item.strip()]
        if value and value.strip()
        else []
    )

    result: list[str] = []
    for item in [*defaults, *configured]:
        normalized = item.strip().rstrip("/")
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value.strip()) if value and value.strip() else default


class Settings:
    APP_NAME: str = os.getenv("APP_NAME", "PLN Analytics Platform API")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    DEBUG: bool = _env_bool("DEBUG", False)

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------
    # This dashboard shows real PLN customer data (IDPEL, name, address,
    # usage, location). Confirmed live 2026-08-31: with the old defaults
    # below (PUBLIC_DASHBOARD=True, AUTH_ENABLED=False), the deployed API
    # had ZERO authentication -- anyone with the dashboard URL could read
    # every customer's data with no login. Login infrastructure (JWT,
    # PBKDF2 user store, /auth/login, the frontend's token interceptor)
    # already existed but was never switched on. Defaults now require
    # login unless explicitly opted out (e.g. for local development via
    # .env, which sets AUTH_REQUIRED=false explicitly and is unaffected by
    # this change). Set PUBLIC_DASHBOARD=true in the deployment's own env
    # vars only if this dashboard is deliberately meant to be public.
    PUBLIC_DASHBOARD: bool = _env_bool("PUBLIC_DASHBOARD", False)
    AUTH_ENABLED: bool = _env_bool("AUTH_ENABLED", True)
    AUTH_REQUIRED: bool = (
        not PUBLIC_DASHBOARD
        and AUTH_ENABLED
        and _env_bool("AUTH_REQUIRED", True)
    )

    # Optional bootstrap admin account. When USERS_FILE does not exist yet
    # (always true on this host's ephemeral disk after a fresh restart --
    # see UserStore.reload) and both of these are set, UserStore creates a
    # single admin account from them instead of leaving nobody able to log
    # in. The env vars are the durable source of truth (configured once in
    # the deployment's own settings, same place as JWT_SECRET_KEY); the
    # generated users.json is just a disposable local cache of them.
    ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "").strip()
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "")

    CORS_ORIGINS: list[str] = _env_list(
        "CORS_ORIGINS",
        [
            "https://pln-analytics.vercel.app",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:4173",
            "http://127.0.0.1:4173",
        ],
    )

    JWT_SECRET_KEY: str = os.getenv(
        "JWT_SECRET_KEY",
        "CHANGE-ME-IN-PRODUCTION-this-is-not-secure",
    )
    JWT_EXPIRES_MINUTES: int = _env_int("JWT_EXPIRES_MINUTES", 480)

    USERS_FILE: Path = _env_path(
        "USERS_FILE",
        BACKEND_ROOT / "data" / "auth" / "users.json",
    )

    DATA_PROCESSED_DIR: Path = _env_path(
        "DATA_PROCESSED_DIR",
        PROCESSED,
    )
    DATA_RAW_DIR: Path = _env_path(
        "DATA_RAW_DIR",
        RAW,
    )
    DATA_INCOMING_DIR: Path = _env_path(
        "DATA_INCOMING_DIR",
        RAW_UPLOAD,
    )
    DATA_AUTH_DIR: Path = _env_path(
        "DATA_AUTH_DIR",
        BACKEND_ROOT / "data" / "auth",
    )

    DEFAULT_PAGE_SIZE: int = _env_int("DEFAULT_PAGE_SIZE", 50)
    MAX_PAGE_SIZE: int = _env_int("MAX_PAGE_SIZE", 500)
    CACHE_TTL_SECONDS: int = _env_int("CACHE_TTL_SECONDS", 120)

    # ----------------------------------------------------------
    # OFFLOADED ETL (GitHub Actions)
    # ----------------------------------------------------------
    # The production API host (FastAPI Cloud Hobby tier: 0.1-0.5 vCPU,
    # 512MB RAM) has repeatedly recycled its container mid-merge, silently
    # killing an in-process ETL run (see .github/workflows/etl-merge.yml's
    # own header comment, and app/application/etl/etl_dispatch.py). Until
    # 2026-09-03 this offloaded pipeline could only be started by a human
    # manually pushing a file under .github/etl-jobs/ -- every "boss
    # uploads a file" run this whole debugging session needed a developer
    # to do that by hand. Setting GITHUB_ETL_TOKEN here lets the API do
    # that same push itself the moment a Drive-sourced job reaches READY
    # FOR ETL, so a normal upload gets the reliable 7GB-RAM/6-hour runner
    # automatically instead of the fragile in-process path. Leave unset to
    # keep the previous (manual-trigger-only) behavior -- nothing here
    # changes unless this token is configured.
    #
    # GITHUB_ETL_TOKEN must be a GitHub Personal Access Token (fine-
    # grained, scoped to ONLY this one repository, with "Contents:
    # Read and write" permission and nothing else) that a human creates
    # and pastes into this deployment's own environment variables --
    # this application code never generates, stores, or logs the token
    # value itself beyond reading it from the environment to make the
    # API call below.
    GITHUB_ETL_TOKEN: str = os.getenv("GITHUB_ETL_TOKEN", "").strip()
    GITHUB_ETL_REPO: str = os.getenv(
        "GITHUB_ETL_REPO",
        "dillcipher/pln-analytics-platform",
    ).strip()
    GITHUB_ETL_BRANCH: str = os.getenv("GITHUB_ETL_BRANCH", "main").strip()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
