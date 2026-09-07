"""
User Store
==========
Lightweight, file-backed user credential store for an internal tool
with a small, centrally-managed user list (no self-service signup).
Deliberately NOT a full RDBMS-backed user table — that would mean
running/paying for a database server for a handful of rows. If PLN
later wants self-service accounts, SSO, or per-request role changes,
swap this class for a real repository without touching any caller (it's
accessed only through the `UserStore` interface below).

File format (`data/auth/users.json`):
    {
      "<username>": {
        "username": "...",
        "full_name": "...",
        "password_hash": "pbkdf2_sha256$...",
        "role": "admin" | "analyst" | "viewer",
        "unitupi_scope": null | "UID LAMPUNG"   # null = full access
      }, ...
    }
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from app.core.security import hash_password, verify_password

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthenticatedUser:
    username: str
    full_name: str
    role: str
    unitupi_scope: str | None


class UserStore:
    def __init__(self, users_file: Path):
        self._users_file = users_file
        self._users: dict[str, dict] = {}
        self.reload()

    def reload(self) -> None:
        if not self._users_file.exists():
            if self._bootstrap_admin_from_env():
                logger.warning(
                    "USERS FILE BOOTSTRAPPED FROM ADMIN_USERNAME/ADMIN_PASSWORD | path=%s",
                    self._users_file,
                )
            else:
                logger.error(
                    "Users file not found at %s and ADMIN_USERNAME/ADMIN_PASSWORD are not "
                    "both set — no one will be able to log in",
                    self._users_file,
                )
                self._users = {}
                return
        with self._users_file.open(encoding="utf-8") as fh:
            self._users = json.load(fh)
        logger.info("Loaded %d user(s) from %s", len(self._users), self._users_file)

    def _bootstrap_admin_from_env(self) -> bool:
        """Create a single admin account from ADMIN_USERNAME/ADMIN_PASSWORD.

        This host's disk is ephemeral (confirmed repeatedly this project --
        every container restart wipes local files), and users.json is
        deliberately never committed to git. Without this, enabling
        AUTH_REQUIRED would just lock everyone out on the very first
        restart. The env vars are the durable source of truth (set once in
        the deployment's own settings, same place as JWT_SECRET_KEY); this
        regenerates the local file from them on every fresh boot.

        Confirmed live 2026-09-01: on a fresh boot, more than one worker
        process can race in here at once (the `@lru_cache` around
        `get_user_store()` in deps.py only serializes calls *within* one
        process -- it does nothing across separate OS processes sharing
        the same container disk). The old code used a single shared temp
        filename (`users.json.tmp`) for its atomic write-then-rename, so a
        second writer's `.replace()` could find its own temp file already
        consumed by a first writer's successful rename, raising
        `FileNotFoundError` up through FastAPI's dependency resolution and
        failing that request's login/auth check. Fixed by (1) giving each
        writer a unique temp filename (pid + random suffix), so no two
        writers can ever contend for the same rename source, and (2)
        treating a rename failure as benign when the target file already
        exists by the time we'd write it -- that just means another writer
        already finished the exact same bootstrap first.
        """
        from app.core.config import get_settings

        settings = get_settings()
        username = settings.ADMIN_USERNAME
        password = settings.ADMIN_PASSWORD
        if not username or not password:
            return False

        if self._users_file.exists():
            # Another writer already bootstrapped the file between our
            # caller's exists() check and this call -- nothing to do.
            return True

        self._users_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            username: {
                "username": username,
                "full_name": "Administrator",
                "password_hash": hash_password(password),
                "role": "admin",
                "unitupi_scope": None,
            }
        }
        unique_suffix = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        temporary = self._users_file.with_suffix(
            f"{self._users_file.suffix}.tmp-{unique_suffix}"
        )
        try:
            with temporary.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            temporary.replace(self._users_file)
        except FileNotFoundError:
            # Extremely unlikely now that each writer has a unique temp
            # filename, but stay defensive: if the target file exists by
            # now, some other writer won the race and we're fine.
            if not self._users_file.exists():
                raise
        finally:
            temporary.unlink(missing_ok=True)
        return True

    def authenticate(self, username: str, password: str) -> AuthenticatedUser | None:
        record = self._users.get(username)
        if record is None:
            return None
        if not verify_password(password, record["password_hash"]):
            return None
        return AuthenticatedUser(
            username=record["username"],
            full_name=record["full_name"],
            role=record["role"],
            unitupi_scope=record.get("unitupi_scope"),
        )

    def get(self, username: str) -> AuthenticatedUser | None:
        record = self._users.get(username)
        if record is None:
            return None
        return AuthenticatedUser(
            username=record["username"],
            full_name=record["full_name"],
            role=record["role"],
            unitupi_scope=record.get("unitupi_scope"),
        )
