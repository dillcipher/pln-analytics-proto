"""Trigger the offloaded GitHub Actions ETL pipeline directly from the API.

Why this exists: every "boss uploads a file" run debugged in the
2026-09-02/03 production session had to be started by a developer manually
-- editing/adding a file under .github/etl-jobs/<job_id>.json and pushing
it, which is exactly how .github/workflows/etl-merge.yml documents its
"automated trigger" option. This module does that same push over the
GitHub REST API (Contents API), so the backend itself can kick off the
reliable 7GB-RAM/6-hour Actions runner the moment a Drive-sourced job is
ready, instead of falling back to the fragile in-process merge on the tiny
API host (see etl_dispatch.py for where this is called from and the
in-process fallback).

This is a no-op (returns False, changes nothing) unless GITHUB_ETL_TOKEN is
configured -- see app/core/config.py's "OFFLOADED ETL" section for what
that token needs (repo-scoped, Contents: Read and write only) and where a
human sets it. This module never generates, stores, or logs the token
value itself.

Uses only the standard library (urllib) -- no new dependency needed for
one simple authenticated PUT.
"""

from __future__ import annotations

import base64
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_API_ROOT = "https://api.github.com"
_TIMEOUT_SECONDS = 20


def is_configured() -> bool:
    settings = get_settings()
    return bool(settings.GITHUB_ETL_TOKEN and settings.GITHUB_ETL_REPO)


def _contents_url(path: str) -> str:
    settings = get_settings()
    return f"{_API_ROOT}/repos/{settings.GITHUB_ETL_REPO}/contents/{path}"


def _request(method: str, url: str, *, body: dict | None = None) -> tuple[int, dict]:
    settings = get_settings()
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Bearer {settings.GITHUB_ETL_TOKEN}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            payload = response.read()
            return response.status, (json.loads(payload) if payload else {})
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        try:
            parsed = json.loads(payload) if payload else {}
        except ValueError:
            parsed = {"raw": payload.decode("utf-8", errors="replace")}
        return exc.code, parsed


def trigger_github_actions_etl(job_id: str, *, reason: str = "") -> bool:
    """Push/update .github/etl-jobs/<job_id>.json to start run-etl-from-push.

    Returns True only once the GitHub API has confirmed the commit landed
    on the configured branch. Any failure (not configured, network error,
    permission error, ...) is logged and returns False -- callers must
    fall back to running ETL in-process, exactly as if this function did
    not exist.
    """
    if not is_configured():
        return False

    job_id = str(job_id or "").strip()
    if not job_id:
        logger.warning("GITHUB ETL TRIGGER: empty job_id, refusing to call GitHub API.")
        return False

    settings = get_settings()
    path = f".github/etl-jobs/{job_id}.json"
    url = _contents_url(path)

    try:
        get_status, get_body = _request(
            "GET",
            f"{url}?ref={settings.GITHUB_ETL_BRANCH}",
        )
    except Exception:
        logger.exception("GITHUB ETL TRIGGER: failed to check existing trigger file | job=%s", job_id)
        return False

    existing_sha: str | None = None
    if get_status == 200 and isinstance(get_body, dict):
        existing_sha = get_body.get("sha")
    elif get_status not in (200, 404):
        logger.error(
            "GITHUB ETL TRIGGER: unexpected status checking trigger file | job=%s | status=%s | body=%s",
            job_id,
            get_status,
            get_body,
        )
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    content = {
        "job_id": job_id,
        "note": (
            "Triggers .github/workflows/etl-merge.yml. The filename "
            "(without .json) is the job_id read by the workflow -- the "
            "content of this file is not otherwise used."
        ),
        "retry": (
            f"{timestamp} -- auto-triggered by the API "
            "(app/services/github_etl_trigger.py) the moment this "
            f"Drive-sourced job reached READY FOR ETL.{(' ' + reason) if reason else ''}"
        ),
    }
    encoded = base64.b64encode(
        json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")

    put_body = {
        "message": f"chore: auto-trigger ETL for {job_id}",
        "content": encoded,
        "branch": settings.GITHUB_ETL_BRANCH,
    }
    if existing_sha:
        put_body["sha"] = existing_sha

    try:
        put_status, put_response = _request("PUT", url, body=put_body)
    except Exception:
        logger.exception("GITHUB ETL TRIGGER: failed to push trigger file | job=%s", job_id)
        return False

    if put_status not in (200, 201):
        logger.error(
            "GITHUB ETL TRIGGER: push failed | job=%s | status=%s | body=%s",
            job_id,
            put_status,
            put_response,
        )
        return False

    commit_sha = None
    if isinstance(put_response, dict):
        commit_sha = (put_response.get("commit") or {}).get("sha")
    logger.info(
        "GITHUB ETL TRIGGER: pushed .github/etl-jobs/%s.json | job=%s | commit=%s",
        job_id,
        job_id,
        commit_sha,
    )
    return True
