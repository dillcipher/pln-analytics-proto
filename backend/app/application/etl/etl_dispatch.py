"""Single choke point deciding whether an ETL run goes to GitHub Actions
or runs in-process on the API host.

Why this exists: the production API host (FastAPI Cloud Hobby tier:
0.1-0.5 vCPU, 512MB RAM) has repeatedly recycled its container mid-merge,
silently killing an in-process ETL run -- see
.github/workflows/etl-merge.yml's own header comment, and every "DLPD
Prabayar quarantined" incident debugged across the 2026-09-02/03 session.
GitHub Actions gives ~7GB RAM and up to 6 hours of uninterrupted runtime
and already has a working entry point for this (see
scripts/run_etl_from_github_actions.py) -- but until now that pipeline
could only be started by a human manually pushing a file under
.github/etl-jobs/, so every real run needed a developer to do that by
hand.

dispatch_etl() is the one place every "start ETL for this job" call site
(async_job_runner.run_etl_background, upload.py's process_existing_job)
should go through from now on, instead of calling run_etl_serialized()
directly. It offloads to GitHub Actions when BOTH of these hold:
  - app/services/github_etl_trigger.is_configured() -- a human has set
    GITHUB_ETL_TOKEN on this deployment (see config.py's "OFFLOADED ETL"
    section); this is a no-op, in-process-only fallback until they do.
  - the job is Google-Drive-sourced -- scripts/run_etl_from_github_actions.py
    only knows how to restore a Drive job's raw files on a fresh runner
    (see that script's own early return for non-Drive jobs). A plain
    browser-upload job still runs in-process for now; extending the
    runner script to also restore browser-upload jobs from their durable
    S3 cache is a separate, not-yet-done piece of work.

Either branch failing to apply (not configured, not a Drive job, or the
GitHub API call itself fails) falls through to the exact same in-process
run_etl_serialized() call that ran before this module existed -- nothing
about the fallback path changes.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from app.application.etl.etl_execution import run_etl_serialized
from app.application.jobs.job_manager import JobManager
from app.services import github_etl_trigger

logger = logging.getLogger(__name__)


def _is_drive_job(job_id: str) -> bool:
    try:
        metadata = JobManager.load_durable_job_state(job_id) or {}
    except Exception:
        logger.exception("ETL DISPATCH: could not read durable job state | job=%s", job_id)
        return False
    return str(metadata.get("storage") or "").strip().lower() == "google_drive"


async def dispatch_etl(job_id: str, job_folder: Path) -> dict[str, Any]:
    """Start ETL for job_id, preferring the offloaded GitHub Actions runner.

    Returns a dict compatible with run_etl_serialized()'s own return shape
    (`success: bool`, plus `offloaded: bool` and a human-readable
    `message` this function adds) so every caller can keep checking
    `result.get("success")` unchanged.
    """
    if github_etl_trigger.is_configured() and await asyncio.to_thread(_is_drive_job, job_id):
        triggered = await asyncio.to_thread(
            github_etl_trigger.trigger_github_actions_etl,
            job_id,
            reason="",
        )
        if triggered:
            logger.info(
                "ETL DISPATCH: offloaded to GitHub Actions | job=%s",
                job_id,
            )
            return {
                "success": True,
                "offloaded": True,
                "message": (
                    "ETL dijadwalkan lewat GitHub Actions (biasanya 1.5-2 "
                    "jam) -- data akan otomatis muncul di dashboard "
                    "setelah run itu selesai, tanpa perlu langkah manual "
                    "lagi."
                ),
            }
        logger.warning(
            "ETL DISPATCH: GitHub Actions trigger failed, falling back to "
            "in-process run | job=%s",
            job_id,
        )

    result = await asyncio.to_thread(run_etl_serialized, job_folder)
    if isinstance(result, dict):
        result.setdefault("offloaded", False)
    return result
