"""Run one Drive-sourced ETL job to completion on a GitHub Actions runner.

Why this exists: the production API host (FastAPI Cloud Hobby tier) has
0.1-0.5 vCPU burst / 512MB RAM and can recycle the container mid-run --
confirmed live, repeatedly, on 2026-08-31/09-01 -- which silently kills the
in-process background ETL task (see app/application/etl/async_job_runner.py)
and loses hours of merge progress every time. GitHub-hosted Actions runners
get ~7GB RAM, multiple cores, and up to 6 hours of *uninterrupted* runtime
per job with no autoscale-to-zero and no request-based recycling, which is
a much better fit for this one-shot, occasionally-very-heavy merge step.

This script does NOT reimplement any ETL/business logic. It imports and
calls the exact same code path the production API uses
(app.main for every runtime patch, then
app.application.etl.etl_execution.run_etl_serialized for the merge itself,
and app.interface.api.v1.drive's own _sync_drive_job/_drive_job_complete
for restoring raw files from the durable S3 cache) so behaviour is
identical to what already runs -- and has been debugged -- in production.

Logging discipline: only the application's own logger.info/warning calls
are emitted (same ones already visible in the FastAPI Cloud log viewer
throughout this project) -- status, step names, row *counts*. Nothing in
this script or the code it calls prints raw customer rows. Keep it that
way if this script is ever extended: never log a DataFrame, a row dict, or
any column value from ANEV/DLPD/PENGECEKAN/customer_location. The repo
should also be private (see docs/github-actions-etl.md) as a second layer
of protection regardless of what ends up in a log line.

Usage (from the `backend/` directory, or anywhere -- this script fixes up
sys.path itself):

    python scripts/run_etl_from_github_actions.py JOB_20260829_122308_DRIVE_d0b33523

Required environment (see .github/workflows/etl-merge.yml for the secret
names that populate these):

    S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET
        -- the same Supabase Storage (S3-compatible) credentials the API
        already uses for durable raw-file cache, processed-data storage,
        and job-state storage.

Optional:
    GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_JSON_B64
        -- only needed if some raw file was never fully cached to S3 and
        has to be re-downloaded from Drive. For a job that has already
        reached "READY FOR ETL" at least once, every file already has a
        durable_raw_key and this is not needed.

    SKIP_DRIVE_JOB_CREATION=1
        -- if the job_id given on the command line has no durable state at
        all (e.g. because the durable storage backend was migrated -- see
        2026-09 Backblaze B2 migration -- and the job's old manifest lived
        only in the previous bucket), this script's DEFAULT behaviour is to
        create a brand-new "google_drive"-type manifest for that job_id
        pointed at GOOGLE_DRIVE_FOLDER_ID (or its hardcoded default in
        google_drive_service.py) instead of failing outright. This exactly
        mirrors what app.interface.api.v1.drive's POST /drive/sync does,
        just invoked from here instead of over HTTP (that endpoint is
        behind login, which this offline runner has no way to do). This is
        considered safe to default to *on* because a job_id only ever gets
        to this script by someone deliberately pushing a file under
        .github/etl-jobs/ or typing it into workflow_dispatch -- there is
        no code path that invents a job_id on its own. Set this flag to
        restore the old strict behaviour (fail instead of creating) for a
        one-off run, e.g. while diagnosing why a job_id that SHOULD already
        exist doesn't.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

# Make `import app...` work regardless of the current working directory --
# this file lives at backend/scripts/, so backend/ (its parent) is what
# needs to be on sys.path.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# ETLOrchestrator._runtime_version() (etl_orchestrator.py) reads
# GIT_COMMIT_SHA (or APP_VERSION) to fingerprint a quarantined group's
# checkpoint entry -- a group only stays skipped on a later run if the
# stored version still matches the current one. Every GitHub Actions run
# already exposes the commit it checked out as GITHUB_SHA (no workflow
# change needed), so map it across before app.main / the orchestrator
# import anything. Without this, GIT_COMMIT_SHA is unset on every run, so
# every run's version collapses to the same "unknown" value -- meaning a
# group quarantined by a bug that has since been fixed and pushed (e.g.
# the DLPD ETL_SKIP_DATASETS default this runner used to hit on every
# call) would stay quarantined forever instead of getting retried once the
# fix actually lands.
if not os.environ.get("GIT_COMMIT_SHA") and os.environ.get("GITHUB_SHA"):
    os.environ["GIT_COMMIT_SHA"] = os.environ["GITHUB_SHA"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("github_actions_etl_runner")

MAX_SYNC_ATTEMPTS = 200  # each _sync_drive_job call is itself time-bounded


def _create_drive_job(job_id: str, folder_id: str) -> dict:
    """Hand-build and durably persist a fresh "google_drive"-type manifest
    for `job_id`, mirroring app.interface.api.v1.drive's POST /drive/sync
    handler exactly (same dict shape, same write path) so everything
    downstream (JobManager.load_durable_job_state, _drive_job_complete,
    _sync_drive_job) treats it identically to a job the API created."""
    from datetime import datetime as _datetime

    from app.application.jobs.job_manager import JobManager
    from app.application.jobs.job_status import JobStatus
    from app.core.constants import RAW_UPLOAD
    from app.interface.api.v1.drive import DRIVE_DOWNLOAD_CONCURRENCY, _write_manifest

    job_folder = RAW_UPLOAD / job_id
    created_at = _datetime.now().isoformat()
    manifest = {
        "job_id": job_id,
        "status": JobStatus.UPLOADED.value,
        "progress": 0,
        "current_step": "GOOGLE DRIVE QUEUED",
        "uploaded_at": created_at,
        "started_at": None,
        "finished_at": None,
        "total_files": 0,
        "processed_files": 0,
        "storage": "google_drive",
        "drive_folder_id": folder_id,
        "recovery_attempts": 0,
        "download_concurrency": DRIVE_DOWNLOAD_CONCURRENCY,
        "files": [],
    }
    _write_manifest(job_folder, manifest)
    JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=0, step="GOOGLE DRIVE QUEUED")
    return JobManager.load_durable_job_state(job_id) or manifest


def main() -> int:
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        logger.error("Usage: run_etl_from_github_actions.py <job_id>")
        return 2
    job_id = sys.argv[1].strip()

    logger.info("=" * 80)
    logger.info(
        "GITHUB ACTIONS ETL RUNNER | job_id=%s | runtime_version=%s",
        job_id,
        os.environ.get("GIT_COMMIT_SHA", "unknown"),
    )
    logger.info("=" * 80)

    # Import app.main first: this is where every ETL/runtime correctness
    # patch gets installed (DLPD transformer, ANEV calamine streaming,
    # dedup guards, etc.) exactly as production does it, in the same
    # order, via the same single entry point. Do not hand-roll a subset of
    # these -- see the module docstring above.
    import app.main  # noqa: F401  (import for its patch-installing side effects)

    from app.application.etl.etl_execution import run_etl_serialized
    from app.application.jobs.job_manager import JobManager
    from app.core.constants import RAW_UPLOAD
    from app.interface.api.v1.drive import _drive_job_complete, _sync_drive_job

    metadata = JobManager.load_durable_job_state(job_id)
    if not metadata:
        if os.environ.get("SKIP_DRIVE_JOB_CREATION", "").strip().lower() in {"1", "true", "yes"}:
            logger.error("Job '%s' was not found in durable storage. Nothing to do (SKIP_DRIVE_JOB_CREATION is set).", job_id)
            return 1
        from app.services.google_drive_service import GOOGLE_DRIVE_FOLDER_ID as DEFAULT_DRIVE_FOLDER_ID

        folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip() or DEFAULT_DRIVE_FOLDER_ID
        if not folder_id:
            logger.error("Job '%s' has no durable state and no GOOGLE_DRIVE_FOLDER_ID is configured to create one.", job_id)
            return 1
        logger.warning(
            "Job '%s' has no durable state -- creating a fresh Google Drive job pointed at "
            "folder_id=%s instead of failing (set SKIP_DRIVE_JOB_CREATION=1 to disable this).",
            job_id, folder_id,
        )
        metadata = _create_drive_job(job_id, folder_id)

    if str(metadata.get("storage") or "").strip().lower() != "google_drive":
        logger.error("Job '%s' is not a Google Drive job (storage=%r). This runner only handles Drive jobs.", job_id, metadata.get("storage"))
        return 1

    folder_id = str(metadata.get("drive_folder_id") or "").strip()
    if not folder_id:
        logger.error("Job '%s' has no drive_folder_id in its durable metadata.", job_id)
        return 1

    # Same cross-replica lock the API's own /drive/retry endpoint takes,
    # so this runner and the production API never restore/download the
    # same job's raw files at the same time.
    got_lock = JobManager.acquire_drive_recovery_lock(job_id)
    if not got_lock:
        logger.warning(
            "Could not acquire the Drive recovery lock for job=%s -- another worker "
            "(the production API, or a previous Actions run) currently owns it. "
            "Exiting without doing anything rather than racing it.",
            job_id,
        )
        return 1

    try:
        if metadata.get("finished"):
            logger.info("Job '%s' durable metadata already marks it finished. Nothing to restore.", job_id)
        elif _drive_job_complete(job_id):
            logger.info("Job '%s' raw files are already fully restored (READY FOR ETL).", job_id)
        else:
            logger.info("Restoring raw files for job=%s from durable S3 cache...", job_id)
            attempt = 0
            while not _drive_job_complete(job_id):
                attempt += 1
                if attempt > MAX_SYNC_ATTEMPTS:
                    logger.error(
                        "Gave up restoring job=%s after %s chunked sync calls without reaching READY FOR ETL.",
                        job_id,
                        MAX_SYNC_ATTEMPTS,
                    )
                    return 1
                logger.info("Drive sync chunk #%s for job=%s ...", attempt, job_id)
                asyncio.run(_sync_drive_job(job_id, folder_id))
            logger.info("Job '%s' is READY FOR ETL after %s chunk(s).", job_id, attempt)
    finally:
        JobManager.release_drive_recovery_lock(job_id)

    job_folder = RAW_UPLOAD / job_id
    logger.info("Starting ETL merge for job=%s (job_folder=%s) ...", job_id, job_folder)
    result = run_etl_serialized(job_folder)

    if not isinstance(result, dict) or not result.get("success"):
        logger.error("ETL merge for job=%s did NOT finish successfully: %r", job_id, result)
        return 1

    logger.info("ETL merge for job=%s finished successfully.", job_id)

    # The orchestrator's own end-of-run persist_processed_data() call uses a
    # short (60s) time budget -- fine on the production API host, where it
    # gets called again on every GET /jobs/{job_id} poll and makes
    # incremental progress each time. Nothing polls this script: it runs
    # once and the GitHub Actions runner's entire filesystem is destroyed
    # the moment it exits. Confirmed live on run #17: that 60s budget alone
    # left ~98% of a real run's freshly-processed output stranded on local
    # disk and then gone forever. Sweep with a much longer, looped budget
    # here so nothing processed in this run is lost -- see
    # persist_processed_data_until_done()'s docstring for the full story.
    try:
        from app.infrastructure.storage.processed_storage import persist_processed_data_until_done

        logger.info(
            "Sweeping to ensure ALL processed data from this run is durably persisted "
            "(this runner's filesystem will not survive past this script's exit)..."
        )
        summary = persist_processed_data_until_done()
        if not summary.get("finished_cleanly"):
            logger.warning(
                "Processed data persistence sweep hit its safety cap before confirming everything "
                "was uploaded (summary=%r). Some files may still be missing from durable storage -- "
                "check the 'Processed data persistence completed' log lines above for the last known "
                "uploaded/skipped/deferred counts.",
                summary,
            )
    except Exception:
        logger.exception(
            "Processed data persistence sweep raised unexpectedly for job=%s "
            "(the merge itself already succeeded -- this only affects whether its output made it "
            "to durable storage).",
            job_id,
        )

    logger.info("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
