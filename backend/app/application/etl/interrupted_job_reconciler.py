"""Reconcile jobs interrupted by an API runtime restart.

The API process cannot keep an in-memory background task alive across a
container restart. Leaving its durable manifest in a non-terminal state makes
the frontend show a permanently running MERGING job. When automatic recovery
is disabled, startup must explicitly close those orphaned jobs as retryable.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.application.jobs.job_manager import JobManager, JOB_STATE_STORAGE, S3_BUCKET

logger = logging.getLogger(__name__)

_TERMINAL = {"FINISHED", "FAILED", "CANCELLED", "COMPLETED"}


def reconcile_interrupted_drive_jobs() -> dict:
    """Mark every non-terminal Google Drive job from a previous runtime failed.

    This function runs only during application startup. A background asyncio
    task from the previous process cannot still be executing after that process
    has restarted, so any durable non-terminal state is orphaned rather than
    genuinely running.
    """
    if JOB_STATE_STORAGE == "local":
        return {"reconciled": 0, "reason": "local_job_state"}

    client = JobManager._create_s3_client()
    reconciled = 0
    scanned = 0
    now = datetime.now(timezone.utc).isoformat()

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="jobs/"):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith("/manifest.json"):
                continue
            scanned += 1
            try:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                body = response["Body"]
                try:
                    data = json.loads(body.read().decode("utf-8"))
                finally:
                    body.close()
            except Exception:
                logger.exception("INTERRUPTED JOB RECONCILIATION READ FAILED | key=%s", key)
                continue

            if not isinstance(data, dict):
                continue
            if str(data.get("storage") or "").strip().lower() != "google_drive":
                continue
            status = str(data.get("status") or "").strip().upper()
            if status in _TERMINAL:
                continue

            job_id = str(data.get("job_id") or "").strip()
            if not job_id:
                continue

            progress = max(0, min(int(data.get("progress") or 0), 99))
            reason = (
                "API runtime restarted while this Google Drive ETL job was active. "
                "The previous background worker no longer exists; retry the job "
                "from Google Drive to start a fresh, bounded run."
            )
            data.update(
                {
                    "status": "FAILED",
                    "progress": progress,
                    "current_step": "INTERRUPTED BY RUNTIME RESTART — RETRY FROM DRIVE",
                    "last_error": reason,
                    "last_failed_at": now,
                    "finished_at": now,
                    "updated_at": now,
                }
            )
            try:
                JobManager._put_json(JobManager._job_manifest_key(job_id), data)
                JobManager._put_json(
                    JobManager._job_metadata_key(job_id),
                    {
                        "job_id": job_id,
                        "status": data["status"],
                        "progress": data["progress"],
                        "current_step": data["current_step"],
                        "uploaded_at": data.get("uploaded_at"),
                        "started_at": data.get("started_at"),
                        "finished_at": data.get("finished_at"),
                        "total_files": data.get("total_files", 0),
                        "processed_files": data.get("processed_files", 0),
                        "storage": data.get("storage"),
                        "drive_folder_id": data.get("drive_folder_id"),
                        "files": data.get("files", []),
                        "recovery_attempts": data.get("recovery_attempts", 0),
                        "last_error": reason,
                        "last_failed_at": now,
                        "job_folder": data.get("job_folder"),
                        "updated_at": now,
                    },
                )
                JobManager.release_drive_recovery_lock(job_id)
                reconciled += 1
                logger.warning(
                    "INTERRUPTED DRIVE JOB RECONCILED | job=%s | previous_status=%s | progress=%s",
                    job_id,
                    status,
                    progress,
                )
            except Exception:
                logger.exception(
                    "INTERRUPTED JOB RECONCILIATION WRITE FAILED | job=%s", job_id
                )

    return {"reconciled": reconciled, "scanned": scanned}
