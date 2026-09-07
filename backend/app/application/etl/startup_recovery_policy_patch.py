"""Recovery policy overlay for interrupted Google Drive ETL jobs."""
from __future__ import annotations

import json
import logging
from datetime import datetime

from . import startup_recovery_safe as base

logger = logging.getLogger(__name__)
_INSTALLED = False
_MAX_RESETS = 1


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    original_load_jobs = base._load_jobs

    def load_jobs_with_interrupted_drive_recovery():
        jobs = original_load_jobs()
        known = {str(job.get("job_id") or "") for job in jobs}
        client = base._client()
        if client is None:
            return jobs
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=base.S3_BUCKET, Prefix="jobs/"):
                for item in page.get("Contents", []):
                    key = str(item.get("Key") or "")
                    if not key.endswith("/job.json"):
                        continue
                    parts = key.split("/")
                    if len(parts) != 3:
                        continue
                    job_id = parts[1]
                    if job_id in known:
                        continue
                    try:
                        response = client.get_object(Bucket=base.S3_BUCKET, Key=key)
                        body = response["Body"]
                        try:
                            metadata = json.loads(body.read().decode("utf-8"))
                        finally:
                            body.close()
                    except Exception:
                        continue
                    if not isinstance(metadata, dict):
                        continue
                    status = str(metadata.get("status") or "").upper()
                    error = str(metadata.get("last_error") or "")
                    folder = str(
                        metadata.get("drive_folder_id")
                        or metadata.get("google_drive_folder_id")
                        or ""
                    ).strip()
                    resets = int(metadata.get("recovery_reset_count") or 0)
                    if (
                        status == "FAILED"
                        and folder
                        and "Recovery attempt limit reached" in error
                        and resets < _MAX_RESETS
                    ):
                        metadata.update({
                            "status": "UPLOADED",
                            "current_step": "RECOVERY_RETRY_ELIGIBLE",
                            "recovery_attempts": 0,
                            "recovery_reset_count": resets + 1,
                            "last_recovery_reset_at": datetime.now().isoformat(),
                            "recovery_policy_version": f"{base.RECOVERY_POLICY_VERSION}-retry1",
                        })
                        metadata.pop("last_error", None)
                        jobs.append(metadata)
                        known.add(job_id)
                        logger.warning(
                            "STARTUP RECOVERY RETRY ENABLED | job=%s | previous_attempt_limit=%s",
                            job_id,
                            error,
                        )
        except Exception:
            logger.exception("STARTUP RECOVERY: failed scanning interrupted Drive jobs")
        return jobs

    base._load_jobs = load_jobs_with_interrupted_drive_recovery
    _INSTALLED = True
    logger.info("Installed interrupted Drive recovery policy overlay.")


install()
recover_pending_jobs = base.recover_pending_jobs
