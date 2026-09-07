"""Durable upload queue reset controls."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
_INSTALLED = False


async def _reset_pending_batches() -> dict:
    from app.interface.api.v1 import batch_upload
    from app.services.upload_service import UploadService

    batches = await batch_upload._read_batch_objects()
    reset_batches = 0
    reset_jobs = 0
    deleted_chunk_prefixes = 0
    now = datetime.now(timezone.utc).isoformat()

    for batch in batches:
        status = str(batch.get("status", "")).upper()
        if status not in {"PROCESSING", "RETRYING", "COMPLETED_WITH_ERRORS"}:
            continue

        jobs = batch.get("jobs")
        if isinstance(jobs, list):
            for job in jobs:
                if str(job.get("status", "")).upper() == "FINISHED":
                    continue

                job["status"] = "CANCELLED"
                job["cancelled_at"] = now
                reset_jobs += 1

                upload_id = str(job.get("upload_id") or "").strip()
                if upload_id:
                    try:
                        if await UploadService._s3_delete_prefix(f"chunks/{upload_id}/"):
                            deleted_chunk_prefixes += 1
                    except Exception:
                        logger.exception("Could not delete cancelled upload chunks | upload=%s", upload_id)

                job_id = str(job.get("job_id") or "").strip()
                if job_id:
                    try:
                        metadata_key = UploadService._job_metadata_s3_key(job_id)
                        metadata = await UploadService._s3_get_json(metadata_key)
                        if isinstance(metadata, dict):
                            metadata.update({
                                "status": "CANCELLED",
                                "cancelled_at": now,
                                "last_error": "Reset by administrator before next clean upload run.",
                            })
                            await UploadService._s3_put_json(metadata_key, metadata)
                    except Exception:
                        logger.exception("Could not cancel durable job metadata | job=%s", job_id)

        batch["status"] = "CANCELLED"
        batch["cancelled_at"] = now
        batch_id = str(batch.get("batch_id") or "").strip()
        if batch_id:
            await batch_upload._persist_batch(batch_id, batch)
            reset_batches += 1

    return {
        "success": True,
        "status": "RESET",
        "reset_batches": reset_batches,
        "reset_jobs": reset_jobs,
        "deleted_chunk_prefixes": deleted_chunk_prefixes,
        "message": "Pending upload/ETL work was cancelled and its source chunks were cleared. Finished data was untouched.",
    }


def install_durable_upload_queue_patch() -> None:
    """Register one-shot reset after the v1 router has been constructed."""
    global _INSTALLED
    if _INSTALLED:
        return

    from app.interface.api.v1.router import api_v1_router

    async def reset_pending_uploads():
        return await _reset_pending_batches()

    api_v1_router.add_api_route(
        "/upload/batch/reset-pending",
        reset_pending_uploads,
        methods=["POST"],
        name="reset_pending_uploads",
        tags=["Upload Batch"],
    )

    logger.info("Registered POST /api/v1/upload/batch/reset-pending")
    _INSTALLED = True
