from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.constants import RAW_UPLOAD
from app.interface.api.v1.upload import _run_assembly_and_etl, _run_etl
from app.services.upload_service import S3_BUCKET, UploadService, _create_s3_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/upload/batch", tags=["Upload Batch"])
_BATCH_TASKS: set[asyncio.Task] = set()
_BATCH_LOCK = asyncio.Lock()
MAX_ETL_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 5


class StagedUpload(BaseModel):
    upload_id: str = Field(min_length=1)
    filename: str = Field(min_length=1)
    total_chunks: int = Field(gt=0)
    content_type: str | None = None


class BatchCompleteRequest(BaseModel):
    batch_id: str | None = None
    uploads: list[StagedUpload] = Field(min_length=1)


class RetryJobRequest(BaseModel):
    reset_retry_count: bool = True


def _schedule_batch(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _BATCH_TASKS.add(task)

    def _done(completed: asyncio.Task) -> None:
        _BATCH_TASKS.discard(completed)
        try:
            completed.exception()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("BATCH TASK FAILED")

    task.add_done_callback(_done)
    return task


async def _persist_batch(batch_id: str, payload: dict[str, Any]) -> None:
    try:
        await UploadService._s3_put_json(f"batches/{batch_id}.json", payload)
    except Exception:
        logger.exception("Could not persist batch metadata | batch=%s", batch_id)


async def _read_batch_objects() -> list[dict[str, Any]]:
    def _read() -> list[dict[str, Any]]:
        client = _create_s3_client()
        results: list[dict[str, Any]] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="batches/"):
            for obj in page.get("Contents", []):
                key = str(obj.get("Key", ""))
                if not key.startswith("batches/") or not key.endswith(".json"):
                    continue
                try:
                    raw = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                    data = json.loads(raw.decode("utf-8"))
                    if isinstance(data, dict):
                        results.append(data)
                except Exception:
                    logger.exception("Could not read durable batch metadata | key=%s", key)
        return results

    try:
        return await asyncio.to_thread(_read)
    except Exception:
        logger.exception("Could not list durable batches")
        return []


async def _get_batch(batch_id: str) -> dict[str, Any] | None:
    batches = await _read_batch_objects()
    return next((item for item in batches if str(item.get("batch_id")) == batch_id), None)


async def _find_job_in_batches(job_id: str) -> dict[str, Any] | None:
    """Recover filename/upload metadata when legacy job metadata is incomplete."""
    for batch in await _read_batch_objects():
        jobs = batch.get("jobs")
        if not isinstance(jobs, list):
            continue
        for job in jobs:
            if str(job.get("job_id")) == job_id:
                return dict(job)
    return None


async def _find_job_manifest(job_id: str) -> dict[str, Any] | None:
    """Recover legacy filename from the durable manifest stored with the job."""
    try:
        manifest = await UploadService._s3_get_json(UploadService._job_manifest_s3_key(job_id))
    except Exception as exc:
        logger.info("No durable job manifest for retry | job=%s | error=%s", job_id, exc)
        return None
    if not isinstance(manifest, dict):
        return None
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        return None
    first = files[0]
    if not isinstance(first, dict):
        return None
    return dict(first)


async def _job_status(job_id: str) -> dict[str, Any]:
    try:
        data = await UploadService._s3_get_json(UploadService._job_metadata_s3_key(job_id))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def _mark_batch_job(batch_id: str, job: dict[str, Any], **updates: Any) -> None:
    current = await _get_batch(batch_id)
    if not current:
        return
    jobs = current.get("jobs")
    if not isinstance(jobs, list):
        return
    target_id = str(job.get("job_id"))
    for item in jobs:
        if str(item.get("job_id")) == target_id:
            item.update(updates)
            break
    statuses = [str(item.get("status", "")).upper() for item in jobs]
    if statuses and all(status == "FINISHED" for status in statuses):
        current["status"] = "COMPLETED"
        current["completed_at"] = datetime.now(timezone.utc).isoformat()
    await _persist_batch(batch_id, current)


async def _run_one_job(job: dict[str, Any], batch_id: str, retry_limit: int = MAX_ETL_RETRIES) -> bool:
    job_id = str(job["job_id"])
    filename = str(job["filename"])
    upload_id = str(job["upload_id"])
    total_chunks = int(job["total_chunks"])
    content_type = job.get("content_type")
    metadata = await _job_status(job_id)
    status = str(metadata.get("status") or job.get("status") or "ASSEMBLY_QUEUED").upper()

    if status == "FINISHED":
        await _mark_batch_job(batch_id, job, status="FINISHED")
        return True

    last_error = str(metadata.get("last_error") or "")
    for attempt in range(1, retry_limit + 1):
        try:
            await _mark_batch_job(batch_id, job, status="PROCESSING", retry_count=attempt - 1)
            metadata = await _job_status(job_id)
            current_status = str(metadata.get("status") or "").upper()

            if current_status == "FINISHED":
                await _mark_batch_job(batch_id, job, status="FINISHED", retry_count=attempt - 1)
                return True

            if current_status in {
                "ASSEMBLY_COMPLETED", "DETECTING", "VALIDATING", "MERGING",
                "TRANSFORMING", "EXPORTING", "FAILED", "ERROR", "RETRYING",
            }:
                job_folder = RAW_UPLOAD / job_id
                await UploadService.recover_assembled_job(
                    job_id=job_id,
                    filename=filename,
                    content_type=content_type,
                )
                await asyncio.to_thread(_run_etl, job_folder)
            else:
                await _run_assembly_and_etl(
                    upload_id=upload_id,
                    job_id=job_id,
                    filename=filename,
                    total_chunks=total_chunks,
                    content_type=content_type,
                )

            final_metadata = await _job_status(job_id)
            final_status = str(final_metadata.get("status") or "").upper()
            if final_status == "FINISHED":
                await _mark_batch_job(batch_id, job, status="FINISHED", retry_count=attempt - 1)
                return True
            last_error = f"ETL ended with status {final_status or 'UNKNOWN'}"
            raise RuntimeError(last_error)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = str(exc)
            logger.exception("BATCH ETL ATTEMPT FAILED | batch=%s | job=%s | attempt=%s/%s", batch_id, job_id, attempt, retry_limit)
            await _mark_batch_job(
                batch_id,
                job,
                status="RETRYING" if attempt < retry_limit else "FAILED",
                retry_count=attempt,
                last_error=last_error,
                last_retry_at=datetime.now(timezone.utc).isoformat(),
            )
            if attempt < retry_limit:
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)

    await _mark_batch_job(batch_id, job, status="FAILED", retry_count=retry_limit, last_error=last_error)
    return False


async def _run_batch(batch_id: str, jobs: list[dict[str, Any]]) -> None:
    async with _BATCH_LOCK:
        for job in jobs:
            try:
                await _run_one_job(job, batch_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("BATCH FILE FAILED | batch=%s | job=%s", batch_id, job["job_id"])

        current = await _get_batch(batch_id)
        if current:
            current_jobs = current.get("jobs") if isinstance(current.get("jobs"), list) else jobs
            statuses = [str(item.get("status", "")).upper() for item in current_jobs]
            current["status"] = "COMPLETED" if statuses and all(status == "FINISHED" for status in statuses) else "COMPLETED_WITH_ERRORS"
            current["completed_at"] = datetime.now(timezone.utc).isoformat()
            current["successful_files"] = sum(status == "FINISHED" for status in statuses)
            current["failed_files"] = sum(status == "FAILED" for status in statuses)
            await _persist_batch(batch_id, current)


async def _recover_pending_batches() -> None:
    batches = await _read_batch_objects()
    for batch in batches:
        if str(batch.get("status", "")).upper() not in {"PROCESSING", "COMPLETED_WITH_ERRORS"}:
            continue
        jobs = batch.get("jobs")
        if not isinstance(jobs, list) or not jobs:
            continue
        pending_jobs = [job for job in jobs if str(job.get("status", "")).upper() != "FINISHED"]
        batch_id = str(batch.get("batch_id") or "").strip()
        if batch_id and pending_jobs:
            _schedule_batch(_run_batch(batch_id, pending_jobs))
            return


 # No startup recovery: ETL is always started explicitly by the user.


@router.post("/complete")
async def complete_batch(request: BatchCompleteRequest):
    batch_id = request.batch_id.strip() if request.batch_id and request.batch_id.strip() else f"BATCH_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    jobs: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    for staged in request.uploads:
        try:
            result = await UploadService.prepare_chunk_upload(
                upload_id=staged.upload_id,
                filename=staged.filename,
                total_chunks=staged.total_chunks,
                content_type=staged.content_type,
            )
            jobs.append({
                "filename": staged.filename,
                "upload_id": staged.upload_id,
                "job_id": result["job_id"],
                "total_chunks": staged.total_chunks,
                "content_type": staged.content_type,
                "status": "ASSEMBLY_QUEUED",
                "retry_count": 0,
            })
        except Exception as exc:
            failures.append({"filename": staged.filename, "error": str(exc)})

    if not jobs:
        raise HTTPException(status_code=400, detail={"message": "Tidak ada file yang berhasil masuk ke batch.", "failures": failures})

    for job in jobs:
        _schedule_batch(_run_assembly_and_etl(
            upload_id=str(job["upload_id"]),
            job_id=str(job["job_id"]),
            filename=str(job["filename"]),
            total_chunks=int(job["total_chunks"]),
            content_type=job.get("content_type"),
        ))
    return {
        "success": len(failures) == 0,
        "batch_id": batch_id,
        "total_files": len(request.uploads),
        "accepted_files": len(jobs),
        "failures": failures,
        "jobs": jobs,
        "status": "ASSEMBLY_QUEUED",
        "message": "All accepted files are being assembled only. ETL will not start automatically; start each job explicitly.",
        "max_etl_retries": 0,
    }


@router.post("/retry/{job_id}")
async def retry_failed_job(job_id: str, request: RetryJobRequest | None = None):
    """Manually retry one interrupted/failed ETL job from durable metadata."""
    job_id = job_id.strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="Job ID is required.")

    metadata = await _job_status(job_id)
    batch_job = await _find_job_in_batches(job_id)
    manifest_file = await _find_job_manifest(job_id)
    if not metadata and not batch_job and not manifest_file:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    # Legacy jobs may have lost both job.json filename and batch metadata.
    # The assembled-job manifest is durable in Supabase Storage and contains
    # the original filename, so use it as the final recovery source.
    metadata = dict(metadata)
    for source in (batch_job or {}, manifest_file or {}):
        for key in ("filename", "original_filename", "upload_id", "total_chunks", "content_type"):
            if not metadata.get(key) and source.get(key):
                metadata[key] = source[key]

    status = str(metadata.get("status") or (batch_job or {}).get("status") or "").upper()
    if status == "FINISHED":
        return {"success": True, "job_id": job_id, "status": "ALREADY_FINISHED"}

    filename = str(metadata.get("filename") or metadata.get("original_filename") or "").strip()
    if not filename:
        raise HTTPException(status_code=500, detail=f"Durable metadata has no filename for job: {job_id}")

    content_type = metadata.get("content_type")
    job_folder = RAW_UPLOAD / job_id

    try:
        await UploadService.recover_assembled_job(
            job_id=job_id,
            filename=filename,
            content_type=content_type,
        )
    except Exception as exc:
        logger.exception("Could not recover assembled job | job=%s", job_id)
        raise HTTPException(status_code=500, detail=f"Could not recover durable source for {job_id}: {exc}") from exc

    manifest_path = job_folder / "manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=500, detail=f"Manifest recovery failed: {job_id}")

    if request is None or request.reset_retry_count:
        try:
            metadata["retry_count"] = 0
            metadata["last_error"] = None
            metadata["status"] = "ETL_QUEUED"
            await UploadService._s3_put_json(
                UploadService._job_metadata_s3_key(job_id),
                metadata,
            )
        except Exception:
            logger.exception("Could not reset retry metadata | job=%s", job_id)

    async def _manual_retry() -> None:
        try:
            await asyncio.to_thread(_run_etl, job_folder)
        except Exception:
            logger.exception("MANUAL ETL RETRY FAILED | job=%s", job_id)

    _schedule_batch(_manual_retry())
    return {
        "success": True,
        "job_id": job_id,
        "status": "ETL_RETRY_QUEUED",
        "message": "ETL retry queued from durable source.",
    }


@router.post("/retry-failed/{batch_id}")
async def retry_failed_batch(batch_id: str):
    """Retry only FAILED jobs in a batch; FINISHED jobs are untouched."""
    batch_id = batch_id.strip()
    batch = await _get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail=f"Batch not found: {batch_id}")

    jobs = batch.get("jobs")
    if not isinstance(jobs, list):
        raise HTTPException(status_code=500, detail="Batch jobs metadata is invalid.")

    failed = [job for job in jobs if str(job.get("status", "")).upper() in {"FAILED", "ERROR"}]
    if not failed:
        return {"success": True, "batch_id": batch_id, "status": "NO_FAILED_JOBS", "retried": 0}

    batch["status"] = "PROCESSING"
    await _persist_batch(batch_id, batch)
    _schedule_batch(_run_batch(batch_id, failed))
    return {"success": True, "batch_id": batch_id, "status": "ETL_RETRY_QUEUED", "retried": len(failed), "jobs": failed}
