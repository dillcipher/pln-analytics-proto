from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException

from app.core.constants import RAW_UPLOAD
from app.infrastructure.storage.processed_storage import persist_processed_data
from app.services.upload_service import UploadService


router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
)


def _read_json_file(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


async def _get_s3_json(key: str) -> dict | None:
    try:
        if not await UploadService._s3_head(key):
            return None
        data = await UploadService._s3_get_json(key)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def _persist_when_finished(data: dict) -> None:
    status = str(data.get("status", "")).upper()
    if status != "FINISHED":
        return
    try:
        await asyncio.to_thread(persist_processed_data)
    except Exception:
        pass


async def _return_job(data: dict, job_id: str) -> dict:
    response = {**data, "success": True, "job_id": job_id}
    await _persist_when_finished(response)
    return response


@router.get("/{job_id}")
async def get_job(job_id: str):
    """Get current upload/ETL status and persist finished analytics artifacts.

    S3 is the source of truth in production because multiple Cloud instances
    can serve the same request. A local manifest can legitimately be stale
    when another replica is running/recovering the same job, so local state is
    only used as a fallback when durable state is unavailable.
    """
    job_id = job_id.strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="Job ID is required.")

    job_folder = RAW_UPLOAD / job_id
    local_manifest = job_folder / "manifest.json"
    local_job_json = job_folder / "job.json"
    local_chunk_metadata = job_folder / "chunk_upload.json"
    local_mode = str(os.getenv("JOB_STATE_STORAGE", "")).strip().lower() == "local"

    # Explicit/local mode has no durable job recovery. Read only the manifest
    # created by the current instance so stale S3 jobs cannot overwrite status.
    if local_mode:
        for path in (local_manifest, local_job_json, local_chunk_metadata):
            data = _read_json_file(path)
            if data is not None:
                return await _return_job(data, job_id)
        raise HTTPException(status_code=404, detail=f"Job not found in local explicit ETL state: {job_id}")

    # Production job state is durable in S3. Always prefer it over a local
    # copy so the UI cannot display an old UPLOADED/12% snapshot from another
    # replica while the real worker is already processing the ETL.
    try:
        manifest_data = await _get_s3_json(
            UploadService._job_manifest_s3_key(job_id),
        )
        if manifest_data is not None:
            return await _return_job(manifest_data, job_id)
    except Exception:
        pass

    try:
        metadata = await _get_s3_json(
            UploadService._job_metadata_s3_key(job_id),
        )
        if metadata is not None:
            return await _return_job(metadata, job_id)
    except Exception:
        pass

    # Local fallback is retained for development/local-storage deployments.
    local_manifest_data = _read_json_file(local_manifest)
    if local_manifest_data is not None:
        return await _return_job(local_manifest_data, job_id)

    local_job_data = _read_json_file(local_job_json)
    if local_job_data is not None:
        return await _return_job(local_job_data, job_id)

    chunk_data = _read_json_file(local_chunk_metadata)
    if chunk_data is not None:
        return await _return_job(chunk_data, job_id)

    raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
