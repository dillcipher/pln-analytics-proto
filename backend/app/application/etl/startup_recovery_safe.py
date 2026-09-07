from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any

import boto3
from botocore.client import Config

from app.application.etl.etl_execution import run_etl_serialized
from app.core.constants import RAW_UPLOAD
from app.services.upload_service import UploadService

logger = logging.getLogger(__name__)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "").strip()
S3_REGION = os.getenv("S3_REGION", "ap-southeast-1").strip()
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "pln-analytics-uploads").strip()
RECOVERABLE_STATUSES = {
    "UPLOADED",
    "DETECTING",
    "VALIDATING",
    "MERGING",
    "TRANSFORMING",
    "EXPORTING",
    "ASSEMBLY_QUEUED",
    "ASSEMBLY_COMPLETED",
}
MAX_FAILED_RECOVERY_ATTEMPTS = max(1, int(os.getenv("MAX_FAILED_RECOVERY_ATTEMPTS", "5")))
# A job stuck at UPLOADED never even reached the ETL pipeline (Drive download /
# chunk assembly finished, but processing never started). Repeated failures at
# this exact stage almost always mean a structurally bad upload (corrupt file,
# unrecognized schema) rather than a transient issue, so it gets a tighter
# retry budget than jobs that made it further into the pipeline.
MAX_UPLOADED_RECOVERY_ATTEMPTS = max(1, int(os.getenv("MAX_UPLOADED_RECOVERY_ATTEMPTS", "3")))
RECOVERY_POLICY_VERSION = "2026-08-26-v8"
_LOCK = asyncio.Lock()


def _client():
    if not S3_ENDPOINT or not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
        return None
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "adaptive"},
            connect_timeout=30,
            read_timeout=600,
            s3={"addressing_style": "path"},
        ),
    )


def _load_jobs() -> list[dict[str, Any]]:
    client = _client()
    if client is None:
        return []
    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="jobs/"):
            for item in page.get("Contents", []):
                key = str(item.get("Key") or "")
                if not key.endswith("/job.json"):
                    continue
                parts = key.split("/")
                if len(parts) != 3 or parts[1] in seen:
                    continue
                job_id = parts[1]
                seen.add(job_id)
                try:
                    response = client.get_object(Bucket=S3_BUCKET, Key=key)
                    body = response["Body"]
                    try:
                        metadata = json.loads(body.read().decode("utf-8"))
                    finally:
                        body.close()
                except Exception:
                    logger.exception("STARTUP RECOVERY: failed reading %s", key)
                    continue
                if not isinstance(metadata, dict):
                    continue
                if str(metadata.get("status", "")).upper() not in RECOVERABLE_STATUSES:
                    continue
                metadata["job_id"] = metadata.get("job_id") or job_id
                jobs.append(metadata)
    except Exception:
        logger.exception("STARTUP RECOVERY: failed listing durable jobs")
        return []
    jobs.sort(key=lambda x: str(x.get("uploaded_at") or x.get("created_at") or ""))
    return jobs


async def _persist(metadata: dict[str, Any]) -> None:
    job_id = str(metadata.get("job_id") or "").strip()
    if job_id:
        await UploadService._s3_put_json(UploadService._job_metadata_s3_key(job_id), metadata)


def _manifest(job_id: str) -> dict[str, Any]:
    path = RAW_UPLOAD / job_id / "manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _manifest_dataset(job_id: str) -> str | None:
    value = _manifest(job_id)
    files = value.get("files") or []
    if files and isinstance(files[0], dict):
        dataset = files[0].get("dataset")
        return str(dataset).strip().upper() if dataset else None
    return None


def _bind_manifest_to_job(job_id: str) -> None:
    path = RAW_UPLOAD / job_id / "manifest.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for record in manifest.get("files") or []:
            if isinstance(record, dict) and record.get("job_id") != job_id:
                record["job_id"] = job_id
                changed = True
        if changed:
            path.write_text(json.dumps(manifest, indent=4, ensure_ascii=False, default=str), encoding="utf-8")
    except Exception:
        logger.exception("STARTUP RECOVERY: could not bind manifest to job | job=%s", job_id)


def _is_drive_job(metadata: dict[str, Any], job_id: str) -> bool:
    storage = str(metadata.get("storage") or "").strip().lower()
    return bool(
        storage == "google_drive"
        or metadata.get("drive_folder_id")
        or metadata.get("google_drive_folder_id")
        or "_DRIVE_" in job_id.upper()
    )


async def _mark_failed_without_traceback(
    metadata: dict[str, Any],
    job_id: str,
    reason: str,
) -> str:
    metadata.update({
        "status": "FAILED",
        "current_step": "RECOVERY_UNAVAILABLE",
        "last_error": reason,
        "last_failed_at": datetime.now().isoformat(),
        "recovery_policy_version": RECOVERY_POLICY_VERSION,
    })
    try:
        await _persist(metadata)
    except Exception:
        logger.exception("STARTUP RECOVERY: could not persist unavailable state | job=%s", job_id)
    logger.warning("STARTUP RECOVERY SKIPPED | job=%s | reason=%s", job_id, reason)
    return "failed"


async def _recover_drive_job(metadata: dict[str, Any], job_id: str, attempts: int) -> str:
    folder_id = str(
        metadata.get("drive_folder_id")
        or metadata.get("google_drive_folder_id")
        or ""
    ).strip()
    if not folder_id:
        return await _mark_failed_without_traceback(
            metadata,
            job_id,
            "Google Drive job has no drive_folder_id.",
        )

    try:
        from app.interface.api.v1 import drive

        logger.info(
            "STARTUP DRIVE RECOVERY START | job=%s | folder=%s | attempt=%s/%s",
            job_id,
            folder_id,
            attempts,
            MAX_FAILED_RECOVERY_ATTEMPTS,
        )
        await drive._sync_drive_job(
            job_id=job_id,
            folder_id=folder_id,
            recovery_attempts=attempts,
        )

        current = _manifest(job_id)
        if str(current.get("status") or "").upper() == "FINISHED":
            metadata.update({
                "status": "FINISHED",
                "progress": 100,
                "current_step": "FINISHED",
                "finished_at": datetime.now().isoformat(),
                "recovery_completed_at": datetime.now().isoformat(),
                "recovery_policy_version": RECOVERY_POLICY_VERSION,
            })
            metadata.pop("last_error", None)
            await _persist(metadata)
            logger.info("STARTUP DRIVE RECOVERY FINISHED | job=%s", job_id)
            return "recovered"

        reason = str(current.get("current_step") or "Google Drive recovery did not finish.")
        metadata.update({
            "status": "FAILED",
            "current_step": "RECOVERY_FAILED",
            "last_error": reason,
            "last_failed_at": datetime.now().isoformat(),
            "recovery_policy_version": RECOVERY_POLICY_VERSION,
        })
        await _persist(metadata)
        logger.warning("STARTUP DRIVE RECOVERY FAILED | job=%s | reason=%s", job_id, reason)
        return "failed"
    except Exception as exc:
        return await _mark_failed_without_traceback(
            metadata,
            job_id,
            f"Google Drive recovery could not start: {exc}",
        )


async def _recover_one(metadata: dict[str, Any]) -> str:
    job_id = str(metadata.get("job_id") or "").strip()
    if not job_id:
        return "failed"

    status = str(metadata.get("status") or "").upper()
    if status == "FAILED":
        return "failed"

    attempts = int(metadata.get("recovery_attempts") or 0)
    if attempts >= MAX_FAILED_RECOVERY_ATTEMPTS:
        return await _mark_failed_without_traceback(
            metadata,
            job_id,
            f"Recovery attempt limit reached ({MAX_FAILED_RECOVERY_ATTEMPTS}).",
        )

    attempts += 1
    metadata["recovery_attempts"] = attempts
    metadata["recovery_policy_version"] = RECOVERY_POLICY_VERSION
    metadata["last_recovery_at"] = datetime.now().isoformat()
    await _persist(metadata)

    if _is_drive_job(metadata, job_id):
        return await _recover_drive_job(metadata, job_id, attempts)

    files = metadata.get("files")
    file_meta = files[0] if isinstance(files, list) and files and isinstance(files[0], dict) else metadata
    filename = str(file_meta.get("filename") or file_meta.get("original_filename") or "").strip()
    if not filename:
        return await _mark_failed_without_traceback(
            metadata,
            job_id,
            "Chunk-upload job has no filename metadata.",
        )

    job_folder = RAW_UPLOAD / job_id
    try:
        content_type = file_meta.get("content_type")
        upload_id = str(file_meta.get("upload_id") or metadata.get("upload_id") or "").strip()
        total_chunks_raw = file_meta.get("total_chunks") or metadata.get("total_chunks")
        total_chunks = int(total_chunks_raw or 0)

        existing_manifest = _manifest(job_id)
        destination = job_folder / UploadService._safe_filename(filename)
        if destination.exists() and existing_manifest.get("files"):
            _bind_manifest_to_job(job_id)
            logger.info("STARTUP RECOVERY: local assembly exists; resuming ETL | job=%s", job_id)
        else:
            if not upload_id:
                return await _mark_failed_without_traceback(
                    metadata,
                    job_id,
                    "No durable chunk source remains for this job.",
                )
            if total_chunks <= 0:
                total_chunks = await UploadService._s3_chunk_count(upload_id)
            if total_chunks <= 0:
                return await _mark_failed_without_traceback(
                    metadata,
                    job_id,
                    "No durable chunk source remains for this job.",
                )

            result = await UploadService.recover_assembled_job(
                job_id=job_id,
                filename=filename,
                content_type=content_type,
            )
            if not isinstance(result, dict) or not result.get("success", True):
                raise RuntimeError(f"Durable recovery returned unsuccessful result: {result}")

        dataset = _manifest_dataset(job_id)
        if not dataset or dataset == "UNKNOWN":
            raise RuntimeError(
                "Unable to detect dataset for uploaded file(s): "
                f"{filename}. Use a supported PLN dataset or upload a valid Excel workbook."
            )

        etl_result = await asyncio.to_thread(run_etl_serialized, job_folder)
        if not isinstance(etl_result, dict) or not etl_result.get("success"):
            raise RuntimeError(f"Recovered ETL failed: {etl_result}")

        metadata.update({
            "status": "FINISHED",
            "progress": 100,
            "current_step": "FINISHED",
            "finished_at": datetime.now().isoformat(),
            "recovery_completed_at": datetime.now().isoformat(),
            "recovery_policy_version": RECOVERY_POLICY_VERSION,
        })
        metadata.pop("last_error", None)
        await _persist(metadata)
        logger.info("STARTUP RECOVERY FINISHED | job=%s", job_id)
        return "recovered"
    except Exception as exc:
        metadata.update({
            "status": "FAILED",
            "current_step": "RECOVERY_FAILED",
            "last_error": str(exc),
            "last_failed_at": datetime.now().isoformat(),
            "recovery_policy_version": RECOVERY_POLICY_VERSION,
        })
        try:
            await _persist(metadata)
        except Exception:
            logger.exception("STARTUP RECOVERY: failed persisting failure | job=%s", job_id)
        logger.warning("STARTUP RECOVERY FAILED | job=%s | error=%s", job_id, exc)
        return "failed"


async def recover_pending_jobs() -> dict[str, int]:
    async with _LOCK:
        jobs = await asyncio.to_thread(_load_jobs)
        recovered = failed = 0
        for metadata in jobs:
            result = await _recover_one(metadata)
            if result == "recovered":
                recovered += 1
            else:
                failed += 1
        logger.info(
            "STARTUP RECOVERY COMPLETED | found=%s recovered=%s failed=%s",
            len(jobs),
            recovered,
            failed,
        )
        return {"found": len(jobs), "recovered": recovered, "failed": failed}
