from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

from app.core.constants import RAW_UPLOAD

logger = logging.getLogger(__name__)

RUNTIME_GUARD_VERSION = "2026-08-24-drive-oom-v1"
S3_UPLOAD_RETRIES = max(1, int(os.getenv("S3_UPLOAD_RETRIES", "3")))
S3_MULTIPART_PART_SIZE = max(5 * 1024 * 1024, int(os.getenv("S3_MULTIPART_PART_SIZE", str(16 * 1024 * 1024))))
S3_MULTIPART_THRESHOLD = max(8 * 1024 * 1024, int(os.getenv("S3_MULTIPART_THRESHOLD", str(8 * 1024 * 1024))))


def _s3_configured() -> bool:
    return all(os.getenv(name, "").strip() for name in ("S3_ENDPOINT", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"))


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "").strip(),
        region_name=os.getenv("S3_REGION", "ap-southeast-1").strip(),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID", "").strip(),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY", "").strip(),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "adaptive"},
            connect_timeout=30,
            read_timeout=600,
            s3={"addressing_style": "path"},
        ),
    )


def _transfer_config() -> TransferConfig:
    return TransferConfig(
        multipart_threshold=S3_MULTIPART_THRESHOLD,
        multipart_chunksize=S3_MULTIPART_PART_SIZE,
        max_concurrency=1,
        use_threads=False,
    )


async def _put_job_state(upload_service, job_id: str, **updates) -> None:
    """Best-effort durable state for legacy/Supabase-backed jobs."""
    try:
        key = upload_service._job_metadata_s3_key(job_id)
        try:
            current = await upload_service._s3_get_json(key)
        except Exception:
            current = {"job_id": job_id}
        if not isinstance(current, dict):
            current = {"job_id": job_id}
        current.update(updates)
        current["updated_at"] = datetime.now(timezone.utc).isoformat()
        await upload_service._s3_put_json(key, current)
    except Exception:
        logger.exception("DURABLE JOB STATE UPDATE FAILED | JOB=%s | UPDATES=%s", job_id, updates)


def _put_job_state_from_thread(upload_service, job_id: str, **updates) -> None:
    try:
        asyncio.run(_put_job_state(upload_service, job_id, **updates))
    except Exception:
        logger.exception("DURABLE THREAD JOB STATE UPDATE FAILED | JOB=%s", job_id)


def _is_drive_job(job_folder: Path) -> bool:
    """Direct Drive jobs must not make any Supabase state calls."""
    try:
        manifest = Path(job_folder) / "manifest.json"
        if not manifest.exists():
            return False
        with manifest.open(encoding="utf-8") as source:
            data = json.load(source)
        return str(data.get("storage", "")).strip().lower() == "google_drive"
    except Exception:
        return False


def install_runtime_guards() -> None:
    """Install durable legacy storage guards without coupling Drive ETL to S3."""
    from app.application.etl.etl_orchestrator import ETLOrchestrator
    from app.services.upload_service import UploadService, S3_BUCKET

    UploadService.UPLOAD_FOLDER = RAW_UPLOAD

    if not getattr(UploadService, "_pln_stable_s3_upload", False):
        async def _stable_s3_put_file(cls, local_path: Path, s3_key: str) -> None:
            if not _s3_configured():
                raise RuntimeError("Durable storage is not configured: S3_ENDPOINT, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required.")
            size = local_path.stat().st_size
            if size <= 0:
                raise ValueError(f"Cannot upload empty file: {local_path.name}")
            last_error: Exception | None = None
            for attempt in range(1, S3_UPLOAD_RETRIES + 1):
                try:
                    client = _s3_client()
                    logger.info(
                        "STABLE S3 TRANSFER START | VERSION=%s | FILE=%s | BYTES=%s | ATTEMPT=%s/%s",
                        RUNTIME_GUARD_VERSION, local_path.name, size, attempt, S3_UPLOAD_RETRIES,
                    )
                    await asyncio.to_thread(
                        client.upload_file,
                        str(local_path),
                        S3_BUCKET,
                        s3_key,
                        ExtraArgs={"ContentType": "application/octet-stream"},
                        Config=_transfer_config(),
                    )
                    head = await asyncio.to_thread(client.head_object, Bucket=S3_BUCKET, Key=s3_key)
                    stored_size = int(head.get("ContentLength", -1))
                    if stored_size != size:
                        raise IOError(f"Durable storage size mismatch: expected {size}, got {stored_size}")
                    logger.info(
                        "STABLE S3 TRANSFER OK | VERSION=%s | FILE=%s | BYTES=%s | KEY=%s | ATTEMPT=%s",
                        RUNTIME_GUARD_VERSION, local_path.name, size, s3_key, attempt,
                    )
                    return
                except Exception as exc:
                    last_error = exc
                    logger.exception(
                        "STABLE S3 TRANSFER RETRY | VERSION=%s | FILE=%s | ATTEMPT=%s/%s | ERROR=%r",
                        RUNTIME_GUARD_VERSION, local_path.name, attempt, S3_UPLOAD_RETRIES, exc,
                    )
                    if attempt < S3_UPLOAD_RETRIES:
                        await asyncio.sleep(min(2 * attempt, 8))
            raise last_error or RuntimeError("Durable S3 upload failed")
        UploadService._s3_put_file = classmethod(_stable_s3_put_file)
        UploadService._pln_stable_s3_upload = True

    if not getattr(UploadService, "_pln_stable_s3_download", False):
        async def _stable_s3_download_file(cls, s3_key: str, local_path: Path) -> None:
            if not _s3_configured():
                raise RuntimeError("Durable storage is not configured.")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            def _download() -> None:
                client = _s3_client()
                response = client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                body = response["Body"]
                temporary = local_path.with_suffix(local_path.suffix + ".download")
                try:
                    with temporary.open("wb") as target:
                        while True:
                            chunk = body.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            target.write(chunk)
                    os.replace(temporary, local_path)
                finally:
                    body.close()
                    temporary.unlink(missing_ok=True)
            await asyncio.to_thread(_download)
        UploadService._s3_download_file = classmethod(_stable_s3_download_file)
        UploadService._pln_stable_s3_download = True

    if not getattr(UploadService, "_pln_durable_assembly_state", False):
        original_assemble = UploadService.assemble_chunk_upload.__func__
        async def _durable_assemble(cls, *args, **kwargs):
            job_id = str(kwargs.get("job_id") or (args[1] if len(args) > 1 else ""))
            try:
                result = await original_assemble(cls, *args, **kwargs)
                final_key = result.get("s3_key") if isinstance(result, dict) else None
                await _put_job_state(
                    UploadService,
                    job_id,
                    status="ASSEMBLY_COMPLETED",
                    assembly_completed_at=datetime.now(timezone.utc).isoformat(),
                    final_s3_key=final_key,
                    last_error=None,
                )
                return result
            except Exception as exc:
                await _put_job_state(
                    UploadService,
                    job_id,
                    status="ASSEMBLY_FAILED",
                    last_error=str(exc),
                    failed_at=datetime.now(timezone.utc).isoformat(),
                )
                raise
        UploadService.assemble_chunk_upload = classmethod(_durable_assemble)
        UploadService._pln_durable_assembly_state = True

    if not getattr(ETLOrchestrator, "_pln_durable_etl_state", False):
        original_process = ETLOrchestrator.process.__func__
        def _durable_process(cls, job_folder: Path):
            job_id = str(job_folder.name or "").strip()
            drive_job = _is_drive_job(job_folder)
            if not drive_job and job_id:
                _put_job_state_from_thread(
                    UploadService,
                    job_id,
                    status="ETL_PROCESSING",
                    etl_started_at=datetime.now(timezone.utc).isoformat(),
                )
            result = original_process(cls, job_folder)
            if not drive_job and job_id:
                success = bool((result or {}).get("success"))
                status = str((result or {}).get("status") or "").upper()
                _put_job_state_from_thread(
                    UploadService,
                    job_id,
                    status="FINISHED" if success and status == "FINISHED" else "FAILED",
                    etl_finished_at=datetime.now(timezone.utc).isoformat(),
                    last_error=None if success else (result or {}).get("error"),
                )
            return result
        ETLOrchestrator.process = classmethod(_durable_process)
        ETLOrchestrator._pln_durable_etl_state = True

    # This call happens after the existing DLPD/ANEV memory guards in main.py.
    # It therefore becomes the final CUSTOMER_LOCATION merge implementation
    # instead of being overwritten by an earlier monkey patch.
    from app.etl.merger.streaming_customer_location_patch import (
        install_streaming_customer_location_patch,
    )
    install_streaming_customer_location_patch()

    logger.info(
        "DURABLE PIPELINE GUARD INSTALLED | VERSION=%s | MULTIPART_THRESHOLD=%s | PART_SIZE=%s | CONCURRENCY=1 | DRIVE_STATE=LOCAL",
        RUNTIME_GUARD_VERSION, S3_MULTIPART_THRESHOLD, S3_MULTIPART_PART_SIZE,
    )
