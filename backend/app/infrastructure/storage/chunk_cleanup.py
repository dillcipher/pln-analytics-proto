from __future__ import annotations

import asyncio
import json
import logging
import os

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "").strip()
S3_REGION = os.getenv("S3_REGION", "ap-southeast-1").strip()
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "pln-analytics-uploads").strip()
S3_JOB_PREFIX = "jobs"
S3_CHUNK_PREFIX = "chunks"

TERMINAL_STATUSES = {"FINISHED", "COMPLETED", "CANCELLED"}
DEFAULT_INTERVAL_SECONDS = 300


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
            read_timeout=60,
            s3={"addressing_style": "path"},
        ),
    )


def _delete_prefix(client, prefix: str) -> int:
    """Delete every object below a prefix.

    Supabase Storage's S3-compatible endpoint can reject the multi-object
    DeleteObjects API even though individual DeleteObject requests work.
    Delete one object at a time so cleanup is reliable on both S3 and
    Supabase-compatible storage.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if key:
                keys.append(key)

    deleted = 0
    for key in keys:
        try:
            client.delete_object(Bucket=S3_BUCKET, Key=key)
            deleted += 1
        except Exception:
            logger.exception("STORAGE CLEANUP DELETE FAILED | KEY=%s", key)

    return deleted


def cleanup_finished_upload_chunks() -> dict[str, int]:
    """Remove durable upload chunks only after their job is terminal."""
    client = _client()
    if client is None:
        return {"jobs_scanned": 0, "chunks_deleted": 0, "prefixes_deleted": 0}

    jobs_scanned = 0
    chunks_deleted = 0
    prefixes_deleted = 0
    seen_upload_ids: set[str] = set()

    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_JOB_PREFIX}/"):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith("/manifest.json"):
                continue

            jobs_scanned += 1
            try:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                body = response["Body"]
                try:
                    data = json.loads(body.read().decode("utf-8"))
                finally:
                    body.close()
            except Exception:
                logger.exception("STORAGE CLEANUP MANIFEST READ FAILED | KEY=%s", key)
                continue

            if not isinstance(data, dict):
                continue

            status = str(data.get("status") or "").strip().upper()
            if status not in TERMINAL_STATUSES:
                continue

            storage = str(data.get("storage") or "").strip().lower()
            upload_id = str(data.get("upload_id") or "").strip()

            for file_record in data.get("files") or []:
                if not isinstance(file_record, dict):
                    continue
                if not upload_id:
                    upload_id = str(file_record.get("upload_id") or "").strip()
                if upload_id:
                    break

            if not upload_id:
                continue
            if storage not in {"supabase_chunks", "supabase", ""}:
                continue
            if upload_id in seen_upload_ids:
                continue

            seen_upload_ids.add(upload_id)
            prefix = f"{S3_CHUNK_PREFIX}/{upload_id}/"
            try:
                deleted = _delete_prefix(client, prefix)
                if deleted:
                    prefixes_deleted += 1
                    chunks_deleted += deleted
                    logger.info(
                        "STORAGE CLEANUP | JOB=%s | STATUS=%s | UPLOAD_ID=%s | OBJECTS=%s",
                        data.get("job_id"),
                        status,
                        upload_id,
                        deleted,
                    )
            except Exception:
                logger.exception(
                    "STORAGE CLEANUP PREFIX FAILED | JOB=%s | UPLOAD_ID=%s",
                    data.get("job_id"),
                    upload_id,
                )

    return {
        "jobs_scanned": jobs_scanned,
        "chunks_deleted": chunks_deleted,
        "prefixes_deleted": prefixes_deleted,
    }


def cleanup_finished_raw_drive_cache() -> dict[str, int]:
    """Remove durable raw Google Drive workbook caches once their job is terminal.

    ``_persist_raw_drive_file`` (app/interface/api/v1/drive.py) durably
    caches the FULL source Excel workbook to
    ``jobs/<job_id>/raw/<file_id>_<filename>`` (plus ``.chunkNNNNN`` /
    ``.manifest.json`` siblings for large files) so a restarted replica can
    resume ETL without re-downloading from Drive mid-run. Root-caused
    2026-09-03: nothing ever deleted this prefix, and it was the primary
    driver of this project's Supabase Storage free-tier overage
    (10.753GB against a 1GB quota -- "Services restricted") -- source
    workbooks run 100-140MB each, multiple per month, since ~Feb 2026.

    Once a Drive job reaches a status in TERMINAL_STATUSES (the same set
    already used by cleanup_finished_upload_chunks above), that raw cache
    is no longer a recovery dependency: the workbook is always
    re-downloadable from Drive, and the durable outputs the live app
    actually depends on (processed/ parquet + warehouse.duckdb) do not
    read from jobs/<job_id>/raw/ at all. FAILED is deliberately excluded
    from TERMINAL_STATUSES (see JobManager.list_recoverable_drive_jobs'
    "FAILED Drive jobs are resumable" comment) -- a failed Drive job may
    still be retried straight from its raw cache without hitting Drive
    again, so this function must never delete a FAILED job's cache.

    Only ``jobs/<job_id>/raw/`` is ever touched. This never deletes
    ``jobs/<job_id>/manifest.json``, ``job.json``, ``etl_checkpoint.json``,
    ``recovery.lock``, or anything under ``processed/`` -- the prefix is
    scoped to the trailing ``/raw/`` segment only.
    """
    client = _client()
    if client is None:
        return {"jobs_scanned": 0, "raw_objects_deleted": 0, "jobs_cleaned": 0}

    jobs_scanned = 0
    raw_objects_deleted = 0
    jobs_cleaned = 0
    seen_job_ids: set[str] = set()

    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_JOB_PREFIX}/"):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith("/manifest.json"):
                continue

            jobs_scanned += 1
            try:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                body = response["Body"]
                try:
                    data = json.loads(body.read().decode("utf-8"))
                finally:
                    body.close()
            except Exception:
                logger.exception("RAW DRIVE CACHE CLEANUP MANIFEST READ FAILED | KEY=%s", key)
                continue

            if not isinstance(data, dict):
                continue

            # Only Google Drive jobs ever write jobs/<job_id>/raw/ -- see
            # _drive_raw_key in app/interface/api/v1/drive.py. Skipping
            # other storage kinds here avoids an always-empty list call
            # against their (nonexistent) raw/ prefix.
            storage = str(data.get("storage") or "").strip().lower()
            if storage != "google_drive":
                continue

            status = str(data.get("status") or "").strip().upper()
            if status not in TERMINAL_STATUSES:
                continue

            job_id = str(data.get("job_id") or "").strip()
            if not job_id:
                continue
            if job_id in seen_job_ids:
                continue
            seen_job_ids.add(job_id)

            prefix = f"{S3_JOB_PREFIX}/{job_id}/raw/"
            try:
                deleted = _delete_prefix(client, prefix)
                if deleted:
                    jobs_cleaned += 1
                    raw_objects_deleted += deleted
                    logger.info(
                        "RAW DRIVE CACHE CLEANED UP | JOB=%s | STATUS=%s | objects_deleted=%s",
                        job_id,
                        status,
                        deleted,
                    )
            except Exception:
                logger.exception(
                    "RAW DRIVE CACHE CLEANUP PREFIX FAILED | JOB=%s",
                    job_id,
                )

    return {
        "jobs_scanned": jobs_scanned,
        "raw_objects_deleted": raw_objects_deleted,
        "jobs_cleaned": jobs_cleaned,
    }


async def run_storage_cleanup_loop() -> None:
    """Run terminal-job cleanup (upload chunks + raw Drive cache) once, then
    periodically in the background."""
    interval = max(
        60,
        int(os.getenv("STORAGE_CLEANUP_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS))),
    )

    while True:
        try:
            result = await asyncio.to_thread(cleanup_finished_upload_chunks)
            if result["chunks_deleted"]:
                logger.warning("STORAGE CLEANUP COMPLETED | %s", result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("STORAGE CLEANUP ITERATION FAILED")

        # Independent try/except from the chunk cleanup above: a failure in
        # one must never suppress or block the other -- this is best-effort
        # background housekeeping, not a critical path.
        try:
            raw_result = await asyncio.to_thread(cleanup_finished_raw_drive_cache)
            if raw_result["raw_objects_deleted"]:
                logger.warning("RAW DRIVE CACHE CLEANUP COMPLETED | %s", raw_result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("RAW DRIVE CACHE CLEANUP ITERATION FAILED")

        await asyncio.sleep(interval)
