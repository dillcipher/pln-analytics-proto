from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from fastapi import APIRouter

from app.core.constants import RAW_UPLOAD
from app.services.upload_service import (
    S3_BUCKET,
    S3_JOB_PREFIX,
    _create_s3_client,
)


router = APIRouter(
    prefix="/history",
    tags=["History"],
)

RAW_FOLDER = RAW_UPLOAD


def _read_json_body(client, key: str) -> dict | None:
    try:
        response = client.get_object(Bucket=S3_BUCKET, Key=key)
        raw = response["Body"].read()
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _history_fingerprint(job: dict) -> str:
    """Return a stable source identity used to hide duplicate job rows."""
    files = job.get("files")
    if isinstance(files, list) and files:
        identities: list[str] = []
        for item in files:
            if not isinstance(item, dict):
                continue
            drive_id = str(item.get("drive_file_id") or "").strip()
            md5 = str(item.get("drive_md5") or "").strip()
            name = str(item.get("original_filename") or item.get("filename") or "").strip().lower()
            size = str(item.get("size") or "").strip()
            dataset = str(item.get("dataset") or "").strip().upper()
            if drive_id:
                identities.append(f"drive:{drive_id}")
            elif md5:
                identities.append(f"md5:{md5}:{size}")
            else:
                identities.append(f"file:{name}:{size}:{dataset}")
        if identities:
            return "bundle:" + "|".join(sorted(identities))

    drive_id = str(job.get("drive_file_id") or "").strip()
    if drive_id:
        return f"drive:{drive_id}"
    md5 = str(job.get("drive_md5") or "").strip()
    if md5:
        return f"md5:{md5}:{job.get('size', '')}"

    name = str(job.get("original_filename") or job.get("filename") or "").strip().lower()
    size = str(job.get("size") or "").strip()
    dataset = str(job.get("dataset") or "").strip().upper()
    storage = str(job.get("storage") or "").strip().lower()
    if name:
        return f"file:{storage}:{name}:{size}:{dataset}"
    return "job:" + str(job.get("job_id") or "")


def _history_rank(job: dict) -> tuple[str, int]:
    """Prefer the newest copy; status only breaks exact timestamp ties."""
    status = str(job.get("status") or "").upper()
    terminal_rank = 2 if status == "FINISHED" else 1 if status not in {"FAILED", "ERROR"} else 0
    timestamp = str(
        job.get("updated_at")
        or job.get("finished_at")
        or job.get("uploaded_at")
        or job.get("created_at")
        or job.get("job_id", "")
    )
    return timestamp, terminal_rank


def _deduplicate_history(jobs: list[dict]) -> list[dict]:
    """Collapse repeated submissions of the same source into one dashboard row."""
    unique: dict[str, dict] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        fingerprint = _history_fingerprint(job)
        existing = unique.get(fingerprint)
        if existing is None or _history_rank(job) > _history_rank(existing):
            unique[fingerprint] = job
    result = list(unique.values())
    result.sort(
        key=lambda item: str(
            item.get("updated_at")
            or item.get("uploaded_at")
            or item.get("created_at")
            or item.get("job_id", "")
        ),
        reverse=True,
    )
    return result


async def _read_local_history() -> list[dict]:
    def _read() -> list[dict]:
        jobs: list[dict] = []
        if not RAW_FOLDER.exists():
            return jobs

        for folder in sorted(RAW_FOLDER.iterdir(), reverse=True):
            data = None
            for candidate in (
                folder / "manifest.json",
                folder / "job.json",
                folder / "chunk_upload.json",
            ):
                if not candidate.exists():
                    continue
                try:
                    with open(candidate, encoding="utf-8") as f:
                        candidate_data = json.load(f)
                    if isinstance(candidate_data, dict):
                        data = candidate_data
                        break
                except (OSError, json.JSONDecodeError):
                    continue

            if data is not None:
                jobs.append(data)

        return jobs

    return await asyncio.to_thread(_read)


async def _read_storage_history() -> list[dict]:
    """Read every durable upload job from object storage."""

    def _read() -> list[dict]:
        client = _create_s3_client()
        jobs_by_id: dict[str, dict] = {}
        manifest_job_ids: set[str] = set()

        paginator = client.get_paginator("list_objects_v2")
        objects: list[str] = []
        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_JOB_PREFIX}/",
        ):
            for obj in page.get("Contents", []):
                key = str(obj.get("Key", ""))
                parts = key.split("/")
                if len(parts) != 3 or parts[0] != S3_JOB_PREFIX:
                    continue
                if parts[2] in {"manifest.json", "job.json"}:
                    objects.append(key)

        objects.sort(key=lambda key: 0 if key.endswith("/manifest.json") else 1)

        for key in objects:
            parts = key.split("/")
            job_id = parts[1]
            filename = parts[2]
            data = _read_json_body(client, key)
            if not isinstance(data, dict):
                continue

            data_job_id = str(data.get("job_id") or job_id).strip()
            if not data_job_id:
                continue

            if filename == "manifest.json":
                jobs_by_id[data_job_id] = {
                    **jobs_by_id.get(data_job_id, {}),
                    **data,
                    "job_id": data_job_id,
                }
                manifest_job_ids.add(data_job_id)
            elif data_job_id not in manifest_job_ids:
                jobs_by_id[data_job_id] = {
                    **jobs_by_id.get(data_job_id, {}),
                    **data,
                    "job_id": data_job_id,
                }

        return list(jobs_by_id.values())

    try:
        return await asyncio.to_thread(_read)
    except Exception:
        return []


@router.get("")
async def get_history():
    """Return a deduplicated durable upload/ETL history."""
    # Explicit ETL mode deliberately ignores legacy durable history. Those
    # records belong to the old auto-resume architecture and must never revive
    # MERGING jobs in the new upload → READY → Start ETL flow.
    local_mode = str(os.getenv("JOB_STATE_STORAGE", "")).strip().lower() == "local"
    storage_jobs = [] if local_mode else await _read_storage_history()
    local_jobs = await _read_local_history()

    merged: dict[str, dict] = {}

    for job in local_jobs:
        job_id = str(job.get("job_id", "")).strip()
        key = job_id or str(job.get("manifest_path", ""))
        if key:
            merged[key] = job

    for job in storage_jobs:
        job_id = str(job.get("job_id", "")).strip()
        key = job_id or str(job.get("manifest_path", ""))
        if key:
            merged[key] = {**merged.get(key, {}), **job}

    return _deduplicate_history(list(merged.values()))
