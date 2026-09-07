from __future__ import annotations

import json
import logging
from pathlib import Path

from app.core.constants import RAW_UPLOAD
from app.interface.api.v1 import upload as upload_api

logger = logging.getLogger(__name__)

_ORIGINAL_QUEUE_ETL = upload_api._queue_etl


def _manifest_state(job_folder: Path) -> dict:
    manifest_path = job_folder / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        with manifest_path.open(encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _guarded_queue_etl(job_id: str, job_folder: Path) -> None:
    """Prevent generic /upload/process from hijacking a Drive sync job.

    The Drive sync endpoint owns the Drive download -> ETL lifecycle. A generic
    process request is allowed only after the Drive manifest proves that all
    source files are present. It never starts a hidden Drive recovery task.
    """
    manifest = _manifest_state(job_folder)
    storage = str(manifest.get("storage") or "").strip().lower()
    total = int(manifest.get("total_files") or 0)
    processed = int(manifest.get("processed_files") or 0)

    if storage == "google_drive" and total > 0 and processed < total:
        logger.info(
            "ETL QUEUE BLOCKED: Drive sync still downloading | job=%s | files=%s/%s",
            job_id,
            processed,
            total,
        )
        return

    _ORIGINAL_QUEUE_ETL(job_id, job_folder)


upload_api._queue_etl = _guarded_queue_etl
