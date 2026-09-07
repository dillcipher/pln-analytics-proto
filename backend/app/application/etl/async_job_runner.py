from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from app.application.etl.etl_dispatch import dispatch_etl

logger = logging.getLogger(__name__)

_tasks: dict[str, asyncio.Task[Any]] = {}


def _cleanup(job_id: str) -> None:
    _tasks.pop(job_id, None)


async def run_etl_background(job_id: str, job_folder: Path) -> None:
    """Run ETL outside the HTTP request lifecycle, serializing workers.

    Goes through dispatch_etl() (see etl_dispatch.py), which offloads to
    GitHub Actions when configured and eligible, falling back to the same
    in-process run this function always did otherwise.
    """
    try:
        result = await dispatch_etl(job_id, job_folder)
        if not isinstance(result, dict) or result.get("success") is not True:
            logger.error("Background ETL failed | job=%s | result=%r", job_id, result)
        else:
            logger.info("Background ETL completed | job=%s", job_id)
    except asyncio.CancelledError:
        logger.warning("Background ETL task cancelled | job=%s", job_id)
        raise
    except Exception:
        logger.exception("Background ETL crashed | job=%s", job_id)
    finally:
        _cleanup(job_id)


def start_etl_background(job_id: str, job_folder: Path) -> bool:
    """Start at most one in-process ETL task per job."""
    existing = _tasks.get(job_id)
    if existing and not existing.done():
        return False

    task = asyncio.create_task(run_etl_background(job_id, job_folder))
    _tasks[job_id] = task
    return True


def is_etl_running(job_id: str) -> bool:
    task = _tasks.get(job_id)
    return bool(task and not task.done())
