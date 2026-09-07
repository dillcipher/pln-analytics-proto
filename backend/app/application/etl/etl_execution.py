from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

# Install the large-workbook pandas reader before importing the ETL modules.
# MonthlyMerger and MonthResolver both use pandas.read_excel.
from app.core.excel_memory import install_large_excel_reader

install_large_excel_reader()

from app.application.etl.etl_orchestrator import ETLOrchestrator
from app.application.etl.durable_etl_checkpoint import install_durable_checkpoint_patch
from app.infrastructure.storage.processed_storage import mark_process_as_etl_writer

logger = logging.getLogger(__name__)

# ETL is deliberately serialized. The orchestrator writes shared warehouse
# artifacts and can consume a large amount of RAM/CPU, so starting several
# workbook merges at once causes the 25% MERGING jobs seen in production.
_ETL_LOCK = threading.Lock()
_PATCH_MARKER = "_pln_serialized_etl_patch"


def _install_process_lock() -> None:
    """Wrap the orchestrator once so every caller in this API worker is queued."""
    if getattr(ETLOrchestrator.process, _PATCH_MARKER, False):
        return

    original = ETLOrchestrator.process

    def serialized_process(cls, job_folder: Path) -> dict[str, Any]:
        job_folder = Path(job_folder)
        # Mark this process as the ETL writer BEFORE doing any work, not
        # after -- see ensure_self_heal_hydrated()'s docstring. This process
        # is about to build authoritative local parquet/warehouse state; a
        # concurrent read request on this same process must never have that
        # in-progress (not yet persisted to S3) state clobbered by a forced
        # self-heal hydration pulling an older S3 copy back over it.
        mark_process_as_etl_writer()
        logger.info("ETL QUEUED | job_folder=%s", job_folder)
        with _ETL_LOCK:
            logger.info("ETL LOCK ACQUIRED | job_folder=%s", job_folder)
            try:
                return original(job_folder)
            finally:
                logger.info("ETL LOCK RELEASED | job_folder=%s", job_folder)

    setattr(serialized_process, _PATCH_MARKER, True)
    ETLOrchestrator.process = classmethod(serialized_process)


_install_process_lock()

# This was already fully implemented (durable checkpoint save/load, with
# completed parquet outputs uploaded before their checkpoint entry is
# advertised as durable -- see durable_etl_checkpoint.py's docstrings) but
# was never actually wired up anywhere: nothing in the codebase called
# install_durable_checkpoint_patch(). Confirmed live: etl_checkpoint.json
# for a finished Drive-sync job's ETL run did not exist at all on a later
# replica, and only CUSTOMER_LOCATION parquet (produced during Phase 1) had
# ever been durably persisted -- Phase 2's per-dataset merges (ANEV, DLPD,
# PENGECEKAN) have no durable checkpoint to resume from across a replica
# change, so a job that shows FINISHED can still be missing real dataset
# output. Installed here (module import time), same pattern as
# _install_process_lock() above, so every ETL run through this single entry
# point gets a durable, cross-replica-resumable checkpoint.
install_durable_checkpoint_patch()


def run_etl_serialized(job_folder: Path) -> dict[str, Any]:
    """Run ETL through the single process-wide serialized executor."""
    from app.etl.merger.monthly_merger import MonthlyMerger

    try:
        return ETLOrchestrator.process(Path(job_folder))
    finally:
        # Drop any DLPD frames MonthlyMerger cached across this job's
        # per-month merge() calls (see _prepare_coordinate_dataset_frame).
        # A GitHub Actions runner process exits right after this anyway,
        # but the API host's background job runner is long-lived, so this
        # keeps a finished job's multi-hundred-MB DLPD frame from sitting
        # in memory indefinitely -- success or failure, always clear.
        MonthlyMerger.clear_coordinate_frame_cache()
