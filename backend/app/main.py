"""PLN Analytics Platform — FastAPI application entry point."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from pathlib import Path

# Local development only: load backend/.env into the process environment
# before anything reads os.environ. Settings (app/core/config.py) reads
# straight from os.environ and has no dotenv logic of its own, so without
# this, backend/.env was silently ignored -- every local .env value
# (ADMIN_USERNAME/ADMIN_PASSWORD, AUTH_ENABLED, ports, etc.) had zero
# effect no matter what was written there. Confirmed live 2026-09-04: a
# fresh local clone's login always failed because no admin account was
# ever bootstrapped, since ADMIN_USERNAME/ADMIN_PASSWORD from .env never
# reached the process. In deployment (FastAPI Cloud, GitHub Actions) there
# is no .env file on disk, so this is a no-op there -- real env vars set
# by the host still take priority (load_dotenv default: never overrides
# an already-set os.environ value).
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

os.environ.setdefault("DLPD_STREAM_CHUNK_ROWS", "100")
os.environ.setdefault("ANEV_STREAM_CHUNK_ROWS", "1000")
os.environ.setdefault("PENGECEKAN_STREAM_CHUNK_ROWS", "250")
os.environ.setdefault("DRIVE_DOWNLOAD_CHUNK_MB", "8")
os.environ.setdefault("DRIVE_DOWNLOAD_CONCURRENCY", "1")
os.environ.setdefault("MALLOC_ARENA_MAX", "2")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("JOB_STATE_STORAGE", "s3")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.application.etl.etl_orchestrator import ETLOrchestrator
from app.core.config import get_settings
from app.core.logging_config import configure_logging
from app.database.warehouse import Warehouse
from app.etl.detector.dlpd_month_fallback_patch import install_dlpd_month_fallback_patch
from app.etl.detector.dlpd_transformer_patch import install_dlpd_transformer_patch
from app.etl.detector.streaming_month_resolver_patch import install_streaming_month_resolver_patch
from app.etl.merger.anev_calamine_stream_patch import install as install_anev_calamine_stream_patch
from app.etl.merger.idpel_normalization_patch import install_idpel_normalization_patch
from app.etl.merger.streaming_dlpd_dedup_guard import install_streaming_dlpd_dedup_guard
from app.etl.merger.streaming_dlpd_merger_patch import install_streaming_dlpd_merger_patch
from app.etl.merger.streaming_dlpd_publish_guard import install_streaming_dlpd_publish_guard
from app.etl.merger.streaming_pengecekan_guard import install_streaming_pengecekan_guard
from app.etl.runtime_guard import install_runtime_guards
from app.infrastructure.duckdb.dlpd_query_guard import install_dlpd_query_guard
from app.infrastructure.storage.chunk_cleanup import run_storage_cleanup_loop

install_dlpd_transformer_patch()
install_streaming_month_resolver_patch()
install_idpel_normalization_patch()
install_streaming_dlpd_merger_patch()
install_streaming_dlpd_dedup_guard()
install_streaming_dlpd_publish_guard()
install_anev_calamine_stream_patch()
install_streaming_pengecekan_guard()
install_dlpd_month_fallback_patch()
install_runtime_guards()
install_dlpd_query_guard()

_ETL_ACTIVE_JOBS: set[str] = set()
_ETL_ACTIVE_LOCK = threading.Lock()
_ORIGINAL_ETL_PROCESS = ETLOrchestrator.process.__func__


def _deduplicated_etl_process(cls, job_folder: Path):
    job_id = str(job_folder.name or "").strip()
    if not job_id:
        raise ValueError("ETL job folder does not contain a valid job_id.")
    with _ETL_ACTIVE_LOCK:
        if job_id in _ETL_ACTIVE_JOBS:
            logging.getLogger(__name__).warning("ETL DUPLICATE TRIGGER IGNORED | JOB=%s", job_id)
            return {"success": True, "job_id": job_id, "status": "ALREADY_RUNNING", "message": "ETL job is already running in this instance."}
        _ETL_ACTIVE_JOBS.add(job_id)
    try:
        return _ORIGINAL_ETL_PROCESS(cls, job_folder)
    finally:
        with _ETL_ACTIVE_LOCK:
            _ETL_ACTIVE_JOBS.discard(job_id)

ETLOrchestrator.process = classmethod(_deduplicated_etl_process)

from app.interface.api.v1.router import api_v1_router  # noqa: E402
import app.interface.api.v1.upload_process_guard_patch  # noqa: E402,F401

settings = get_settings()
configure_logging(settings.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(title=settings.APP_NAME, description="Enterprise Analytics Platform for PLN. Modules: Upload Center, Executive Dashboard, Suspect Analytics, and Warehouse Management.", version="1.0.0", docs_url="/docs", redoc_url="/redoc", openapi_url="/openapi.json")
app.add_middleware(CORSMiddleware, allow_origins=settings.CORS_ORIGINS, allow_origin_regex=r"https://([a-zA-Z0-9-]+\.)*vercel\.app$", allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(api_v1_router)

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok", "application": settings.APP_NAME, "environment": settings.ENVIRONMENT}

@app.get("/health/ready", tags=["Health"])
def readiness_check():
    tables: list[str] = []
    warehouse_accessible = False
    try:
        tables = Warehouse.list_tables()
        warehouse_accessible = True
    except Exception:
        logger.exception("Readiness warehouse inspection failed.")
    warehouse_ready = warehouse_accessible
    return {"status": "ready" if warehouse_ready else "degraded", "warehouse_ready": warehouse_ready, "mode": "explicit_etl", "dlpd_stream_chunk_rows": int(os.getenv("DLPD_STREAM_CHUNK_ROWS", "250")), "anev_stream_chunk_rows": int(os.getenv("ANEV_STREAM_CHUNK_ROWS", "1000")), "tables": tables}

def _background_warehouse_warmup() -> None:
    """Best-effort warm-up: pre-build any missing warehouse views off the
    request path, in a background thread, right after boot.

    Not a return to the old blocking-startup rebuild (that's what caused
    the OOM-crash-restart loop -- see the comment this replaced). The
    difference is this thread never blocks `on_startup()`, so the app
    still reaches "Application startup complete" and serves health
    checks/login immediately either way.

    Without this, a freshly booted replica's *first* real dashboard
    request is the one that pays for hydration + view rebuild (confirmed
    live 2026-09-01: users see an empty/"Network Error" dashboard for the
    ~1-3 minutes that request is blocked on it, since the frontend's own
    request timeout is shorter than a cold rebuild). Firing the same
    self-heal here means that cost is usually already paid by the time a
    real user's request arrives, instead of being paid by that request.
    If it isn't -- e.g. a request lands mid-warm-up anyway -- nothing
    breaks: get_connection() takes the same self-heal lock either way and
    simply waits for whichever thread is already rebuilding.
    """
    try:
        from app.infrastructure.duckdb.connection import get_connection

        get_connection()
        logger.info("Background warehouse warm-up completed.")
    except Exception:
        logger.exception(
            "Background warehouse warm-up failed; will retry lazily on first request."
        )


@app.on_event("startup")
async def on_startup():
    logger.info("=" * 80)
    logger.info("%s", settings.APP_NAME)
    logger.info("Environment : %s", settings.ENVIRONMENT)
    logger.info("Processed Data : %s", settings.DATA_PROCESSED_DIR)
    logger.info("DLPD chunk rows : %s", os.getenv("DLPD_STREAM_CHUNK_ROWS", "250"))
    logger.info("ANEV chunk rows : %s", os.getenv("ANEV_STREAM_CHUNK_ROWS", "1000"))
    logger.info("PENGECEKAN chunk rows : %s", os.getenv("PENGECEKAN_STREAM_CHUNK_ROWS", "250"))
    logger.info("Drive download concurrency : %s", os.getenv("DRIVE_DOWNLOAD_CONCURRENCY", "1"))
    logger.info("Job state storage : %s", os.getenv("JOB_STATE_STORAGE", "s3"))
    logger.info("Startup mode: explicit ETL. No storage hydration or automatic job recovery.")
    # Do NOT eagerly hydrate + rebuild all 5 warehouse views INLINE here
    # (i.e. do not `await` it / block on it). Confirmed live 2026-09-01:
    # this used to call Warehouse.refresh_tables() (full, unconditional
    # rebuild of every view) unconditionally and synchronously on every
    # boot, even when the hydrated warehouse.duckdb already had current
    # views from a prior successful ETL run -- and the host got stuck in a
    # continuous OOM-crash-restart loop as a result, because every restart
    # repeated that same maximal-cost rebuild from scratch with no way to
    # ever get past it, and the process never reached "startup complete".
    # A background *thread* (not blocking this coroutine) is safe: the app
    # still finishes startup immediately either way, and this just gets a
    # head start on the same lazy self-heal every read repository already
    # does via get_connection()/ensure_ready() -- see
    # _background_warehouse_warmup()'s docstring.
    threading.Thread(
        target=_background_warehouse_warmup,
        name="warehouse-warmup",
        daemon=True,
    ).start()

    # Terminal-job S3 storage cleanup (upload chunks under chunks/<upload_id>/
    # and raw Google Drive workbook caches under jobs/<job_id>/raw/) has
    # existed in app/infrastructure/storage/chunk_cleanup.py since 2026-08-24
    # but this loop was never actually started anywhere in the app -- neither
    # prefix has ever been cleaned up in production. Root-caused 2026-09-03:
    # this is the primary driver of the Supabase Storage free-tier overage
    # (10.753GB against a 1GB quota, "Services restricted"). Fire-and-forget
    # background task, same pattern as the warm-up thread above: it must
    # never block "Application startup complete", and a failed iteration
    # (see run_storage_cleanup_loop's own per-phase try/except) never raises
    # out to here. The task reference is kept on the app object so it is not
    # garbage-collected mid-run (asyncio only holds a weak reference).
    app.state.storage_cleanup_task = asyncio.create_task(run_storage_cleanup_loop())

    logger.info("Application startup complete.")
    logger.info("=" * 80)
