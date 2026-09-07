from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.application.etl.async_job_runner import start_etl_background
from app.application.etl.etl_dispatch import dispatch_etl
from app.application.etl.etl_execution import run_etl_serialized
from app.application.jobs.job_manager import JOB_STATE_STORAGE, JobManager
from app.application.jobs.job_status import JobStatus
from app.core.constants import RAW_UPLOAD
from app.services.google_drive_service import GOOGLE_DRIVE_FOLDER_ID, GoogleDriveService
from app.services.upload_service import UploadService

router = APIRouter(prefix="/upload", tags=["Upload"])
_RUNNING_TASKS: set[asyncio.Task] = set()
logger = logging.getLogger(__name__)


def _restore_local_job_state_from_durable_cache(job_id: str, job_folder: Path) -> None:
    """Reconstruct job_folder/manifest.json and its source files from durable
    state before ETL runs, when the local copies are missing.

    FastAPI Cloud's Hobby tier scales this worker to zero between requests, so
    anything written to local disk by an earlier /drive/retry call (manifest.json,
    downloaded xlsx files) is not guaranteed to still be there by the time
    /upload/process/{job_id} is called -- confirmed live: both were gone here
    even though the job's durable S3 state (JobManager.load_durable_job_state)
    still listed several files as already durably cached. Without this, a
    missing local manifest.json makes process_existing_job fall through to the
    legacy recovery branch below, which re-downloads everything from Google
    Drive from scratch via a detached fire-and-forget task (_schedule_task) --
    the exact unreliable pattern already fixed for the main Drive-sync path,
    and a waste of the durable raw cache this job already built up.
    """
    if JOB_STATE_STORAGE == "local":
        return
    try:
        durable_state = JobManager.load_durable_job_state(job_id) or {}
    except Exception:
        logger.exception("ETL PRE-HYDRATE: could not load durable state | job=%s", job_id)
        return
    records = [r for r in (durable_state.get("files") or []) if isinstance(r, dict)]
    if not records:
        return

    job_folder.mkdir(parents=True, exist_ok=True)
    manifest_path = job_folder / "manifest.json"
    if not manifest_path.exists():
        try:
            with manifest_path.open("w", encoding="utf-8") as handle:
                json.dump(durable_state, handle)
        except OSError:
            logger.exception("ETL PRE-HYDRATE: could not write manifest.json | job=%s", job_id)
            return

    # Local import: avoids a module-level circular import between upload.py
    # and drive.py (both import from each other's neighbourhood at startup).
    from app.interface.api.v1.drive import _hydrate_raw_drive_file

    for record in records:
        if str(record.get("status") or "").upper() == "FAILED":
            continue
        durable_raw_key = str(record.get("durable_raw_key") or "").strip()
        filename = record.get("filename")
        if not durable_raw_key or not filename:
            continue
        destination = job_folder / str(filename)
        expected_size = int(record.get("size") or 0) if str(record.get("size") or "").isdigit() else 0
        if destination.exists() and (not expected_size or destination.stat().st_size == expected_size):
            continue
        try:
            _hydrate_raw_drive_file(record, destination)
            logger.info("ETL PRE-HYDRATE: restored file from durable cache | job=%s | file=%s", job_id, filename)
        except Exception:
            logger.exception("ETL PRE-HYDRATE: failed to restore file | job=%s | file=%s", job_id, filename)


def _schedule_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _RUNNING_TASKS.add(task)

    def _discard(completed_task: asyncio.Task) -> None:
        _RUNNING_TASKS.discard(completed_task)
        try:
            completed_task.exception()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    task.add_done_callback(_discard)
    return task


def _run_etl(job_folder: Path) -> None:
    try:
        print("BACKGROUND ETL START", job_folder)
        if not job_folder.exists():
            raise FileNotFoundError(f"Job folder not found: {job_folder}")
        manifest = job_folder / "manifest.json"
        if not manifest.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest}")
        run_etl_serialized(job_folder)
        print("BACKGROUND ETL FINISHED", job_folder)
    except Exception:
        print("BACKGROUND ETL FAILED", job_folder)
        traceback.print_exc()


def _queue_etl(job_id: str, job_folder: Path) -> None:
    """Queue ETL without tying execution to the HTTP request coroutine lifetime."""
    if not start_etl_background(job_id, job_folder):
        print("ETL ALREADY RUNNING", job_id)


def _write_manifest(job_folder: Path, manifest: dict) -> None:
    job_folder.mkdir(parents=True, exist_ok=True)
    temporary = job_folder / "manifest.json.tmp"
    manifest_path = job_folder / "manifest.json"
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(manifest, output, indent=2, ensure_ascii=False, default=str)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(manifest_path)


async def _recover_legacy_drive_job(job_id: str, folder_id: str) -> None:
    """Rebuild a Drive-origin job when its old S3 metadata has no filename.

    Legacy Drive jobs keep their authoritative source in Google Drive, not in
    the chunk-upload metadata schema. Recreate the local manifest from Drive,
    then hand the same job ID to the durable ETL runner. This avoids creating a
    second job and does not require the user to upload the 87 files again.
    """
    job_folder = RAW_UPLOAD / job_id
    logger = __import__("logging").getLogger(__name__)
    try:
        items = await asyncio.to_thread(GoogleDriveService.list_xlsx_files, folder_id)
        if not items:
            raise ValueError("No Excel files were found in the configured Google Drive folder.")

        unique_items: list[dict] = []
        seen_ids: set[str] = set()
        for item in items:
            file_id = str(item.get("id") or "").strip()
            if not file_id or file_id in seen_ids:
                continue
            seen_ids.add(file_id)
            unique_items.append(item)

        if not unique_items:
            raise ValueError("No downloadable Excel files were found in the configured Google Drive folder.")

        manifest = {
            "job_id": job_id,
            "status": JobStatus.UPLOADED.value,
            "progress": 0,
            "current_step": "LEGACY DRIVE RECOVERY",
            "uploaded_at": datetime.now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "total_files": len(unique_items),
            "processed_files": 0,
            "storage": "google_drive",
            "drive_folder_id": folder_id,
            "recovery_attempts": 0,
            "download_concurrency": 1,
            "files": [],
        }
        _write_manifest(job_folder, manifest)
        JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=0, step="LEGACY DRIVE RECOVERY")

        total = len(unique_items)
        records: list[dict] = []
        for index, item in enumerate(unique_items, start=1):
            filename = UploadService._safe_filename(
                str(item.get("name") or f"drive_file_{index}.xlsx")
            )
            file_id = str(item["id"])
            destination = job_folder / filename
            if destination.exists():
                destination = job_folder / f"{file_id}_{filename}"

            await asyncio.to_thread(
                GoogleDriveService.download_file,
                file_id,
                destination,
            )
            record = GoogleDriveService.build_file_record(item, destination)
            record["job_id"] = job_id
            records.append(record)
            manifest["files"] = records
            manifest["processed_files"] = index
            manifest["progress"] = min(18, max(1, round(index / total * 18)))
            manifest["current_step"] = f"RECOVERING FROM GOOGLE DRIVE {index}/{total}"
            _write_manifest(job_folder, manifest)
            JobManager.update(
                job_folder,
                status=JobStatus.UPLOADED,
                progress=manifest["progress"],
                step=manifest["current_step"],
            )

        records.sort(key=lambda row: str(row.get("filename") or "").lower())
        manifest["files"] = records
        manifest["processed_files"] = total
        manifest["progress"] = 18
        manifest["current_step"] = f"GOOGLE DRIVE RECOVERY COMPLETE • {total}/{total} FILES"
        _write_manifest(job_folder, manifest)
        JobManager.update(
            job_folder,
            status=JobStatus.DETECTING,
            progress=20,
            step="GOOGLE DRIVE RECOVERY COMPLETE — ETL STARTING",
        )

        _queue_etl(job_id, job_folder)
        logger.info("LEGACY DRIVE JOB RECOVERED AND ETL QUEUED | job=%s | files=%s", job_id, total)
    except Exception as exc:
        logger.exception("LEGACY DRIVE JOB RECOVERY FAILED | job=%s", job_id)
        try:
            if job_folder.exists():
                JobManager.update(
                    job_folder,
                    status=JobStatus.FAILED,
                    progress=0,
                    step=f"LEGACY DRIVE RECOVERY FAILED: {str(exc)[:300]}",
                )
        except Exception:
            logger.exception("FAILED TO PERSIST LEGACY DRIVE RECOVERY ERROR | job=%s", job_id)


async def _cleanup_chunks_after_success(upload_id: str, job_id: str) -> None:
    try:
        metadata = await UploadService._s3_get_json(UploadService._job_metadata_s3_key(job_id))
        if str(metadata.get("status") or "").upper() != "FINISHED":
            print("KEEPING SOURCE CHUNKS: ETL IS NOT FINISHED")
            return
        if await UploadService._s3_delete_prefix(f"chunks/{upload_id}/"):
            print("SOURCE CHUNKS DELETED AFTER ETL FINISHED")
        else:
            print("SOURCE CHUNK CLEANUP INCOMPLETE; ETL REMAINS FINISHED")
    except Exception:
        print("SOURCE CHUNK CLEANUP CHECK FAILED; CHUNKS LEFT INTACT")
        traceback.print_exc()


async def _run_assembly_and_etl(upload_id: str, job_id: str, filename: str, total_chunks: int, content_type: str | None) -> None:
    job_folder = RAW_UPLOAD / job_id
    try:
        print("BACKGROUND ASSEMBLY + ETL START", job_id)
        result = await UploadService.assemble_chunk_upload(upload_id, job_id, filename, total_chunks, content_type)
        print("ASSEMBLY RESULT", result)
        manifest = job_folder / "manifest.json"
        if not manifest.exists():
            raise FileNotFoundError(f"Manifest not found after assembly: {manifest}")
        print("ASSEMBLY FINISHED — ETL NOT STARTED", job_id)
    except Exception:
        print("ASSEMBLY + ETL FAILED", job_id)
        traceback.print_exc()


async def _wait_for_finished(job_id: str, timeout_seconds: int = 86400) -> None:
    """Wait on durable state only; do not execute ETL in the assembly coroutine."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            metadata = await UploadService._s3_get_json(UploadService._job_metadata_s3_key(job_id))
            status = str(metadata.get("status") or "").upper()
            if status in {"FINISHED", "FAILED", "CANCELLED", "COMPLETED"}:
                return
        except Exception:
            pass
        await asyncio.sleep(5)


@router.post("/files")
async def upload_files(files: Annotated[list[UploadFile], File(...)]) -> dict:
    start = time.perf_counter()
    try:
        if not files:
            raise HTTPException(status_code=400, detail="At least one file is required.")
        result = await UploadService.save_files(files)
        print(f"UPLOAD FINISHED — ETL NOT STARTED ({time.perf_counter() - start:.2f}s)")
        return {**result, "status": "READY_FOR_ETL", "message": "Upload complete. Start ETL explicitly when you are ready."}
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/chunk")
async def upload_chunk(
    upload_id: Annotated[str, Form(...)],
    filename: Annotated[str, Form(...)],
    chunk_number: Annotated[int, Form(..., ge=0)],
    total_chunks: Annotated[int, Form(..., gt=0)],
    file: Annotated[UploadFile, File(...)],
) -> dict:
    try:
        return await UploadService.save_chunk(upload_id, filename, chunk_number, total_chunks, file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/complete")
async def complete_upload(
    upload_id: Annotated[str, Form(...)],
    filename: Annotated[str, Form(...)],
    total_chunks: Annotated[int, Form(..., gt=0)],
    content_type: Annotated[str | None, Form()] = None,
) -> dict:
    start = time.perf_counter()
    try:
        result = await UploadService.prepare_chunk_upload(upload_id, filename, total_chunks, content_type)
        _schedule_task(_run_assembly_and_etl(upload_id, result["job_id"], filename, total_chunks, content_type))
        print(f"CHUNKED UPLOAD ACCEPTED — ETL NOT STARTED ({time.perf_counter() - start:.2f}s)", result["job_id"])
        return {**result, "status": "ASSEMBLY_QUEUED", "message": "Assembly is running. Start ETL explicitly after assembly is ready."}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/process/{job_id}")
async def process_existing_job(job_id: str) -> dict:
    job_id = job_id.strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="Job ID is required.")
    job_folder = RAW_UPLOAD / job_id
    manifest = job_folder / "manifest.json"
    manifest_existed_before_restore = manifest.exists()
    if not manifest_existed_before_restore:
        # See _restore_local_job_state_from_durable_cache's docstring: local
        # disk from an earlier Drive-sync call may already be gone by the
        # time this endpoint runs. Try to rebuild it from durable state
        # before falling through to the legacy recovery branch below.
        await asyncio.to_thread(_restore_local_job_state_from_durable_cache, job_id, job_folder)
    elif JOB_STATE_STORAGE != "local":
        # manifest.json already existing locally is NOT proof every source
        # file is also on local disk. Confirmed live: a Drive sync's own
        # "RESTORING DURABLE CACHE" prefetch (see _sync_drive_job in
        # drive.py) is bound by the same per-call time budget as everything
        # else there -- when the last remaining Drive download finishes
        # close to that deadline, the prefetch loop that re-hydrates every
        # already-durable cached file back to local disk can itself run out
        # of time partway through and simply stop (READY FOR ETL only
        # requires every file to have a durable_raw_key in S3, not that it
        # already sits on local disk). Nothing in the ETL pipeline itself
        # hydrates a missing source file on demand, so a gap here used to
        # surface many minutes later as the Excel-assembly guard in
        # sitecustomize.py giving up after 300s waiting for a local file
        # that was never coming (RuntimeError: "Excel assembly did not
        # become a valid workbook"). _restore_local_job_state_from_durable_
        # cache's own per-file check already skips any file that is already
        # correctly sized on disk, so calling it unconditionally here is a
        # cheap no-op for a fully-hydrated job and a real fix for a partial
        # one.
        await asyncio.to_thread(_restore_local_job_state_from_durable_cache, job_id, job_folder)
    if manifest.exists():
        # Run ETL inside this request/response lifecycle instead of
        # detaching it via create_task(). FastAPI Cloud's Hobby tier
        # autoscales per-request and scales to zero; a detached background
        # task is not guaranteed to keep running once this endpoint's
        # response has been sent (confirmed in production for the
        # equivalent Drive-sync background task, which stalled/crashed with
        # "[Errno 32] Broken pipe" once the request that started it
        # returned). ETLOrchestrator.process() already checkpoints per
        # phase, so an interruption here is recoverable by calling this
        # endpoint again rather than losing all progress.
        #
        # dispatch_etl() (etl_dispatch.py) offloads this to GitHub Actions
        # when configured and this is a Drive-sourced job -- the trigger
        # call itself is one quick HTTP request, so this endpoint still
        # returns promptly either way, but the actual multi-hour merge
        # then runs on GitHub's runner instead of this request's thread,
        # sidestepping both the tiny-host resource limits and the
        # request-lifecycle-tied background-task problem described above.
        # Falls back to the exact same in-process run otherwise.
        result = await dispatch_etl(job_id, job_folder)
        success = isinstance(result, dict) and result.get("success") is True
        offloaded = isinstance(result, dict) and result.get("offloaded") is True
        return {
            "success": success,
            "job_id": job_id,
            "status": "ETL_QUEUED_GITHUB_ACTIONS" if offloaded else ("FINISHED" if success else "FAILED"),
            "message": (
                result.get("message", "ETL dijadwalkan lewat GitHub Actions.")
                if offloaded
                else (
                    "ETL selesai."
                    if success
                    else f"ETL gagal: {(result or {}).get('error', 'unknown error') if isinstance(result, dict) else result}"
                )
            ),
        }
    if str(os.getenv("JOB_STATE_STORAGE", "")).strip().lower() == "local":
        raise HTTPException(status_code=404, detail=f"Job not found in local explicit ETL state: {job_id}")
    try:
        metadata_key = UploadService._job_metadata_s3_key(job_id)
        metadata: dict = {}
        metadata_exists = await UploadService._s3_head(metadata_key)
        if metadata_exists:
            metadata = await UploadService._s3_get_json(metadata_key)

        filename = metadata.get("filename") or metadata.get("original_filename")
        storage = str(metadata.get("storage") or metadata.get("source") or "").strip().lower()
        drive_folder_id = str(
            metadata.get("drive_folder_id")
            or metadata.get("google_drive_folder_id")
            or GOOGLE_DRIVE_FOLDER_ID
            or ""
        ).strip()
        is_drive_job = (
            storage == "google_drive"
            or "_DRIVE_" in job_id.upper()
            or (metadata_exists and not filename and bool(drive_folder_id))
        )

        # Drive-origin jobs are recoverable from their authoritative source.
        # Never send them through recover_assembled_job(), which expects browser
        # upload chunks and produces the misleading "No durable chunk source".
        if is_drive_job:
            if not drive_folder_id:
                raise HTTPException(
                    status_code=409,
                    detail=f"Drive job cannot be recovered because no Google Drive folder is configured: {job_id}",
                )
            _schedule_task(_recover_legacy_drive_job(job_id, drive_folder_id))
            return {
                "success": True,
                "job_id": job_id,
                "status": "DRIVE_RECOVERY_QUEUED",
                "message": "Drive job recovery queued; files will be recovered from Google Drive and ETL will start automatically.",
                "drive_folder_id": drive_folder_id,
            }

        if not metadata_exists:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        if not filename:
            raise HTTPException(
                status_code=409,
                detail=f"Job metadata has no filename: {job_id}",
            )
        await UploadService.recover_assembled_job(job_id, filename, metadata.get("content_type"))
        if not manifest.exists():
            raise FileNotFoundError(f"Manifest was not created during recovery: {job_id}")
        _queue_etl(job_id, job_folder)
        return {"success": True, "job_id": job_id, "status": "ETL_QUEUED", "message": "Job recovered and ETL scheduled"}
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/recover/{job_id}")
async def recover_existing_job(
    job_id: str,
    filename: Annotated[str, Form(...)],
    content_type: Annotated[str | None, Form()] = None,
) -> dict:
    job_id = job_id.strip()
    job_folder = RAW_UPLOAD / job_id
    manifest = job_folder / "manifest.json"
    try:
        if manifest.exists():
            _queue_etl(job_id, job_folder)
            return {"success": True, "job_id": job_id, "status": "ETL_QUEUED", "message": "Manifest exists; ETL scheduled"}
        result = await UploadService.recover_assembled_job(job_id, filename, content_type)
        if not manifest.exists():
            raise FileNotFoundError(f"Manifest was not created during recovery: {job_id}")
        _queue_etl(job_id, job_folder)
        return {**result, "status": "ETL_QUEUED", "message": "Recovered and ETL scheduled"}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))