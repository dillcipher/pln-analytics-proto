from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from app.application.etl.etl_execution import run_etl_serialized
from app.application.jobs.job_manager import (
    ALLOW_LOCAL_DURABLE_JOBS,
    JOB_STATE_STORAGE,
    S3_BUCKET,
    JobManager,
)
from app.application.jobs.job_status import JobStatus
from app.application.etl.durable_etl_checkpoint import (
    completed_source_filenames,
    load_durable_checkpoint,
)
from app.core.constants import RAW_UPLOAD
from app.services.google_drive_service import (
    GOOGLE_DRIVE_FOLDER_ID,
    DriveDownloadDeadlineExceeded,
    GoogleDriveService,
)
from app.services.upload_service import UploadService

class S3PersistDeadlineExceeded(Exception):
    """Raised by _persist_raw_drive_file when the caller-supplied per-HTTP-
    call time budget runs out. Same contract as DriveDownloadDeadlineExceeded
    on the download side: this means "pause, resume next /drive/retry call",
    not a real persist failure, so callers must not record it as a FAILED
    file."""


router = APIRouter(prefix="/drive", tags=["Google Drive"])

_RUNNING: set[str] = set()
_TASKS: set[asyncio.Task] = set()
_RUNNING_LOCK = asyncio.Lock()
DRIVE_DOWNLOAD_CONCURRENCY = 1

# FastAPI Cloud (Hobby tier) autoscales per-request and scales to zero: a
# detached asyncio.create_task() started by an endpoint is NOT guaranteed to
# keep running once that endpoint's HTTP response has been sent (confirmed in
# production -- the process gets torn down mid-download, surfacing as
# "[Errno 32] Broken pipe"). To make Drive sync work on this kind of host,
# each HTTP call to /sync or /retry does at most this many seconds of actual
# downloading (still inside the request/response lifecycle the platform
# keeps alive), persists progress durably, and returns. The frontend calls
# retry again automatically until the job reaches READY FOR ETL.
#
# 20s was an initial conservative guess before the platform's actual request
# lifetime was known. Confirmed live since: a request that runs past roughly
# 100-120s gets its connection killed outright (TypeError: Failed to fetch /
# a raw reset, not an HTTP error), while requests well inside that window
# return cleanly. A single download attempt is only checked BETWEEN whole
# chunks (see download_file's deadline handling and DRIVE_DOWNLOAD_CHUNK_MB
# below), never resumes a part-way-downloaded file on the next call, and
# this folder's workbooks run up to hundreds of MB -- so a too-small budget
# means a large file can never finish (every retry restarts it from byte
# zero and gets cut off again at the same point). Also confirmed live: one
# real chunk read at the old 8MB chunk size overshot a 20s deadline by
# roughly 30s on this host's connection, so the true per-call ceiling is
# (this budget + worst-case single-chunk overshoot), not this number alone.
# Raised to 45s (was 20s) to leave roughly 30-40s of that overshoot margin
# comfortably under the observed ~100-120s connection lifetime, rather than
# pushing all the way to it.
#
# Raised again to 150s (was 45s): live testing this session showed the
# fixed per-call SETUP overhead alone -- load_durable_checkpoint,
# load_durable_job_state, the "GOOGLE DRIVE DISCOVERY" JobManager.update
# (which durably PUTs both job.json and the full manifest.json, then
# refreshes the recovery lock -- three more S3 writes), plus two
# _hydrate_partial_download probes per file -- adds up to roughly a dozen
# small S3 metadata calls against Supabase Storage before a single Drive
# download byte is ever requested. At a 45s budget this overhead alone was
# confirmed live to consume the ENTIRE budget for a file with no prior
# partial state (two consecutive /drive/retry calls both paused with
# "downloading X: 0/62450887 bytes" -- i.e. the deadline was already gone
# by the time GoogleDriveService.download_file took its very first
# attempt). Separately, repeated ALREADY_RUNNING responses this session
# confirmed individual calls can genuinely keep running server-side for
# 200-300+s (well past the ~100-120s client-visible connection lifetime
# noted above -- the browser's fetch can drop while the server keeps
# going), so there is real room to raise this. 150s leaves a solid margin
# under that observed ~200s+ floor while giving the setup overhead enough
# slack that real download progress can still happen in the same call.
DRIVE_SYNC_TIME_BUDGET_SECONDS = max(5, int(os.getenv("DRIVE_SYNC_TIME_BUDGET_SECONDS", "150")))


def _source_identity(item: dict) -> str:
    file_id = str(item.get("id") or "").strip()
    if file_id:
        return f"id:{file_id}"
    md5 = str(item.get("md5Checksum") or "").strip()
    name = UploadService._safe_filename(str(item.get("name") or "")).lower()
    size = str(item.get("size") or "")
    return f"fallback:{md5}:{size}:{name}"


def _record_matches_drive_item(record: dict, item: dict) -> bool:
    previous_id = str(record.get("drive_file_id") or "").strip()
    current_id = str(item.get("id") or "").strip()
    if previous_id and current_id and previous_id != current_id:
        return False
    previous_md5 = str(record.get("drive_md5") or "").strip()
    current_md5 = str(item.get("md5Checksum") or "").strip()
    if previous_md5 and current_md5 and previous_md5 != current_md5:
        return False
    previous_modified = str(record.get("drive_modified_time") or "").strip()
    current_modified = str(item.get("modifiedTime") or "").strip()
    if previous_modified and current_modified and previous_modified != current_modified:
        return False
    return True


def _dedupe_records(records: list[dict]) -> list[dict]:
    result: list[dict] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        drive_id = str(record.get("drive_file_id") or "").strip()
        filename = str(record.get("filename") or "").strip().lower()
        key = f"id:{drive_id}" if drive_id else f"name:{filename}"
        if key in seen:
            continue
        seen.add(key)
        result.append(record)
    result.sort(key=lambda row: (str(row.get("filename") or "").lower(), str(row.get("drive_file_id") or "")))
    return result


def _new_job_id() -> str:
    return datetime.now().strftime("JOB_%Y%m%d_%H%M%S") + "_DRIVE_" + uuid.uuid4().hex[:8]


def _schedule(coro) -> None:
    task = asyncio.create_task(coro)
    _TASKS.add(task)
    task.add_done_callback(lambda completed: _TASKS.discard(completed))


def _write_manifest(job_folder: Path, manifest: dict) -> None:
    job_folder.mkdir(parents=True, exist_ok=True)
    temporary = job_folder / "manifest.json.tmp"
    manifest_path = job_folder / "manifest.json"
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(manifest, output, indent=2, ensure_ascii=False, default=str)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(manifest_path)


def _drive_raw_key(job_id: str, item: dict) -> str:
    file_id = str(item.get("id") or "").strip() or _source_identity(item).replace(":", "_")
    filename = UploadService._safe_filename(str(item.get("name") or "source.xlsx"))
    return f"jobs/{job_id}/raw/{file_id}_{filename}"


# Supabase Storage's Free plan hard-caps every single object at 50MB and this
# cannot be raised without a paid upgrade (confirmed live via a reproducible
# HTTP 413 "EntityTooLarge... The object exceeded the maximum allowed size"
# response, and confirmed against Supabase's own docs). A multipart upload
# was tried first, but S3 multipart still assembles into ONE final object
# server-side, so it hit this exact same ceiling at completion regardless of
# part size. Storing large files as several independent chunk objects (see
# _compute_chunk_ranges / _persist_raw_drive_file below) sidesteps the limit
# entirely -- no object this code writes is ever within reach of 50MB.
#
# 5MB (not something closer to the 50MB ceiling) because of a SEPARATE,
# earlier-confirmed constraint: this host's upload throughput to Supabase's
# storage backend cannot reliably clear a single PUT within the S3 transfer
# client's own read timeout (90s) much above this size -- confirmed live,
# a 40MB chunk made zero progress across two consecutive /drive/retry calls
# (each with its own 3 retry attempts) after this scheme first shipped.
# 5MB is the size that was already proven, over many calls, to upload
# reliably before the multipart-vs-50MB-cap issue was even found.
DURABLE_CHUNK_SIZE = 5 * 1024 * 1024


def _compute_chunk_ranges(size: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split `size` bytes into fixed-size (start_offset, length) chunks, each
    at most `chunk_size`. Unlike _compute_part_ranges (used for the abandoned
    S3-multipart approach, which has a 5MB *minimum* part size), a plain PUT
    has no minimum object size, so the final chunk is simply whatever is
    left over -- no merging needed, and every chunk this produces stays
    safely under Supabase's 50MB cap.
    """
    if size <= 0:
        return []
    ranges: list[tuple[int, int]] = []
    offset = 0
    while offset < size:
        length = min(chunk_size, size - offset)
        ranges.append((offset, length))
        offset += length
    return ranges


def _drive_raw_chunk_key(base_key: str, chunk_index: int) -> str:
    return f"{base_key}.chunk{chunk_index:05d}"


def _drive_raw_manifest_key(base_key: str) -> str:
    return f"{base_key}.manifest.json"


def _compute_part_ranges(size: int, part_size: int) -> list[tuple[int, int]]:
    """Split `size` bytes into (start_offset, length) chunks for multipart upload.

    Any remainder from a plain size//part_size split is merged into the last
    full-size chunk instead of becoming its own small trailing part. This
    exists because of a live, twice-reproduced failure: a 52,465,323-byte
    file (17_ANNEV_20260701-20260705.xlsx) got stuck forever on its 11th and
    final part -- exactly 36,523 bytes -- across two independent fresh
    multipart uploads (fresh upload_id both times, so it wasn't stale state).
    Every other part of that same file (ten full 5MB parts) uploaded fine.
    That signature -- consistent, repeatable failure isolated to a small
    final part while normal-size parts succeed -- points at this Supabase
    S3-compatible storage backend rejecting or mishandling unusually small
    trailing multipart parts, not a transient network issue (which the
    existing 3-attempt-with-backoff retry already rules out). Rather than
    keep guessing at the exact backend error, this sidesteps the situation
    entirely: no part this function produces is ever smaller than
    `part_size` (except the sole part of a file that fits in one chunk),
    since a nonzero remainder is folded into the previous chunk instead of
    standing alone.
    """
    if size <= part_size:
        return [(0, size)]
    n_full = size // part_size
    remainder = size - n_full * part_size
    if remainder == 0:
        return [(i * part_size, part_size) for i in range(n_full)]
    ranges = [(i * part_size, part_size) for i in range(n_full - 1)]
    last_start = (n_full - 1) * part_size
    ranges.append((last_start, part_size + remainder))
    return ranges


def _persist_raw_drive_file(
    job_id: str,
    item: dict,
    source: Path,
    deadline: float | None = None,
    resume_upload_id: str | None = None,
) -> str:
    """Persist a Drive download durably before the job may advance.

    A successful local download is not enough on ephemeral runtimes.  Returning
    READY FOR ETL without a verified durable copy causes exactly the data-loss
    behaviour we are guarding against after a restart.

    `deadline` mirrors GoogleDriveService.download_file's contract: confirmed
    in production, boto3's managed multipart uploader (the previous
    implementation here) has no deadline awareness at all -- a single
    upload_file() call internally retries every part via botocore's own
    backoff, so on this host's slow/unreliable connection one call could run
    for several minutes past this HTTP request's actual time budget, and the
    old 3-attempt outer loop (also with no deadline check) could triple that.
    That meant a large file whose Drive download had just succeeded (see
    download_file's own resume fix) could still spend the entire remaining
    budget -- and then some -- on an upload that was ultimately doomed by the
    same network unreliability, before the caller ever got a chance to return
    a timely response.

    This version drives the multipart upload part-by-part so the deadline is
    checked between parts (same pattern as download_file's next_chunk loop),
    and is resumable across separate /drive/retry calls: the caller passes
    back `resume_upload_id`, its own durably-remembered id from a previous
    call's S3PersistDeadlineExceeded (see the exception handling in
    _sync_drive_job's main download loop), and list_parts asks S3 directly
    which parts of THAT upload are already durably stored, so a paused or
    retried upload continues instead of re-uploading bytes that already made
    it. An earlier version tried rediscovering the upload_id via
    list_multipart_uploads instead of the caller persisting it; that was
    dropped after live testing showed it unreliable on this storage backend
    (see the comment above the multipart-upload creation below).
    """
    source = Path(source)
    if not source.exists() or source.stat().st_size <= 0:
        raise RuntimeError(f"Downloaded Drive file is missing or empty: {source}")
    if JOB_STATE_STORAGE == "local":
        if not ALLOW_LOCAL_DURABLE_JOBS:
            raise RuntimeError("Durable job storage is disabled; refusing to acknowledge Drive download.")
        # Local development only (see ALLOW_LOCAL_DURABLE_JOBS in
        # job_manager.py): `source` is already sitting on this machine's own
        # disk, which -- unlike a recycled cloud container -- does not
        # disappear out from under a running process. Nothing to upload;
        # the local file itself IS the durable copy. Return the same key
        # shape S3 mode would have used so downstream bookkeeping
        # (`durable_raw_key`, the missing_durable check) sees this file as
        # accounted for.
        return _drive_raw_key(job_id, item)

    key = _drive_raw_key(job_id, item)
    size = source.stat().st_size
    # Defense in depth: verify the local file's actual size against Drive's
    # own reported size for this item before trusting it as upload source.
    # A debug/diagnostic call once read a still-downloading file mid-write
    # on the same replica and treated its transient, too-small size as
    # final, silently uploading a corrupt (too-short) part that S3 accepted
    # without complaint (see remediate-upload's docstring). download_one's
    # own already_downloaded check and the DRIVE_DOWNLOAD_CONCURRENCY=1
    # lock make this unlikely on the normal retry path, but the cost of
    # getting it wrong (a truncated file baked in by complete_multipart_
    # upload) is high enough to check explicitly rather than assume.
    expected_size = int(item.get("size") or 0) or None
    if expected_size and size != expected_size:
        raise RuntimeError(
            f"Refusing to persist {item.get('name')}: local size {size} does not match "
            f"Drive-reported size {expected_size} (stale or still-downloading file)"
        )
    content_type = str(item.get("mimeType") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    metadata = {"job-id": job_id, "drive-file-id": str(item.get("id") or "")}
    client = JobManager._create_s3_transfer_client()
    upload_id: str | None = resume_upload_id

    def _deadline_hit() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    # Small files: a single PUT, no multipart bookkeeping (and nothing
    # partial to resume if it fails) needed.
    single_part_threshold = 16 * 1024 * 1024
    if size <= single_part_threshold:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            if _deadline_hit():
                raise S3PersistDeadlineExceeded(
                    f"Time budget exceeded before S3 upload attempt {attempt}/3 for {key}"
                )
            try:
                with source.open("rb") as handle:
                    client.put_object(Bucket=S3_BUCKET, Key=key, Body=handle, ContentType=content_type, Metadata=metadata)
                head = client.head_object(Bucket=S3_BUCKET, Key=key)
                if int(head.get("ContentLength") or 0) != size:
                    raise RuntimeError(f"Durable raw cache size mismatch for {key}")
                return key
            except Exception as exc:
                last_error = exc
                logging.getLogger(__name__).warning(
                    "DRIVE RAW CACHE ATTEMPT FAILED | job=%s | file=%s | attempt=%s/3 | error=%r",
                    job_id, item.get("name"), attempt, exc,
                )
        raise RuntimeError(
            f"Drive file was downloaded locally but could not be durably persisted after 3 attempts: "
            f"{item.get('name')} ({last_error!r})"
        )

    # Larger files: store as several independent chunk objects instead of
    # one S3 multipart upload. Multipart was tried first (see the git log
    # for _compute_part_ranges/list_parts-scheme-mismatch history) but still
    # assembles into ONE final object server-side on complete_multipart_
    # upload, so it hit Supabase Storage's 50MB per-object hard cap at
    # completion regardless of part size -- confirmed live: a 52,465,323-byte
    # file consistently failed with HTTP 413 EntityTooLarge specifically on
    # whichever part pushed the running total past 52,428,800 bytes (50MiB),
    # no matter how that file was chunked into parts. Independent objects
    # sidestep the cap entirely: each one is its own complete upload, none
    # anywhere near 50MB, and resumability comes for free from a head_object
    # check against each chunk's deterministic key -- no upload_id/list_parts
    # bookkeeping needed, which also removes the list_parts-reliability class
    # of bugs multipart hit on this backend (see _compute_part_ranges above).
    if upload_id:
        # Leftover state from before this fix shipped -- an old multipart
        # upload_id that will never be resumed or completed now. Best-effort
        # abort so it doesn't linger consuming this project's free-tier
        # storage quota indefinitely.
        try:
            client.abort_multipart_upload(Bucket=S3_BUCKET, Key=key, UploadId=upload_id)
        except Exception:
            pass

    chunk_ranges = _compute_chunk_ranges(size, DURABLE_CHUNK_SIZE)
    total_chunks = len(chunk_ranges)
    done_chunks = 0

    try:
        with source.open("rb") as handle:
            for chunk_index, (chunk_start, chunk_length) in enumerate(chunk_ranges):
                chunk_number = chunk_index + 1
                chunk_key = _drive_raw_chunk_key(key, chunk_index)
                try:
                    existing = client.head_object(Bucket=S3_BUCKET, Key=chunk_key)
                    if int(existing.get("ContentLength") or -1) == chunk_length:
                        done_chunks += 1
                        continue
                except Exception:
                    pass  # Not present yet (or a transient check failure) -- upload it below.

                if _deadline_hit():
                    exc = S3PersistDeadlineExceeded(
                        f"Time budget exceeded before chunk {chunk_number}/{total_chunks} for {key}"
                    )
                    exc.parts_done = done_chunks
                    exc.parts_total = total_chunks
                    raise exc

                handle.seek(chunk_start)
                body = handle.read(chunk_length)
                chunk_error: Exception | None = None
                for attempt in range(1, 4):
                    if _deadline_hit():
                        exc = S3PersistDeadlineExceeded(
                            f"Time budget exceeded retrying chunk {chunk_number}/{total_chunks} for {key}"
                        )
                        exc.parts_done = done_chunks
                        exc.parts_total = total_chunks
                        raise exc
                    try:
                        client.put_object(Bucket=S3_BUCKET, Key=chunk_key, Body=body)
                        chunk_error = None
                        break
                    except Exception as exc_inner:
                        chunk_error = exc_inner
                        logging.getLogger(__name__).warning(
                            "DRIVE RAW CACHE CHUNK UPLOAD FAILED | job=%s | file=%s | chunk=%s/%s | attempt=%s/3 | error=%r",
                            job_id, item.get("name"), chunk_number, total_chunks, attempt, exc_inner,
                        )
                        if attempt < 3 and not _deadline_hit():
                            time.sleep(min(2.0 * attempt, max(0.0, deadline - time.monotonic()) if deadline is not None else 2.0 * attempt))
                if chunk_error is not None:
                    # Same reasoning as the old multipart code: a transient
                    # failure on one chunk should pause-and-resume, not
                    # discard every chunk already durably confirmed.
                    exc = S3PersistDeadlineExceeded(
                        f"Chunk {chunk_number}/{total_chunks} failed after 3 attempts for {key}: {chunk_error!r}"
                    )
                    exc.parts_done = done_chunks
                    exc.parts_total = total_chunks
                    error_detail = repr(chunk_error)
                    response_meta = getattr(chunk_error, "response", None)
                    if isinstance(response_meta, dict):
                        error_detail += f" | response={response_meta!r}"[:800]
                    exc.last_error = error_detail
                    exc.failed_part_number = chunk_number
                    exc.failed_part_bytes = len(body)
                    raise exc
                done_chunks += 1

        manifest_key = _drive_raw_manifest_key(key)
        client.put_object(
            Bucket=S3_BUCKET, Key=manifest_key,
            Body=json.dumps({"chunk_count": total_chunks, "chunk_size": DURABLE_CHUNK_SIZE, "total_size": size}).encode("utf-8"),
            ContentType="application/json",
        )
        return key
    except S3PersistDeadlineExceeded:
        # Chunks already durably confirmed via head_object stay exactly
        # where they are -- the next /drive/retry call's head_object checks
        # will skip them and continue from the first missing chunk.
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Drive file was downloaded locally but could not be durably persisted: {item.get('name')} ({exc!r})"
        ) from exc


def _hydrate_raw_drive_file(record: dict, destination: Path) -> None:
    key = str(record.get("durable_raw_key") or "").strip()
    if not key:
        raise RuntimeError("Drive record does not contain a durable raw cache key.")
    # Same reasoning as _persist_raw_drive_file: restoring a large cached
    # workbook is a real file transfer, not a small metadata call.
    client = JobManager._create_s3_transfer_client()
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".restore")

    # A file persisted under the current chunked scheme (see
    # _persist_raw_drive_file) has a manifest object alongside it; a small
    # file (<=16MB) persisted via the single-PUT path does not -- reassemble
    # from chunks when a manifest exists, otherwise fall back to reading
    # `key` directly as one object (the small-file / legacy layout).
    manifest: dict | None = None
    try:
        manifest_response = client.get_object(Bucket=S3_BUCKET, Key=_drive_raw_manifest_key(key))
        manifest = json.loads(manifest_response["Body"].read().decode("utf-8"))
    except Exception:
        manifest = None

    try:
        with temporary.open("wb") as output:
            if manifest and int(manifest.get("chunk_count") or 0) > 0:
                for chunk_index in range(int(manifest["chunk_count"])):
                    response = client.get_object(Bucket=S3_BUCKET, Key=_drive_raw_chunk_key(key, chunk_index))
                    body = response["Body"]
                    try:
                        while True:
                            piece = body.read(8 * 1024 * 1024)
                            if not piece:
                                break
                            output.write(piece)
                    finally:
                        body.close()
            else:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                body = response["Body"]
                try:
                    while True:
                        piece = body.read(8 * 1024 * 1024)
                        if not piece:
                            break
                        output.write(piece)
                finally:
                    body.close()
            output.flush()
            os.fsync(output.fileno())
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    if not temporary.exists() or temporary.stat().st_size <= 0:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Durable Drive cache restored an empty file: {key}")
    expected_size = int(record.get("size") or 0)
    if expected_size and temporary.stat().st_size != expected_size:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Durable Drive cache size mismatch for {key}: expected={expected_size} actual={temporary.stat().st_size}")
    os.replace(temporary, destination)


def _drive_partial_key(job_id: str, item: dict) -> str:
    file_id = str(item.get("id") or "").strip() or _source_identity(item).replace(":", "_")
    filename = UploadService._safe_filename(str(item.get("name") or "source.xlsx"))
    return f"jobs/{job_id}/raw-partial/{file_id}_{filename}"


def _persist_partial_download(job_id: str, item: dict, temp_path: Path) -> None:
    """Best-effort durable snapshot of an in-progress (not-yet-complete)
    Drive download.

    GoogleDriveService.download_file's own resume logic (see its docstring)
    keeps a partial ".drive-download" temp file so a paused download can
    continue via an HTTP Range request instead of restarting from byte
    zero -- but that temp file lives on local disk, which is NOT shared
    across FastAPI Cloud replicas and has no session affinity guarantee.
    Confirmed live: a large file's reported download progress went
    backwards across consecutive /drive/retry calls (16MB -> 8MB) because a
    later call landed on a different replica that had never seen the first
    replica's partial temp file, so it silently started that file over from
    byte zero. That is the same "ephemeral local disk" root cause already
    fixed for the completed-file cache (_persist_raw_drive_file /
    _hydrate_raw_drive_file above) and for processed ETL output
    (processed_storage.py) -- this closes the same gap for the one
    remaining piece of per-file state that was still local-disk-only.

    Called right after a DriveDownloadDeadlineExceeded pause. Deliberately
    best-effort/non-fatal: if this fails, behavior just falls back to what
    it was before this fix (resume if lucky enough to hit the same replica
    again, otherwise restart that one file) -- never worse.
    """
    try:
        if JOB_STATE_STORAGE == "local":
            return
        temp_path = Path(temp_path)
        if not temp_path.exists() or temp_path.stat().st_size <= 0:
            return
        client = JobManager._create_s3_transfer_client()
        key = _drive_partial_key(job_id, item)
        size = temp_path.stat().st_size
        # Chunked the same way as _persist_raw_drive_file and for the same
        # reason: a single put_object of the whole (possibly-partial, but
        # for a large source file still potentially well over 50MB) local
        # file would hit Supabase Storage's 50MB per-object hard cap. See
        # DURABLE_CHUNK_SIZE's docstring.
        chunk_ranges = _compute_chunk_ranges(size, DURABLE_CHUNK_SIZE)
        with temp_path.open("rb") as handle:
            for chunk_index, (chunk_start, chunk_length) in enumerate(chunk_ranges):
                handle.seek(chunk_start)
                body = handle.read(chunk_length)
                client.put_object(Bucket=S3_BUCKET, Key=_drive_raw_chunk_key(key, chunk_index), Body=body)
        client.put_object(
            Bucket=S3_BUCKET, Key=_drive_raw_manifest_key(key),
            Body=json.dumps({"chunk_count": len(chunk_ranges), "chunk_size": DURABLE_CHUNK_SIZE, "total_size": size}).encode("utf-8"),
            ContentType="application/json",
        )
        logging.getLogger(__name__).info(
            "DRIVE PARTIAL DOWNLOAD SNAPSHOT SAVED | job=%s | file=%s | bytes=%s | chunks=%s",
            job_id, item.get("name"), size, len(chunk_ranges),
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "DRIVE PARTIAL DOWNLOAD SNAPSHOT FAILED (non-fatal, resume will restart this file) | "
            "job=%s | file=%s | error=%r",
            job_id, item.get("name"), exc,
        )


def _hydrate_partial_download(job_id: str, item: dict, temp_path: Path) -> None:
    """Restore a durable partial-download snapshot (see
    _persist_partial_download above), if one exists, before a fresh download
    attempt starts. A quick head_object check first avoids a noisy failed
    get_object on the very first attempt at a file, when no snapshot has
    been saved yet -- that is the normal, expected case, not an error.
    Non-fatal: on any failure, the download just proceeds as if there were
    no snapshot (starts from byte zero, same as before this fix).
    """
    try:
        if JOB_STATE_STORAGE == "local":
            return
        temp_path = Path(temp_path)
        if temp_path.exists():
            return
        client = JobManager._create_s3_transfer_client()
        key = _drive_partial_key(job_id, item)
        manifest: dict | None = None
        try:
            manifest_response = client.get_object(Bucket=S3_BUCKET, Key=_drive_raw_manifest_key(key))
            manifest = json.loads(manifest_response["Body"].read().decode("utf-8"))
        except Exception:
            manifest = None
        if not manifest:
            # Legacy (pre-chunking) snapshot layout: one direct object at
            # `key`. head_object first avoids a noisy failed get_object on
            # the normal, expected case of no snapshot existing at all yet.
            try:
                client.head_object(Bucket=S3_BUCKET, Key=key)
            except Exception:
                return
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        staging = temp_path.with_suffix(temp_path.suffix + ".restore")
        with staging.open("wb") as output:
            if manifest and int(manifest.get("chunk_count") or 0) > 0:
                for chunk_index in range(int(manifest["chunk_count"])):
                    response = client.get_object(Bucket=S3_BUCKET, Key=_drive_raw_chunk_key(key, chunk_index))
                    body = response["Body"]
                    try:
                        while True:
                            piece = body.read(8 * 1024 * 1024)
                            if not piece:
                                break
                            output.write(piece)
                    finally:
                        body.close()
            else:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                body = response["Body"]
                try:
                    while True:
                        piece = body.read(8 * 1024 * 1024)
                        if not piece:
                            break
                        output.write(piece)
                finally:
                    body.close()
            output.flush()
            os.fsync(output.fileno())
        if staging.exists() and staging.stat().st_size > 0:
            os.replace(staging, temp_path)
            logging.getLogger(__name__).info(
                "DRIVE PARTIAL DOWNLOAD SNAPSHOT RESTORED | job=%s | file=%s | bytes=%s",
                job_id, item.get("name"), temp_path.stat().st_size,
            )
        else:
            staging.unlink(missing_ok=True)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "DRIVE PARTIAL DOWNLOAD SNAPSHOT RESTORE FAILED (non-fatal, download restarts from zero) | "
            "job=%s | file=%s | error=%r",
            job_id, item.get("name"), exc,
        )


def _discard_partial_download_snapshot(job_id: str, item: dict) -> None:
    """Clean up a durable partial-download snapshot once the file's full
    download has actually completed, so these don't accumulate forever in
    the bucket. Best-effort; a leftover snapshot is harmless dead weight at
    worst (only ever read back keyed by this exact job+file id).
    """
    try:
        if JOB_STATE_STORAGE == "local":
            return
        client = JobManager._create_s3_transfer_client()
        key = _drive_partial_key(job_id, item)
        try:
            manifest_response = client.get_object(Bucket=S3_BUCKET, Key=_drive_raw_manifest_key(key))
            manifest = json.loads(manifest_response["Body"].read().decode("utf-8"))
            for chunk_index in range(int(manifest.get("chunk_count") or 0)):
                try:
                    client.delete_object(Bucket=S3_BUCKET, Key=_drive_raw_chunk_key(key, chunk_index))
                except Exception:
                    pass
            try:
                client.delete_object(Bucket=S3_BUCKET, Key=_drive_raw_manifest_key(key))
            except Exception:
                pass
        except Exception:
            pass
        try:
            client.delete_object(Bucket=S3_BUCKET, Key=key)  # legacy layout / no-op if absent
        except Exception:
            pass
    except Exception:
        pass


def _run_etl(job_folder: Path) -> dict:
    return run_etl_serialized(job_folder)


async def _sync_drive_job(job_id: str, folder_id: str, recovery_attempts: int = 0, source_items: list[dict] | None = None) -> None:
    job_folder = RAW_UPLOAD / job_id
    logger = logging.getLogger(__name__)
    try:
        # One deadline governs this entire HTTP call -- durable-state loads,
        # Drive folder discovery, durable-cache restores, and downloads all
        # share it (see DRIVE_SYNC_TIME_BUDGET_SECONDS above). Computed here,
        # up front, so the discovery step below can also respect it.
        deadline = time.monotonic() + DRIVE_SYNC_TIME_BUDGET_SECONDS
        checkpoint = await asyncio.to_thread(load_durable_checkpoint, job_id)
        previous_manifest: dict = await asyncio.to_thread(JobManager.load_durable_job_state, job_id) or {}
        completed_sources = completed_source_filenames(checkpoint)
        previous_records = [dict(record) for record in (previous_manifest.get("files") or []) if isinstance(record, dict)]
        previous_records_by_source: dict[str, dict] = {}
        for record in previous_records:
            drive_id = str(record.get("drive_file_id") or "").strip()
            if drive_id:
                previous_records_by_source[f"id:{drive_id}"] = record
            else:
                filename = str(record.get("original_filename") or record.get("filename") or "").strip().lower()
                if filename:
                    previous_records_by_source[f"name:{filename}"] = record
        resume_mode = bool(completed_sources or any(str(record.get("durable_raw_key") or "").strip() for record in previous_records))
        # Caller-persisted S3 multipart upload_id per Drive file_id, keyed
        # across /drive/retry calls -- see _persist_raw_drive_file's
        # docstring for why this replaced list_multipart_uploads-based
        # discovery. Mutated in place by download_one and the main loop's
        # S3PersistDeadlineExceeded handler below, and written back into
        # every manifest so it survives across calls.
        s3_upload_resume: dict[str, dict] = dict(previous_manifest.get("s3_upload_resume") or {})

        all_items = source_items
        if all_items is None:
            # _sync_drive_job is called again for every /drive/retry HTTP
            # call (that is the whole point of the bounded-chunk design), and
            # this Drive folder can be several subfolders deep -- a full
            # recursive relist is itself an unbounded number of paginated
            # Drive API calls with no overall deadline, so it was capable of
            # eating an entire chunk (or more) on its own before a single
            # byte of any file was downloaded (confirmed in production: the
            # platform's own request timeout killed the connection with the
            # walk still running, well before it returned or raised). The
            # file list for one sync job is a stable snapshot for that job's
            # lifetime, so discover it once and reuse the durably cached copy
            # on every later call instead of re-listing the whole tree.
            cached_listing = previous_manifest.get("drive_items_cache")
            if isinstance(cached_listing, list) and cached_listing:
                all_items = cached_listing
            else:
                discovery_resume_state = previous_manifest.get("drive_discovery_resume_state")
                discovery = await asyncio.to_thread(
                    GoogleDriveService.list_xlsx_files_chunked, folder_id, deadline, discovery_resume_state
                )
                if not discovery["complete"]:
                    # The folder walk itself hit this call's time budget.
                    # Persist how far it got and pause exactly like a
                    # download-time-budget pause: the frontend's /drive/retry
                    # loop calls again, which resumes the walk from where it
                    # left off instead of starting over from the root folder.
                    found_so_far = discovery["files"]
                    manifest = {
                        "job_id": job_id,
                        "status": JobStatus.UPLOADED.value,
                        "progress": 0,
                        "current_step": (
                            f"DISCOVERING DRIVE FILES • {len(found_so_far)} FOUND SO FAR • "
                            "call /drive/retry to continue"
                        ),
                        "uploaded_at": previous_manifest.get("uploaded_at") or datetime.now().isoformat(),
                        "started_at": previous_manifest.get("started_at"),
                        "finished_at": None,
                        "total_files": len(found_so_far),
                        "processed_files": 0,
                        "storage": "google_drive",
                        "drive_folder_id": folder_id,
                        "recovery_attempts": recovery_attempts,
                        "download_concurrency": DRIVE_DOWNLOAD_CONCURRENCY,
                        "files": previous_records,
                        "resume": bool(previous_records),
                        "drive_discovery_resume_state": discovery["resume_state"],
                        "updated_at": datetime.now().isoformat(),
                    }
                    _write_manifest(job_folder, manifest)
                    JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=0, step=manifest["current_step"])
                    if JOB_STATE_STORAGE != "local":
                        JobManager.release_drive_recovery_lock(job_id)
                    return
                all_items = discovery["files"]
        if not all_items:
            raise ValueError("No Excel files were found in the configured Google Drive folder.")

        unique_items: list[dict] = []
        seen_sources: set[str] = set()
        for item in all_items:
            identity = _source_identity(item)
            if identity in seen_sources:
                continue
            seen_sources.add(identity)
            unique_items.append(item)
        all_items = unique_items

        skipped_items: list[dict] = []
        cached_items: list[tuple[dict, dict]] = []
        items: list[dict] = []
        for item in all_items:
            filename = UploadService._safe_filename(str(item.get("name") or "source.xlsx"))
            previous = previous_records_by_source.get(_source_identity(item))
            if filename in completed_sources:
                skipped_items.append(item)
            elif previous and str(previous.get("durable_raw_key") or "").strip() and _record_matches_drive_item(previous, item):
                cached_items.append((item, previous))
            else:
                items.append(item)

        # Smallest files first. Each HTTP call only has a short time budget
        # (see DRIVE_SYNC_TIME_BUDGET_SECONDS) and a single file's download
        # cannot be interrupted mid-transfer, so a huge file picked first
        # (Drive workbooks here range from a few MB to 700+ MB on a 0.1 vCPU
        # host) can silently consume an entire chunk -- or several -- before
        # anything else gets a chance to complete. Downloading small files
        # first makes visible progress quickly and leaves large files to be
        # handled one at a time, on their own chunk(s), later.
        items.sort(key=lambda item: int(str(item.get("size") or 0) or 0))

        logger.warning(
            "DRIVE RESUME PLAN | job=%s | resume=%s | total=%s | drive_download=%s | durable_restore=%s | etl_skipped=%s",
            job_id, resume_mode, len(all_items), len(items), len(cached_items), len(skipped_items),
        )

        resumed_records: list[dict] = []
        for item in skipped_items:
            previous = previous_records_by_source.get(_source_identity(item))
            if previous:
                previous["status"] = "ETL_RESUMED"
                previous["job_id"] = job_id
                resumed_records.append(previous)
        for item, previous in cached_items:
            restored_record = dict(previous)
            restored_record["status"] = "DURABLE_RESTORED"
            restored_record["job_id"] = job_id
            resumed_records.append(restored_record)

        manifest = {
            "job_id": job_id,
            "status": JobStatus.UPLOADED.value,
            "progress": 0,
            "current_step": "GOOGLE DRIVE DISCOVERY",
            "uploaded_at": previous_manifest.get("uploaded_at") or datetime.now().isoformat(),
            "started_at": previous_manifest.get("started_at"),
            "finished_at": None,
            "total_files": len(all_items),
            "processed_files": len(skipped_items) + len(cached_items),
            "storage": "google_drive",
            "drive_folder_id": folder_id,
            "recovery_attempts": recovery_attempts,
            "download_concurrency": DRIVE_DOWNLOAD_CONCURRENCY,
            "files": list(resumed_records),
            "resume": resume_mode,
            "resume_skipped_files": [str(item.get("name") or "") for item in skipped_items],
            "durable_restored_files": [str(item.get("name") or "") for item, _ in cached_items],
            # Durable snapshot of the Drive listing so the next /drive/retry
            # call (see above) can skip the expensive recursive relist.
            "drive_items_cache": all_items,
            "s3_upload_resume": s3_upload_resume,
        }
        _write_manifest(job_folder, manifest)
        JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=0, step="GOOGLE DRIVE DISCOVERY")

        total = len(all_items)
        restore_total = len(cached_items)
        restored = 0
        completed = 0
        records: list[dict] = list(resumed_records)
        semaphore = asyncio.Semaphore(DRIVE_DOWNLOAD_CONCURRENCY)
        progress_lock = asyncio.Lock()
        # `deadline` was already computed at the top of this function so the
        # discovery step above could share it too.
        time_budget_exceeded = False
        # Diagnostic only (not used for any control-flow decision): a short
        # human-readable note on exactly where the in-flight file's pause
        # happened, folded into the generic "PAUSED (TIME BUDGET)" manifest
        # message below. Added after a real incident where a 52MB file's
        # persist step appeared to make zero progress across many
        # /drive/retry calls and there was no way to tell, from the manifest
        # alone, whether it was stuck re-downloading from Drive every call or
        # stuck uploading the same S3 part every call -- this makes that
        # visible without needing raw server logs.
        pause_detail = ""

        # Skip this local-disk prefetch entirely while there are still NEW
        # files left to download (`items` non-empty). It exists purely to
        # warm the local job_folder for whatever ETL will eventually need --
        # it does NOT gate "READY FOR ETL" (that only checks durable_raw_key
        # in the manifest, set unconditionally above regardless of whether
        # local hydration ever runs) and does not affect this call's own
        # download progress in any way. Confirmed live: even with an 8s
        # fixed cap, restoring 5-6 already-durable cached files (several
        # 50MB+, each needing ~11-12 chunk GETs on a cold replica -- see
        # _hydrate_raw_drive_file) routinely ate enough of the call's setup
        # time that the file actually still downloading made ZERO progress
        # across six consecutive /drive/retry calls in a row. Every call
        # spent while there is still real Drive-download work to do is far
        # better spent entirely on that work; whatever ends up in job_folder
        # locally when the LAST new file finishes is whatever it is, and any
        # cached item not already there gets restored once here, on the one
        # call where `items` finally goes empty and this prefetch is worth
        # doing before falling through to the READY FOR ETL check below.
        if not items:
            for restore_index, (item, previous) in enumerate(cached_items, start=1):
                if time.monotonic() >= deadline:
                    time_budget_exceeded = True
                    break
                # Use the REGISTERED filename from the manifest record
                # (previous["filename"]), not a fresh name recomputed from
                # Drive's live item listing. Confirmed live 2026-09-02: when
                # download_one (below) hit a local filename collision, it
                # renamed the destination to f"{file_id}_{filename}" and
                # that ID-prefixed name is what got persisted into the
                # manifest via build_file_record and is what the ETL
                # orchestrator later looks up by (etl_orchestrator.py's
                # _build_paths uses file_record["filename"] verbatim, with
                # no existence check). Recomputing from item.get("name")
                # here drops that prefix, so this restore step downloaded
                # the correct S3 bytes -- durable_restore counted it as a
                # real success -- but wrote them to a path nothing
                # downstream ever reads, leaving the registered path
                # missing and the whole dataset group quarantined with
                # FileNotFoundError deep inside the merge, long after this
                # step reported success.
                filename = UploadService._safe_filename(
                    str(
                        previous.get("filename")
                        or item.get("name")
                        or f"drive_cached_{restore_index}.xlsx"
                    )
                )
                destination = job_folder / filename
                expected_size = int(previous.get("size") or 0) or None
                already_local = bool(
                    expected_size and destination.exists() and destination.stat().st_size == expected_size
                )
                if not already_local:
                    # Without this check, every already-durable file got
                    # re-downloaded from S3 on EVERY /drive/retry call, even when
                    # THIS SAME replica already had it on local disk from the
                    # call just before. Confirmed live: as more files finished
                    # (each one added to cached_items on the next call), this
                    # restore step's own cost grew every time, to the point
                    # where it alone could consume the whole time budget before
                    # the loop ever reached the file actually still being
                    # downloaded -- a call would end back at "RESTORING DURABLE
                    # CACHE" with zero progress on the paused file. Left
                    # unfixed, this would only get worse as Pengecekan (237MB)
                    # and Pascabayar (711MB) finish, eventually stalling the
                    # sync permanently. Mirrors the same already-downloaded
                    # check download_one already does below.
                    await asyncio.to_thread(_hydrate_raw_drive_file, previous, destination)
                restored += 1
                done_total = len(skipped_items) + restored
                progress = min(18, max(1, round(done_total / max(total, 1) * 18)))
                manifest["processed_files"] = done_total
                manifest["progress"] = progress
                manifest["current_step"] = f"RESTORING DURABLE CACHE {restored}/{restore_total} • DRIVE REMAINING {len(items)}"
                manifest["files"] = _dedupe_records(records)
                _write_manifest(job_folder, manifest)
                JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=progress, step=manifest["current_step"])

        async def download_one(index: int, item: dict) -> dict:
            nonlocal completed
            filename = UploadService._safe_filename(str(item.get("name") or f"drive_file_{index}.xlsx"))
            file_id = str(item["id"])
            expected_size = int(item.get("size") or 0) or None
            destination = job_folder / filename
            already_downloaded = bool(
                expected_size and destination.exists() and destination.stat().st_size == expected_size
            )
            if destination.exists() and not already_downloaded:
                # Only treat an existing file at this path as a name
                # collision (a different Drive item that happens to share
                # this sanitized filename) when it does NOT already match
                # this item's own size -- otherwise it's this exact file,
                # left over from an earlier call in this same job whose S3
                # persist step failed after the Drive download had already
                # succeeded. Re-downloading it every retry wasted the whole
                # (slow) download all over again for no reason; reuse it.
                destination = job_folder / f"{file_id}_{filename}"
                already_downloaded = bool(
                    expected_size and destination.exists() and destination.stat().st_size == expected_size
                )
            async with semaphore:
                if not already_downloaded:
                    # A file can also be FULLY downloaded already but not
                    # yet fully durably persisted (multipart upload still in
                    # progress) -- confirmed live: a 52MB file's upload got
                    # stuck re-reporting "part 10/11 done" for 5+ calls in a
                    # row because whichever replica answered those calls had
                    # no local copy of the completed download at all
                    # (upload_part needs the local bytes to read the still-
                    # missing part from). Try restoring a full snapshot
                    # straight to `destination` first -- see
                    # _persist_partial_download's docstring -- before
                    # falling back to the partial-download resume path
                    # below, so a cold replica picking up mid-upload doesn't
                    # need to re-download the whole file from Drive just to
                    # read bytes it already fetched once.
                    await asyncio.to_thread(_hydrate_partial_download, job_id, item, destination)
                    already_downloaded = bool(
                        expected_size and destination.exists() and destination.stat().st_size == expected_size
                    )
                if not already_downloaded:
                    # Partial-download resume is only durable across
                    # /drive/retry calls if the same replica happens to
                    # handle them (see _persist_partial_download's
                    # docstring for the incident that exposed this). Restore
                    # a durable snapshot first, if this replica doesn't
                    # already have the local temp file, so a resumed
                    # download continues from where a DIFFERENT replica left
                    # off instead of silently restarting from byte zero.
                    temp_path = destination.with_suffix(destination.suffix + ".drive-download")
                    if not temp_path.exists():
                        await asyncio.to_thread(_hydrate_partial_download, job_id, item, temp_path)
                    try:
                        await asyncio.to_thread(
                            GoogleDriveService.download_file, file_id, destination, deadline, expected_size
                        )
                    except DriveDownloadDeadlineExceeded:
                        # Snapshot whatever partial bytes made it to disk so
                        # the NEXT /drive/retry call can resume this file
                        # even if it lands on a different replica. Purely
                        # additive: does not change what this exception
                        # means to the caller (still "pause, retry next
                        # call"), just makes the pause point durable too.
                        await asyncio.to_thread(_persist_partial_download, job_id, item, temp_path)
                        raise
                    # Full download just succeeded on this replica. Snapshot
                    # it durably too (not only partial pauses) -- if the
                    # multipart upload below doesn't finish in this same
                    # call and a DIFFERENT replica answers the next one, the
                    # restore above will find this and skip re-downloading
                    # from Drive entirely.
                    await asyncio.to_thread(_persist_partial_download, job_id, item, destination)
                resume_info = s3_upload_resume.get(file_id)
                resume_upload_id = resume_info.get("upload_id") if resume_info else None
                durable_raw_key = await asyncio.to_thread(
                    _persist_raw_drive_file, job_id, item, destination, deadline, resume_upload_id
                )
                await asyncio.to_thread(_discard_partial_download_snapshot, job_id, item)
                s3_upload_resume.pop(file_id, None)
                record = GoogleDriveService.build_file_record(item, destination)
                record["job_id"] = job_id
                record["durable_raw_key"] = durable_raw_key
                record["durable_raw_cached_at"] = datetime.now().isoformat()
            async with progress_lock:
                completed += 1
                done_total = len(skipped_items) + restored + completed
                progress = min(18, max(1, round(done_total / max(total, 1) * 18)))
                manifest["processed_files"] = done_total
                manifest["progress"] = progress
                manifest["current_step"] = f"DOWNLOADING {completed}/{len(items)} • DURABLE RESTORE {restored}/{restore_total} • ETL SKIP {len(skipped_items)}"
                records.append(record)
                manifest["files"] = _dedupe_records(records)
                _write_manifest(job_folder, manifest)
                JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=progress, step=manifest["current_step"])
            return record

        for index, item in enumerate(items, start=1):
            if time.monotonic() >= deadline:
                time_budget_exceeded = True
                break
            try:
                await download_one(index, item)
            except DriveDownloadDeadlineExceeded as exc:
                # Not a real failure: a single file (large workbook, or a
                # run of transient Drive errors) hit this HTTP call's time
                # budget mid-download/mid-retry. Stop the batch here (do NOT
                # mark the file FAILED -- it stays "not yet downloaded" so
                # the next /drive/retry call picks it back up) rather than
                # let one slow file monopolize the whole request lifetime.
                # download_file preserves its partial ".drive-download" temp
                # file on this exact exception, so the next call resumes
                # from the byte offset already on disk instead of
                # restarting the whole file from zero.
                dl_bytes = getattr(exc, "bytes_downloaded", None)
                dl_total = getattr(exc, "total_bytes", None)
                dl_last_error = getattr(exc, "last_error", None)
                dl_last_status = getattr(exc, "last_status_code", None)
                dl_last_attempt = getattr(exc, "last_error_attempt", None)
                filename_hint = str(item.get("name") or "?")
                if dl_bytes is not None and dl_total:
                    pause_detail = f" -- downloading {filename_hint}: {dl_bytes}/{dl_total} bytes"
                elif dl_bytes is not None:
                    pause_detail = f" -- downloading {filename_hint}: {dl_bytes} bytes so far"
                else:
                    pause_detail = f" -- downloading {filename_hint}"
                if dl_last_error:
                    pause_detail += f" | LAST CHUNK ERROR (attempt {dl_last_attempt}, http={dl_last_status or 'n/a'}): {dl_last_error[:600]}"
                time_budget_exceeded = True
                logger.warning(
                    "GOOGLE DRIVE FILE DOWNLOAD PAUSED (TIME BUDGET) | job=%s | index=%s/%s | file_id=%s | bytes=%s/%s | last_error=%s | will retry next call",
                    job_id, index, total, item.get("id"), dl_bytes, dl_total, dl_last_error,
                )
                break
            except S3PersistDeadlineExceeded as exc:
                # Same contract as DriveDownloadDeadlineExceeded above, just
                # for the S3 persist step instead of the Drive download:
                # this file's bytes are already downloaded (or the Drive
                # download would have raised first) but ran out of budget
                # while durably uploading them. _persist_raw_drive_file's
                # multipart path already confirmed whichever parts finished
                # via S3 itself; we additionally persist its own upload_id
                # in the manifest's s3_upload_resume map (list_multipart_uploads
                # discovery was tried and found unreliable on this storage
                # backend -- see _persist_raw_drive_file's docstring) so the
                # next call resumes the SAME multipart upload and continues
                # uploading only the remaining parts, rather than a fresh
                # upload silently starting from part 1 again.
                resume_file_id = str(item.get("id") or "")
                resume_upload_id = getattr(exc, "upload_id", None)
                parts_done = getattr(exc, "parts_done", None)
                parts_total = getattr(exc, "parts_total", None)
                last_error = getattr(exc, "last_error", None)
                failed_part_number = getattr(exc, "failed_part_number", None)
                failed_part_bytes = getattr(exc, "failed_part_bytes", None)
                if resume_file_id and resume_upload_id:
                    s3_upload_resume[resume_file_id] = {"upload_id": resume_upload_id}
                    manifest["s3_upload_resume"] = s3_upload_resume
                    if last_error:
                        manifest["s3_upload_resume"][resume_file_id]["last_error"] = last_error
                        manifest["s3_upload_resume"][resume_file_id]["failed_part_number"] = failed_part_number
                        manifest["s3_upload_resume"][resume_file_id]["failed_part_bytes"] = failed_part_bytes
                filename_hint = str(item.get("name") or "?")
                if parts_done is not None and parts_total is not None:
                    pause_detail = f" -- uploading {filename_hint}: part {parts_done}/{parts_total} done"
                    if last_error:
                        pause_detail += f" | LAST ERROR (part {failed_part_number}, {failed_part_bytes} bytes): {last_error[:900]}"
                else:
                    pause_detail = f" -- uploading {filename_hint}"
                time_budget_exceeded = True
                logger.warning(
                    "GOOGLE DRIVE FILE PERSIST PAUSED (TIME BUDGET) | job=%s | index=%s/%s | file_id=%s | upload_id=%s | parts=%s/%s | will retry next call",
                    job_id, index, total, item.get("id"), resume_upload_id, parts_done, parts_total,
                )
                break
            except Exception as file_exc:
                filename = UploadService._safe_filename(str(item.get("name") or f"drive_file_{index}.xlsx"))
                failure = {
                    "filename": filename, "original_filename": str(item.get("name") or filename),
                    "size": int(item.get("size") or 0) if str(item.get("size") or "").isdigit() else 0,
                    "content_type": item.get("mimeType"), "dataset": "UNKNOWN", "month": None,
                    "validation": "FAILED", "status": "FAILED", "error": str(file_exc)[:1000],
                    "storage": "google_drive", "drive_file_id": item.get("id"),
                    "drive_modified_time": item.get("modifiedTime"), "drive_md5": item.get("md5Checksum"),
                    "drive_web_view_link": item.get("webViewLink"), "job_id": job_id,
                }
                records.append(failure)
                s3_upload_resume.pop(str(item.get("id") or ""), None)
                manifest["s3_upload_resume"] = s3_upload_resume
                manifest["files"] = _dedupe_records(records)
                manifest["current_step"] = "DOWNLOAD FILE FAILED; CONTINUING BATCH"
                _write_manifest(job_folder, manifest)
                logger.exception("GOOGLE DRIVE FILE DOWNLOAD FAILED | job=%s | index=%s/%s | file_id=%s | continuing", job_id, index, total, item.get("id"))

        records = _dedupe_records(records)
        manifest["files"] = records
        manifest["processed_files"] = len(skipped_items) + restored + completed
        manifest["progress"] = min(18, max(0, round((len(skipped_items) + restored + completed) / max(total, 1) * 18)))
        failed_downloads = [r for r in records if r.get("status") == "FAILED"]
        if failed_downloads:
            manifest["current_step"] = f"GOOGLE DRIVE DOWNLOAD FAILED FOR {len(failed_downloads)}/{total} FILES"
            manifest["resume_required"] = True
            _write_manifest(job_folder, manifest)
            raise RuntimeError(f"Google Drive could not download {len(failed_downloads)}/{total} file(s)")

        if time_budget_exceeded:
            # This HTTP call's time budget ran out before every file was
            # downloaded. This is expected and NOT a failure: the platform
            # only guarantees this worker stays alive while a request is in
            # flight (see DRIVE_SYNC_TIME_BUDGET_SECONDS above), so progress
            # so far is written durably and the caller (frontend) is expected
            # to call POST /drive/retry/{job_id} again to continue -- already
            # downloaded files are restored from the durable cache, not
            # re-downloaded.
            done_total = len(skipped_items) + restored + completed
            remaining = max(total - done_total, 0)
            manifest["current_step"] = (
                f"PAUSED (TIME BUDGET) • {done_total}/{total} DONE • {remaining} REMAINING • "
                "call /drive/retry to continue" + pause_detail
            )
            manifest["updated_at"] = datetime.now().isoformat()
            _write_manifest(job_folder, manifest)
            JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=manifest["progress"], step=manifest["current_step"])
            # Release (not refresh) the lock: this chunk ended cleanly and
            # nothing is running during the gap before the next /retry call,
            # so the very next call must be able to re-acquire it immediately
            # instead of seeing ALREADY_RUNNING and stalling the continuation
            # loop.
            if JOB_STATE_STORAGE != "local":
                JobManager.release_drive_recovery_lock(job_id)
        else:
            # A Drive sync is complete only when every non-ETL-skipped source has
            # a verified durable raw object. Never advertise READY FOR ETL based on
            # ephemeral container files.
            missing_durable = [
                record for record in records
                if str(record.get("status") or "").upper() not in {"ETL_RESUMED"}
                and not str(record.get("durable_raw_key") or "").strip()
            ]
            if missing_durable:
                names = ", ".join(str(r.get("filename") or "?") for r in missing_durable[:5])
                raise RuntimeError(
                    f"Refusing READY FOR ETL: {len(missing_durable)} file(s) have no verified durable raw copy"
                    + (f" ({names})" if names else "")
                )

            # Explicit ETL architecture: syncing Drive only downloads files.
            # Never start ETL here and never mark the job as DETECTING/MERGING.
            manifest["processed_files"] = total
            manifest["progress"] = 100
            manifest["status"] = JobStatus.UPLOADED.value
            manifest["current_step"] = f"READY FOR ETL • {len(items)} DOWNLOADED • {restored} RESTORED"
            manifest["updated_at"] = datetime.now().isoformat()
            _write_manifest(job_folder, manifest)
            JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=100, step="READY FOR ETL")
            if JOB_STATE_STORAGE != "local":
                JobManager.release_drive_recovery_lock(job_id)
    except Exception as exc:
        logger.exception("GOOGLE DRIVE SYNC/ETL FAILED | job=%s", job_id)
        try:
            if (job_folder / "manifest.json").exists():
                manifest_data = json.loads((job_folder / "manifest.json").read_text(encoding="utf-8"))
                manifest_data["last_error"] = str(exc)[:1000]
                manifest_data["resume_required"] = True
                manifest_data["status"] = JobStatus.UPLOADED.value
                manifest_data["current_step"] = f"RESUME REQUIRED: {str(exc)[:220]}"
                manifest_data["updated_at"] = datetime.now().isoformat()
                _write_manifest(job_folder, manifest_data)
                JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=min(99, int(manifest_data.get("progress") or 0)), step=manifest_data["current_step"])
                # Release (not refresh/hold) the lock on a real failure too.
                # The old 15-minute hold assumed retries came from a rare,
                # separate background recovery pass; now that /drive/retry is
                # the normal, expected way a user (or the frontend's
                # auto-continue loop) resumes a job, holding the lock would
                # lock them out of retrying their own just-failed job for 15
                # minutes.
                if JOB_STATE_STORAGE != "local":
                    JobManager.release_drive_recovery_lock(job_id)
        except Exception:
            logger.exception("Failed to persist Google Drive resumable failure state")
    finally:
        async with _RUNNING_LOCK:
            _RUNNING.discard(job_id)


async def recover_drive_jobs_on_startup() -> dict:
    logger = logging.getLogger(__name__)
    try:
        candidates = await asyncio.to_thread(JobManager.list_recoverable_drive_jobs)
    except Exception:
        logger.exception("GOOGLE DRIVE RECOVERY DISCOVERY FAILED")
        return {"recovered": 0, "candidates": 0}
    candidates = [m for m in candidates if str(m.get("job_id") or "").strip() and str(m.get("drive_folder_id") or "").strip()]
    if not candidates:
        return {"recovered": 0, "candidates": 0}
    candidates.sort(key=lambda m: (str(m.get("updated_at") or m.get("uploaded_at") or ""), str(m.get("job_id") or "")), reverse=True)
    selected = candidates[0]
    selected_job_id = str(selected.get("job_id") or "").strip()
    selected_folder_id = str(selected.get("drive_folder_id") or "").strip()
    async with _RUNNING_LOCK:
        if selected_job_id in _RUNNING:
            return {"recovered": 0, "candidates": len(candidates)}
        if JOB_STATE_STORAGE != "local" and not JobManager.acquire_drive_recovery_lock(selected_job_id):
            return {"recovered": 0, "candidates": len(candidates), "locked": selected_job_id}
        _RUNNING.add(selected_job_id)
        _schedule(_sync_drive_job(selected_job_id, selected_folder_id, int(selected.get("recovery_attempts", 0) or 0)))
    logger.warning("GOOGLE DRIVE JOB RECOVERED | job=%s | folder=%s | attempt=%s", selected_job_id, selected_folder_id, selected.get("recovery_attempts", 0))
    return {"recovered": 1, "candidates": len(candidates), "selected_job": selected_job_id}


def _drive_job_complete(job_id: str) -> bool:
    try:
        state = JobManager.load_durable_job_state(job_id) or {}
    except Exception:
        return False
    return str(state.get("current_step") or "").upper().startswith("READY FOR ETL")


@router.post("/sync")
async def sync_google_drive(folder_id: str | None = Query(default=None)) -> dict:
    resolved_folder_id = (folder_id or GOOGLE_DRIVE_FOLDER_ID or "").strip()
    if not resolved_folder_id:
        raise HTTPException(status_code=500, detail="Google Drive folder is not configured.")
    job_id = _new_job_id()
    job_folder = RAW_UPLOAD / job_id
    created_at = datetime.now().isoformat()
    manifest = {"job_id": job_id, "status": JobStatus.UPLOADED.value, "progress": 0, "current_step": "GOOGLE DRIVE QUEUED", "uploaded_at": created_at, "started_at": None, "finished_at": None, "total_files": 0, "processed_files": 0, "storage": "google_drive", "drive_folder_id": resolved_folder_id, "recovery_attempts": 0, "download_concurrency": DRIVE_DOWNLOAD_CONCURRENCY, "files": []}
    _write_manifest(job_folder, manifest)
    JobManager.update(job_folder, status=JobStatus.UPLOADED, progress=0, step="GOOGLE DRIVE QUEUED")
    if not JobManager.load_durable_job_state(job_id):
        raise HTTPException(
            status_code=503,
            detail="Durable job storage is unavailable. Drive sync was not started to prevent downloaded files from being lost on restart.",
        )
    async with _RUNNING_LOCK:
        _RUNNING.add(job_id)
    # Run this bounded chunk of the sync INSIDE the request/response
    # lifecycle instead of detaching it via create_task(). On a
    # request-based autoscaler (FastAPI Cloud Hobby) that is the only way
    # the platform is known to keep the worker alive for this work -- see
    # DRIVE_SYNC_TIME_BUDGET_SECONDS above.
    await _sync_drive_job(job_id, resolved_folder_id)
    complete = _drive_job_complete(job_id)
    return {
        "success": True,
        "job_id": job_id,
        "status": "DRIVE_SYNC_COMPLETE" if complete else "DRIVE_SYNC_PARTIAL",
        "complete": complete,
        "folder_id": resolved_folder_id,
        "message": (
            "Google Drive sync selesai; files will be downloaded only. Start ETL explicitly after status becomes READY FOR ETL."
            if complete
            else "Sebagian file sudah terunduh dalam batas waktu satu panggilan; panggil /drive/retry untuk melanjutkan sisanya."
        ),
    }


@router.post("/retry/{job_id}")
async def retry_drive_sync(job_id: str) -> dict:
    source_job_id = str(job_id or "").strip()
    if not source_job_id:
        raise HTTPException(status_code=400, detail="job_id cannot be empty.")
    metadata = await asyncio.to_thread(JobManager.load_durable_job_state, source_job_id)
    if not metadata:
        raise HTTPException(status_code=404, detail=f"Drive job '{source_job_id}' was not found in durable storage.")
    if str(metadata.get("storage") or "").strip().lower() != "google_drive":
        raise HTTPException(status_code=409, detail=f"Job '{source_job_id}' is not a Google Drive job.")
    folder_id = str(metadata.get("drive_folder_id") or "").strip()
    if not folder_id:
        raise HTTPException(status_code=422, detail=f"Drive job '{source_job_id}' does not contain a Drive folder id.")
    checkpoint = await asyncio.to_thread(load_durable_checkpoint, source_job_id)
    if checkpoint and checkpoint.get("finished"):
        return {"success": True, "job_id": source_job_id, "status": "FINISHED", "message": "Job is already finished; no Drive files were downloaded again."}
    async with _RUNNING_LOCK:
        if source_job_id in _RUNNING:
            return {"success": True, "job_id": source_job_id, "status": "ALREADY_RUNNING", "message": "This Drive job is already running."}
        if JOB_STATE_STORAGE != "local" and not JobManager.acquire_drive_recovery_lock(source_job_id):
            return {"success": True, "job_id": source_job_id, "status": "ALREADY_RUNNING", "message": "Another worker already owns this Drive job."}
        _RUNNING.add(source_job_id)
    # A healthy time-budget pause is routine continuation, not a recovery from
    # failure -- only bump recovery_attempts when actually resuming after a
    # real error, so a long multi-chunk sync doesn't burn through
    # MAX_UPLOADED_RECOVERY_ATTEMPTS (used by startup auto-recovery) after a
    # handful of ordinary continuation calls.
    was_real_failure = bool(str(metadata.get("last_error") or "").strip())
    next_attempts = int(metadata.get("recovery_attempts", 0) or 0) + (1 if was_real_failure else 0)
    # Same reasoning as /sync: await this chunk inside the request/response
    # lifecycle instead of detaching it, so the platform has a reason to keep
    # the worker alive for it.
    await _sync_drive_job(source_job_id, folder_id, next_attempts)
    complete = _drive_job_complete(source_job_id)
    return {
        "success": True,
        "job_id": source_job_id,
        "status": "FINISHED" if complete else "DRIVE_RESUME_PARTIAL",
        "complete": complete,
        "retry_of": source_job_id,
        "message": (
            "Drive resume selesai; semua file sudah terunduh."
            if complete
            else "Drive resume berjalan menggunakan durable raw cache dan checkpoint; masih ada file tersisa, panggil /drive/retry lagi untuk melanjutkan."
        ),
    }


@router.get("/config")
async def drive_config() -> dict:
    configured = bool(GOOGLE_DRIVE_FOLDER_ID)
    credentials_configured = bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip() or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip())
    return {"configured": configured, "credentials_configured": credentials_configured, "folder_id_configured": bool(GOOGLE_DRIVE_FOLDER_ID), "folder_id": GOOGLE_DRIVE_FOLDER_ID}


@router.get("/debug-upload-parts/{job_id}")
async def debug_upload_parts(job_id: str) -> dict:
    """Read-only diagnostic: for whichever file currently has a paused
    multipart upload (s3_upload_resume in the manifest), list what S3
    itself actually has for that upload_id.

    Added after a real incident: a 52MB file's last part (part 11/11, a
    ~36KB remainder) failed 3 attempts on every single /drive/retry call
    for 5+ consecutive calls in a row, always stuck reporting "part 10/11
    done" -- with no way to see the actual botocore exception from outside
    (no server shell access), this surfaces list_parts' own view of the
    upload directly. Temporary; safe to remove once the underlying issue is
    resolved and confirmed stable.
    """
    job_id = job_id.strip()
    manifest = await asyncio.to_thread(JobManager.load_durable_job_state, job_id)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"No durable job state found for {job_id}")
    resume_map = manifest.get("s3_upload_resume") or {}
    if not resume_map:
        return {"success": True, "data": {"message": "No paused multipart upload recorded in this job's manifest.", "s3_upload_resume": resume_map}}

    items_by_id = {str(item.get("id") or ""): item for item in (manifest.get("drive_items_cache") or [])}
    client = JobManager._create_s3_transfer_client()
    results = []
    for file_id, info in resume_map.items():
        upload_id = str(info.get("upload_id") or "")
        item = items_by_id.get(file_id) or {}
        key = _drive_raw_key(job_id, item) if item else None
        entry: dict = {"file_id": file_id, "upload_id": upload_id, "key": key, "item_name": item.get("name"), "item_size": item.get("size")}
        uploaded_part_numbers: set[int] = set()
        if key and upload_id:
            try:
                parts_resp = client.list_parts(Bucket=S3_BUCKET, Key=key, UploadId=upload_id)
                found_parts = parts_resp.get("Parts") or []
                entry["parts"] = [
                    {"PartNumber": p.get("PartNumber"), "Size": p.get("Size"), "ETag": p.get("ETag")}
                    for p in found_parts
                ]
                entry["is_truncated"] = parts_resp.get("IsTruncated")
                uploaded_part_numbers = {int(p["PartNumber"]) for p in found_parts}
            except Exception as exc:
                entry["list_parts_error"] = repr(exc)

        # Rule in/out the "different replica, local file missing" theory
        # directly: does THIS replica (the one answering this debug call)
        # actually have the fully-downloaded local file on disk right now?
        filename = UploadService._safe_filename(str(item.get("name") or f"drive_file_{file_id}.xlsx"))
        job_folder = RAW_UPLOAD / job_id
        candidates = [job_folder / filename, job_folder / f"{file_id}_{filename}"]
        local = next((p for p in candidates if p.exists()), None)
        entry["local_file_found"] = str(local) if local else None
        entry["local_file_size"] = local.stat().st_size if local else None
        entry["expected_size"] = int(item.get("size") or 0) or None

        # Attempt the one missing part directly (bounded to a single try, no
        # retry loop) so a real botocore exception -- not just "it failed
        # three times" -- comes back in the response instead of only in
        # server logs we can't read from outside.
        part_size = 5 * 1024 * 1024
        size = int(item.get("size") or 0)
        part_ranges = _compute_part_ranges(size, part_size) if size else []
        total_parts = len(part_ranges)
        missing = sorted(set(range(1, total_parts + 1)) - uploaded_part_numbers)
        entry["missing_part_numbers"] = missing
        # SAFETY: only attempt the missing part when the local file's size
        # exactly matches the expected full size. An earlier version of
        # this endpoint attempted a part against whatever bytes happened to
        # be on disk -- confirmed live, that read past EOF of a local file
        # that was only 25MB into a re-download of a 52MB source (a
        # concurrent /drive/retry call was actively still downloading it on
        # the same replica) and silently uploaded a 0-BYTE part as if it
        # were real data (S3 accepted it without error, and this endpoint
        # reported "SUCCESS"). That bogus part would have been picked up as
        # already-uploaded by the real retry logic's list_parts check next
        # time, skipped, and baked into a truncated/corrupted final file by
        # complete_multipart_upload. See remediate-upload below for how
        # that got cleaned up. Never attempt against a partial local file.
        expected_size = int(item.get("size") or 0) or None
        local_complete = bool(local and expected_size and local.stat().st_size == expected_size)
        entry["local_file_complete"] = local_complete
        if local_complete and missing:
            part_number = missing[0]
            part_start, expected_part_bytes = part_ranges[part_number - 1]
            try:
                with local.open("rb") as handle:
                    handle.seek(part_start)
                    chunk = handle.read(expected_part_bytes)
                entry["attempted_part_number"] = part_number
                entry["attempted_chunk_bytes"] = len(chunk)
                if len(chunk) != expected_part_bytes:
                    entry["attempt_result"] = "SKIPPED_SIZE_MISMATCH"
                    entry["expected_part_bytes"] = expected_part_bytes
                else:
                    response = client.upload_part(
                        Bucket=S3_BUCKET, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk,
                    )
                    entry["attempt_result"] = "SUCCESS"
                    entry["attempt_etag"] = response.get("ETag")
            except Exception as exc:
                entry["attempt_result"] = "FAILED"
                entry["attempt_error"] = repr(exc)
                entry["attempt_error_str"] = str(exc)
        results.append(entry)
    return {"success": True, "data": results}


@router.post("/remediate-upload/{job_id}/{file_id}")
async def remediate_stuck_upload(job_id: str, file_id: str) -> dict:
    """One-off cleanup: abort a specific file's in-progress multipart
    upload and clear its resume state from the manifest, so the next
    /drive/retry starts that ONE file's upload fresh with a brand new
    upload_id.

    Needed after debug_upload_parts (an earlier version, before the
    local_file_complete safety check above) uploaded a 0-byte part against
    a still-downloading local file's stale size, polluting that upload's
    part list with bad data at the part number the real code would
    otherwise treat as already-done and skip re-uploading -- leaving no
    other way to make the next real completion attempt correct. Aborting
    discards the whole multipart upload (all parts, good and bad); the
    already-durable download itself (raw bytes on disk / the partial-
    download snapshot in S3) is untouched, so the next /drive/retry just
    redoes the S3 multipart upload from part 1, not the Drive download.
    Temporary; safe to remove once no longer needed.
    """
    job_id = job_id.strip()
    file_id = file_id.strip()
    manifest = await asyncio.to_thread(JobManager.load_durable_job_state, job_id)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"No durable job state found for {job_id}")
    resume_map = dict(manifest.get("s3_upload_resume") or {})
    info = resume_map.get(file_id)
    if not info:
        raise HTTPException(status_code=404, detail=f"No paused multipart upload recorded for file_id={file_id}")
    upload_id = str(info.get("upload_id") or "")
    items_by_id = {str(item.get("id") or ""): item for item in (manifest.get("drive_items_cache") or [])}
    item = items_by_id.get(file_id) or {}
    key = _drive_raw_key(job_id, item) if item else None
    result: dict = {"file_id": file_id, "upload_id": upload_id, "key": key}
    if key and upload_id:
        client = JobManager._create_s3_transfer_client()
        try:
            client.abort_multipart_upload(Bucket=S3_BUCKET, Key=key, UploadId=upload_id)
            result["abort"] = "SUCCESS"
        except Exception as exc:
            result["abort"] = "FAILED"
            result["abort_error"] = repr(exc)
    resume_map.pop(file_id, None)
    manifest["s3_upload_resume"] = resume_map
    # Write straight to durable state (not local manifest.json + JobManager
    # .update, which only durably persists on the specific replica that
    # happens to hold the local file) -- this must take effect regardless
    # of which replica answers the very next /drive/retry call.
    try:
        await asyncio.to_thread(JobManager._persist_durable_state, job_id, manifest)
        result["durable_state_updated"] = True
    except Exception as exc:
        result["durable_state_updated"] = False
        result["durable_state_error"] = repr(exc)
    # Best-effort local mirror too, for whichever replica happens to have
    # this job's local manifest.json already.
    job_folder = RAW_UPLOAD / job_id
    manifest_path = job_folder / "manifest.json"
    if manifest_path.exists():
        try:
            _write_manifest(job_folder, manifest)
        except Exception:
            pass
    return {"success": True, "data": result}
