from __future__ import annotations

import base64
import concurrent.futures
import io
import json
import logging
import os
import threading
import time
import random
from pathlib import Path
from typing import Any

from app.etl.detector.detector import FileDetector

logger = logging.getLogger(__name__)


class DriveDownloadDeadlineExceeded(Exception):
    """Raised by GoogleDriveService.download_file when the caller-supplied
    per-HTTP-call time budget runs out. This is expected/benign under the
    bounded-chunk Drive sync design (see drive.py) -- it means "resume this
    file on the next /drive/retry call", not a real download failure, so
    callers must not record it as a FAILED file."""

GOOGLE_DRIVE_FOLDER_ID = os.getenv(
    "GOOGLE_DRIVE_FOLDER_ID",
    "1FhaB_rp04Xtj4uy0h7wfg6G_fOXgY4N-",
).strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
# Keep download buffers small on the production memory tier. Also bounds
# how much a single chunk read can overshoot the per-call deadline in
# download_file: the deadline is only checked BETWEEN whole chunks (a
# request already in flight for one chunk can't be cancelled mid-read), and
# a real 8MB chunk was observed to take up to ~30s on this host's
# connection to Google Drive -- smaller chunks make that worst-case
# overshoot smaller and more predictable.
DRIVE_DOWNLOAD_CHUNK_MB = max(1, min(8, int(os.getenv("DRIVE_DOWNLOAD_CHUNK_MB", "4"))))
DRIVE_UPLOAD_CHUNK_MB = max(5, min(16, int(os.getenv("DRIVE_UPLOAD_CHUNK_MB", "16"))))

# Large Excel batches must not download many 100-200 MB workbooks at once.
# The previous implementation forced a minimum of 8, so even a deployment
# configured with a lower value still ran eight concurrent downloads. That
# created unnecessary memory/network pressure on the small production tier.
try:
    _configured_download_concurrency = int(os.getenv("DRIVE_DOWNLOAD_CONCURRENCY", "2"))
except ValueError:
    _configured_download_concurrency = 2
os.environ["DRIVE_DOWNLOAD_CONCURRENCY"] = str(max(1, min(2, _configured_download_concurrency)))

_XLSX_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel.sheet.macroEnabled.12",
    "application/vnd.ms-excel",
}

# googleapiclient's underlying HTTP transport should not be shared across
# worker threads. Reuse one Drive client per worker thread instead of rebuilding
# discovery + credentials for every single 100-200 MB Excel file.
_THREAD_LOCAL = threading.local()
_CREDENTIAL_INFO: dict[str, Any] | None = None
_CREDENTIAL_INFO_LOCK = threading.Lock()

# httplib2.Http() has NO socket timeout by default (blocks forever on a
# stalled connection) unless one is passed explicitly. Confirmed in
# production: a Drive-sync retry chunk -- which re-lists the whole folder on
# every call -- sat with zero progress for minutes at a time with no error
# ever surfacing, consistent with this call hanging indefinitely rather than
# failing. Give every Drive API call a bounded timeout so a stalled network
# call raises promptly instead of blocking the request (and, on this small
# host, starving other concurrent requests of what little CPU there is).
#
# Raised from 30s to 60s: live testing with DRIVE_DOWNLOAD_CHUNK_MB=8 (the
# render.yaml value) showed single 8MB chunk reads hitting this timeout
# fairly often ("TimeoutError('The read operation timed out')", surfaced
# via DriveDownloadDeadlineExceeded.last_error) well before the chunk
# actually failed to transfer -- each timeout just burns an attempt and a
# backoff sleep for a chunk that likely would have completed given a
# little more time on this host's connection to Google's API. Now that
# DRIVE_SYNC_TIME_BUDGET_SECONDS is 150s (see drive.py), there is enough
# slack in a single call to let one chunk take up to 60s without starving
# the retry loop of attempts.
GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS = max(1, int(os.getenv("GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS", "60")))

# httplib2's `timeout=` above is a per-socket-operation (send/recv) timeout,
# NOT a cap on one next_chunk() call's total duration. Confirmed live in this
# session: the 745.9MB file in a 29-file Drive sync sat on a single
# /drive/retry call for 40+ minutes with the manifest completely frozen and
# no exception ever raised -- consistent with a slow/throttled connection
# that keeps trickling a few bytes at a time, each individual recv()
# finishing well inside 60s, while the FULL chunk (assembled from many such
# recv() calls) never finishes. Wrap each next_chunk() call in a background
# thread with its own hard wall-clock bound so a trickling chunk can never
# block this whole HTTP request past a predictable ceiling -- the outer
# per-call time budget (DRIVE_SYNC_TIME_BUDGET_SECONDS) can then actually do
# its job instead of never getting a chance to run.
GOOGLE_DRIVE_CHUNK_WALL_TIMEOUT_SECONDS = max(10, int(os.getenv("GOOGLE_DRIVE_CHUNK_WALL_TIMEOUT_SECONDS", "90")))

# Small, dedicated pool: chunk reads are submitted here so a timed-out one can
# be abandoned (its thread left to die on its own whenever the trickling
# socket op finally unblocks) without blocking the caller. Sized generously
# above DRIVE_DOWNLOAD_CONCURRENCY so an abandoned-but-still-running thread
# from a previous timeout never starves a new chunk read of a worker.
_CHUNK_READ_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="gdrive-chunk-read",
)


class GoogleDriveService:
    """Google Drive is the raw-data authority for both sync and browser uploads.

    Files are streamed to/from local disk. Supabase/S3 is not used for raw
    Excel files in this service.
    """

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    @classmethod
    def _credential_info(cls) -> dict[str, Any]:
        global _CREDENTIAL_INFO
        if _CREDENTIAL_INFO is not None:
            return _CREDENTIAL_INFO

        with _CREDENTIAL_INFO_LOCK:
            if _CREDENTIAL_INFO is not None:
                return _CREDENTIAL_INFO

            raw = GOOGLE_SERVICE_ACCOUNT_JSON
            if not raw and GOOGLE_SERVICE_ACCOUNT_JSON_B64:
                raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_JSON_B64).decode("utf-8")
            if not raw:
                raise RuntimeError(
                    "Google Drive is not configured. Set GOOGLE_SERVICE_ACCOUNT_JSON "
                    "or GOOGLE_SERVICE_ACCOUNT_JSON_B64."
                )

            try:
                _CREDENTIAL_INFO = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc

            return _CREDENTIAL_INFO

    @classmethod
    def _credentials(cls):
        from google.oauth2 import service_account

        credentials = getattr(_THREAD_LOCAL, "credentials", None)
        if credentials is None:
            credentials = service_account.Credentials.from_service_account_info(
                cls._credential_info(),
                scopes=cls.SCOPES,
            )
            _THREAD_LOCAL.credentials = credentials
        return credentials

    @classmethod
    def _client(cls):
        import httplib2
        from google_auth_httplib2 import AuthorizedHttp
        from googleapiclient.discovery import build

        client = getattr(_THREAD_LOCAL, "drive_client", None)
        if client is None:
            authorized_http = AuthorizedHttp(
                cls._credentials(),
                http=httplib2.Http(timeout=GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS),
            )
            client = build(
                "drive",
                "v3",
                http=authorized_http,
                cache_discovery=False,
            )
            _THREAD_LOCAL.drive_client = client
        return client

    @classmethod
    def _walk_xlsx_files(
        cls,
        root: str,
        deadline: float | None = None,
        resume_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Recursively walk a Drive folder tree for Excel files.

        Returns {"files": [...], "complete": bool, "resume_state": dict|None}.
        When `deadline` (a time.monotonic() timestamp) passes before the walk
        finishes, returns whatever has been found so far with complete=False
        and a resume_state that can be passed back in on a later call to
        continue exactly where this one left off, instead of restarting the
        walk from the root folder. Some Drive folders here are deep enough
        (an unknown, unbounded number of paginated list() calls, each
        individually bounded only by GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS) that
        an unbounded walk was observed in production to run past the
        platform's own request timeout with the connection killed before any
        result -- not even an error -- ever reached the caller.
        """
        service = cls._client()

        if resume_state:
            queue = list(resume_state.get("queue") or [root])
            seen_folders: set[str] = set(resume_state.get("seen_folders") or [])
            files: list[dict[str, Any]] = list(resume_state.get("files") or [])
        else:
            queue = [root]
            seen_folders = set()
            files = []

        def _paused() -> dict[str, Any]:
            return {
                "files": files,
                "complete": False,
                "resume_state": {"queue": queue, "seen_folders": list(seen_folders), "files": files},
            }

        while queue:
            if deadline is not None and time.monotonic() >= deadline:
                return _paused()
            current = queue.pop(0)
            if current in seen_folders:
                continue

            # Buffer this folder's own new files/subfolders locally and only
            # merge them into the shared files/queue lists once every page of
            # THIS folder has been fetched. Committing partial pages as they
            # arrive would duplicate files on resume (a paused call restarts
            # the interrupted folder from its first page next time, since
            # page_token itself isn't part of resume_state).
            folder_files: list[dict[str, Any]] = []
            folder_subfolders: list[str] = []
            page_token = None
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    # Retry this folder (from its first page) on the next call.
                    queue.insert(0, current)
                    return _paused()
                response = (
                    service.files()
                    .list(
                        q=f"'{current}' in parents and trashed = false",
                        spaces="drive",
                        pageSize=1000,
                        pageToken=page_token,
                        fields=(
                            "nextPageToken,files(id,name,mimeType,size,modifiedTime,"
                            "md5Checksum,webViewLink,parents,capabilities/canDownload)"
                        ),
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                    )
                    .execute()
                )

                for item in response.get("files", []):
                    mime = str(item.get("mimeType") or "")
                    name = str(item.get("name") or "")
                    if mime == "application/vnd.google-apps.folder":
                        folder_subfolders.append(str(item["id"]))
                        continue
                    if not name.lower().endswith((".xlsx", ".xlsm", ".xltx", ".xltm")):
                        continue
                    if mime not in _XLSX_MIME_TYPES and "google-apps" in mime:
                        continue
                    if item.get("capabilities", {}).get("canDownload") is False:
                        logger.warning("Drive file cannot be downloaded: %s", name)
                        continue
                    folder_files.append(item)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            files.extend(folder_files)
            queue.extend(folder_subfolders)
            seen_folders.add(current)

        files.sort(key=lambda item: (str(item.get("name", "")).lower(), str(item.get("id", ""))))
        return {"files": files, "complete": True, "resume_state": None}

    @classmethod
    def list_xlsx_files(cls, folder_id: str | None = None) -> list[dict[str, Any]]:
        """Recursively list Excel files under a Drive folder (runs to completion)."""
        root = (folder_id or GOOGLE_DRIVE_FOLDER_ID).strip()
        if not root:
            raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is empty.")
        return cls._walk_xlsx_files(root)["files"]

    @classmethod
    def list_xlsx_files_chunked(
        cls,
        folder_id: str | None = None,
        deadline: float | None = None,
        resume_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Time-budget-aware variant of list_xlsx_files for the bounded-chunk
        Drive sync flow. See _walk_xlsx_files for the return shape."""
        root = (folder_id or GOOGLE_DRIVE_FOLDER_ID).strip()
        if not root:
            raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is empty.")
        return cls._walk_xlsx_files(root, deadline=deadline, resume_state=resume_state)

    @classmethod
    def upload_file(
        cls,
        source: Path,
        filename: str,
        folder_id: str | None = None,
        mime_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload one local Excel file directly into the configured Drive folder."""
        from googleapiclient.http import MediaFileUpload

        source = Path(source)
        if not source.exists():
            raise FileNotFoundError(f"Local upload source does not exist: {source}")

        target_folder = (folder_id or GOOGLE_DRIVE_FOLDER_ID).strip()
        if not target_folder:
            raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is empty.")

        service = cls._client()
        body = {
            "name": filename,
            "parents": [target_folder],
        }
        media = MediaFileUpload(
            str(source),
            mimetype=mime_type or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            chunksize=DRIVE_UPLOAD_CHUNK_MB * 1024 * 1024,
            resumable=True,
        )
        created = (
            service.files()
            .create(
                body=body,
                media_body=media,
                fields="id,name,mimeType,size,modifiedTime,md5Checksum,webViewLink,parents,capabilities/canDownload",
                supportsAllDrives=True,
            )
            .execute()
        )
        logger.info(
            "DRIVE UPLOAD | filename=%s | file_id=%s | size=%s",
            filename,
            created.get("id"),
            created.get("size"),
        )
        return created

    @classmethod
    def _reset_thread_client(cls) -> None:
        """Discard a poisoned HTTP transport after a transient Drive failure."""
        for name in ("drive_client", "credentials"):
            try:
                delattr(_THREAD_LOCAL, name)
            except AttributeError:
                pass

    @classmethod
    def download_file(
        cls,
        file_id: str,
        destination: Path,
        deadline: float | None = None,
        expected_size: int | None = None,
    ) -> None:
        """Download one Drive file with bounded retries and disk-only buffering.

        A single 429/5xx response must not kill a multi-file ETL job. Google
        Drive can transiently throttle long sequential batches, so retry at the
        file boundary with exponential backoff and a fresh thread-local HTTP
        client.

        `deadline` is a `time.monotonic()` timestamp (typically the same
        per-HTTP-call time budget the caller is already tracking, see
        DRIVE_SYNC_TIME_BUDGET_SECONDS in drive.py). Confirmed in production:
        without this, a single large/slow workbook (files here range from a
        few MB to 700+ MB) or a run of transient failures burning through the
        exponential backoff (worst case ~12 attempts x up to 90s of sleep,
        many minutes) could keep this call running long after the outer
        per-chunk time budget was meant to expire, so the HTTP
        request/response the platform keeps alive never returned and the job
        manifest never advanced. When the deadline passes -- whether between
        chunks mid-download or between retry attempts -- this raises
        DriveDownloadDeadlineExceeded immediately (no further sleep/retry) so
        the caller can treat it the same as "ran out of time budget, resume
        on the next /drive/retry call" rather than a real failure.

        Resuming across calls: a large workbook's total download time can
        exceed one call's time budget many times over (confirmed live: a
        ~50-200MB file made zero net progress across several 45-90s calls
        under the old "restart from byte zero every time" design, since a
        DriveDownloadDeadlineExceeded pause discarded whatever had been
        downloaded so far). The partial temp file is now KEPT (not deleted)
        on a deadline-exceeded pause, and the next call/attempt continues it
        with an HTTP Range request instead of re-fetching from the start --
        MediaIoBaseDownload has no public API for this, so its internal
        `_progress` counter (which is what it uses to build each chunk's
        Range header) is seeded directly from the partial file's size. Only
        a genuine (non-deadline) failure discards the partial file, since a
        real error may mean its bytes are unreliable.
        """
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaIoBaseDownload

        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".drive-download")

        max_attempts = max(8, min(15, int(os.getenv("DRIVE_DOWNLOAD_MAX_ATTEMPTS", "12"))))
        base_delay = max(1.0, min(30.0, float(os.getenv("DRIVE_DOWNLOAD_RETRY_BASE_SECONDS", "2"))))
        # Diagnostic only, mirrors S3PersistDeadlineExceeded.last_error in
        # drive.py: a DriveDownloadDeadlineExceeded pause previously only
        # reported byte counts, never WHY no further progress was made, so
        # "stuck at the same byte offset across consecutive /drive/retry
        # calls" was indistinguishable from "genuinely just slow" without
        # raw server logs. Track the most recent chunk failure (if any) and
        # fold it into every DriveDownloadDeadlineExceeded raised below.
        last_error_detail: str | None = None
        last_status_code: int | None = None
        last_error_attempt: int | None = None

        for attempt in range(1, max_attempts + 1):
            if deadline is not None and time.monotonic() >= deadline:
                exc = DriveDownloadDeadlineExceeded(
                    f"Time budget exceeded before attempt {attempt}/{max_attempts} for file_id={file_id}"
                )
                exc.bytes_downloaded = temporary.stat().st_size if temporary.exists() else 0
                exc.total_bytes = expected_size
                exc.last_error = last_error_detail
                exc.last_status_code = last_status_code
                exc.last_error_attempt = last_error_attempt
                raise exc
            try:
                resume_offset = temporary.stat().st_size if temporary.exists() else 0
                if expected_size and resume_offset >= expected_size:
                    # A stale/oversized leftover from an earlier run -- not a
                    # valid resumable prefix. Start clean.
                    temporary.unlink(missing_ok=True)
                    resume_offset = 0
                service = cls._client()
                request = service.files().get_media(
                    fileId=file_id,
                    supportsAllDrives=True,
                )
                with temporary.open("ab" if resume_offset > 0 else "wb") as output:
                    downloader = MediaIoBaseDownload(
                        output,
                        request,
                        chunksize=DRIVE_DOWNLOAD_CHUNK_MB * 1024 * 1024,
                    )
                    if resume_offset > 0:
                        downloader._progress = resume_offset
                        logger.info(
                            "DRIVE DOWNLOAD RESUME | file_id=%s | resume_offset=%s",
                            file_id, resume_offset,
                        )
                    done = False
                    while not done:
                        if deadline is not None and time.monotonic() >= deadline:
                            exc = DriveDownloadDeadlineExceeded(
                                f"Time budget exceeded mid-download for file_id={file_id}"
                            )
                            exc.bytes_downloaded = int(getattr(downloader, "_progress", resume_offset) or resume_offset)
                            exc.total_bytes = expected_size
                            exc.last_error = last_error_detail
                            exc.last_status_code = last_status_code
                            exc.last_error_attempt = last_error_attempt
                            raise exc
                        # num_retries=0: MediaIoBaseDownload's own internal
                        # retry (previously 3) runs its exponential backoff
                        # INSIDE this single next_chunk() call, blind to
                        # `deadline` and to the outer attempt loop below --
                        # a chunk that keeps failing transiently could by
                        # itself burn well past this call's time budget
                        # before ever returning control to check it. Let a
                        # single chunk failure raise immediately instead;
                        # the attempt loop below already retries with its
                        # own deadline-aware backoff.
                        #
                        # Run it in a background thread with its own hard
                        # wall-clock bound (see GOOGLE_DRIVE_CHUNK_WALL_
                        # TIMEOUT_SECONDS above): httplib2's socket timeout
                        # alone does not bound a trickling transfer's total
                        # duration. On a timeout here the read is abandoned,
                        # not cancelled (Python cannot force-stop a blocked
                        # thread) -- `output` is closed and `temporary` is
                        # unlinked from under it first, so if the abandoned
                        # thread's write eventually goes through, it lands on
                        # a detached (POSIX-unlinked) inode nothing else will
                        # ever read, instead of corrupting the next attempt's
                        # fresh file at the same path.
                        chunk_future = _CHUNK_READ_EXECUTOR.submit(downloader.next_chunk, num_retries=0)
                        try:
                            status, done = chunk_future.result(timeout=GOOGLE_DRIVE_CHUNK_WALL_TIMEOUT_SECONDS)
                        except concurrent.futures.TimeoutError:
                            bytes_so_far = int(getattr(downloader, "_progress", resume_offset) or resume_offset)
                            try:
                                output.close()
                            except Exception:
                                pass
                            temporary.unlink(missing_ok=True)
                            raise TimeoutError(
                                f"Chunk read exceeded {GOOGLE_DRIVE_CHUNK_WALL_TIMEOUT_SECONDS}s wall-clock "
                                f"(slow/stalled connection) for file_id={file_id} at {bytes_so_far} bytes"
                            )
                        if status is not None:
                            logger.info(
                                "DRIVE DOWNLOAD | file_id=%s | progress=%s%% | attempt=%s/%s",
                                file_id,
                                int(status.progress() * 100),
                                attempt,
                                max_attempts,
                            )
                    output.flush()
                    os.fsync(output.fileno())

                if not temporary.exists() or temporary.stat().st_size <= 0:
                    raise RuntimeError("Drive returned an empty download.")
                if expected_size and temporary.stat().st_size != expected_size:
                    temporary.unlink(missing_ok=True)
                    raise RuntimeError(
                        f"Downloaded size mismatch for file_id={file_id}: "
                        f"expected {expected_size}, got {temporary.stat().st_size}"
                    )
                os.replace(temporary, destination)
                return

            except DriveDownloadDeadlineExceeded:
                # Keep `temporary` -- it is a valid resumable prefix for the
                # next attempt/call, not a failure.
                raise
            except HttpError as exc:
                status_code = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
                retryable = status_code in {0, 408, 409, 425, 429, 500, 502, 503, 504}
                detail = str(exc)
            except (OSError, TimeoutError, ConnectionError) as exc:
                status_code = 0
                retryable = True
                detail = repr(exc)
            except Exception as exc:
                status_code = 0
                retryable = True
                detail = repr(exc)

            last_error_detail = detail[:900]
            last_status_code = status_code
            last_error_attempt = attempt

            cls._reset_thread_client()

            if not retryable or attempt >= max_attempts:
                # Giving up on this file for good (non-retryable error, or
                # attempts exhausted): the partial bytes aren't a dependable
                # resume point to leave lying around for a future attempt at
                # this same file, so start clean next time.
                temporary.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Google Drive download failed for file_id={file_id} "
                    f"after {attempt}/{max_attempts} attempts "
                    f"(http_status={status_code or 'n/a'}): {detail}"
                )

            # A transient, retryable failure (connection reset, timeout,
            # 429/5xx, ...) -- NOT a reason to discard progress. next_chunk()
            # only advances _progress and writes to `output` AFTER a chunk
            # request succeeds (see MediaIoBaseDownload.next_chunk), so a
            # failed request never touches bytes already on disk -- the
            # temp file stays a valid, uncorrupted prefix regardless of why
            # this particular chunk failed. Confirmed live: on this host,
            # transient connection errors during a long download were common
            # enough that discarding on every one of them (the previous
            # behavior here) meant a large file could restart from byte zero
            # almost every attempt and never accumulate enough progress to
            # finish, even with the deadline-preserving resume above already
            # in place. Keep it and retry/resume from where it left off.

            delay = min(90.0, base_delay * (2 ** min(attempt - 1, 5)))
            delay += random.uniform(0.0, min(3.0, delay * 0.15))
            if deadline is not None and time.monotonic() + delay >= deadline:
                exc = DriveDownloadDeadlineExceeded(
                    f"Time budget would be exceeded waiting {delay:.1f}s before retry "
                    f"{attempt + 1}/{max_attempts} for file_id={file_id}"
                )
                exc.bytes_downloaded = temporary.stat().st_size if temporary.exists() else 0
                exc.total_bytes = expected_size
                exc.last_error = last_error_detail
                exc.last_status_code = last_status_code
                exc.last_error_attempt = last_error_attempt
                raise exc
            logger.warning(
                "DRIVE DOWNLOAD RETRY | file_id=%s | attempt=%s/%s | http_status=%s | sleep=%.1fs | error=%s",
                file_id,
                attempt,
                max_attempts,
                status_code or "n/a",
                delay,
                detail[:500],
            )
            time.sleep(delay)

        raise RuntimeError(f"Google Drive download exhausted retries for file_id={file_id}")

    @classmethod
    def detect_dataset_fast(cls, filename: str, path: Path) -> str:
        """Prefer filename detection; fall back to bounded schema detection."""
        detected = FileDetector.detect(Path(filename))
        if detected != FileDetector.UNKNOWN:
            return detected
        return FileDetector.detect(path)

    @classmethod
    def build_file_record(
        cls,
        item: dict[str, Any],
        destination: Path,
    ) -> dict[str, Any]:
        filename = str(item.get("name") or destination.name)
        dataset = cls.detect_dataset_fast(filename, destination)
        is_coordinate_master = FileDetector.is_coordinate_master(Path(filename))
        if is_coordinate_master:
            dataset = FileDetector.CUSTOMER_LOCATION

        return {
            "filename": destination.name,
            "original_filename": filename,
            "size": destination.stat().st_size,
            "content_type": item.get("mimeType"),
            "dataset": dataset,
            "month": None,
            "is_coordinate_master": is_coordinate_master,
            "validation": "PENDING",
            "missing_columns": [],
            "error": None,
            "storage": "google_drive",
            "drive_file_id": item.get("id"),
            "drive_modified_time": item.get("modifiedTime"),
            "drive_md5": item.get("md5Checksum"),
            "drive_web_view_link": item.get("webViewLink"),
        }
