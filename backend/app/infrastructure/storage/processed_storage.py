from __future__ import annotations

import io
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path

import boto3
from botocore.client import Config

from app.core.constants import PROCESSED, WAREHOUSE

logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "").strip()
S3_REGION = os.getenv("S3_REGION", "ap-southeast-1").strip()
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "pln-analytics-uploads").strip()
S3_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed").strip().strip("/")

# Supabase Storage (our S3-compatible backend, confirmed via
# deployment/render.yaml's S3_ENDPOINT comment) hard-caps a single object at
# 50MB on the Free plan -- not a default, a plan ceiling that cannot be
# raised without upgrading. Confirmed live: customer_location parquet files
# (~4.4MB each) always uploaded fine, while ANEV/DLPD/PENGECEKAN merged
# monthly parquet (source workbooks run 100-140MB each, several merged per
# month) never appeared in S3 across every check this project -- consistent
# with every such upload silently exceeding the 50MB cap and failing.
# Anything above this threshold is split into independently-uploaded
# ~40MB parts plus a ".manifest.json" marker (written last, only once every
# part is confirmed) instead of one oversized put_object call.
S3_MAX_PART_BYTES = int(os.getenv("S3_MAX_PART_BYTES", str(40 * 1024 * 1024)))
_PART_SUFFIX_RE = re.compile(r"\.part\d{4}$")
_MANIFEST_SUFFIX = ".manifest.json"


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
            retries={"max_attempts": 0, "mode": "standard"},
            connect_timeout=30,
            read_timeout=600,
            s3={"addressing_style": "path"},
        ),
    )


def _key(path: Path) -> str:
    return f"{S3_PREFIX}/{path.relative_to(PROCESSED).as_posix()}"


def _part_key(key: str, index: int) -> str:
    return f"{key}.part{index:04d}"


def debug_state() -> dict:
    """Read-only snapshot of local vs. durable processed-data state.

    Added to diagnose why /data-management/overview kept reporting every
    dataset UNAVAILABLE even after a Drive-sync job's ETL reached FINISHED --
    with no server shell access, this is the fastest way to see, from the
    outside, whether local parquet/warehouse files actually exist on
    whatever replica answers this call, and whether anything has actually
    landed in S3 under the processed prefix.
    """
    local_files = []
    if PROCESSED.exists():
        for path in sorted(PROCESSED.rglob("*")):
            if path.is_file():
                local_files.append({"path": str(path.relative_to(PROCESSED)), "size": path.stat().st_size})
    result = {
        "processed_dir": str(PROCESSED),
        "processed_dir_exists": PROCESSED.exists(),
        "local_files": local_files,
        "warehouse_path": str(WAREHOUSE),
        "warehouse_exists": WAREHOUSE.exists(),
        "warehouse_size": WAREHOUSE.stat().st_size if WAREHOUSE.exists() else None,
        "s3_configured": _client() is not None,
        "s3_objects": None,
        "s3_error": None,
    }
    client = _client()
    if client is not None:
        try:
            objects = []
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/"):
                for item in page.get("Contents", []):
                    objects.append({"key": item.get("Key"), "size": item.get("Size")})
            result["s3_objects"] = objects
        except Exception as exc:
            result["s3_error"] = repr(exc)
    return result


def _drop_page_cache(path: Path) -> None:
    """Advise the kernel it can evict this file's cached pages immediately.

    Confirmed live 2026-09-01: the FastAPI Cloud host (512MB Hobby tier)
    OOM-killed within milliseconds of finishing a ~20-file hydration burst
    from S3, right as DuckDB opened the just-downloaded parquet files for
    the warehouse view. Nothing in that window is individually expensive
    -- the killer is almost certainly the *page cache* built up while
    writing (and then immediately re-reading) tens to hundreds of MB of
    freshly downloaded files, which counts against the container's cgroup
    memory limit until the kernel reclaims it. Reclaim under pressure can
    lose the race against an allocation that pushes past the hard limit.
    POSIX_FADV_DONTNEED tells the kernel these pages are safe to drop right
    away instead of leaving them cached "just in case" -- the file is
    unaffected on disk, a later read just costs a normal (cheap, local
    disk) re-read instead of a cache hit. Best-effort: POSIX_FADV_DONTNEED
    isn't available on every platform, so any failure here is silently
    ignored rather than turning a successful download into a failure.
    """
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except Exception:
        return
    try:
        advise = getattr(os, "posix_fadvise", None)
        dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
        if advise is not None and dontneed is not None:
            advise(fd, 0, 0, dontneed)
    except Exception:
        pass
    finally:
        os.close(fd)


def _unique_temp_path(destination: Path) -> Path:
    """A per-call-unique sibling path to download into before an atomic swap.

    Confirmed live 2026-09-02: both callers of these download functions are
    reached through hydration entry points (ensure_hydrated()'s
    _HYDRATED_THIS_PROCESS flag, ensure_self_heal_hydrated()'s
    _SELF_HEAL_HYDRATION_DONE flag) whose own check-then-set is NOT atomic
    across threads -- two threads can both observe the flag as False before
    either sets it, so both call hydrate_processed_data() concurrently. When
    that happened, two threads downloading the SAME destination file (e.g.
    anev_202602.parquet) both used the OLD fixed temp name
    (``destination.with_suffix(destination.suffix + ".download")``) -- one
    thread's os.replace() moved that shared temp file into place, then the
    other thread's own os.replace() call on the (now-vanished, someone else
    already renamed it) temp file crashed with
    ``FileNotFoundError: ... '<file>.download' -> '<file>'``, and that
    replica's hydration silently lost that file for the rest of its
    lifetime (each subsequent request just moved on to other files instead
    of retrying).
    A per-call random suffix means concurrent downloads of the same
    destination never share a temp path -- each finishes into its own file
    and atomically replaces the destination independently (last writer
    wins, which is fine: both downloaded byte-identical content). This is
    the same "build in an isolated temp file, then atomic swap" pattern
    already used by Warehouse.refresh_tables()'s rebuild-<uuid>.duckdb.
    """
    return destination.parent / f"{destination.name}.{uuid.uuid4().hex}.download"


def _download_object(client, bucket: str, key: str, destination: Path) -> None:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = _unique_temp_path(destination)
    try:
        with temporary.open("wb") as fh:
            while True:
                chunk = body.read(8 * 1024 * 1024)
                if not chunk:
                    break
                fh.write(chunk)
        os.replace(temporary, destination)
        _drop_page_cache(destination)
    finally:
        body.close()
        temporary.unlink(missing_ok=True)


def _download_chunked_object(client, bucket: str, key: str, num_parts: int, destination: Path) -> None:
    """Reassemble a file that was split into ordered ``key.partNNNN`` objects."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = _unique_temp_path(destination)
    try:
        with temporary.open("wb") as fh:
            for index in range(num_parts):
                response = client.get_object(Bucket=bucket, Key=_part_key(key, index))
                body = response["Body"]
                try:
                    while True:
                        chunk = body.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        fh.write(chunk)
                finally:
                    body.close()
        os.replace(temporary, destination)
        _drop_page_cache(destination)
    finally:
        temporary.unlink(missing_ok=True)


def _already_uploaded(client, bucket: str, key: str, size: int) -> bool:
    if size > S3_MAX_PART_BYTES:
        try:
            response = client.get_object(Bucket=bucket, Key=key + _MANIFEST_SUFFIX)
            manifest = json.loads(response["Body"].read())
        except Exception:
            return False
        return int(manifest.get("size", -1)) == size
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return False
    return int(head.get("ContentLength") or -1) == size


def _upload_object(client, bucket: str, key: str, source: Path, content_type: str) -> bool:
    """One bounded, non-blocking persistence attempt.

    Processed ETL output stays valid locally even when object storage is down.
    The caller can mark the artifact as durable only when this function returns True.
    """
    source = Path(source)
    size = source.stat().st_size
    if size > S3_MAX_PART_BYTES:
        return _upload_object_chunked(client, bucket, key, source, size)
    try:
        with source.open("rb") as fh:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=fh,
                ContentLength=size,
                ContentType=content_type,
            )
        head = client.head_object(Bucket=bucket, Key=key)
        remote_size = int(head.get("ContentLength") or 0)
        if remote_size != size:
            raise RuntimeError(f"Post-upload verification failed for {key}: local={size} remote={remote_size}")
        return True
    except Exception as exc:
        logger.warning(
            "PROCESSED PERSIST DEFERRED | key=%s | local_artifact_kept=true | error=%r",
            key,
            exc,
            exc_info=True,
        )
        return False


def _upload_object_chunked(client, bucket: str, key: str, source: Path, size: int) -> bool:
    """Upload a file too large for one Supabase Storage object (50MB Free-plan
    cap) as several ``key.partNNNN`` objects, each individually size-verified.

    The ``key.manifest.json`` marker is written only after every part is
    confirmed present at its correct size -- its presence is what
    _already_uploaded/hydrate_processed_data treat as "this chunked upload is
    complete", so a run that dies partway through leaves only orphaned,
    ignored part objects rather than a manifest pointing at missing parts.
    """
    num_parts = (size + S3_MAX_PART_BYTES - 1) // S3_MAX_PART_BYTES
    uploaded_parts = 0
    try:
        with source.open("rb") as fh:
            for index in range(num_parts):
                chunk = fh.read(S3_MAX_PART_BYTES)
                part_key = _part_key(key, index)
                client.put_object(
                    Bucket=bucket,
                    Key=part_key,
                    Body=io.BytesIO(chunk),
                    ContentLength=len(chunk),
                    ContentType="application/octet-stream",
                )
                head = client.head_object(Bucket=bucket, Key=part_key)
                remote_size = int(head.get("ContentLength") or 0)
                if remote_size != len(chunk):
                    raise RuntimeError(
                        f"Post-upload verification failed for {part_key}: local={len(chunk)} remote={remote_size}"
                    )
                uploaded_parts += 1

        manifest_body = json.dumps({"parts": num_parts, "size": size}).encode("utf-8")
        client.put_object(
            Bucket=bucket,
            Key=key + _MANIFEST_SUFFIX,
            Body=io.BytesIO(manifest_body),
            ContentLength=len(manifest_body),
            ContentType="application/json",
        )
        return True
    except Exception as exc:
        logger.warning(
            "PROCESSED PERSIST DEFERRED (chunked) | key=%s | parts_uploaded=%s/%s | local_artifact_kept=true | error=%r",
            key,
            uploaded_parts,
            num_parts,
            exc,
            exc_info=True,
        )
        return False


def _reconcile_stale_shape(client, bucket: str, key: str, chunked: bool, num_parts: int = 0) -> None:
    """After a successful (re)upload, delete any objects left over from a
    DIFFERENT upload "shape" for this same key.

    A month's merged parquet file can cross the S3_MAX_PART_BYTES chunking
    threshold in either direction between two ETL runs of the SAME
    dataset+month (more/less source data, a dedup fix that changes row
    count, etc.) -- e.g. run N uploads it as 5 chunked ``key.partNNNN``
    objects + a manifest, run N+1 produces a smaller file that fits in one
    direct ``put_object`` at ``key``. Without this, the old shape's objects
    are never referenced again (hydrate_processed_data() only follows
    whichever shape is present under the CURRENT key/manifest) but keep
    sitting in the bucket forever, so repeated retriggers of the same job
    silently accumulate storage even though every individual upload
    correctly overwrote its own deterministic key. Mirrors the equivalent
    local-disk cleanup already done for stale ``_partNNNNN`` files in
    streaming_dlpd_publish_guard.py.

    Best-effort: any failure here is logged and swallowed -- the artifact
    that was JUST uploaded above is already durable and correct either way,
    this only reclaims space, so it must never turn a successful persist
    into a reported failure.
    """
    try:
        if chunked:
            # This key is now chunked -- a lone direct object under the
            # bare key (from before it needed chunking) is stale.
            try:
                client.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
            # Parts at or beyond this run's part count are leftovers from
            # a PREVIOUS run that produced a larger file (more parts).
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{key}.part"):
                for item in page.get("Contents", []):
                    part_key = item.get("Key") or ""
                    if not _PART_SUFFIX_RE.search(part_key):
                        continue
                    index = int(part_key[-4:])
                    if index >= num_parts:
                        client.delete_object(Bucket=bucket, Key=part_key)
        else:
            # This key is now a direct object -- any manifest + part
            # objects left from when it used to be chunked are stale.
            try:
                client.delete_object(Bucket=bucket, Key=key + _MANIFEST_SUFFIX)
            except Exception:
                pass
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{key}.part"):
                for item in page.get("Contents", []):
                    part_key = item.get("Key") or ""
                    if _PART_SUFFIX_RE.search(part_key):
                        client.delete_object(Bucket=bucket, Key=part_key)
    except Exception:
        logger.warning(
            "Stale-shape cleanup failed for key=%s (non-fatal, the freshly "
            "uploaded artifact is already durable) -- some old objects may "
            "remain until this key's shape changes again.",
            key,
            exc_info=True,
        )


_HYDRATED_THIS_PROCESS = False

# Guards the check-then-set on _HYDRATED_THIS_PROCESS and
# _SELF_HEAL_HYDRATION_DONE below. Held only for the instant it takes to
# check and flip a bool -- never across the actual hydrate_processed_data()
# S3 sync, which can take minutes -- so this cannot reintroduce the
# whole-worker-blocked-behind-one-slow-call regression that moving
# ensure_self_heal_hydrated() out of _REFRESH_LOCK (connection.py /
# warehouse.py) fixed earlier today. Its only job is making sure at most one
# thread ever wins the "should I hydrate" decision, instead of two threads
# both observing the flag as False and both calling hydrate_processed_data()
# concurrently -- confirmed live 2026-09-02: that race is what caused two
# threads to collide on _download_object/_download_chunked_object's shared
# temp file for the same destination (see _unique_temp_path's docstring),
# losing that file's hydration silently for the rest of the process's life.
# The unique-temp-file fix above already makes a concurrent double-download
# safe; this lock on top makes it also non-wasteful (no redundant full S3
# resync racing itself).
_HYDRATION_FLAG_LOCK = threading.Lock()

# Set by etl_execution.py's serialized_process() wrapper the moment THIS
# process starts running an ETL job through the single process-wide
# serialized executor (covers both the in-process backend upload path and
# the GitHub Actions runner -- both call run_etl_serialized() -> that
# wrapper). Lets ensure_self_heal_hydrated() (below) tell "this process is
# the ETL writer, its local state is authoritative" apart from "this
# process never ran ETL, its local state might just be a stale hydration
# or an incomplete on-demand self-heal build".
_IS_ETL_WRITER_PROCESS = False


def mark_process_as_etl_writer() -> None:
    global _IS_ETL_WRITER_PROCESS
    _IS_ETL_WRITER_PROCESS = True


def ensure_hydrated() -> None:
    """Restore processed parquet/warehouse artifacts from durable storage once
    per worker process, but only when this replica has none locally yet.

    FastAPI Cloud can autoscale to several replicas and scale each one to
    zero independently, so the replica that runs ETL (writes parquet +
    warehouse.duckdb to local disk and, via persist_processed_data, up to
    S3 -- see jobs.py's GET /jobs/{job_id} handler) is very often NOT the
    same replica that later answers a dashboard/data-management read.
    Confirmed live: a Drive-sync job's ETL reached FINISHED with real ANEV
    data, yet /data-management/overview kept reporting every dataset as
    UNAVAILABLE (0 rows) right after, because nothing ever called
    hydrate_processed_data() before serving a read -- this was defined but
    never invoked anywhere. Guarded on WAREHOUSE already existing locally so
    a replica that just wrote fresh data (or already hydrated once) never
    pulls a possibly-older durable snapshot over its own newer local state.
    """
    global _HYDRATED_THIS_PROCESS
    with _HYDRATION_FLAG_LOCK:
        if _HYDRATED_THIS_PROCESS:
            return
        _HYDRATED_THIS_PROCESS = True
    if WAREHOUSE.exists():
        return
    hydrate_processed_data()


_SELF_HEAL_HYDRATION_DONE = False


def ensure_self_heal_hydrated() -> None:
    """Force one real S3 sync before the on-demand self-heal path (in
    connection.py's _ensure_warehouse_tables() / warehouse.py's
    Warehouse.ensure_ready()) trusts local disk to decide what's missing.

    ensure_hydrated() above is guarded on WAREHOUSE already existing
    locally, which is correct for the replica that's actively writing ETL
    output (never clobber its fresh-but-not-yet-persisted local state with
    an older S3 copy) but WRONG for a replica that never ran ETL itself:
    once THAT replica's local warehouse.duckdb exists for ANY reason --
    including one built by the self-heal path itself, from whatever
    parquet happened to already be present -- ensure_hydrated() becomes a
    permanent no-op for the rest of that process's life, even after a
    later ETL run publishes a corrected, complete warehouse.duckdb (and
    any newly-added datasets' parquet) to S3.

    Confirmed live 2026-09-02: replica `pnjx2` kept failing with
    `_duckdb.CatalogException: Table with name fact_pengecekan does not
    exist!` and `_duckdb.BinderException: Values list "d" does not have a
    column named "IDPEL"` (the latter is what DuckDB's read_parquet()
    returns when a view's glob pattern currently matches zero local files
    -- i.e. fact_dlpd_prabayar's VIEW existed in this replica's stale local
    catalog, but the parquet files it points at were never hydrated here)
    long after ETL had already republished a correct, complete
    warehouse.duckdb to S3 -- this replica just never looked at S3 again to
    find out.

    Only called from the self-heal/read path, never from
    Warehouse.connect()'s or _open_connection()'s own ensure_hydrated()
    call, and skipped entirely on a process that mark_process_as_etl_writer()
    has flagged as the ETL writer -- so this can never race with or
    overwrite an in-progress (not yet persisted to S3) ETL run on this same
    process. Runs the real, unguarded hydrate_processed_data() at most once
    per process.
    """
    global _SELF_HEAL_HYDRATION_DONE
    with _HYDRATION_FLAG_LOCK:
        if _SELF_HEAL_HYDRATION_DONE or _IS_ETL_WRITER_PROCESS:
            return
        _SELF_HEAL_HYDRATION_DONE = True
    hydrate_processed_data()


def hydrate_processed_data() -> int:
    client = _client()
    if client is None:
        logger.warning("Processed storage is not configured; using local data only.")
        return 0
    PROCESSED.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        all_keys = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/"):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key and not key.endswith("/"):
                    all_keys.append(key)

        for key in all_keys:
            if _PART_SUFFIX_RE.search(key):
                # Reassembled below via its .manifest.json entry.
                continue

            if key.endswith(_MANIFEST_SUFFIX):
                base_key = key[: -len(_MANIFEST_SUFFIX)]
                relative = base_key[len(f"{S3_PREFIX}/"):]
                destination = PROCESSED / relative
                try:
                    response = client.get_object(Bucket=S3_BUCKET, Key=key)
                    manifest = json.loads(response["Body"].read())
                    num_parts = int(manifest["parts"])
                    _download_chunked_object(client, S3_BUCKET, base_key, num_parts, destination)
                    downloaded += 1
                    logger.info("Hydrated processed artifact (chunked, %s parts): %s", num_parts, relative)
                except Exception:
                    logger.exception("Failed to hydrate chunked processed artifact: %s", relative)
                continue

            relative = key[len(f"{S3_PREFIX}/"):]
            destination = PROCESSED / relative
            _download_object(client, S3_BUCKET, key, destination)
            downloaded += 1
            logger.info("Hydrated processed artifact: %s", relative)
    except Exception:
        logger.exception("Failed to hydrate processed data from object storage.")

    # Release boto3/urllib3 response buffers before the caller immediately
    # turns around and opens these same freshly-downloaded files again
    # (Warehouse view creation runs right after this on the self-heal
    # path) -- cheap, and one less thing competing for headroom on a
    # memory-constrained host during that exact window.
    import gc
    gc.collect()

    logger.info("Processed data hydration completed: %s file(s).", downloaded)
    return downloaded


def persist_processed_data(time_budget_seconds: float | None = 60.0) -> int:
    """Persist available processed artifacts without blocking ETL on storage outages.

    Bounded by `time_budget_seconds` (checked between files, same pattern as
    the Drive raw-file persist path) because warehouse.duckdb and per-dataset
    parquet files can be large and this host's upload throughput is slow --
    confirmed live: an unbounded call here got cut off by the platform's
    connection kill partway through. Each file is skipped if an object of the
    same size already exists at its key, so repeated calls (this already
    runs on every GET /jobs/{job_id} poll for a FINISHED job, see jobs.py)
    make real incremental progress across calls instead of restarting from
    the first file every time.
    """
    client = _client()
    if client is None:
        logger.warning("Processed storage unavailable; processed data remains local for this runtime.")
        return 0
    deadline = time.monotonic() + time_budget_seconds if time_budget_seconds is not None else None
    uploaded = 0
    deferred = 0
    skipped = 0
    candidates = [path for path in PROCESSED.rglob("*") if path.is_file()]
    if WAREHOUSE.exists() and WAREHOUSE not in candidates:
        candidates.append(WAREHOUSE)
    for path in candidates:
        if deadline is not None and time.monotonic() >= deadline:
            logger.info(
                "Processed data persistence paused (time budget): %s file(s) remaining, will resume on next call.",
                len(candidates) - uploaded - deferred - skipped,
            )
            break
        content_type = "application/octet-stream"
        if path.suffix.lower() == ".parquet":
            content_type = "application/vnd.apache.parquet"
        elif path.suffix.lower() == ".json":
            content_type = "application/json"
        elif path.suffix.lower() == ".duckdb":
            content_type = "application/vnd.duckdb"
        key = _key(path)
        size = path.stat().st_size
        if _already_uploaded(client, S3_BUCKET, key, size):
            skipped += 1
            continue
        if _upload_object(client, S3_BUCKET, key, path, content_type):
            uploaded += 1
            chunked = size > S3_MAX_PART_BYTES
            num_parts = (size + S3_MAX_PART_BYTES - 1) // S3_MAX_PART_BYTES if chunked else 0
            _reconcile_stale_shape(client, S3_BUCKET, key, chunked, num_parts)
        else:
            deferred += 1
    logger.info(
        "Processed data persistence completed: uploaded=%s skipped=%s deferred=%s local_artifacts_retained=true",
        uploaded,
        skipped,
        deferred,
    )
    return uploaded


def persist_processed_data_until_done(
    per_call_budget_seconds: float = 240.0,
    overall_timeout_seconds: float = 3 * 3600.0,
    max_rounds: int = 200,
) -> dict:
    """Call `persist_processed_data` repeatedly until nothing is left to upload.

    Why this exists: `persist_processed_data`'s default 60s time budget is
    deliberately short so it never blocks the request-response cycle of
    `GET /jobs/{job_id}` on the production API host (see that function's
    docstring) -- it relies on being called again on *every* subsequent poll
    to make incremental progress. `etl_orchestrator.py`'s own one-shot call
    at the end of `process()` inherits that same 60s default for the exact
    same non-blocking reason.

    A GitHub Actions run (see scripts/run_etl_from_github_actions.py) has no
    such caller polling it afterwards -- the script does one merge and exits,
    and the runner's entire filesystem is destroyed the moment it does.
    Confirmed live on run #17 (2026-09-03): the orchestrator's single 60s
    call uploaded only 109 of ~6227 freshly-processed files before pausing
    on the time budget ("Processed data persistence paused (time budget):
    6103 file(s) remaining, will resume on next call.") -- a "next call"
    that never came, so ~98% of that run's new ANEV/DLPD_PASCABAYAR/
    PENGECEKAN output was silently lost: never durable in object storage,
    and gone from local disk the instant the runner shut down. The run still
    reported overall "Success" because persistence failures are deliberately
    non-fatal (see the try/except around the orchestrator's call) -- so this
    is invisible unless someone reads the log lines above, not something a
    green workflow badge would ever surface.

    This function is the fix for that specific gap: after the merge itself
    finishes, sweep with a much longer per-call budget, in a loop, until a
    call reports the same zero-progress result twice in a row (skips-only
    passes are cheap -- a HEAD-style existence check per file -- so one
    extra confirming pass costs little) or a generous overall wall-clock/
    round-count safety cap is hit (so a persistent storage outage can't hang
    the runner for its full 350-minute job timeout). Only call this from an
    offline, one-shot runner context -- never from the request-scoped API
    host, where it would reintroduce the exact blocking behaviour
    `persist_processed_data`'s short default budget exists to avoid.
    """
    deadline = time.monotonic() + overall_timeout_seconds
    rounds = 0
    total_uploaded = 0
    consecutive_zero_rounds = 0
    while rounds < max_rounds and time.monotonic() < deadline:
        rounds += 1
        uploaded = persist_processed_data(time_budget_seconds=per_call_budget_seconds)
        total_uploaded += uploaded
        if uploaded == 0:
            consecutive_zero_rounds += 1
            if consecutive_zero_rounds >= 2:
                break
        else:
            consecutive_zero_rounds = 0
    finished_cleanly = rounds < max_rounds and time.monotonic() < deadline
    logger.info(
        "Processed data persistence sweep finished: rounds=%s total_uploaded=%s finished_cleanly=%s",
        rounds,
        total_uploaded,
        finished_cleanly,
    )
    return {"rounds": rounds, "total_uploaded": total_uploaded, "finished_cleanly": finished_cleanly}
