from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from botocore.exceptions import ClientError
import psycopg

from app.application.jobs.job_status import JobStatus

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "").strip()
S3_REGION = os.getenv("S3_REGION", "ap-southeast-1").strip()
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "pln-analytics-uploads").strip()
S3_JOB_PREFIX = "jobs"
JOB_STATE_STORAGE = os.getenv("JOB_STATE_STORAGE", "s3").strip().lower()
# Local development only. On a real deployment (FastAPI Cloud Hobby) the
# container's disk is ephemeral and gets recycled mid-job -- that is the
# whole reason JOB_STATE_STORAGE=="local" is treated as "not durable" and
# Drive sync refuses to run under it (see drive.py). A developer's own
# machine does not have that problem: nothing recycles its disk out from
# under a running process. This flag is an explicit, off-by-default opt-in
# so that distinction is a deliberate choice, never an accident -- it must
# never be set in a real deployment's environment variables.
ALLOW_LOCAL_DURABLE_JOBS = os.getenv("ALLOW_LOCAL_DURABLE_JOBS", "false").strip().lower() in {
    "1", "true", "yes", "y", "on",
}
DRIVE_RECOVERY_LEASE_SECONDS = max(300, int(os.getenv("DRIVE_RECOVERY_LEASE_SECONDS", "900")))
# A job that has not updated its durable manifest for this long is treated as
# abandoned during startup recovery. This prevents a crashed worker from
# permanently blocking every future Drive sync.
DRIVE_RECOVERY_STALE_SECONDS = max(
    3600,
    int(os.getenv("DRIVE_RECOVERY_STALE_SECONDS", "43200")),
)
# The old defaults here (connect 30s / read 60s / 3 adaptive-backoff retries)
# let a single S3 call take several minutes in the worst case -- confirmed in
# production: a Drive-sync chunk sat on "RESTORING DURABLE CACHE" with zero
# manifest movement for well over two minutes (twice), consistent with boto3
# still being inside its own retry/backoff cycle long after the frontend's
# own request timeout had already given up and moved on. Each S3 call here is
# a small JSON blob or a bounded 8-16MB chunk, so it should complete in a few
# seconds under normal conditions; fail fast instead so a genuinely stuck or
# unreachable endpoint surfaces as a per-file error (already handled -- the
# batch continues and the file can be retried) rather than a silent stall.
#
# Tightened again (10s/20s/2 attempts -> 5s/10s/1 attempt): one bounded Drive
# sync chunk can involve several SEQUENTIAL S3 calls (durable checkpoint load,
# durable job-state load, recovery-lock acquire, per-file cache restore,
# manifest persist on every progress update) that happen outside -- or before
# -- the per-chunk download deadline is even enforced. Even at the previous
# 10s/20s/2-attempts setting, a handful of those calls each hitting their own
# worst case could still stack up past the platform's own ~100-120s request
# timeout with the connection killed before anything was ever persisted
# (confirmed live: a /drive/retry call failed with a raw connection reset
# after ~125s and zero manifest movement, even though normal calls complete
# in well under 30s). One retryable attempt with a short timeout means a
# single slow S3 call now costs at most ~15s instead of up to ~60s; genuine
# failures are handled one level up by the application's own retry (the
# frontend calling /drive/retry again), so an internal boto3 retry here is
# redundant time, not resilience.
S3_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv("S3_CONNECT_TIMEOUT_SECONDS", "5")))
S3_READ_TIMEOUT_SECONDS = max(1, int(os.getenv("S3_READ_TIMEOUT_SECONDS", "10")))
S3_MAX_ATTEMPTS = max(1, int(os.getenv("S3_MAX_ATTEMPTS", "1")))

# The tight timeout above is for small metadata calls (job state, checkpoints,
# the recovery lock) and is deliberately too short for a raw Drive workbook
# upload/download -- confirmed in production: persisting an 11.6MB file hit
# ReadTimeoutError on all 3 attempts under the tightened 10s read timeout,
# even though the same transfer succeeded before it was tightened. Durable
# raw-file transfers (_persist_raw_drive_file / _hydrate_raw_drive_file in
# drive.py) can legitimately move tens to hundreds of MB over this host's
# slow connection, so they get their own, more generous client instead of
# inheriting the fast-fail metadata timeout.
S3_TRANSFER_READ_TIMEOUT_SECONDS = max(1, int(os.getenv("S3_TRANSFER_READ_TIMEOUT_SECONDS", "90")))
S3_TRANSFER_MAX_ATTEMPTS = max(1, int(os.getenv("S3_TRANSFER_MAX_ATTEMPTS", "2")))


class JobManager:
    """Central job-state manager with durable S3 job state and recovery leases."""

    @staticmethod
    def _database_configured() -> bool:
        return bool(os.getenv("DATABASE_URL", "").strip())

    @staticmethod
    def _database_connect():
        url = os.getenv("DATABASE_URL", "").strip()
        if not url:
            raise RuntimeError("DATABASE_URL is not configured")
        return psycopg.connect(
            url,
            connect_timeout=max(3, int(os.getenv("JOB_STATE_DB_CONNECT_TIMEOUT", "10"))),
            autocommit=True,
        )

    @staticmethod
    def _ensure_database_job_state_table(conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pln_job_states (
                    job_id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

    @staticmethod
    def _put_database_job_state(job_id: str, data: dict) -> None:
        with closing(JobManager._database_connect()) as conn:
            JobManager._ensure_database_job_state_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pln_job_states (job_id, payload, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (job_id)
                    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                    """,
                    (job_id, json.dumps(data, ensure_ascii=False, default=str)),
                )

    @staticmethod
    def _get_database_job_state(job_id: str) -> dict | None:
        with closing(JobManager._database_connect()) as conn:
            JobManager._ensure_database_job_state_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM pln_job_states WHERE job_id = %s",
                    (job_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _list_database_recoverable_drive_jobs() -> list[dict]:
        if not JobManager._database_configured():
            return []
        with closing(JobManager._database_connect()) as conn:
            JobManager._ensure_database_job_state_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM pln_job_states ORDER BY updated_at DESC"
                )
                rows = cur.fetchall()
        terminal = {"FINISHED", "CANCELLED", "COMPLETED"}
        jobs: list[dict] = []
        for row in rows:
            data = row[0]
            if isinstance(data, str):
                data = json.loads(data)
            if not isinstance(data, dict):
                continue
            if str(data.get("storage") or "").strip().lower() != "google_drive":
                continue
            if str(data.get("status") or "").strip().upper() in terminal:
                continue
            if bool(data.get("recovery_blocked")) or data.get("auto_retry") is False:
                continue
            job_id = str(data.get("job_id") or "").strip()
            folder_id = str(data.get("drive_folder_id") or "").strip()
            if not job_id or not folder_id:
                continue
            data["recovery_attempts"] = int(data.get("recovery_attempts", 0) or 0) + 1
            jobs.append(data)
        return jobs

    @staticmethod
    def load_durable_job_state(job_id: str) -> dict | None:
        """Load a job manifest from S3 first, then PostgreSQL fallback.

        Local development (ALLOW_LOCAL_DURABLE_JOBS=true): reads the same
        job.json that _persist_local() already writes on every update, and
        treats it as durable -- see the comment on ALLOW_LOCAL_DURABLE_JOBS
        above for why that is a reasonable thing to do only on a developer's
        own machine, never in a real deployment.
        """
        job_id = str(job_id or "").strip()
        if not job_id:
            return None
        if JOB_STATE_STORAGE == "local":
            if not ALLOW_LOCAL_DURABLE_JOBS:
                return None
            try:
                from app.core.constants import RAW_UPLOAD

                job_json = RAW_UPLOAD / job_id / "job.json"
                if job_json.exists():
                    with job_json.open(encoding="utf-8") as fh:
                        data = json.load(fh)
                    if isinstance(data, dict):
                        return data
            except Exception:
                logging.getLogger(__name__).warning(
                    "LOCAL DURABLE JOB STATE READ FAILED | JOB=%s", job_id, exc_info=True
                )
            return None
        if JOB_STATE_STORAGE != "local":
            try:
                client = JobManager._create_s3_client()
                response = client.get_object(
                    Bucket=S3_BUCKET,
                    Key=JobManager._job_manifest_key(job_id),
                )
                body = response["Body"]
                try:
                    data = json.loads(body.read().decode("utf-8"))
                finally:
                    body.close()
                if isinstance(data, dict):
                    return data
            except Exception as exc:
                # 2026-09-03: run #11 failed immediately with "not found in
                # durable storage" and this warning gave zero detail on
                # WHY the S3 get_object() call failed (transient 5xx that
                # exhausted retries vs. a genuine 404/NoSuchKey meaning the
                # manifest object itself is gone) -- log the real
                # exception so the next failure is diagnosable instead of
                # another guessing round.
                logging.getLogger(__name__).warning(
                    "PRIMARY JOB STATE READ FAILED | JOB=%s | error=%s: %s",
                    job_id,
                    type(exc).__name__,
                    exc,
                )
        if JobManager._database_configured():
            try:
                data = JobManager._get_database_job_state(job_id)
                if data:
                    logging.getLogger(__name__).warning(
                        "JOB STATE RESTORED FROM DATABASE | JOB=%s", job_id
                    )
                    return data
            except Exception:
                logging.getLogger(__name__).warning(
                    "DATABASE JOB STATE READ FAILED | JOB=%s", job_id, exc_info=True
                )
        return None

    @staticmethod
    def _create_s3_client(*, read_timeout: int | None = None, max_attempts: int | None = None):
        """Build an S3 client. Defaults are tuned for small, frequent metadata
        calls (fail fast); pass read_timeout/max_attempts explicitly for a
        raw-file transfer, which needs more time (see
        S3_TRANSFER_READ_TIMEOUT_SECONDS above)."""
        if not S3_ENDPOINT:
            raise RuntimeError("S3_ENDPOINT environment variable is not configured.")
        if not S3_ACCESS_KEY_ID:
            raise RuntimeError("S3_ACCESS_KEY_ID environment variable is not configured.")
        if not S3_SECRET_ACCESS_KEY:
            raise RuntimeError("S3_SECRET_ACCESS_KEY environment variable is not configured.")
        import boto3
        from botocore.client import Config
        return boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": max_attempts or S3_MAX_ATTEMPTS, "mode": "standard"},
                connect_timeout=S3_CONNECT_TIMEOUT_SECONDS,
                read_timeout=read_timeout or S3_READ_TIMEOUT_SECONDS,
                s3={"addressing_style": "path"},
            ),
        )

    @staticmethod
    def _create_s3_transfer_client():
        """S3 client tuned for a raw Drive workbook upload/download rather
        than a small metadata call -- see S3_TRANSFER_READ_TIMEOUT_SECONDS."""
        return JobManager._create_s3_client(
            read_timeout=S3_TRANSFER_READ_TIMEOUT_SECONDS,
            max_attempts=S3_TRANSFER_MAX_ATTEMPTS,
        )

    @staticmethod
    def _job_manifest_key(job_id: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/manifest.json"

    @staticmethod
    def _job_metadata_key(job_id: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/job.json"

    @staticmethod
    def _drive_recovery_lock_key(job_id: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/recovery.lock"

    @staticmethod
    def _extract_job_id(data: dict, job_folder: Path) -> str:
        return str(data.get("job_id") or job_folder.name)

    @staticmethod
    def _put_json(key: str, data: dict) -> None:
        client = JobManager._create_s3_client()
        body = json.dumps(data, ensure_ascii=False, indent=4, default=str).encode("utf-8")
        client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentLength=len(body), ContentType="application/json")

    @staticmethod
    def _persist_local(job_folder: Path, data: dict) -> None:
        job_folder = Path(job_folder)
        job_folder.mkdir(parents=True, exist_ok=True)
        job_json = job_folder / "job.json"
        temporary = job_json.with_suffix(".json.tmp")
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(data, output, ensure_ascii=False, indent=2, default=str)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, job_json)

    @staticmethod
    def _persist_durable_state(job_id: str, data: dict) -> None:
        if JOB_STATE_STORAGE == "local":
            JobManager._persist_local(Path(data.get("job_folder") or "."), data)
            return
        try:
            JobManager._persist_local(Path(data.get("job_folder") or "."), data)
        except Exception as exc:
            logging.getLogger(__name__).warning("LOCAL JOB STATE SYNC FAILED | JOB=%s | ERROR=%r", job_id, exc)
        job_metadata = {
            "job_id": job_id,
            "status": data.get("status"),
            "progress": data.get("progress", 0),
            "current_step": data.get("current_step", ""),
            "uploaded_at": data.get("uploaded_at"),
            "started_at": data.get("started_at"),
            "finished_at": data.get("finished_at"),
            "total_files": data.get("total_files", 0),
            "processed_files": data.get("processed_files", 0),
            "storage": data.get("storage"),
            "drive_folder_id": data.get("drive_folder_id"),
            "files": data.get("files", []),
            "recovery_attempts": data.get("recovery_attempts", 0),
            "last_error": data.get("last_error"),
            "last_failed_at": data.get("last_failed_at"),
            "job_folder": data.get("job_folder"),
            "updated_at": data.get("updated_at"),
        }
        try:
            JobManager._put_json(JobManager._job_metadata_key(job_id), job_metadata)
            JobManager._put_json(JobManager._job_manifest_key(job_id), data)
        except Exception:
            if not JobManager._database_configured():
                raise
            logging.getLogger(__name__).warning(
                "PRIMARY JOB STATE WRITE FAILED; USING DATABASE FALLBACK | JOB=%s",
                job_id,
                exc_info=True,
            )
            JobManager._put_database_job_state(job_id, data)

    @staticmethod
    def _new_recovery_lock_body(job_id: str) -> bytes:
        now = datetime.now(timezone.utc)
        return json.dumps(
            {
                "job_id": job_id,
                "token": uuid.uuid4().hex,
                "acquired_at": now.isoformat(),
                "expires_at": (now + timedelta(seconds=DRIVE_RECOVERY_LEASE_SECONDS)).isoformat(),
            },
            ensure_ascii=False,
        ).encode("utf-8")

    @staticmethod
    def acquire_drive_recovery_lock(job_id: str) -> bool:
        """Claim one unfinished Drive job across API replicas."""
        if JOB_STATE_STORAGE == "local":
            return True
        client = JobManager._create_s3_client()
        key = JobManager._drive_recovery_lock_key(job_id)
        body = JobManager._new_recovery_lock_body(job_id)
        try:
            client.put_object(
                Bucket=S3_BUCKET, Key=key, Body=body, ContentLength=len(body),
                ContentType="application/json", IfNoneMatch="*",
            )
            return True
        except Exception as exc:
            code = ""
            if isinstance(exc, ClientError):
                code = str(exc.response.get("Error", {}).get("Code", ""))
            else:
                # Backblaze B2's S3-compatible gateway has been observed
                # (live, 2026-09-03, reproduced twice in a row on the same
                # call) to respond to this conditional PUT (IfNoneMatch=*)
                # with a malformed/truncated HTTP response that botocore's
                # transport can't parse into a ClientError at all -- it
                # raises a raw ConnectionClosedError/BadStatusLine instead,
                # which used to fall straight past the PreconditionFailed
                # handling below into an unconditional failure. Treat it the
                # same as an ambiguous conditional-write outcome instead of
                # giving up outright: fall through to the same
                # inspect-the-existing-lock-then-plain-put path already used
                # for a real PreconditionFailed/409 response.
                logging.getLogger(__name__).warning(
                    "DRIVE RECOVERY LOCK conditional PUT raised a non-ClientError "
                    "(likely a Backblaze B2 If-None-Match quirk, not a genuine "
                    "lock conflict) -- falling back to inspect-then-plain-put | JOB=%s",
                    job_id, exc_info=True,
                )
            try:
                response = client.get_object(Bucket=S3_BUCKET, Key=key)
                existing_raw = response["Body"].read()
                existing = json.loads(existing_raw.decode("utf-8"))
                expires_at = datetime.fromisoformat(str(existing.get("expires_at")))
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if expires_at > datetime.now(timezone.utc):
                    return False
                client.delete_object(Bucket=S3_BUCKET, Key=key)
            except Exception:
                if code in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                    return False
                try:
                    client.head_object(Bucket=S3_BUCKET, Key=key)
                    return False
                except Exception:
                    pass
            try:
                client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentLength=len(body), ContentType="application/json")
                return True
            except Exception:
                logging.getLogger(__name__).warning("DRIVE RECOVERY LOCK FAILED | JOB=%s", job_id, exc_info=True)
                return False

    @staticmethod
    def _refresh_drive_recovery_lock(job_id: str) -> None:
        if JOB_STATE_STORAGE == "local":
            return
        try:
            client = JobManager._create_s3_client()
            key = JobManager._drive_recovery_lock_key(job_id)
            response = client.get_object(Bucket=S3_BUCKET, Key=key)
            body = response["Body"]
            try:
                lock = json.loads(body.read().decode("utf-8"))
            finally:
                body.close()
            lock["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=DRIVE_RECOVERY_LEASE_SECONDS)).isoformat()
            updated = json.dumps(lock, ensure_ascii=False).encode("utf-8")
            client.put_object(Bucket=S3_BUCKET, Key=key, Body=updated, ContentLength=len(updated), ContentType="application/json")
        except Exception:
            pass

    @staticmethod
    def release_drive_recovery_lock(job_id: str) -> None:
        if JOB_STATE_STORAGE == "local":
            return
        try:
            client = JobManager._create_s3_client()
            client.delete_object(Bucket=S3_BUCKET, Key=JobManager._drive_recovery_lock_key(job_id))
        except Exception:
            logging.getLogger(__name__).warning("DRIVE RECOVERY LOCK RELEASE FAILED | JOB=%s", job_id, exc_info=True)

    @staticmethod
    def _parse_manifest_updated_at(data: dict) -> datetime | None:
        raw = str(data.get("updated_at") or data.get("started_at") or data.get("uploaded_at") or "").strip()
        if not raw:
            return None
        try:
            value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _close_stale_drive_job(data: dict, reason: str) -> None:
        job_id = str(data.get("job_id") or "").strip()
        if not job_id:
            return
        now = datetime.now(timezone.utc).isoformat()
        data["status"] = JobStatus.FAILED.value
        data["current_step"] = "STALE GOOGLE DRIVE JOB AUTO-CLOSED"
        data["last_error"] = reason
        data["last_failed_at"] = now
        data["finished_at"] = now
        data["updated_at"] = now
        try:
            JobManager._put_json(JobManager._job_manifest_key(job_id), data)
            JobManager._put_json(
                JobManager._job_metadata_key(job_id),
                {
                    "job_id": job_id,
                    "status": data.get("status"),
                    "progress": data.get("progress", 0),
                    "current_step": data.get("current_step", ""),
                    "uploaded_at": data.get("uploaded_at"),
                    "started_at": data.get("started_at"),
                    "finished_at": data.get("finished_at"),
                    "total_files": data.get("total_files", 0),
                    "processed_files": data.get("processed_files", 0),
                    "storage": data.get("storage"),
                    "drive_folder_id": data.get("drive_folder_id"),
                    "files": data.get("files", []),
                    "recovery_attempts": data.get("recovery_attempts", 0),
                    "last_error": data.get("last_error"),
                    "last_failed_at": data.get("last_failed_at"),
                    "job_folder": data.get("job_folder"),
                    "updated_at": data.get("updated_at"),
                },
            )
        except Exception:
            logging.getLogger(__name__).warning("STALE DRIVE JOB STATE UPDATE FAILED | JOB=%s", job_id, exc_info=True)
        finally:
            JobManager.release_drive_recovery_lock(job_id)

    @staticmethod
    def list_recoverable_drive_jobs() -> list[dict]:
        """Return only non-terminal Drive jobs successfully claimed by this replica."""
        if JOB_STATE_STORAGE == "local":
            return []
        try:
            client = JobManager._create_s3_client()
        except Exception:
            logging.getLogger(__name__).warning(
                "PRIMARY DRIVE RECOVERY DISCOVERY FAILED; USING DATABASE FALLBACK",
                exc_info=True,
            )
            return JobManager._list_database_recoverable_drive_jobs()
        paginator = client.get_paginator("list_objects_v2")
        jobs: list[dict] = []
        # FAILED Drive jobs are resumable. Only an explicit cancellation or a
        # completed job is terminal for automatic recovery.
        terminal = {"FINISHED", "CANCELLED", "COMPLETED"}
        now = datetime.now(timezone.utc)
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_JOB_PREFIX}/"):
            for item in page.get("Contents", []):
                key = str(item.get("Key") or "")
                if not key.endswith("/manifest.json"):
                    continue
                try:
                    response = client.get_object(Bucket=S3_BUCKET, Key=key)
                    raw = response["Body"].read()
                    data = json.loads(raw.decode("utf-8"))
                except Exception:
                    logging.getLogger(__name__).exception("RECOVERY MANIFEST READ FAILED | KEY=%s", key)
                    continue
                if not isinstance(data, dict):
                    continue
                if str(data.get("storage", "")).strip().lower() != "google_drive":
                    continue
                if str(data.get("status", "")).strip().upper() in terminal:
                    continue
                # Permanent failures are intentionally paused instead of being
                # resurrected on every deployment. Their checkpoint and job id
                # remain intact and an explicit retry resumes the same job
                # after credentials/storage/input/code are fixed.
                if bool(data.get("recovery_blocked")) or data.get("auto_retry") is False:
                    logging.getLogger(__name__).warning(
                        "DRIVE RECOVERY PAUSED | JOB=%s | KIND=%s",
                        data.get("job_id"),
                        data.get("failure_kind"),
                    )
                    continue
                job_id = str(data.get("job_id") or "").strip()
                folder_id = str(data.get("drive_folder_id") or "").strip()
                if not job_id or not folder_id:
                    continue

                # Do not permanently close an old unfinished Drive job. A long
                # platform outage or repeated OOM must still be resumable from
                # the durable checkpoint when the service returns.
                if not JobManager.acquire_drive_recovery_lock(job_id):
                    continue
                data["recovery_attempts"] = int(data.get("recovery_attempts", 0) or 0) + 1
                jobs.append(data)
        return jobs

    @staticmethod
    def update(job_folder: Path, *, status: JobStatus, progress: int, step: str):
        manifest = job_folder / "manifest.json"
        with open(manifest, encoding="utf-8") as f:
            data = json.load(f)
        data["status"] = status.value if isinstance(status, JobStatus) else str(status)
        data["progress"] = int(progress)
        data["current_step"] = step
        data["job_folder"] = str(job_folder)
        data["updated_at"] = datetime.now().isoformat()
        if progress > 0 and data.get("started_at") is None:
            data["started_at"] = datetime.now().isoformat()
        if progress >= 100:
            data["finished_at"] = datetime.now().isoformat()
        temporary = manifest.with_suffix(".json.tmp")
        with open(temporary, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, manifest)
        job_id = JobManager._extract_job_id(data, job_folder)
        try:
            JobManager._persist_durable_state(job_id, data)
            if str(data.get("storage") or "").strip().lower() == "google_drive":
                if str(data.get("status") or "").upper() in {"FINISHED", "FAILED", "CANCELLED", "COMPLETED"}:
                    JobManager.release_drive_recovery_lock(job_id)
                else:
                    JobManager._refresh_drive_recovery_lock(job_id)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "DURABLE JOB STATE SYNC FAILED | JOB=%s | STORAGE=%s | ERROR=%r",
                job_id, JOB_STATE_STORAGE, exc,
            )
