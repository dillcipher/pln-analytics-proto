"""Durable ETL checkpoint helpers.

The API runtime has ephemeral disk, while job state and processed parquet are
stored in S3-compatible object storage. This module makes the ETL checkpoint
durable so a resumed Drive job can continue from completed dataset/month
boundaries instead of starting ETL from zero.
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import closing

import boto3
import psycopg
from botocore.client import Config
from pathlib import Path
from typing import Any

from app.core.constants import PROCESSED
from app.application.jobs.job_manager import (
    JOB_STATE_STORAGE,
    JobManager,
    S3_BUCKET,
    S3_JOB_PREFIX,
)

logger = logging.getLogger(__name__)


def _database_configured() -> bool:
    return bool(os.getenv("DATABASE_URL", "").strip())


def _database_connect():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(
        url,
        connect_timeout=max(3, int(os.getenv("CHECKPOINT_DB_CONNECT_TIMEOUT", "10"))),
        autocommit=True,
    )


def _ensure_database_checkpoint_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pln_etl_checkpoints (
                job_id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def _put_database_json(job_id: str, payload: dict[str, Any]) -> None:
    with closing(_database_connect()) as conn:
        _ensure_database_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pln_etl_checkpoints (job_id, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (job_id)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                """,
                (job_id, json.dumps(payload, ensure_ascii=False, default=str)),
            )


def _get_database_json(job_id: str) -> dict[str, Any] | None:
    with closing(_database_connect()) as conn:
        _ensure_database_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM pln_etl_checkpoints WHERE job_id = %s",
                (job_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    data = row[0]
    if isinstance(data, str):
        data = json.loads(data)
    return data if isinstance(data, dict) else None


def _secondary_configured() -> bool:
    return bool(os.getenv("SECONDARY_S3_ENDPOINT", "").strip() and os.getenv("SECONDARY_S3_ACCESS_KEY_ID", "").strip() and os.getenv("SECONDARY_S3_SECRET_ACCESS_KEY", "").strip() and os.getenv("SECONDARY_S3_BUCKET", "").strip())


def _secondary_client():
    if not _secondary_configured():
        return None
    return boto3.client("s3", endpoint_url=os.getenv("SECONDARY_S3_ENDPOINT", "").strip(), region_name=os.getenv("SECONDARY_S3_REGION", "ap-southeast-1").strip(), aws_access_key_id=os.getenv("SECONDARY_S3_ACCESS_KEY_ID", "").strip(), aws_secret_access_key=os.getenv("SECONDARY_S3_SECRET_ACCESS_KEY", "").strip(), config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "adaptive"}))


def _secondary_bucket() -> str:
    return os.getenv("SECONDARY_S3_BUCKET", "").strip()


def _put_secondary_json(key: str, payload: dict[str, Any]) -> None:
    client = _secondary_client()
    if client is None:
        raise RuntimeError("secondary durable storage is not configured")
    client.put_object(Bucket=_secondary_bucket(), Key=key, Body=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"), ContentType="application/json")


def _get_secondary_json(key: str) -> dict[str, Any] | None:
    client = _secondary_client()
    if client is None:
        return None
    response = client.get_object(Bucket=_secondary_bucket(), Key=key)
    body = response["Body"]
    try:
        data = json.loads(body.read().decode("utf-8"))
        return data if isinstance(data, dict) else None
    finally:
        body.close()

def checkpoint_key(job_id: str) -> str:
    return f"{S3_JOB_PREFIX}/{job_id}/etl_checkpoint.json"


def load_durable_checkpoint(job_id: str | None) -> dict[str, Any] | None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return None
    key = checkpoint_key(job_id)
    if JOB_STATE_STORAGE != "local":
        try:
            client = JobManager._create_s3_client()
            response = client.get_object(Bucket=S3_BUCKET, Key=key)
            body = response["Body"]
            try:
                data = json.loads(body.read().decode("utf-8"))
            finally:
                body.close()
            if isinstance(data, dict) and str(data.get("job_id") or "") == job_id:
                return data
        except Exception:
            logger.warning("PRIMARY DURABLE CHECKPOINT READ FAILED | JOB=%s", job_id)
    if _secondary_configured():
        try:
            data = _get_secondary_json(key)
            if isinstance(data, dict) and str(data.get("job_id") or "") == job_id:
                logger.warning("DURABLE CHECKPOINT RESTORED FROM SECONDARY | JOB=%s", job_id)
                return data
        except Exception:
            logger.warning("SECONDARY DURABLE CHECKPOINT READ FAILED | JOB=%s", job_id)
    if _database_configured():
        try:
            data = _get_database_json(job_id)
            if isinstance(data, dict) and str(data.get("job_id") or "") == job_id:
                logger.warning("DURABLE CHECKPOINT RESTORED FROM DATABASE | JOB=%s", job_id)
                return data
        except Exception:
            logger.warning("DATABASE DURABLE CHECKPOINT READ FAILED | JOB=%s", job_id, exc_info=True)
    return None

def save_durable_checkpoint(job_id: str | None, checkpoint: dict[str, Any]) -> None:
    job_id = str(job_id or checkpoint.get("job_id") or "").strip()
    if not job_id:
        return
    payload = dict(checkpoint)
    payload["job_id"] = job_id
    key = checkpoint_key(job_id)
    persisted = False
    errors: list[str] = []
    if JOB_STATE_STORAGE != "local":
        try:
            JobManager._put_json(key, payload)
            persisted = True
        except Exception as exc:
            errors.append(f"primary:{exc}")
            logger.warning("PRIMARY DURABLE CHECKPOINT SYNC FAILED | JOB=%s", job_id, exc_info=True)
    if _secondary_configured():
        try:
            _put_secondary_json(key, payload)
            persisted = True
        except Exception as exc:
            errors.append(f"secondary:{exc}")
            logger.warning("SECONDARY DURABLE CHECKPOINT SYNC FAILED | JOB=%s", job_id, exc_info=True)
    if _database_configured():
        try:
            _put_database_json(job_id, payload)
            persisted = True
        except Exception as exc:
            errors.append(f"database:{exc}")
            logger.warning("DATABASE DURABLE CHECKPOINT SYNC FAILED | JOB=%s", job_id, exc_info=True)
    if not persisted:
        raise RuntimeError("No durable checkpoint backend accepted checkpoint: " + " | ".join(errors))

def _persist_completed_outputs(checkpoint: dict[str, Any]) -> bool:
    """Upload each completed parquet before advertising its checkpoint durably."""
    try:
        from app.infrastructure.storage import processed_storage

        client = processed_storage._client()
        if client is None:
            return False

        durable = set(checkpoint.get("durable_outputs") or [])
        candidates: list[str] = []
        for section in ("phase1_completed", "phase2_completed"):
            for value in (checkpoint.get(section) or {}).values():
                if value:
                    candidates.append(str(value))

        for raw_path in candidates:
            path = Path(raw_path)
            if not path.exists() or not path.is_file():
                return False
            try:
                path.relative_to(PROCESSED)
            except ValueError:
                return False

            key = processed_storage._key(path)

            # Verify against S3 itself rather than trusting raw_path in
            # durable_outputs blindly. Confirmed live 2026-08-31: a
            # checkpoint saved before the upload-verification fix could
            # carry a "durable_outputs" entry for a file whose upload had
            # actually failed (the old bug silently recorded it as durable
            # regardless). That stale claim persists forward through every
            # checkpoint restore/save afterward -- durable_load restores it
            # from S3, and this loop used to trust it and skip re-uploading
            # the freshly-reprocessed file, reproducing the exact same
            # "processed but never actually durable" bug this file exists
            # to prevent. _already_uploaded does a real head_object (or, for
            # a chunked upload, reads back the manifest) so a false claim
            # self-heals on the next checkpoint save instead of persisting
            # forever.
            if processed_storage._already_uploaded(
                client, processed_storage.S3_BUCKET, key, path.stat().st_size
            ):
                durable.add(raw_path)
                checkpoint["durable_outputs"] = sorted(durable)
                continue

            suffix = path.suffix.lower()
            content_type = (
                "application/vnd.apache.parquet"
                if suffix == ".parquet"
                else "application/octet-stream"
            )
            uploaded = processed_storage._upload_object(
                client,
                processed_storage.S3_BUCKET,
                key,
                path,
                content_type,
            )
            if not uploaded:
                # _upload_object already logged "PROCESSED PERSIST DEFERRED"
                # with the specific reason. Do not advertise this checkpoint
                # as durable when the bytes are not confirmed in S3 -- the
                # caller (durable_save) only writes the durable checkpoint
                # metadata when this function returns True, so deferring
                # here means the next successful run will retry this same
                # file instead of silently treating a lost upload as saved.
                return False
            durable.add(raw_path)
            checkpoint["durable_outputs"] = sorted(durable)

        return True
    except Exception:
        logger.warning("DURABLE ETL OUTPUT PERSIST FAILED", exc_info=True)
        return False

def install_durable_checkpoint_patch() -> None:
    """Patch the existing orchestrator without changing its public contract."""
    from app.application.etl.etl_orchestrator import ETLOrchestrator

    if getattr(ETLOrchestrator, "_pln_durable_checkpoint_installed", False):
        return

    original_load = ETLOrchestrator._load_checkpoint.__func__
    original_save = ETLOrchestrator._save_checkpoint.__func__

    @classmethod
    def durable_load(cls, job_folder: Path, job_id: str | None) -> dict:
        local = original_load(cls, job_folder, job_id)
        has_state = bool(
            local.get("phase1_completed")
            or local.get("phase2_completed")
            or local.get("warehouse_refreshed")
            or local.get("finished")
        )
        if has_state:
            return local

        durable = load_durable_checkpoint(job_id)
        if durable:
            try:
                original_save(cls, job_folder, durable)
            except Exception:
                pass
            logger.warning(
                "DURABLE ETL CHECKPOINT RESTORED | JOB=%s | PHASE1=%s | PHASE2=%s",
                job_id,
                len(durable.get("phase1_completed") or {}),
                len(durable.get("phase2_completed") or {}),
            )
            return durable
        return local

    @classmethod
    def durable_save(cls, job_folder: Path, checkpoint: dict) -> None:
        original_save(cls, job_folder, checkpoint)

        # A durable checkpoint is valid only when every output it claims as
        # completed is already durable too. Otherwise a restarted container
        # could skip work whose parquet existed only on the dead instance.
        if not _persist_completed_outputs(checkpoint):
            logger.warning(
                "DURABLE CHECKPOINT DEFERRED | JOB=%s | completed outputs are not fully durable",
                checkpoint.get("job_id") or Path(job_folder).name,
            )
            return

        save_durable_checkpoint(
            checkpoint.get("job_id") or Path(job_folder).name,
            checkpoint,
        )

    ETLOrchestrator._load_checkpoint = durable_load
    ETLOrchestrator._save_checkpoint = durable_save
    ETLOrchestrator._pln_durable_checkpoint_installed = True
    logger.info("Durable ETL checkpoint patch installed.")


def _output_confirmed_in_s3(client, raw_output: str) -> bool:
    """True only if a group's output parquet is actually present in S3.

    This is called at *restore* time, potentially on a brand-new host (a
    fresh GitHub Actions runner, or a production container that just
    recycled) where the local parquet a checkpoint entry points at will
    almost never exist on disk -- ephemeral disk is exactly why the
    checkpoint had to become durable in the first place. So, unlike
    _persist_completed_outputs (which runs moments after producing the file
    locally, in the same process, and can compare an exact byte size),
    this can only check S3 directly for *something* at the expected key --
    a plain object (head_object) or, for a chunked upload, its manifest.
    Either is proof the group's output actually landed durably; neither
    requires the local file to exist.
    """
    try:
        from app.infrastructure.storage import processed_storage

        output_path = Path(str(raw_output))
        try:
            output_path.relative_to(PROCESSED)
        except ValueError:
            return False
        key = processed_storage._key(output_path)
        try:
            client.head_object(Bucket=processed_storage.S3_BUCKET, Key=key)
            return True
        except Exception:
            pass
        try:
            client.head_object(
                Bucket=processed_storage.S3_BUCKET,
                Key=key + processed_storage._MANIFEST_SUFFIX,
            )
            return True
        except Exception:
            return False
    except Exception:
        return False


def completed_source_filenames(checkpoint: dict[str, Any] | None) -> set[str]:
    """Return source filenames belonging to fully checkpointed non-DLPD groups.

    A group only counts as "completed" here -- safe to skip re-restoring its
    raw source files from the durable cache -- when its output parquet is
    actually confirmed present in S3, not merely because a `phase2_completed`
    entry exists for it. Confirmed live 2026-09-01: a GitHub Actions runner
    resuming a job restored only 10/29 raw files because this function
    trusted a stale `phase2_completed` entry (written by an earlier, partial
    run on the old host) whose parquet output was never actually uploaded --
    every ANEV month then failed with FileNotFoundError and was silently
    quarantined per-group, while the job still reported overall success.
    This mirrors the same S3-verification fix already applied to
    _persist_completed_outputs above, for the same class of bug: a checkpoint
    entry that claims durability without proof self-heals here by simply not
    being trusted, instead of persisting forever. Any entry that can't be
    verified (no S3 client, unexpected path, S3 error) is treated as NOT
    completed -- re-restoring a file that turns out to already be present is
    cheap; wrongly skipping one that's actually missing loses the whole
    group to a silent per-group quarantine later.
    """
    checkpoint = checkpoint or {}
    phase2 = checkpoint.get("phase2_completed") or {}
    if not phase2:
        return set()

    client = None
    try:
        from app.infrastructure.storage import processed_storage

        client = processed_storage._client()
    except Exception:
        logger.warning(
            "COMPLETED SOURCE FILENAMES | could not init S3 client for verification; "
            "treating no groups as completed",
            exc_info=True,
        )
        client = None

    completed: set[str] = set()
    skipped_unverified = 0
    for raw_key, raw_output in phase2.items():
        try:
            group = json.loads(raw_key)
        except Exception:
            continue
        dataset = str(group.get("dataset") or "").upper()
        # One DLPD workbook can feed many months. Never skip it merely because
        # some months completed; it is still required for an unfinished month.
        if dataset.startswith("DLPD"):
            continue

        if client is None or not raw_output or not _output_confirmed_in_s3(client, raw_output):
            skipped_unverified += 1
            continue

        for name in group.get("files") or []:
            if name:
                completed.add(str(name))

    if skipped_unverified:
        logger.warning(
            "COMPLETED SOURCE FILENAMES | %s group(s) had a phase2_completed "
            "entry with no confirmed S3 output; their raw files will be "
            "restored again rather than skipped",
            skipped_unverified,
        )
    return completed
