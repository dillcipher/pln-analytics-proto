from __future__ import annotations

import asyncio
import json
import os
import traceback
import uuid
from datetime import datetime
from pathlib import Path

import aiofiles
import boto3
from botocore.exceptions import ClientError
from fastapi import UploadFile

from app.application.jobs.job_manager import JobManager
from app.application.jobs.job_status import JobStatus
from app.core.constants import RAW_UPLOAD
from app.etl.detector.detector import FileDetector

UPLOAD_FOLDER = RAW_UPLOAD
CHUNK_SIZE = 20 * 1024 * 1024
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "").strip()
S3_REGION = os.getenv("S3_REGION", "ap-southeast-1").strip()
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "").strip()
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "pln-analytics-uploads").strip()
S3_CHUNK_PREFIX = "chunks"
S3_JOB_PREFIX = "jobs"


def _create_s3_client():
    if not S3_ENDPOINT or not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
        raise RuntimeError("Supabase Storage S3 credentials are not configured.")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )


class UploadService:
    """Upload/assembly service.

    Large files are assembled locally from durable 5 MiB Supabase chunks.
    The assembled workbook is intentionally NOT uploaded as one large S3
    object: the provider's UploadPart/PutObject path rejects these large
    objects. The local workbook is consumed immediately by ETL, while the
    original chunks remain durable until ETL reports FINISHED.
    """

    COORDINATE_MASTER_FILES = {"to_prabayar.xlsx", "to_pascabayar.xlsx"}

    @staticmethod
    def _normalize_filename(filename: str) -> str:
        name = filename.lower().strip().replace("-", "_").replace(" ", "_")
        while "__" in name:
            name = name.replace("__", "_")
        return name

    @classmethod
    def _is_coordinate_master(cls, filename: str) -> bool:
        return cls._normalize_filename(filename) in cls.COORDINATE_MASTER_FILES

    @staticmethod
    def _safe_filename(filename: str | None) -> str:
        safe = (filename or "uploaded_file").replace("\\", "/").split("/")[-1]
        return safe or "uploaded_file"

    @staticmethod
    def _new_job_id() -> str:
        return datetime.now().strftime("JOB_%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]

    @staticmethod
    def _chunk_s3_key(upload_id: str, chunk_number: int) -> str:
        return f"{S3_CHUNK_PREFIX}/{upload_id}/{chunk_number:08d}.part"

    @staticmethod
    def _job_file_s3_key(job_id: str, filename: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/{filename}"

    @staticmethod
    def _job_manifest_s3_key(job_id: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/manifest.json"

    @staticmethod
    def _job_metadata_s3_key(job_id: str) -> str:
        return f"{S3_JOB_PREFIX}/{job_id}/job.json"

    @classmethod
    async def _s3_put_file(cls, local_path: Path, s3_key: str) -> None:
        def _put() -> None:
            client = _create_s3_client()
            with local_path.open("rb") as body:
                client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=body,
                    ContentType="application/octet-stream",
                )
        await asyncio.to_thread(_put)

    @classmethod
    async def _s3_download_file(cls, s3_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        def _download() -> None:
            client = _create_s3_client()
            response = client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            body = response["Body"]
            tmp = local_path.with_suffix(local_path.suffix + ".download")
            try:
                with tmp.open("wb") as target:
                    while True:
                        chunk = body.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        target.write(chunk)
                os.replace(tmp, local_path)
            finally:
                body.close()
                tmp.unlink(missing_ok=True)
        await asyncio.to_thread(_download)

    @classmethod
    async def _s3_head(cls, s3_key: str) -> bool:
        def _head() -> bool:
            try:
                _create_s3_client().head_object(Bucket=S3_BUCKET, Key=s3_key)
                return True
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return False
                raise
        return await asyncio.to_thread(_head)

    @classmethod
    async def _s3_put_json(cls, s3_key: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        await asyncio.to_thread(
            lambda: _create_s3_client().put_object(
                Bucket=S3_BUCKET, Key=s3_key, Body=body, ContentType="application/json"
            )
        )

    @classmethod
    async def _s3_get_json(cls, s3_key: str) -> dict:
        def _get() -> dict:
            response = _create_s3_client().get_object(Bucket=S3_BUCKET, Key=s3_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        return await asyncio.to_thread(_get)

    @classmethod
    async def _s3_chunk_count(cls, upload_id: str) -> int:
        """Discover durable chunk count when legacy metadata omitted it."""
        prefix = f"{S3_CHUNK_PREFIX}/{upload_id}/"

        def _count() -> int:
            client = _create_s3_client()
            paginator = client.get_paginator("list_objects_v2")
            indexes: list[int] = []
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = str(obj.get("Key", ""))
                    if not key.startswith(prefix) or not key.endswith(".part"):
                        continue
                    name = key.rsplit("/", 1)[-1].removesuffix(".part")
                    if name.isdigit():
                        indexes.append(int(name))
            if not indexes:
                return 0
            expected = list(range(max(indexes) + 1))
            if sorted(set(indexes)) != expected:
                raise FileNotFoundError(
                    f"Incomplete durable chunk source for {upload_id}: "
                    f"expected 0..{max(indexes):08d}, found {len(set(indexes))} chunks"
                )
            return max(indexes) + 1

        return await asyncio.to_thread(_count)

    @classmethod
    async def _s3_delete_prefix(cls, prefix: str) -> bool:
        def _delete() -> bool:
            try:
                client = _create_s3_client()
                paginator = client.get_paginator("list_objects_v2")
                keys: list[str] = []
                for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    keys.extend(str(x["Key"]) for x in page.get("Contents", []) if x.get("Key"))
                if not keys:
                    return True
                ok = True
                for offset in range(0, len(keys), 1000):
                    batch = keys[offset:offset + 1000]
                    try:
                        response = client.delete_objects(
                            Bucket=S3_BUCKET,
                            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": False},
                        )
                        failed = {x.get("Key") for x in response.get("Errors", []) if x.get("Key")}
                    except Exception:
                        failed = set(batch)
                    for key in failed:
                        try:
                            client.delete_object(Bucket=S3_BUCKET, Key=key)
                        except Exception:
                            ok = False
                    if failed:
                        ok = False
                return ok
            except Exception:
                traceback.print_exc()
                return False
        return await asyncio.to_thread(_delete)

    @classmethod
    def _inspect_file(cls, destination: Path, filename: str, content_type: str | None) -> dict:
        dataset = None
        month = None
        validation = {"status": "FAILED", "missing_columns": []}
        try:
            from app.etl.detector.month_resolver import MonthResolver
            from app.etl.validator.validator import DatasetValidator
            dataset = FileDetector.detect(destination)
            if dataset == FileDetector.UNKNOWN:
                validation = {"status": "FAILED", "missing_columns": [], "error": "Unable to detect dataset from filename."}
            else:
                month = MonthResolver.resolve(destination)
                validation = DatasetValidator.validate(destination, dataset)
        except Exception as exc:
            validation = {"status": "FAILED", "missing_columns": [], "error": str(exc)}
        is_coordinate_master = cls._is_coordinate_master(filename)
        if is_coordinate_master:
            month = None
            if dataset == FileDetector.UNKNOWN:
                dataset = FileDetector.CUSTOMER_LOCATION
        return {
            "filename": filename,
            "size": destination.stat().st_size,
            "content_type": content_type,
            "dataset": dataset,
            "month": month,
            "is_coordinate_master": is_coordinate_master,
            "validation": validation.get("status"),
            "missing_columns": validation.get("missing_columns", []),
            "error": validation.get("error"),
        }

    @classmethod
    async def save_files(cls, files: list[UploadFile]) -> dict:
        job_id = cls._new_job_id()
        job_folder = UPLOAD_FOLDER / job_id
        job_folder.mkdir(parents=True, exist_ok=True)
        uploaded = []
        for file in files:
            original = file.filename or "uploaded_file"
            safe = cls._safe_filename(original)
            destination = job_folder / safe
            async with aiofiles.open(destination, "wb") as output:
                while True:
                    data = await file.read(1024 * 1024)
                    if not data:
                        break
                    await output.write(data)
            metadata = cls._inspect_file(destination, safe, file.content_type)
            metadata["original_filename"] = original
            uploaded.append(metadata)
        return await cls._finalize_job(job_id, job_folder, uploaded)

    @classmethod
    async def save_chunk(cls, upload_id: str, filename: str, chunk_number: int, total_chunks: int, file: UploadFile) -> dict:
        """Store browser chunks locally; no object-storage round trip."""
        if chunk_number < 0 or total_chunks <= 0 or chunk_number >= total_chunks:
            raise ValueError("Invalid chunk_number/total_chunks")
        safe = cls._safe_filename(filename)
        temp_folder = UPLOAD_FOLDER / "_temp_chunks" / upload_id
        temp_folder.mkdir(parents=True, exist_ok=True)
        temp_path = temp_folder / f"{chunk_number:08d}.part"
        received = 0
        async with aiofiles.open(temp_path, "wb") as output:
            while True:
                data = await file.read(1024 * 1024)
                if not data:
                    break
                received += len(data)
                await output.write(data)
        return {
            "success": True,
            "upload_id": upload_id,
            "filename": safe,
            "chunk_number": chunk_number,
            "total_chunks": total_chunks,
            "received_bytes": received,
            "storage": "local",
        }

    @classmethod
    async def prepare_chunk_upload(cls, upload_id: str, filename: str, total_chunks: int, content_type: str | None = None) -> dict:
        """Validate locally staged chunks and create a local job."""
        if total_chunks <= 0:
            raise ValueError("total_chunks must be > 0")
        safe = cls._safe_filename(filename)
        chunk_folder = UPLOAD_FOLDER / "_temp_chunks" / upload_id
        missing = [
            index for index in range(total_chunks)
            if not (chunk_folder / f"{index:08d}.part").is_file()
        ]
        if missing:
            raise ValueError("Missing local chunks: " + ", ".join(map(str, missing[:20])))
        job_id = cls._new_job_id()
        job_folder = UPLOAD_FOLDER / job_id
        job_folder.mkdir(parents=True, exist_ok=True)
        metadata = {
            "upload_id": upload_id,
            "filename": safe,
            "original_filename": filename,
            "total_chunks": total_chunks,
            "content_type": content_type,
            "job_id": job_id,
            "status": "ASSEMBLY_QUEUED",
            "created_at": datetime.now().isoformat(),
            "storage": "local_chunks",
        }
        async with aiofiles.open(job_folder / "chunk_upload.json", "w", encoding="utf-8") as output:
            await output.write(json.dumps(metadata, indent=2))
        return {
            "success": True,
            "job_id": job_id,
            "uploaded_at": datetime.now().isoformat(),
            "total_files": 1,
            "files": [],
            "status": "ASSEMBLY_QUEUED",
        }

    @classmethod
    async def complete_chunk_upload(cls, upload_id: str, filename: str, total_chunks: int, content_type: str | None = None) -> dict:
        return await cls.prepare_chunk_upload(upload_id, filename, total_chunks, content_type)

    @classmethod
    async def assemble_chunk_upload(cls, upload_id: str, job_id: str, filename: str, total_chunks: int, content_type: str | None = None) -> dict:
        """Assemble already-uploaded local chunks without any S3 dependency."""
        safe = cls._safe_filename(filename)
        job_folder = UPLOAD_FOLDER / job_id
        destination = job_folder / safe
        manifest_path = job_folder / "manifest.json"
        chunk_folder = UPLOAD_FOLDER / "_temp_chunks" / upload_id
        try:
            job_folder.mkdir(parents=True, exist_ok=True)
            missing = [
                index for index in range(total_chunks)
                if not (chunk_folder / f"{index:08d}.part").is_file()
            ]
            if missing:
                raise FileNotFoundError(
                    "Missing local chunk(s): " + ", ".join(map(str, missing[:20]))
                )

            destination.unlink(missing_ok=True)
            async with aiofiles.open(destination, "wb") as output:
                for index in range(total_chunks):
                    source_path = chunk_folder / f"{index:08d}.part"
                    async with aiofiles.open(source_path, "rb") as source:
                        while True:
                            data = await source.read(1024 * 1024)
                            if not data:
                                break
                            await output.write(data)

            file_size = destination.stat().st_size
            dataset = FileDetector.detect(destination)
            is_coordinate_master = cls._is_coordinate_master(safe)
            if dataset == FileDetector.UNKNOWN and is_coordinate_master:
                dataset = FileDetector.CUSTOMER_LOCATION

            metadata = {
                "filename": safe,
                "size": file_size,
                "content_type": content_type,
                "dataset": dataset,
                "month": None,
                "is_coordinate_master": is_coordinate_master,
                "validation": "PENDING",
                "missing_columns": [],
                "error": None,
                "original_filename": filename,
                "storage": "local_chunks",
                "upload_id": upload_id,
                "total_chunks": total_chunks,
            }
            result = await cls._finalize_job(job_id, job_folder, [metadata])
            if not manifest_path.exists():
                raise FileNotFoundError(f"Manifest was not created: {manifest_path}")

            # Chunks are deleted only after a successful local assembly.
            for index in range(total_chunks):
                (chunk_folder / f"{index:08d}.part").unlink(missing_ok=True)
            try:
                chunk_folder.rmdir()
            except OSError:
                pass

            return {
                **result,
                "status": "ASSEMBLY_COMPLETED",
                "manifest_path": str(manifest_path),
                "storage": "local_chunks",
            }
        except Exception:
            try:
                JobManager.update(
                    job_folder=job_folder,
                    status=JobStatus.FAILED,
                    progress=0,
                    step="ASSEMBLY_FAILED",
                )
            except Exception:
                traceback.print_exc()
            raise

    @classmethod
    async def recover_assembled_job(cls, job_id: str, filename: str, content_type: str | None = None) -> dict:
        """Recover only from the current instance's local staging area."""
        safe = cls._safe_filename(filename)
        job_folder = UPLOAD_FOLDER / job_id
        destination = job_folder / safe
        manifest = job_folder / "manifest.json"
        if destination.exists() and manifest.exists():
            return {
                "success": True,
                "job_id": job_id,
                "status": "ASSEMBLY_COMPLETED",
                "manifest_path": str(manifest),
            }

        metadata_path = job_folder / "chunk_upload.json"
        if not metadata_path.exists():
            raise FileNotFoundError(
                f"No local source is available for job {job_id}. Upload the file again."
            )
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        upload_id = str(metadata.get("upload_id") or "")
        total_chunks = int(metadata.get("total_chunks") or 0)
        if not upload_id or total_chunks <= 0:
            raise FileNotFoundError(f"Local chunk metadata is invalid for job {job_id}")
        return await cls.assemble_chunk_upload(
            upload_id,
            job_id,
            safe,
            total_chunks,
            content_type or metadata.get("content_type"),
        )

    @classmethod
    async def _finalize_job(cls, job_id: str, job_folder: Path, uploaded: list[dict]) -> dict:
        manifest = {
            "job_id": job_id, "status": JobStatus.UPLOADED.value, "progress": 0,
            "current_step": "UPLOAD", "uploaded_at": datetime.now().isoformat(),
            "started_at": None, "finished_at": None, "total_files": len(uploaded),
            "processed_files": 0, "files": uploaded,
        }
        manifest_path = job_folder / "manifest.json"
        async with aiofiles.open(manifest_path, "w", encoding="utf-8") as output:
            await output.write(json.dumps(manifest, indent=2, default=str))
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found after creation: {manifest_path}")
        JobManager.update(job_folder=job_folder, status=JobStatus.UPLOADED, progress=0, step="UPLOAD")
        return {
            "success": True, "job_id": job_id, "uploaded_at": manifest["uploaded_at"],
            "total_files": len(uploaded), "files": uploaded,
        }
