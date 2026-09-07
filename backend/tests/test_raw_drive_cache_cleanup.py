import json

from app.infrastructure.storage import chunk_cleanup


class _FakePaginator:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def paginate(self, Bucket, Prefix):
        keys = sorted(k for k in self._store if k.startswith(Prefix))
        yield {"Contents": [{"Key": key} for key in keys]}


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data

    def close(self):
        pass


class _FakeS3Client:
    """Minimal in-memory stand-in for the boto3 S3 client, covering only
    the calls chunk_cleanup.py makes (get_paginator/list_objects_v2,
    get_object, delete_object)."""

    def __init__(self, store: dict[str, bytes]):
        self._store = store
        self.deleted_keys: list[str] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._store)

    def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self._store[Key])}

    def delete_object(self, Bucket, Key):
        self.deleted_keys.append(Key)
        self._store.pop(Key, None)


def _manifest(job_id: str, status: str, storage: str = "google_drive") -> bytes:
    return json.dumps({"job_id": job_id, "status": status, "storage": storage}).encode("utf-8")


def _seed_store() -> dict[str, bytes]:
    store: dict[str, bytes] = {}

    # A FINISHED Drive job: raw cache is stale and must be deleted.
    store["jobs/JOB_DONE/manifest.json"] = _manifest("JOB_DONE", "FINISHED")
    store["jobs/JOB_DONE/raw/file1_source.xlsx"] = b"x" * 10
    store["jobs/JOB_DONE/raw/file2_source.xlsx.chunk00000"] = b"y" * 10
    store["jobs/JOB_DONE/raw/file2_source.xlsx.manifest.json"] = b"{}"

    # Non-raw job state that must never be touched by raw cache cleanup.
    store["jobs/JOB_DONE/job.json"] = b"{}"
    store["jobs/JOB_DONE/etl_checkpoint.json"] = b"{}"
    store["jobs/JOB_DONE/processed/warehouse.duckdb"] = b"z" * 10

    # A still-running Drive job: raw cache must survive.
    store["jobs/JOB_RUNNING/manifest.json"] = _manifest("JOB_RUNNING", "MERGING")
    store["jobs/JOB_RUNNING/raw/file3_source.xlsx"] = b"a" * 10

    # A FAILED Drive job: deliberately excluded from TERMINAL_STATUSES
    # (a failed Drive job is still resumable straight from its raw cache).
    store["jobs/JOB_FAILED/manifest.json"] = _manifest("JOB_FAILED", "FAILED")
    store["jobs/JOB_FAILED/raw/file4_source.xlsx"] = b"b" * 10

    # A terminal non-Drive job: raw/ is a Drive-only concept, nothing to
    # clean, and this job's own (non-raw) prefix must be left alone.
    store["jobs/JOB_UPLOAD/manifest.json"] = _manifest("JOB_UPLOAD", "FINISHED", storage="supabase_chunks")
    store["jobs/JOB_UPLOAD/job.json"] = b"{}"

    return store


def test_cleanup_deletes_raw_cache_only_for_terminal_drive_job(monkeypatch):
    store = _seed_store()
    fake_client = _FakeS3Client(store)
    monkeypatch.setattr(chunk_cleanup, "_client", lambda: fake_client)

    result = chunk_cleanup.cleanup_finished_raw_drive_cache()

    assert result["jobs_cleaned"] == 1
    assert result["raw_objects_deleted"] == 3

    # The finished job's raw/ objects (single file + chunked file + its
    # per-file manifest sibling) are gone...
    assert "jobs/JOB_DONE/raw/file1_source.xlsx" not in store
    assert "jobs/JOB_DONE/raw/file2_source.xlsx.chunk00000" not in store
    assert "jobs/JOB_DONE/raw/file2_source.xlsx.manifest.json" not in store

    # ...but its job manifest, job.json, checkpoint, and processed/ output
    # were never touched.
    assert "jobs/JOB_DONE/manifest.json" in store
    assert "jobs/JOB_DONE/job.json" in store
    assert "jobs/JOB_DONE/etl_checkpoint.json" in store
    assert "jobs/JOB_DONE/processed/warehouse.duckdb" in store

    # A still-running Drive job's raw cache is untouched.
    assert "jobs/JOB_RUNNING/raw/file3_source.xlsx" in store

    # A FAILED Drive job's raw cache is untouched (still resumable).
    assert "jobs/JOB_FAILED/raw/file4_source.xlsx" in store

    # A terminal non-Drive job was skipped entirely (no raw/ prefix to
    # begin with; its own state was never listed for deletion).
    assert "jobs/JOB_UPLOAD/job.json" in store

    assert set(fake_client.deleted_keys) == {
        "jobs/JOB_DONE/raw/file1_source.xlsx",
        "jobs/JOB_DONE/raw/file2_source.xlsx.chunk00000",
        "jobs/JOB_DONE/raw/file2_source.xlsx.manifest.json",
    }


def test_cleanup_is_noop_without_s3_credentials(monkeypatch):
    monkeypatch.setattr(chunk_cleanup, "_client", lambda: None)
    result = chunk_cleanup.cleanup_finished_raw_drive_cache()
    assert result == {"jobs_scanned": 0, "raw_objects_deleted": 0, "jobs_cleaned": 0}


def test_cleanup_swallows_prefix_delete_failure(monkeypatch):
    store = _seed_store()
    fake_client = _FakeS3Client(store)

    def _boom(Bucket, Key):
        raise RuntimeError("simulated S3 outage")

    fake_client.delete_object = _boom  # every delete in the terminal job's prefix fails
    monkeypatch.setattr(chunk_cleanup, "_client", lambda: fake_client)

    # Must not raise -- cleanup failures are logged, never propagated.
    result = chunk_cleanup.cleanup_finished_raw_drive_cache()
    assert result["jobs_scanned"] >= 1
    # Nothing was actually removed since every delete_object call failed.
    assert "jobs/JOB_DONE/raw/file1_source.xlsx" in store
