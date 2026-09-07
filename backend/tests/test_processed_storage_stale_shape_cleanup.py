"""Regression tests for _reconcile_stale_shape() in processed_storage.py.

Root-caused 2026-09-04 while answering "does it matter how many times the
ETL is retriggered?": persist_processed_data() uploads each processed
parquet file under a deterministic key (dataset+month), so a straight
re-upload of the SAME shape never accumulates duplicates -- put_object just
overwrites in place. But a month's merged file can cross the
S3_MAX_PART_BYTES chunking threshold in either direction between two runs
of the same job (more/less source data, a dedup fix that changes row
count, a fix like a58b8a7/094ff4c changing how many rows survive):

  * grows past the threshold: run N was one direct object at `key`, run
    N+1 is now `key.part0000..NNNN` + `key.manifest.json` -- the old
    direct object is never deleted.
  * shrinks back under the threshold: the reverse -- the old manifest and
    parts are never deleted once a smaller run goes back to a plain
    put_object.
  * shrinks but STAYS chunked with fewer parts: the old run's higher-index
    parts (e.g. part0003, part0004) are never referenced by the new
    (smaller) manifest and never deleted.

Any of these would mean repeated ETL retriggers silently accumulate
storage even though every individual key is written deterministically.
_reconcile_stale_shape() is called after every real (non-skipped) upload
in persist_processed_data() specifically to close this gap.
"""

from __future__ import annotations

from app.infrastructure.storage.processed_storage import (
    _MANIFEST_SUFFIX,
    _reconcile_stale_shape,
)


class _FakePaginator:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def paginate(self, Bucket: str, Prefix: str):
        matches = [key for key in self._store if key.startswith(Prefix)]
        yield {"Contents": [{"Key": key} for key in matches]}


class FakeS3Client:
    """Minimal in-memory stand-in for the handful of boto3 S3 calls
    _reconcile_stale_shape() makes: delete_object and
    get_paginator("list_objects_v2").paginate(...)."""

    def __init__(self):
        self.store: dict[str, bytes] = {}

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.store.pop(Key, None)

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return _FakePaginator(self.store)


def test_switch_to_chunked_deletes_stale_direct_object_and_excess_old_parts():
    """Run N: one direct object at `key` (file was small enough). Run N+1:
    the same month's file grew past the chunking threshold -- 2 parts this
    time, but a HYPOTHETICAL earlier chunked run (or the direct object) left
    other objects behind. Both the stale direct object and any part index
    at or beyond this run's part count must be removed; parts still in use
    (0, 1) must survive."""

    client = FakeS3Client()
    key = "processed/dlpd/dlpd_prabayar_202606.parquet"
    client.store[key] = b"stale direct object from before chunking was needed"
    client.store[f"{key}.part0000"] = b"kept"
    client.store[f"{key}.part0001"] = b"kept"
    client.store[f"{key}.part0002"] = b"stale -- this run only has 2 parts (0,1)"
    client.store[f"{key}.part0003"] = b"stale -- this run only has 2 parts (0,1)"
    client.store[f"{key}{_MANIFEST_SUFFIX}"] = b'{"parts": 4, "size": 999}'

    _reconcile_stale_shape(client, "bucket", key, chunked=True, num_parts=2)

    assert key not in client.store
    assert f"{key}.part0000" in client.store
    assert f"{key}.part0001" in client.store
    assert f"{key}.part0002" not in client.store
    assert f"{key}.part0003" not in client.store
    # The manifest itself is rewritten by the caller right after upload,
    # not touched by reconciliation -- only excess parts and the old shape.


def test_switch_to_direct_deletes_stale_manifest_and_all_old_parts():
    """Run N was chunked (manifest + parts). Run N+1's file shrank back
    under the chunking threshold and was uploaded as one direct object --
    the old manifest and every old part must be removed."""

    client = FakeS3Client()
    key = "processed/dlpd/dlpd_prabayar_202606.parquet"
    client.store[key] = b"the fresh, correct, small direct object"
    client.store[f"{key}.part0000"] = b"stale"
    client.store[f"{key}.part0001"] = b"stale"
    client.store[f"{key}{_MANIFEST_SUFFIX}"] = b'{"parts": 2, "size": 999}'

    _reconcile_stale_shape(client, "bucket", key, chunked=False)

    assert key in client.store  # the just-uploaded direct object is untouched
    assert f"{key}.part0000" not in client.store
    assert f"{key}.part0001" not in client.store
    assert f"{key}{_MANIFEST_SUFFIX}" not in client.store


def test_reconcile_is_a_no_op_when_nothing_stale_exists():
    """The common case (no shape change since the last run): reconciliation
    must not raise or delete the object that was just uploaded."""

    client = FakeS3Client()
    key = "processed/anev/anev_202606.parquet"
    client.store[key] = b"the only object that has ever existed for this key"

    _reconcile_stale_shape(client, "bucket", key, chunked=False)

    assert key in client.store


def test_reconcile_never_raises_when_the_client_errors():
    """Best-effort by design: a delete failure must never bubble up and
    turn an already-successful upload into a reported persistence
    failure."""

    class ExplodingClient(FakeS3Client):
        def delete_object(self, Bucket: str, Key: str) -> None:
            raise RuntimeError("simulated transient storage error")

    client = ExplodingClient()
    key = "processed/dlpd/dlpd_prabayar_202606.parquet"
    client.store[f"{key}{_MANIFEST_SUFFIX}"] = b'{"parts": 1, "size": 1}'
    client.store[f"{key}.part0000"] = b"stale"

    _reconcile_stale_shape(client, "bucket", key, chunked=False)  # must not raise
