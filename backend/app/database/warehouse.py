from __future__ import annotations

import logging
import os
import shutil
import threading
import uuid
from pathlib import Path

import duckdb

from app.core.constants import (
    PARQUET,
    WAREHOUSE,
)
from app.database.warehouse_file_lock import WAREHOUSE_FILE_LOCK
from app.infrastructure.storage.processed_storage import (
    ensure_hydrated,
    ensure_self_heal_hydrated,
)

logger = logging.getLogger(__name__)

# Guards Warehouse.ensure_ready()'s check-then-rebuild sequence so two
# threads racing in on a cold worker (e.g. two dashboard requests landing
# right after boot) can't both decide the same view is missing and both
# pay to rebuild it at once -- see ensure_ready()'s docstring.
_REFRESH_LOCK = threading.Lock()


class Warehouse:
    """
    DuckDB Warehouse.

    Processed parquet files are the source of truth. The warehouse exposes
    them as lazy DuckDB views instead of copying the full datasets into a
    second in-memory table. This is required for the production 500 MB
    memory tier, especially for the large Pascabayar workbook.

    These two limits were hardcoded before 2026-09-06, which meant every
    DLPD query ran single-threaded with a 192MB cap EVERYWHERE, including a
    developer's own laptop during local development -- confirmed live to be
    the dominant cause of the DLPD Monitoring page appearing to hang for
    several minutes with "Semua Bulan" selected (a full, filterless scan of
    the largest dataset). Now env-overridable so local development can use
    the machine's real CPU/RAM; the defaults below are unchanged, so
    production keeps the exact same conservative behavior unless its own
    environment variables are explicitly set otherwise.
    """

    _DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "192MB").strip()
    _DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "1"))

    @classmethod
    def connect(cls) -> duckdb.DuckDBPyConnection:
        # Pull down parquet/warehouse artifacts from durable storage first if
        # this replica doesn't have them locally yet -- see ensure_hydrated's
        # docstring. Cheap no-op once this process has already hydrated (or
        # already has local data from running ETL itself).
        ensure_hydrated()
        return cls._connect_to_path(WAREHOUSE)

    @classmethod
    def _connect_to_path(cls, path: Path) -> duckdb.DuckDBPyConnection:
        """Open a read-write DuckDB connection to ``path`` with the
        warehouse's standard PRAGMAs applied.

        Shared by ``connect()`` (the live WAREHOUSE file) and
        ``refresh_tables()`` (an isolated temp-file rebuild copy -- see its
        docstring) so both get identical settings.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_dir = path.parent / "duckdb_tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Opening DuckDB connection: %s", path)
        # See warehouse_file_lock's module docstring: this file's own
        # connect()/_connect_to_path() and connection.py's
        # _open_connection() must never call duckdb.connect() against the
        # SAME path at the same moment, from any thread, in any module --
        # DuckDB can refuse the second call outright (differing
        # configuration) or, confirmed live 2026-09-02, even fail on a
        # same-configuration reconnect that follows a close() too closely
        # ("Unique file handle conflict"). A rebuild's temp path is unique
        # per call (nobody else ever connects to it), so this only matters
        # in practice for WAREHOUSE itself -- but locking unconditionally
        # here keeps this one code path correct for both cases without the
        # caller needing to know which.
        with WAREHOUSE_FILE_LOCK:
            connection = duckdb.connect(str(path))

        connection.execute(
            f"SET memory_limit = '{cls._DUCKDB_MEMORY_LIMIT}'"
        )
        connection.execute(f"SET threads = {cls._DUCKDB_THREADS}")
        connection.execute("SET preserve_insertion_order = false")

        escaped_temp_dir = str(temp_dir).replace("'", "''")
        connection.execute(
            f"SET temp_directory = '{escaped_temp_dir}'"
        )

        return connection

    # REVERTED 2026-09-01 (see below) -- every dataset needs union_by_name.
    #
    # History: this was briefly narrowed to just the two DLPD datasets, on
    # the assumption that fact_anev/fact_pengecekan/fact_customer_location
    # are each produced by one stable transformer with a fixed column set
    # and therefore never legitimately evolve schema across monthly
    # partitions. That assumption was WRONG for fact_anev in production:
    # dropping union_by_name there made read_parquet's default
    # (schema-by-position-from-one-file) resolution pick a partition whose
    # columns don't match the others, and the Executive Dashboard's
    # `WHERE MONTH IS NOT NULL` query failed with `Binder Error: Referenced
    # column "MONTH" not found`, listing unrelated columns (UNITAP,
    # CURRENT_N, ...) as the closest candidates -- i.e. real ANEV monthly
    # partitions DO drift in column set/order, same as DLPD. The narrowing
    # was only ever a reasoned-but-unconfirmed OOM mitigation (see
    # `refresh_tables`/`ensure_ready` docstrings); the crash loop itself
    # was already fixed independently by lazy/selective view rebuilds
    # (`ensure_ready`), fixing S3 hydration on the live read path
    # (`connection.py`), and dropping page cache after hydration
    # (`processed_storage.py`). With those in place, the union_by_name
    # narrowing bought an unconfirmed, marginal memory saving at the cost
    # of a confirmed data-correctness bug -- not a trade worth keeping.
    # All five datasets use union_by_name=true again.
    _UNION_BY_NAME_DATASETS = {
        "fact_anev",
        "fact_dlpd_pascabayar",
        "fact_dlpd_prabayar",
        "fact_pengecekan",
        "fact_customer_location",
    }

    @classmethod
    def _replace_with_parquet_view(
        cls,
        connection: duckdb.DuckDBPyConnection,
        view_name: str,
        parquet_pattern: Path,
    ) -> None:
        """Replace an old table/view with a lazy parquet-backed view."""
        relation_type = connection.execute(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [view_name],
        ).fetchone()

        if relation_type:
            object_type = str(relation_type[0]).upper()
            if object_type == "VIEW":
                connection.execute(f"DROP VIEW {view_name}")
            else:
                connection.execute(f"DROP TABLE {view_name}")

        pattern = str(parquet_pattern).replace("'", "''")
        union_by_name = "true" if view_name in cls._UNION_BY_NAME_DATASETS else "false"
        connection.execute(
            f"""
            CREATE VIEW {view_name}
            AS
            SELECT *
            FROM read_parquet(
                '{pattern}',
                union_by_name = {union_by_name}
            )
            """
        )

    _DATASET_PATTERNS = {
        "fact_anev": PARQUET / "anev" / "*.parquet",
        "fact_dlpd_pascabayar": PARQUET / "dlpd" / "dlpd_pascabayar*.parquet",
        "fact_dlpd_prabayar": PARQUET / "dlpd" / "dlpd_prabayar*.parquet",
        "fact_pengecekan": PARQUET / "pengecekan" / "*.parquet",
        "fact_customer_location": PARQUET / "customer_location" / "*.parquet",
    }

    @classmethod
    def refresh_tables(cls, dataset_names: list[str] | None = None) -> None:
        """Rebuild the DuckDB views over the durable parquet datasets.

        Builds into an ISOLATED TEMP-FILE COPY of warehouse.duckdb, then
        atomically ``os.replace()``s it over the live WAREHOUSE path when
        done -- the live file is never opened for writing, and is only
        touched by that single atomic rename at the very end.

        This replaces an earlier design that rebuilt directly against the
        live WAREHOUSE file and, to avoid colliding with other threads'
        already-open connections to it, forcibly closed every one of them
        first. Confirmed live 2026-09-02 (reproduced in a concurrency
        test) that forcibly closing another thread's connection while it
        may be mid-query is itself unsafe -- DuckDB releases the GIL
        during I/O/computation, so the close can race an in-flight query
        -- and was the suspected cause of a hang that surfaced right after
        that design's last incremental patch (FORCE CHECKPOINT, to work
        around a resulting `TransactionException`).

        The swap-file approach sidesteps the whole class of problem: any
        connection some other thread already has open keeps reading from
        the OLD file via its already-open OS file handle (POSIX
        rename/replace semantics keep an open handle valid after the path
        is repointed), so nothing needs to close it. A brand new
        connection opened after the swap sees the fresh file automatically
        via connection.py's existing dead-connection/missing-table
        recheck. Seeding the temp file from a copy of the current
        WAREHOUSE (when one exists) is cheap -- it's just catalog/view
        metadata, not the underlying data, which lives in the lazily
        -referenced external parquet files -- and preserves any views NOT
        included in this call's ``dataset_names``.

        By default rebuilds all five views -- what a full ETL run (or the
        manual POST /warehouse/refresh route) genuinely needs, since any of
        them may have new data. Pass ``dataset_names`` to rebuild only a
        subset: this is what ``ensure_ready()``/``_ensure_warehouse_tables()``
        use so an on-demand heal pays only for the views actually missing,
        not all five every time.

        The whole build-and-swap sequence is serialized by
        WAREHOUSE_FILE_LOCK (process-wide, reentrant) so two concurrent
        callers can't both copy the pre-rebuild WAREHOUSE, build
        independently, and then have the second replace() silently
        clobber the first's changes (a lost-update race, since each
        temp-file copy only reflects the state at the moment it was
        taken).
        """
        if dataset_names is None:
            datasets = cls._DATASET_PATTERNS
        else:
            datasets = {
                name: cls._DATASET_PATTERNS[name]
                for name in dataset_names
                if name in cls._DATASET_PATTERNS
            }

        ensure_hydrated()
        WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)

        build_path = (
            WAREHOUSE.parent / f"warehouse.rebuild-{uuid.uuid4().hex}.duckdb"
        )

        with WAREHOUSE_FILE_LOCK:
            try:
                if WAREHOUSE.exists():
                    shutil.copy2(WAREHOUSE, build_path)

                connection = cls._connect_to_path(build_path)
                try:
                    for table_name, parquet_pattern in datasets.items():
                        logger.info("=" * 80)
                        logger.info(
                            "Refreshing warehouse view : %s", table_name
                        )
                        logger.info("Source : %s", parquet_pattern)

                        files = sorted(
                            Path(parquet_pattern.parent).glob(
                                parquet_pattern.name
                            )
                        )

                        if not files:
                            logger.warning(
                                "No parquet found for %s", table_name
                            )
                            continue

                        cls._replace_with_parquet_view(
                            connection,
                            table_name,
                            parquet_pattern,
                        )
                        logger.info("%s view ready", table_name)

                    # No other connection is ever attached to build_path
                    # (it's a fresh, unique path this call alone created),
                    # so a plain CHECKPOINT can't hit the
                    # other-write-transactions-active case a shared live
                    # file could.
                    connection.execute("CHECKPOINT")
                finally:
                    connection.close()

                os.replace(build_path, WAREHOUSE)
                logger.info("=")
                logger.info(
                    "WAREHOUSE REFRESH COMPLETED (LAZY PARQUET VIEWS)"
                )
                logger.info("=")
            finally:
                # os.replace() already removed build_path on success, so
                # this only fires if something raised before that point.
                try:
                    if build_path.exists():
                        build_path.unlink()
                except Exception:
                    pass

    @classmethod
    def _missing_tables(cls, connection: duckdb.DuckDBPyConnection) -> list[str]:
        """Datasets with durable parquet on disk but no matching DuckDB view yet."""
        missing: list[str] = []
        for table_name, parquet_pattern in cls._DATASET_PATTERNS.items():
            try:
                has_parquet = any(
                    Path(parquet_pattern.parent).glob(parquet_pattern.name)
                )
            except Exception:
                has_parquet = False
            if not has_parquet:
                continue

            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = ?
                """,
                [table_name],
            ).fetchone()
            if not (row and row[0]):
                missing.append(table_name)
        return missing

    @classmethod
    def ensure_ready(cls) -> duckdb.DuckDBPyConnection:
        """Open a warehouse connection, self-healing any missing views first.

        Every dashboard/suspect/executive read went through ``connect()``
        directly, which never created a view on its own -- something else
        had to have called ``refresh_tables()`` first. That something was
        an unconditional, blocking, all-five-views ``refresh_tables()``
        call in main.py's startup event, which re-hydrated and rebuilt the
        entire warehouse from scratch on *every* container boot, even when
        the hydrated warehouse.duckdb already had current views from a
        prior successful ETL run. Confirmed live 2026-09-01: the FastAPI
        Cloud host got stuck in a continuous OOM-crash-restart loop, always
        dying mid-refresh while building the fact_anev view -- because
        every restart repeated that same maximal-cost rebuild, forever,
        with no way to ever get past it.

        This replaces that eager full rebuild: callers get a connection
        immediately, and only the views actually missing (parquet exists,
        DuckDB catalog doesn't have it yet) are built, on whichever request
        happens to need one -- so a warm boot with an already-current
        warehouse does no rebuild work at all, and a genuinely cold one
        pays for at most the specific views it's missing instead of all
        five at once. The lock only serializes this check-and-heal step
        inside one process; it does not hold the connection open.
        """
        connection = cls.connect()

        # Force a real one-time S3 sync before deciding what's missing -- see
        # ensure_self_heal_hydrated()'s docstring in processed_storage.py.
        # Without this, a replica whose local warehouse.duckdb already
        # exists for any reason (an earlier partial self-heal build, or a
        # hydration that happened before a later ETL run republished a
        # corrected/complete one) can go the rest of its process lifetime
        # never looking at S3 again.
        #
        # Deliberately called BEFORE acquiring _REFRESH_LOCK, not inside it
        # -- confirmed live 2026-09-02: this can be a genuinely slow,
        # multi-minute call on a cold worker (downloads every processed
        # parquet object from S3), and _REFRESH_LOCK is a process-global,
        # non-reentrant lock that every DuckDB-backed request eventually
        # waits on (via connection.py's get_connection() -> its own
        # _ensure_warehouse_tables(), or this method) -- holding it for that
        # whole download serialized every other request on the worker
        # behind one slow S3 resync. ensure_self_heal_hydrated() is already
        # idempotent via its own flag, so calling it unlocked only risks a
        # rare, harmless redundant download race on the very first request,
        # never a correctness issue (files land via atomic os.replace()).
        ensure_self_heal_hydrated()

        with _REFRESH_LOCK:
            # Re-check inside the lock, not just before it: a thread that
            # waited here while another thread was healing must see that
            # thread's result, not repeat the same rebuild on stale info
            # gathered before it blocked.
            missing = cls._missing_tables(connection)
            if not missing:
                return connection

            logger.warning(
                "Warehouse views missing/stale, building on demand: %s",
                missing,
            )

            try:
                connection.close()
            except Exception:
                pass

            cls.refresh_tables(missing)
            return cls.connect()

    @classmethod
    def execute(cls, query: str) -> list[tuple]:
        connection = cls.connect()
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    @classmethod
    def list_tables(cls) -> list[str]:
        connection = cls.connect()
        try:
            rows = connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                ORDER BY table_name
                """
            ).fetchall()
            return [row[0] for row in rows]
        finally:
            connection.close()

    @classmethod
    def table_exists(cls, table_name: str) -> bool:
        return table_name in cls.list_tables()

    @classmethod
    def row_count(cls, table_name: str) -> int:
        connection = cls.connect()
        try:
            return connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
        finally:
            connection.close()
