from __future__ import annotations

import logging
import threading
from pathlib import Path

import duckdb

from app.core.constants import WAREHOUSE, PARQUET
from app.database.warehouse_file_lock import WAREHOUSE_FILE_LOCK
from app.infrastructure.storage.processed_storage import (
    ensure_hydrated,
    ensure_self_heal_hydrated,
)

logger = logging.getLogger(__name__)


_thread_state = threading.local()

# Serializes _ensure_warehouse_tables()'s check-then-rebuild sequence so two
# threads racing in on a cold worker don't both decide a table is missing
# and both trigger a duplicate rebuild.
_REFRESH_LOCK = threading.Lock()


def _get_thread_connection() -> duckdb.DuckDBPyConnection | None:
    return getattr(_thread_state, "connection", None)


def _set_thread_connection(
    conn: duckdb.DuckDBPyConnection | None,
) -> None:
    _thread_state.connection = conn


def _is_connection_alive(
    conn: duckdb.DuckDBPyConnection | None,
) -> bool:
    if conn is None:
        return False

    try:
        conn.execute("SELECT 1").fetchone()
        return True
    except Exception:
        return False


def _open_connection() -> duckdb.DuckDBPyConnection:
    # Pull warehouse.duckdb + processed parquet down from durable storage
    # first if this replica doesn't have them locally yet -- mirrors
    # Warehouse.connect()'s own ensure_hydrated() call. Confirmed live
    # 2026-09-01: this path never called it, so a genuinely fresh replica
    # (no local disk state) opened/created an EMPTY warehouse.duckdb here,
    # found no local parquet either (nothing had been hydrated), decided
    # via _warehouse_needs_refresh() that nothing "needed" a refresh, and
    # every repository using get_connection() silently served empty
    # results forever -- with no error, just quiet "does not exist yet"
    # warnings -- until something else (like the old blocking startup
    # refresh_tables() call) happened to hydrate this replica's disk as a
    # side effect. ensure_hydrated() is a per-process no-op once already
    # hydrated, so this is cheap on a warm replica.
    ensure_hydrated()

    Path(WAREHOUSE).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info("Opening DuckDB read connection: %s", WAREHOUSE)

    # See warehouse_file_lock's module docstring: this must be the ONLY
    # place, across this module AND warehouse.py, that calls
    # duckdb.connect() against WAREHOUSE without holding this lock first --
    # confirmed live 2026-09-02 that two connect() attempts to this file
    # from different threads (this module's read-only opens racing
    # warehouse.py's read-write one) can fail even sequentially, shortly
    # after one closes, with `ConnectionException`/`BinderException: Unique
    # file handle conflict`.
    with WAREHOUSE_FILE_LOCK:
        try:
            return duckdb.connect(
                str(WAREHOUSE),
                read_only=True,
            )
        except Exception:
            return duckdb.connect(str(WAREHOUSE))


def _table_exists_on_connection(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> bool:
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _warehouse_needs_refresh(
    conn: duckdb.DuckDBPyConnection,
) -> list[str]:
    """Datasets with durable parquet on disk but no matching DuckDB table yet."""
    datasets = {
        "fact_anev": PARQUET / "anev" / "*.parquet",
        "fact_dlpd_pascabayar": PARQUET / "dlpd" / "dlpd_pascabayar*.parquet",
        "fact_dlpd_prabayar": PARQUET / "dlpd" / "dlpd_prabayar*.parquet",
        "fact_pengecekan": PARQUET / "pengecekan" / "*.parquet",
        "fact_customer_location": PARQUET / "customer_location" / "*.parquet",
    }

    missing: list[str] = []
    for table_name, pattern in datasets.items():
        try:
            has_parquet = any(pattern.parent.glob(pattern.name))
        except Exception:
            has_parquet = False

        if has_parquet and not _table_exists_on_connection(conn, table_name):
            logger.warning(
                "Durable parquet exists for %s but the DuckDB table is missing.",
                table_name,
            )
            missing.append(table_name)

    return missing


def _ensure_warehouse_tables(
    conn: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyConnection:
    """Self-heal a cloud instance when durable parquet is not registered.

    Rebuilds only the specific tables found missing, not all five --
    confirmed live 2026-09-01 that rebuilding everything unconditionally
    (the previous behavior here, and what main.py's startup event used to
    do on every boot) is what OOM-crashed the FastAPI Cloud host, since it
    repeated that same maximal-cost rebuild on every restart with no way
    to ever get past it.

    ``Warehouse.refresh_tables()`` now builds into an isolated temp-file
    copy of WAREHOUSE and atomically swaps it in -- see its docstring --
    so it never opens a second, differently-configured connection to the
    live WAREHOUSE file while this thread's (or any other thread's)
    connection to it is open. That means this call no longer needs to (and
    must not) forcibly close other threads' connections first: an earlier
    design did that to avoid exactly the collision the swap-file approach
    now makes structurally impossible, and forcibly closing a connection
    from a different thread than the one using it was itself unsafe --
    confirmed live 2026-09-02 via a concurrency-test hang, suspected to be
    a race between the close and an in-flight query on that connection
    (DuckDB releases the GIL during I/O/computation). This thread's own
    stale connection is still closed below, since it's about to be
    replaced with a fresh one that can see the newly rebuilt table(s).
    """
    # Force a real one-time S3 sync (see ensure_self_heal_hydrated()'s
    # docstring) BEFORE deciding what's missing -- otherwise a replica whose
    # local warehouse.duckdb happens to already exist (built by an earlier
    # partial self-heal, or hydrated before a later ETL run republished a
    # corrected/complete one) can go the rest of its process lifetime never
    # looking at S3 again, silently stuck with a stale or incomplete local
    # catalog.
    #
    # Deliberately called BEFORE acquiring _REFRESH_LOCK, not inside it.
    # This can be a genuinely slow, multi-minute call on a cold worker (it
    # downloads every processed parquet object from S3) -- confirmed live
    # 2026-09-02: with it inside the lock, one request's first-ever call
    # here held _REFRESH_LOCK (a process-global, non-reentrant lock) for
    # the whole download, which serialized EVERY other DuckDB-backed
    # request on this worker behind it, since get_connection() -> this
    # function runs on every single request. ensure_self_heal_hydrated()
    # is already idempotent via its own flag (checked-then-set, same
    # pattern as ensure_hydrated()), so calling it unlocked here only risks
    # two threads racing into a redundant download on the very first
    # request each -- wasteful, never unsafe (files land via atomic
    # os.replace()) -- which is a far better trade than blocking the whole
    # worker.
    ensure_self_heal_hydrated()

    with _REFRESH_LOCK:
        # Re-check inside the lock: a thread that waited here while another
        # thread was healing must see that thread's result, not repeat the
        # same rebuild on stale info gathered before it blocked.
        missing = _warehouse_needs_refresh(conn)
        if not missing:
            return conn

        logger.warning(
            "Warehouse catalog is stale/missing; rebuilding on demand: %s",
            missing,
        )

        try:
            conn.close()
        except Exception:
            pass
        _set_thread_connection(None)

        try:
            # Lazy import avoids the connection -> warehouse -> connection cycle.
            from app.database.warehouse import Warehouse

            Warehouse.refresh_tables(missing)
            logger.info("On-demand warehouse refresh completed: %s", missing)
        except Exception:
            logger.exception("On-demand warehouse refresh failed.")

        return _open_connection()


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = _get_thread_connection()

    if _is_connection_alive(conn):
        # A worker can stay alive across an ETL run. In that case its
        # read-only connection may have been opened before new parquet was
        # persisted. Re-check the catalog even for an existing connection.
        conn = _ensure_warehouse_tables(conn)  # type: ignore[arg-type]
        _set_thread_connection(conn)
        return conn

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    conn = _open_connection()
    conn = _ensure_warehouse_tables(conn)
    _set_thread_connection(conn)
    return conn


def close_connection() -> None:
    conn = _get_thread_connection()

    if conn is None:
        return

    try:
        conn.close()
    except Exception:
        pass
    finally:
        _set_thread_connection(None)


def dataset_exists(dataset_name: str) -> bool:
    conn = get_connection()

    try:
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [dataset_name],
        ).fetchone()
        return bool(result and result[0])
    except Exception:
        return False


def table_exists(table_name: str) -> bool:
    return dataset_exists(table_name)


def list_tables() -> list[str]:
    conn = get_connection()
    rows = conn.execute("SHOW TABLES").fetchall()
    return [str(row[0]) for row in rows]


def row_count(table_name: str) -> int:
    conn = get_connection()
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(result[0]) if result else 0


def list_month_partitions(dataset_name: str) -> list[str]:
    if not dataset_exists(dataset_name):
        return []

    conn = get_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT MONTH_KEY
            FROM {dataset_name}
            WHERE MONTH_KEY IS NOT NULL
            ORDER BY MONTH_KEY
            """
        ).fetchall()
        return [str(row[0]) for row in rows]
    except Exception:
        return []


def read_dataset_sql(
    dataset_name: str,
    month_key: str | None = None,
) -> str:
    if not dataset_exists(dataset_name):
        raise ValueError(
            f"Dataset '{dataset_name}' does not exist."
        )

    return dataset_name
