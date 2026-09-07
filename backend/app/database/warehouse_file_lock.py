"""Process-wide lock around every ``duckdb.connect()`` call against the
warehouse database file.

Confirmed live 2026-09-02: DuckDB refuses to have two connections to the
same database file open at once with different configurations
(``_duckdb.ConnectionException: Can't open a connection to same database
file with a different configuration than existing connections``) -- and,
observed while fixing that, closing one connection and immediately opening
another to the same file from a different thread can *also* fail with
``Binder Error: Unique file handle conflict: ... already attached by
database "warehouse"`` even when the configs match, because DuckDB's
internal attach/detach bookkeeping for a file isn't guaranteed to complete
synchronously with a Python-level ``.close()`` call.

Two independent code paths open connections to WAREHOUSE:
``app/database/warehouse.py``'s ``Warehouse.connect()`` (read-write, sets
explicit memory_limit/threads/temp_directory PRAGMAs -- used by ETL and by
the on-demand rebuild) and ``app/infrastructure/duckdb/connection.py``'s
``_open_connection()`` (read-only, cached per-thread -- used by every
dashboard/executive/suspect/dlpd read). Each module previously had its own
independent lock guarding only its own multi-step "check status, decide,
rebuild" sequence -- which does not stop the two modules' raw
``duckdb.connect()`` calls from racing against each other, since neither
lock knows about the other. This lock is the one thing both modules
share: hold it around the ``duckdb.connect()`` call itself (not
necessarily the whole surrounding operation), and no two connection
attempts to this file can ever be in flight at the same time, regardless
of which module or thread initiated them.
"""
from __future__ import annotations

import threading

# Reentrant: the code that holds this for an entire rebuild sequence (to
# block every OTHER thread's connect() attempt for that sequence's full
# duration, not just each individual duckdb.connect() call within it) also
# calls into Warehouse.connect()/_open_connection(), which acquire it again
# for their own brief connect() step -- a plain Lock would deadlock the
# same thread against itself there.
WAREHOUSE_FILE_LOCK = threading.RLock()
