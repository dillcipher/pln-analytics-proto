from __future__ import annotations

"""Memory-safe Excel parsing defaults for large production workbooks."""

import os
from pathlib import Path

import pandas as pd


_ORIGINAL_READ_EXCEL = pd.read_excel
_PATCH_MARKER = "_pln_memory_safe_excel_patch"


def _file_size_bytes(io) -> int:
    try:
        if isinstance(io, (str, os.PathLike)):
            return Path(io).stat().st_size
        name = getattr(io, "name", None)
        if name and isinstance(name, (str, os.PathLike)):
            return Path(name).stat().st_size
    except Exception:
        pass
    return 0


_EXCELFILE_PATCH_MARKER = "_pln_memory_safe_excelfile_patch"


def install_large_excel_reader() -> None:
    """Use the Rust-backed calamine reader for ordinary large Excel files."""
    current = pd.read_excel
    if not getattr(current, _PATCH_MARKER, False):
        threshold = 20 * 1024 * 1024

        def memory_safe_read_excel(io, *args, **kwargs):
            if "engine" not in kwargs and _file_size_bytes(io) >= threshold:
                kwargs["engine"] = "calamine"
            return _ORIGINAL_READ_EXCEL(io, *args, **kwargs)

        setattr(memory_safe_read_excel, _PATCH_MARKER, True)
        pd.read_excel = memory_safe_read_excel

    # DatasetValidator.get_sheet_name() calls pd.ExcelFile(filepath) directly
    # to list sheet names before any read_excel() call happens -- this
    # bypasses the read_excel patch above entirely and (with no engine=
    # given) defaults to openpyxl for every .xlsx regardless of size.
    # Confirmed live: for a 100-200MB ANEV/ANNEV source workbook, that one
    # ExcelFile() call alone could take upwards of ten minutes on this
    # host's CPU tier -- multiplied across every file in every month group,
    # "MERGING ANEV" stalled for the better part of an hour with zero
    # visible progress (no error, just pathologically slow) before the
    # fast, already-patched read_excel() calls that follow it ever ran.
    # Wrap whatever pd.ExcelFile currently is -- sitecustomize.py's
    # Excel-assembly-wait guard replaces it at interpreter startup, before
    # this module even imports -- so that guard behavior is preserved.
    current_excel_file = pd.ExcelFile
    if not getattr(current_excel_file, _EXCELFILE_PATCH_MARKER, False):
        threshold = 20 * 1024 * 1024

        def memory_safe_excel_file(io, *args, **kwargs):
            if "engine" not in kwargs and _file_size_bytes(io) >= threshold:
                kwargs["engine"] = "calamine"
            return current_excel_file(io, *args, **kwargs)

        setattr(memory_safe_excel_file, _EXCELFILE_PATCH_MARKER, True)
        pd.ExcelFile = memory_safe_excel_file


install_large_excel_reader()

# DLPD month discovery: bounded-memory XLSX/XML scan.
from app.etl.detector.dlpd_xml_month_resolver_safe import install as install_dlpd_resolver  # noqa: E402
install_dlpd_resolver()

# DO NOT call app.etl.large_dlpd_stream.install() here.
#
# Root-caused live 2026-09-02: that install() does
# `MonthlyMerger.merge = staticmethod(_merge)`, routing DLPD_PRABAYAR /
# DLPD_PASCABAYAR through the OLD single-file-per-month merge (no dedup
# guard, no coordinate-index enrichment, no restart-safe per-chunk
# staging) -- superseded by app/etl/merger/streaming_dlpd_merger_patch.py
# + streaming_dlpd_publish_guard.py, which app/main.py installs
# explicitly and deliberately, in order, as MonthlyMerger.merge's real
# owner (see the install_* calls near the top of main.py).
#
# This module (app.core.excel_memory) is imported for its OTHER two
# installers above, but it is *also* reached transitively through an
# unrelated chain -- app.interface.api.v1.router (imported by main.py
# AFTER its own explicit install_* calls) pulls in
# app.etl.detector.dlpd_xml_month_resolver_safe -> app.etl.large_dlpd_stream
# -> this module. Because this module used to call
# large_dlpd_stream.install() at its own top level, that transitive import
# silently re-patched MonthlyMerger.merge back to the old implementation
# *after* main.py had already installed the correct one -- with no error,
# no log line indicating the override, and no visible difference except
# that every DLPD file main.py's pipeline would have written with a
# "_part00001"-style suffix instead came out as a bare
# "dlpd_<dataset>_<month>.parquet", and any source file with zero rows
# matching its target month silently published as a MONTH-only stub
# instead of raising (large_dlpd_stream._stream_merge's empty-part_paths
# fallback). Confirmed as the actual cause of "DLPD Prabayar only shows a
# MONTH column": dlpd_prabayar_202606.parquet in storage had exactly this
# shape, and `MonthlyMerger.merge` was empirically confirmed (via
# `python3 -c "import app.main; ..."`) to be bound to
# app.etl.large_dlpd_stream._merge after a full app.main import, despite
# main.py's own install order reading as though the streaming patch
# should win.
#
# large_dlpd_stream.py is left in place (its install() is simply never
# called) rather than deleted, in case it needs to be diffed against
# later -- but it must never be installed again from an import-time side
# effect like this. If it's still needed for anything, wire it explicitly
# from main.py, in the same reviewable install_* sequence as everything
# else that touches MonthlyMerger.merge.
