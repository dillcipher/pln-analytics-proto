"""Low-memory DLPD ETL path."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
from openpyxl import load_workbook

from app.etl.merger.monthly_merger import MonthlyMerger
from app.etl.transformers.dlpd_transformer import DLPDTransformer
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)
_INSTALLED = False
_ORIGINAL_MERGE = MonthlyMerger.merge


def _normalize_final_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.map(str)
    for column in df.columns:
        if pd.api.types.is_object_dtype(df[column]):
            df[column] = df[column].fillna("").astype(str).str.strip()
    return df


def _normalize_calamine_value(value):
    """Reshape one calamine cell value to match openpyxl's shape.

    calamine represents a blank cell as '' (openpyxl: None) and every
    numeric cell as a Python float, even when Excel stores it as an
    integer (openpyxl preserves int). Both differences are dangerous to
    leave alone: blank-cell '' silently turns a numeric column into
    object dtype downstream (see _normalize_final_chunk's is_object_dtype
    check), and clean_idpel()'s bare `.astype(str)` on a float IDPEL
    produces "517123456789.0" instead of "517123456789" -- a corrupted
    customer id that would fail every downstream join. Confirmed exactly
    matching against openpyxl for int-valued, fractional, blank, string,
    and date cells (see chat history / manual repro) before wiring this
    in.
    """
    if value == "":
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _open_calamine_rows(path: Path, sheet_name: str):
    """Open the workbook and validate the sheet, without reading rows yet.

    Kept separate from the row loop below on purpose: we only want to fall
    back to openpyxl on an OPEN-time failure. If we instead fell back after
    already yielding some chunks (a failure deep in a huge file), the
    openpyxl restart would re-read from row 0 and every already-yielded
    chunk would be written to the output parquet a second time, silently
    duplicating rows. Failing outright mid-stream (letting the exception
    propagate to the per-group quarantine handler in etl_orchestrator.py,
    same as an openpyxl mid-stream failure always did) is the safe choice.
    """
    from python_calamine import CalamineWorkbook

    wb = CalamineWorkbook.from_path(str(path))
    ws = wb.get_sheet_by_name(sheet_name)
    return wb, ws


def _iter_excel_chunks(path: Path, sheet_name: str, header: int, chunk_rows: int = 10000):
    """Yield bounded pandas chunks from a low-memory XLSX row iterator.

    Uses the Rust-backed calamine reader instead of openpyxl for the bulk
    row-by-row scan. Confirmed live: for the largest DLPD source workbooks
    (700+MB), openpyxl's pure-Python read_only iteration made the first
    DLPD group after Phase 1 appear to hang for 60-70+ minutes with zero
    progress and no exception -- this streaming path calls
    openpyxl.load_workbook() directly, so it never benefited from the
    pandas-level calamine patch in app/core/excel_memory.py. Benchmarked
    locally at ~9x faster than openpyxl for the same file (200k rows:
    0.9s vs 8.0s), so this should turn that multi-hour class of stall into
    single-digit minutes.

    Falls back to the original openpyxl iterator only if calamine cannot
    even open the file/sheet -- never mid-stream, see _open_calamine_rows.
    """
    wb = None
    try:
        wb, ws = _open_calamine_rows(path, sheet_name)
    except Exception:
        logger.warning(
            "CALAMINE DLPD STREAM OPEN FAILED, FALLING BACK TO OPENPYXL | %s",
            path,
            exc_info=True,
        )

    if wb is not None:
        try:
            header_values = None
            buffer: list[list] = []
            for row_number, raw_values in enumerate(ws.iter_rows(), start=0):
                values = [_normalize_calamine_value(value) for value in raw_values]
                if row_number < header:
                    continue
                if row_number == header:
                    header_values = [str(v).strip() if v is not None else "" for v in values]
                    continue
                if header_values is None:
                    raise ValueError(f"Header row {header} was not found in {path.name}")
                buffer.append(values)
                if len(buffer) >= chunk_rows:
                    yield pd.DataFrame.from_records(buffer, columns=header_values)
                    buffer.clear()
            if buffer:
                yield pd.DataFrame.from_records(buffer, columns=header_values)
            return
        finally:
            close = getattr(wb, "close", None)
            if callable(close):
                wb.close()

    wb2 = load_workbook(path, read_only=True, data_only=True)
    try:
        ws2 = wb2[sheet_name]
        header_values = None
        buffer = []
        for row_number, values in enumerate(ws2.iter_rows(values_only=True), start=0):
            if row_number < header:
                continue
            if row_number == header:
                header_values = [str(v).strip() if v is not None else "" for v in values]
                continue
            if header_values is None:
                raise ValueError(f"Header row {header} was not found in {path.name}")
            buffer.append(values)
            if len(buffer) >= chunk_rows:
                yield pd.DataFrame.from_records(buffer, columns=header_values)
                buffer.clear()
        if buffer:
            yield pd.DataFrame.from_records(buffer, columns=header_values)
    finally:
        wb2.close()


def _output_path(dataset: str, month: str, output_dir: Path) -> Path:
    folder = output_dir / "dlpd"
    folder.mkdir(parents=True, exist_ok=True)
    name = "dlpd_pascabayar" if dataset == "DLPD_PASCABAYAR" else "dlpd_prabayar"
    return folder / f"{name}_{month}.parquet"


def _validate_parquet_file(path: Path) -> None:
    """Raise if `path` is not a genuinely readable parquet file.

    Cheap: reads only the footer metadata, not the row data. Called before
    a freshly written DLPD month file is published under its real
    filename, so a truncated/corrupt write is caught and quarantined right
    here -- with a clear error naming this exact file -- instead of
    surfacing much later as an opaque DuckDB failure when
    Warehouse.refresh_tables() globs every dlpd_*.parquet file into one
    view and takes the whole job down over it.
    """
    import pyarrow.parquet as pq

    pq.ParquetFile(path).metadata


def _stream_merge(dataset: str, month: str | None, files: list[Path], output_dir: Path) -> Path:
    if not files:
        raise ValueError(f"No files supplied for dataset '{dataset}'.")
    if month is None:
        raise ValueError(f"DLPD streaming merge requires a target month: {dataset}")

    target_month = DLPDTransformer._normalize_month_value(month)
    if not target_month:
        raise ValueError(f"Invalid DLPD target month: {month}")

    output_path = _output_path(dataset, target_month, output_dir)
    legacy_path = output_path.parent / ("dlpd_pascabayar.parquet" if dataset == "DLPD_PASCABAYAR" else "dlpd_prabayar.parquet")
    if legacy_path.exists():
        legacy_path.unlink()

    part_paths: list[Path] = []
    total_input = 0
    total_output = 0

    with tempfile.TemporaryDirectory(prefix=f"dlpd_{target_month}_", dir=str(output_path.parent)) as temp_dir:
        temp_root = Path(temp_dir)
        part_no = 0

        for file in files:
            sheet = DatasetValidator.get_sheet_name(file, dataset)
            header = DatasetValidator.detect_header_row(filepath=file, sheet_name=sheet)
            logger.info("DLPD STREAM START | dataset=%s | month=%s | file=%s | sheet=%s | header=%s", dataset, target_month, file.name, sheet, header)

            for chunk in _iter_excel_chunks(file, sheet, header):
                total_input += len(chunk)
                if chunk.empty:
                    continue

                transformed = DLPDTransformer().transform(chunk)
                transformed = transformed[transformed["MONTH"].eq(target_month)].copy()
                if transformed.empty:
                    del chunk, transformed
                    continue

                transformed = MonthlyMerger._enrich_dlp_per_row_month(
                    dataframe=transformed,
                    dataset=dataset,
                    output_dir=output_dir,
                    fallback_month=target_month,
                )
                transformed = _normalize_final_chunk(transformed)

                part = temp_root / f"part_{part_no:06d}.parquet"
                transformed.to_parquet(part, index=False)
                part_paths.append(part)
                total_output += len(transformed)
                part_no += 1
                logger.info("DLPD STREAM CHUNK | dataset=%s | month=%s | input_rows=%s | output_rows=%s | parts=%s", dataset, target_month, len(chunk), len(transformed), part_no)
                del chunk, transformed

        # Write to a temp path next to the real destination, then rename
        # into place, instead of writing output_path directly. Confirmed
        # live 2026-09-01 (first run with DLPD un-skipped): the DuckDB COPY
        # below writes straight to output_path with no atomicity of its
        # own, and dlpd_pascabayar_202601.parquet came out of that run
        # "too small to be a Parquet file" -- some interruption (the exact
        # cause wasn't captured in the retained log) left a truncated file
        # sitting at the real path. Warehouse.refresh_tables() later globs
        # every dlpd_pascabayar*.parquet file into one view, so that single
        # corrupt month failed the ENTIRE job, including every dataset that
        # had already processed successfully. os.replace is atomic at the
        # OS level: a crash or interruption at any point before it leaves
        # only an orphaned .tmp file, never a half-written file at
        # output_path, and a retry simply overwrites the .tmp again.
        temp_output = output_path.with_name(output_path.name + f".tmp-{target_month}")
        try:
            if not part_paths:
                pd.DataFrame({"MONTH": pd.Series(dtype="string")}).to_parquet(temp_output, index=False)
            else:
                conn = duckdb.connect()
                try:
                    paths = ", ".join("'" + str(p).replace("'", "''") + "'" for p in part_paths)
                    out = str(temp_output).replace("'", "''")
                    # union_by_name=true is required here, not optional. Each
                    # part_NNNNNN.parquet comes from one chunk of one SOURCE
                    # FILE (see the loop above -- `files` can be several DLPD
                    # workbooks for the same target month), and
                    # DLPDTransformer.transform() -> BaseTransformer.sort_columns()
                    # only reindexes a chunk to ITS OWN columns (sorted), never
                    # to a fixed canonical column list -- so a chunk whose
                    # source file genuinely lacks a column (e.g. IDPEL under a
                    # different header, or absent entirely in one export batch)
                    # produces a part file without that column at all. Without
                    # union_by_name, DuckDB's multi-file read_parquet() uses
                    # the FIRST file in the list to determine the schema for
                    # ALL of them -- if that first part happens to be the one
                    # missing a column, the column is silently dropped from
                    # the entire merged month output, not just that one part.
                    # Confirmed live 2026-09-02: fact_dlpd_prabayar ended up
                    # with no IDPEL column at all, breaking every downstream
                    # query that references d.IDPEL with `_duckdb.BinderException:
                    # Values list "d" does not have a column named "IDPEL"` --
                    # this is the exact same class of bug already fixed for
                    # ANEV (see streaming_dlpd_publish_guard.py's
                    # _prescan_anev_schema / commit 5f84a57). union_by_name
                    # here unions every part's columns, filling NULL for rows
                    # from a part that didn't have a given column, instead of
                    # dropping the column outright.
                    conn.execute(f"COPY (SELECT * FROM read_parquet([{paths}], union_by_name=true)) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                finally:
                    conn.close()

            # Validate before publishing: a file that passes this is
            # guaranteed readable, so nothing downstream (the warehouse
            # glob view, a re-run's "already complete" checkpoint check)
            # can ever be handed a truncated/corrupt parquet under this
            # dataset+month's real filename again.
            _validate_parquet_file(temp_output)

            temp_output.replace(output_path)
        finally:
            temp_output.unlink(missing_ok=True)

    logger.info("DLPD STREAM COMPLETE | dataset=%s | month=%s | input_rows=%s | output_rows=%s | output=%s", dataset, target_month, total_input, total_output, output_path)
    return output_path


def _merge(dataset: str, month: str | None, files: list[Path], output_dir: Path):
    if dataset in MonthlyMerger.COORDINATE_DATASETS:
        return _stream_merge(dataset, month, files, output_dir)
    return _ORIGINAL_MERGE(dataset, month, files, output_dir)


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    MonthlyMerger.merge = staticmethod(_merge)
    _INSTALLED = True
