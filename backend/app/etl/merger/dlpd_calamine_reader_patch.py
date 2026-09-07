"""Use the Rust-backed calamine iterator for large DLPD XLSX ingestion.

The existing streaming DLPD merger already limits pandas frames to small
chunks, but its workbook reader used openpyxl directly. This patch keeps the
same row/chunk contract while moving workbook parsing into python-calamine,
which avoids the large Python openpyxl object graph on the 500 MB runtime.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from app.etl.detector.month_resolver import MonthResolver
from app.etl.merger import streaming_dlpd_merger_patch as merger
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)
_INSTALLED = False


def _iter_excel_chunks(filepath: Path, dataset: str):
    filepath = Path(filepath)
    if filepath.suffix.lower() not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        raise ValueError(
            f"Streaming DLPD requires XLSX/XLSM format: {filepath.name}"
        )

    try:
        from python_calamine import CalamineWorkbook
    except ImportError as exc:
        raise RuntimeError(
            "python-calamine is required for streaming DLPD ingestion."
        ) from exc

    # Sheet discovery is XML-only. Do not call DatasetValidator.get_sheet_name:
    # that legacy helper uses pandas.ExcelFile and would re-open a huge XLSX.
    sheet = MonthResolver._resolve_sheet_name_lightweight(filepath, dataset)

    workbook = None
    try:
        workbook = CalamineWorkbook.from_path(filepath)
        normalized_sheets = {
            str(name).strip().upper(): name
            for name in workbook.sheet_names
        }
        selected = normalized_sheets.get(
            str(sheet).strip().upper(),
            sheet,
        )
        worksheet = workbook.get_sheet_by_name(selected)

        rows = worksheet.iter_rows()
        columns: list[str] | None = None
        batch: list[tuple] = []
        rows_seen = 0
        header_found = False
        chunk_rows = max(
            100,
            min(500, int(os.getenv("DLPD_STREAM_CHUNK_ROWS", "250"))),
        )

        # Calamine's iterator can begin at the sheet's used range rather than
        # absolute worksheet row 1. Find the real header by schema instead of
        # relying on an absolute row offset.
        for row in rows:
            normalized = [
                str(value).strip().upper() if value is not None else ""
                for value in row
            ]
            normalized_set = {
                DatasetValidator.normalize_column(value)
                for value in normalized
                if value
            }

            if not header_found:
                if (
                    "IDPEL" in normalized_set
                    and (
                        "THBL" in normalized_set
                        or "THBLREK" in normalized_set
                    )
                ):
                    columns = normalized
                    header_found = True
                    logger.info(
                        "DLPD STREAM HEADER FOUND | FILE=%s | DATASET=%s | COLUMNS=%s",
                        filepath.name,
                        dataset,
                        len(columns),
                    )
                continue

            if columns is None:
                continue

            batch.append(tuple(row[: len(columns)]))
            rows_seen += 1

            if len(batch) >= chunk_rows:
                logger.info(
                    "DLPD STREAM PROGRESS | FILE=%s | ROWS=%s | CHUNK=%s",
                    filepath.name,
                    rows_seen,
                    len(batch),
                )
                yield pd.DataFrame.from_records(batch, columns=columns)
                batch.clear()

        if batch and columns is not None:
            logger.info(
                "DLPD STREAM COMPLETE | FILE=%s | ROWS=%s",
                filepath.name,
                rows_seen,
            )
            yield pd.DataFrame.from_records(batch, columns=columns)

        if not header_found:
            raise ValueError(
                f"Unable to locate DLPD header row in '{filepath.name}'."
            )
    finally:
        if workbook is not None:
            try:
                workbook.close()
            except Exception:
                logger.exception("Failed to close calamine workbook: %s", filepath)


def install_dlpd_calamine_reader_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    merger._iter_excel_chunks = _iter_excel_chunks
    _INSTALLED = True
