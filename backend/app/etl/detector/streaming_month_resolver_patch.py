"""Fast, memory-safe DLPD month resolution for large XLSX workbooks."""

from __future__ import annotations

import itertools
import logging
import os
from pathlib import Path

from app.etl.detector.month_resolver import MonthResolver
from app.etl.transformers.dlpd_transformer import DLPDTransformer
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)
_INSTALLED = False

# python-calamine's get_sheet_by_name() materializes the ENTIRE worksheet as
# Python objects the moment it is called -- it is not a row-by-row streaming
# API, whatever its Rust backend does internally. Measured against the real
# ~746MB DLPD PLN.xlsx production file (Main sheet, 621k rows): from_path()
# returns in <1s at ~90MB RSS, but get_sheet_by_name() alone then takes ~26s
# and peaks at ~3.6GB RSS, before a single row has been iterated. That is
# fatal on the 512MB Render free-tier instance this app targets.
#
# Month resolution here is only a grouping optimisation (see
# app/etl/detector/dlpd_month_fallback_patch.py and
# ETLOrchestrator._resolve_dlpd_month_cache, which already quarantines a
# per-file exception into an empty month set). The real DLPD merge
# (app/etl/merger/streaming_dlpd_merger_patch.py) reads the workbook via
# openpyxl(read_only=True) in bounded ~250-row chunks and resolves MONTH
# per row regardless, so skipping the pre-scan for large files costs nothing
# but the grouping optimisation -- it does not lose or corrupt data.
#
# So: refuse calamine for any file above this threshold and let the existing
# per-file quarantine handle it, instead of risking an OOM kill (which is a
# SIGKILL, not a Python exception -- no try/except anywhere can catch it).
DLPD_MONTH_PRESCAN_MAX_BYTES = max(
    1,
    int(os.getenv("DLPD_MONTH_PRESCAN_MAX_BYTES", str(30 * 1024 * 1024))),
)


def _stream_read_dlpd_months(cls, filepath: Path, dataset: str, sheet_name: str) -> list[str]:
    """Resolve DLPD months with the Rust-backed calamine reader.

    Never materialize the workbook as a pandas DataFrame or a Python list.
    The 512 MB Render instance must keep the Python-side working set bounded
    while scanning large DLPD workbooks.
    """
    del dataset
    filepath = Path(filepath)

    try:
        file_size = filepath.stat().st_size
    except OSError:
        file_size = 0

    if file_size > DLPD_MONTH_PRESCAN_MAX_BYTES:
        raise RuntimeError(
            "DLPD month pre-scan skipped for "
            f"{filepath.name} ({file_size / 1e6:.1f}MB > "
            f"{DLPD_MONTH_PRESCAN_MAX_BYTES / 1e6:.1f}MB limit); "
            "calamine would materialize the whole worksheet and risk an OOM "
            "kill. The streaming DLPD merger resolves MONTH per row anyway."
        )

    try:
        from python_calamine import CalamineWorkbook
    except ImportError as exc:
        raise RuntimeError(
            "python-calamine is required for large DLPD month resolution."
        ) from exc

    thbl_key = DatasetValidator.normalize_column("THBL")
    thblrek_key = DatasetValidator.normalize_column("THBLREK")
    date_key = DatasetValidator.normalize_column("DLPD_TGLBACA")
    required = {thbl_key, thblrek_key, date_key}

    workbook = None
    try:
        workbook = CalamineWorkbook.from_path(filepath)
        sheet_names = list(workbook.sheet_names)
        if not sheet_names:
            return []

        normalized_sheets = {
            str(name).strip().upper(): name for name in sheet_names
        }
        selected_name = normalized_sheets.get(
            str(sheet_name).strip().upper(),
            sheet_names[0],
        )
        sheet = workbook.get_sheet_by_name(selected_name)

        # Only inspect the first 15 rows for the business header.  Use the
        # streaming iterator instead of sheet.to_python(...), which may
        # materialize a larger range depending on the calamine version.
        header_rows = list(itertools.islice(sheet.iter_rows(), 15))
        header_row = None
        header_indexes: dict[str, int] = {}

        for row_index, row in enumerate(header_rows, start=1):
            normalized = [DatasetValidator.normalize_column(value) for value in row]
            indexes: dict[str, int] = {}
            for index, name in enumerate(normalized):
                if name in required and name not in indexes:
                    indexes[name] = index

            # Prefer the first row containing the actual DLPD month columns.
            # Do not require IDPEL/LOCATION_CODE because the two DLPD schemas
            # do not have identical leading columns.
            if indexes:
                header_row = row_index
                header_indexes = indexes
                if thbl_key in indexes or thblrek_key in indexes:
                    break

        if header_row is None:
            header_row = 1
            first_row = header_rows[0] if header_rows else []
            header_indexes = {
                DatasetValidator.normalize_column(value): index
                for index, value in enumerate(first_row)
                if DatasetValidator.normalize_column(value) in required
            }

        if not header_indexes:
            logger.warning(
                "DLPD MONTH RESOLUTION: required month columns not found | FILE=%s",
                filepath.name,
            )
            return []

        # IMPORTANT: these are absolute worksheet indexes.  The previous
        # implementation subtracted the first target index and then applied
        # those relative indexes to a full worksheet row, which could read the
        # wrong columns whenever THBL/THBLREK were not adjacent to each other.
        thbl_idx = header_indexes.get(thbl_key)
        thblrek_idx = header_indexes.get(thblrek_key)
        date_idx = header_indexes.get(date_key)

        months: set[str] = set()
        rows_seen = 0
        sheet_height = int(getattr(sheet, "height", 0) or 0)

        # Calamine yields one worksheet row at a time. Never call to_python()
        # on the complete sheet and never build a DataFrame here.
        for absolute_row_index, row in enumerate(sheet.iter_rows(), start=1):
            if absolute_row_index <= header_row:
                continue

            rows_seen += 1
            thbl = (
                row[thbl_idx]
                if thbl_idx is not None and thbl_idx < len(row)
                else None
            )
            thblrek = (
                row[thblrek_idx]
                if thblrek_idx is not None and thblrek_idx < len(row)
                else None
            )
            detail_date = (
                row[date_idx]
                if date_idx is not None and date_idx < len(row)
                else None
            )

            thbl_month = cls._normalize_month(thbl)
            thblrek_month = cls._normalize_month(thblrek)

            if thbl_month and thblrek_month and thbl_month == thblrek_month:
                months.add(thbl_month)
            elif thbl_month or thblrek_month:
                parsed_date = None
                if detail_date is not None and thbl_month and thblrek_month:
                    if thbl_month != thblrek_month:
                        parsed_date = cls._parse_date(detail_date)

                if parsed_date is not None:
                    thbl_start = DLPDTransformer._month_start(thbl_month)
                    thblrek_start = DLPDTransformer._month_start(thblrek_month)
                    if thbl_start is not None and thblrek_start is not None:
                        period_start = min(thbl_start, thblrek_start)
                        period_end = DLPDTransformer._month_end(max(thbl_start, thblrek_start))
                        if period_end is not None and period_start <= parsed_date <= period_end:
                            months.add(parsed_date.strftime("%Y%m"))
                        else:
                            months.update(
                                month for month in (thbl_month, thblrek_month) if month
                            )
                    else:
                        months.update(
                            month for month in (thbl_month, thblrek_month) if month
                        )
                else:
                    months.update(
                        month for month in (thbl_month, thblrek_month) if month
                    )
            elif detail_date is not None:
                parsed_date = cls._parse_date(detail_date)
                if parsed_date is not None:
                    months.add(parsed_date.strftime("%Y%m"))

            if rows_seen % 100_000 == 0:
                pct = (rows_seen / sheet_height * 100.0) if sheet_height else 0.0
                logger.info(
                    "DLPD MONTH SCAN PROGRESS | FILE=%s | ROWS=%s | PCT=%.1f | MONTHS=%s",
                    filepath.name,
                    rows_seen,
                    pct,
                    sorted(months),
                )

        logger.info(
            "DLPD MONTH SCAN COMPLETE | FILE=%s | ROWS=%s | MONTHS=%s",
            filepath.name,
            rows_seen,
            sorted(months),
        )
        return sorted(months)
    except Exception:
        logger.exception(
            "DLPD MONTH STREAMING RESOLUTION FAILED | FILE=%s",
            filepath.name,
        )
        raise
    finally:
        if workbook is not None:
            try:
                workbook.close()
            except Exception:
                pass


def install_streaming_month_resolver_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    MonthResolver._read_dlpd_months = classmethod(_stream_read_dlpd_months)
    _INSTALLED = True
