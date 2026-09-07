"""Bounded-memory DLPD month resolver for low-memory deployments."""

from __future__ import annotations

import logging
import sqlite3
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

from app.etl.detector.month_resolver import MonthResolver
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)
_INSTALLED = False
_ORIGINAL_RESOLVE = MonthResolver.resolve_months.__func__
_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _text(element: ET.Element) -> str:
    return "".join(
        x.text or ""
        for x in element.iter()
        if x.tag.rsplit("}", 1)[-1] == "t"
    ).strip()


def _safe_month_from_parsed(parsed) -> str | None:
    """Return YYYYMM without calling Python strftime on out-of-range dates.

    Some Excel workbooks contain zero/invalid dates. pandas can represent some
    of these timestamps outside Python datetime's supported range, while
    ``Timestamp.strftime`` cannot. Month detection must treat those values as
    invalid rather than crashing the whole ETL job.
    """
    if parsed is None:
        return None
    try:
        year = int(parsed.year)
        month = int(parsed.month)
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None
    if not 1 <= year <= 9999 or not 1 <= month <= 12:
        return None
    return f"{year:04d}{month:02d}"


def _safe_parse_month(raw) -> str | None:
    month = MonthResolver._normalize_month(raw)
    if month:
        return month
    try:
        parsed = MonthResolver._parse_date(raw)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return _safe_month_from_parsed(parsed)


def _sheet_path(archive: zipfile.ZipFile, sheet_name: str) -> str:
    return MonthResolver._workbook_sheet_map(archive)[sheet_name]


def _value(cell: ET.Element, shared_lookup: sqlite3.Connection | None) -> str | None:
    node = cell.find(f"{{{_NS}}}v")
    if cell.attrib.get("t") == "s":
        if node is None or shared_lookup is None:
            return None
        try:
            row = shared_lookup.execute(
                "SELECT month FROM shared_months WHERE id = ?",
                (int(node.text or "-1"),),
            ).fetchone()
        except (TypeError, ValueError):
            return None
        return row[0] if row else None
    if node is not None:
        raw = node.text
    else:
        inline = cell.find(f"{{{_NS}}}is")
        raw = _text(inline) if inline is not None else None
    if raw is None:
        return None
    return _safe_parse_month(raw)


def _build_shared_month_index(
    archive: zipfile.ZipFile,
    wanted_ids: set[int] | None = None,
) -> tuple[sqlite3.Connection, int]:
    """Build a disk-backed shared-string month index.

    The previous implementation accumulated every shared-string id used by
    the workbook in a Python set and then materialised matching strings in a
    dict. Large DLPD workbooks can contain millions of shared strings, making
    that approach capable of exhausting a 500 MB container.

    This version stores only resolved month values in a temporary SQLite file.
    The XLSX XML is still streamed; the potentially large index is disk-backed,
    keeping Python heap usage bounded.
    """
    temp = tempfile.NamedTemporaryFile(prefix="dlpd_months_", suffix=".sqlite3", delete=False)
    temp.close()
    conn = sqlite3.connect(temp.name)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("CREATE TABLE shared_months (id INTEGER PRIMARY KEY, month TEXT NOT NULL)")

    if "xl/sharedStrings.xml" not in archive.namelist():
        conn.commit()
        return conn, 0

    inserted = 0
    index = -1
    skipped_invalid = 0
    with archive.open("xl/sharedStrings.xml") as source:
        for _event, element in ET.iterparse(source, events=("end",)):
            if element.tag.rsplit("}", 1)[-1] != "si":
                continue
            index += 1
            if wanted_ids is not None and index not in wanted_ids:
                element.clear()
                continue
            raw = _text(element)
            month = _safe_parse_month(raw)
            if month:
                conn.execute(
                    "INSERT OR REPLACE INTO shared_months(id, month) VALUES (?, ?)",
                    (index, month),
                )
                inserted += 1
            elif raw:
                # Invalid dates are expected to be possible in source workbooks.
                # They are irrelevant for month detection and must not abort ETL.
                try:
                    parsed = MonthResolver._parse_date(raw)
                    if parsed is not None:
                        skipped_invalid += 1
                except (TypeError, ValueError, OverflowError, OSError):
                    pass
            element.clear()
            if inserted and inserted % 10000 == 0:
                conn.commit()
                logger.info("DLPD MONTH SHARED INDEX | ENTRIES=%s", inserted)
    conn.commit()
    if skipped_invalid:
        logger.info(
            "DLPD MONTH SHARED INDEX | INVALID_DATE_VALUES_SKIPPED=%s",
            skipped_invalid,
        )
    return conn, inserted


def _scan(
    archive: zipfile.ZipFile,
    sheet_path: str,
    header: int,
    targets: dict[int, str],
    shared_lookup: sqlite3.Connection,
    filename: str,
) -> list[str]:
    thbl_key = DatasetValidator.normalize_column("THBL")
    thblrek_key = DatasetValidator.normalize_column("THBLREK")
    date_key = DatasetValidator.normalize_column("DLPD_TGLBACA")
    months: set[str] = set()
    rows = 0
    with archive.open(sheet_path) as source:
        for _event, row in ET.iterparse(source, events=("end",)):
            if row.tag.rsplit("}", 1)[-1] != "row":
                continue
            if int(row.attrib.get("r", "0") or 0) <= header:
                row.clear()
                continue
            rows += 1
            thbl = thblrek = detail_date = None
            for cell in row:
                if cell.tag.rsplit("}", 1)[-1] != "c":
                    continue
                col = MonthResolver._column_number(cell.attrib.get("r", ""))
                if col not in targets:
                    continue
                value = _value(cell, shared_lookup)
                key = targets[col]
                if key == thbl_key:
                    thbl = value
                elif key == thblrek_key:
                    thblrek = value
                elif key == date_key:
                    detail_date = value
            if thbl:
                months.add(thbl)
            if thblrek:
                months.add(thblrek)
            if not thbl and not thblrek and detail_date:
                months.add(detail_date)
            if rows % 50_000 == 0:
                logger.info(
                    "DLPD MONTH SCAN PROGRESS | FILE=%s | ROWS=%s | MONTHS=%s",
                    filename,
                    rows,
                    sorted(months),
                )
            row.clear()
    logger.info(
        "DLPD MONTH SCAN COMPLETE | FILE=%s | ROWS=%s | MONTHS=%s",
        filename,
        rows,
        sorted(months),
    )
    return sorted(months)


def _resolve_dlpd(filepath: Path, dataset: str) -> list[str]:
    filepath = Path(filepath)
    sheet_name = MonthResolver._resolve_sheet_name_lightweight(filepath, dataset)
    header, columns = MonthResolver._read_header(filepath, sheet_name)
    wanted = {
        DatasetValidator.normalize_column("THBL"),
        DatasetValidator.normalize_column("THBLREK"),
        DatasetValidator.normalize_column("DLPD_TGLBACA"),
    }
    targets = {i: v for i, v in columns.items() if v in wanted}
    if not targets:
        logger.warning(
            "DLPD MONTH COLUMNS NOT FOUND | FILE=%s | SHEET=%s",
            filepath.name,
            sheet_name,
        )
        return []

    with zipfile.ZipFile(filepath, "r") as archive:
        sheet_path = _sheet_path(archive, sheet_name)
        shared_lookup, indexed = _build_shared_month_index(archive)
        logger.info(
            "DLPD MONTH RESOLUTION PREPARED | FILE=%s | SHARED_MONTH_ENTRIES=%s",
            filepath.name,
            indexed,
        )
        try:
            return _scan(
                archive,
                sheet_path,
                header,
                targets,
                shared_lookup,
                filepath.name,
            )
        finally:
            db_path = Path(shared_lookup.execute("PRAGMA database_list").fetchone()[2])
            shared_lookup.close()
            try:
                db_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("DLPD MONTH TEMP INDEX CLEANUP FAILED | %s", db_path)


def _resolve_months(cls, filepath: Path, dataset: str | None = None) -> list[str]:
    filepath = Path(filepath)
    if cls.is_coordinate_master(filepath):
        return []
    if dataset is None:
        from app.etl.detector.detector import FileDetector
        dataset = FileDetector.detect(filepath)
    if dataset in {"DLPD_PASCABAYAR", "DLPD_PRABAYAR"}:
        return _resolve_dlpd(filepath, dataset)
    return _ORIGINAL_RESOLVE(cls, filepath, dataset)


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    MonthResolver.resolve_months = classmethod(_resolve_months)
    _INSTALLED = True
