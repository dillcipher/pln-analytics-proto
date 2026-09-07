"""Strictly streaming DLPD month resolver for low-memory deployments.

This patch bypasses both openpyxl/calamine for month discovery. XLSX is a
ZIP/XML container, so only workbook metadata, the header rows, the shared
strings that actually look like dates/months, and the target worksheet cells
are inspected. No workbook-sized Python/Rust object graph is created.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

from app.etl.detector.month_resolver import MonthResolver
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)
_INSTALLED = False
_ORIGINAL_RESOLVE = MonthResolver.resolve_months.__func__


def _text_from_element(element: ET.Element) -> str:
    return "".join(
        text.text or ""
        for text in element.iter()
        if text.tag.rsplit("}", 1)[-1] == "t"
    ).strip()


def _shared_month_map(archive: zipfile.ZipFile) -> dict[int, str]:
    """Return only shared-string IDs whose values resolve to a month."""
    name = "xl/sharedStrings.xml"
    if name not in archive.namelist():
        return {}

    result: dict[int, str] = {}
    index = -1

    with archive.open(name) as source:
        for _event, element in ET.iterparse(source, events=("end",)):
            if element.tag.rsplit("}", 1)[-1] != "si":
                continue

            index += 1
            text = _text_from_element(element)
            month = MonthResolver._normalize_month(text)
            if month is None and text:
                parsed = MonthResolver._parse_date(text)
                if parsed is not None:
                    month = parsed.strftime("%Y%m")

            if month:
                result[index] = month

            element.clear()

    return result


def _cell_value(cell: ET.Element, shared_months: dict[int, str]) -> str | None:
    cell_type = cell.attrib.get("t")
    value_node = cell.find(
        "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v"
    )

    if cell_type == "s" and value_node is not None:
        try:
            return shared_months.get(int(value_node.text or "-1"))
        except ValueError:
            return None

    if cell_type == "inlineStr":
        inline = cell.find(
            "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is"
        )
        if inline is not None:
            text = _text_from_element(inline)
            month = MonthResolver._normalize_month(text)
            if month:
                return month
            parsed = MonthResolver._parse_date(text)
            return parsed.strftime("%Y%m") if parsed is not None else None

    if value_node is not None:
        raw = value_node.text
        month = MonthResolver._normalize_month(raw)
        if month:
            return month

    return None


def _stream_resolve(filepath: Path, dataset: str) -> list[str]:
    filepath = Path(filepath)
    with zipfile.ZipFile(filepath, "r") as archive:
        sheet_name = MonthResolver._resolve_sheet_name_lightweight(filepath, dataset)
        sheet_path = MonthResolver._sheet_xml_path(archive, sheet_name)
        header, targets = MonthResolver._read_header_rows_lightweight(
            filepath,
            sheet_name,
            max_rows=15,
        )
        wanted = {
            DatasetValidator.normalize_column("THBL"),
            DatasetValidator.normalize_column("THBLREK"),
            DatasetValidator.normalize_column("DLPD_TGLBACA"),
        }
        targets = {
            index: value
            for index, value in targets.items()
            if value in wanted
        }
        if not targets:
            logger.warning(
                "DLPD MONTH COLUMNS NOT FOUND | FILE=%s | SHEET=%s",
                filepath.name,
                sheet_name,
            )
            return []

        # Stream the shared-string table once and retain only values that can
        # represent a month/date. The full shared-string table is never kept.
        shared_months = _shared_month_map(archive)

        thbl_key = DatasetValidator.normalize_column("THBL")
        thblrek_key = DatasetValidator.normalize_column("THBLREK")
        date_key = DatasetValidator.normalize_column("DLPD_TGLBACA")
        target_columns = set(targets)
        months: set[str] = set()
        rows = 0

        with archive.open(sheet_path) as source:
            for _event, row in ET.iterparse(source, events=("end",)):
                if row.tag.rsplit("}", 1)[-1] != "row":
                    continue

                row_number = int(row.attrib.get("r", "0") or 0)
                if row_number <= header:
                    row.clear()
                    continue

                rows += 1
                thbl = None
                thblrek = None
                detail_date = None

                for cell in row:
                    if cell.tag.rsplit("}", 1)[-1] != "c":
                        continue
                    reference = cell.attrib.get("r", "")
                    column_number = MonthResolver._column_number(reference)
                    if column_number not in target_columns:
                        continue

                    value = _cell_value(cell, shared_months)
                    key = targets[column_number]
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
                        filepath.name,
                        rows,
                        sorted(months),
                    )

                row.clear()

    logger.info(
        "DLPD MONTH SCAN COMPLETE | FILE=%s | ROWS=%s | MONTHS=%s",
        filepath.name,
        rows,
        sorted(months),
    )
    return sorted(months)


def _resolve_months(cls, filepath: Path, dataset: str | None = None) -> list[str]:
    filepath = Path(filepath)
    if cls.is_coordinate_master(filepath):
        return []

    if dataset is None:
        from app.etl.detector.detector import FileDetector
        dataset = FileDetector.detect(filepath)

    if dataset in {"DLPD_PASCABAYAR", "DLPD_PRABAYAR"}:
        return _stream_resolve(filepath, dataset)

    return _ORIGINAL_RESOLVE(cls, filepath, dataset)


def install_dlpd_xml_month_resolver_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    MonthResolver.resolve_months = classmethod(_resolve_months)
    _INSTALLED = True
