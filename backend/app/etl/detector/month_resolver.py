from __future__ import annotations

import datetime as dt
import logging
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from app.etl.detector.detector import FileDetector
from app.etl.validator.validator import DatasetValidator

logger = logging.getLogger(__name__)


class MonthResolver:
    """Resolve business months without materialising large XLSX workbooks.

    DLPD workbooks can be hundreds of MB. The resolver therefore reads only
    workbook metadata, the header and the required month/date cells from the
    worksheet XML. It never calls pandas/openpyxl for DLPD month detection.
    """

    MONTH_PATTERN = re.compile(r"^(20\d{2})(0[1-9]|1[0-2])$")
    MONTH_SEARCH_PATTERN = re.compile(r"(20\d{2})(0[1-9]|1[0-2])")
    COORDINATE_MASTER_FILES = {"to_prabayar.xlsx", "to_pascabayar.xlsx"}
    XLSX_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    DLPD_DATASETS = {"DLPD_PASCABAYAR", "DLPD_PRABAYAR"}
    DLPD_MONTH_COLUMNS = {
        DatasetValidator.normalize_column("THBL"),
        DatasetValidator.normalize_column("THBLREK"),
        DatasetValidator.normalize_column("DLPD_TGLBACA"),
    }

    @staticmethod
    def _normalized_filename(filepath: Path) -> str:
        name = Path(filepath).name.lower().strip().replace("-", "_").replace(" ", "_")
        while "__" in name:
            name = name.replace("__", "_")
        return name

    @classmethod
    def is_coordinate_master(cls, filepath: Path) -> bool:
        return FileDetector.is_coordinate_master(filepath) or cls._normalized_filename(filepath) in cls.COORDINATE_MASTER_FILES

    @classmethod
    def _normalize_month(cls, value) -> str | None:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y%m")
        if isinstance(value, (dt.datetime, dt.date)):
            return f"{value.year:04d}{value.month:02d}"
        if isinstance(value, (int, float)):
            try:
                text = str(int(value))
                if cls.MONTH_PATTERN.fullmatch(text):
                    return text
                # Excel serial date fallback.
                if 20000 <= float(value) <= 80000:
                    return (dt.datetime(1899, 12, 30) + dt.timedelta(days=float(value))).strftime("%Y%m")
            except Exception:
                pass
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        text = str(value).strip()
        if not text:
            return None
        if cls.MONTH_PATTERN.fullmatch(text):
            return text
        match = cls.MONTH_SEARCH_PATTERN.search(text)
        return f"{match.group(1)}{match.group(2)}" if match else None

    @classmethod
    def _parse_date(cls, value) -> pd.Timestamp | None:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value
        if isinstance(value, (dt.datetime, dt.date)):
            return pd.Timestamp(value)
        if isinstance(value, (int, float)) and 20000 <= float(value) <= 80000:
            return pd.Timestamp(dt.datetime(1899, 12, 30) + dt.timedelta(days=float(value)))
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return pd.Timestamp(dt.datetime.strptime(text[:10], fmt))
            except (ValueError, TypeError):
                pass
        parsed = pd.to_datetime(text, errors="coerce")
        return None if pd.isna(parsed) else pd.Timestamp(parsed)

    @staticmethod
    def _column_number(cell_ref: str) -> int:
        number = 0
        for char in cell_ref:
            if not char.isalpha():
                break
            number = number * 26 + ord(char.upper()) - 64
        return number

    @classmethod
    def _workbook_sheet_map(cls, archive: zipfile.ZipFile) -> dict[str, str]:
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheets = workbook.find(f"{{{cls.XLSX_NS}}}sheets")
        result: dict[str, str] = {}
        if sheets is None:
            return result
        for sheet in sheets:
            name = sheet.attrib.get("name")
            rel_id = sheet.attrib.get(f"{{{cls.REL_NS}}}id")
            target = rel_map.get(rel_id)
            if not name or not target:
                continue
            if target.startswith("/"):
                path = target.lstrip("/")
            elif target.startswith("xl/"):
                path = target
            else:
                path = "xl/" + target.lstrip("/")
            result[name] = path
        return result

    @classmethod
    def _resolve_sheet_name_lightweight(cls, filepath: Path, dataset: str) -> str:
        with zipfile.ZipFile(filepath, "r") as archive:
            sheet_map = cls._workbook_sheet_map(archive)
        normalized = {str(name).strip().upper(): name for name in sheet_map}
        for priority in DatasetValidator.SHEET_PRIORITY.get(dataset, []):
            found = normalized.get(str(priority).strip().upper())
            if found:
                return found
        if sheet_map:
            return next(iter(sheet_map))
        raise ValueError(f"No worksheet found in '{filepath.name}'")

    @classmethod
    def _read_cell_raw(cls, cell: ET.Element) -> tuple[str | None, int | None]:
        value_node = cell.find(f"{{{cls.XLSX_NS}}}v")
        if value_node is None:
            inline = cell.find(f"{{{cls.XLSX_NS}}}is")
            if inline is None:
                return None, None
            return "".join(t.text or "" for t in inline.iter() if t.tag.rsplit("}", 1)[-1] == "t"), None
        if cell.attrib.get("t") == "s":
            try:
                return None, int(value_node.text or "-1")
            except ValueError:
                return None, None
        return value_node.text, None

    @classmethod
    def _read_header(cls, filepath: Path, sheet_name: str) -> tuple[int, dict[int, str]]:
        with zipfile.ZipFile(filepath, "r") as archive:
            sheet_path = cls._workbook_sheet_map(archive)[sheet_name]
            snapshots: list[tuple[int, list[tuple[int, str | None, int | None]]]] = []
            shared_ids: set[int] = set()
            with archive.open(sheet_path) as source:
                for _event, row in ET.iterparse(source, events=("end",)):
                    if row.tag.rsplit("}", 1)[-1] != "row":
                        continue
                    number = int(row.attrib.get("r", "0") or 0)
                    if number > 15:
                        row.clear()
                        break
                    cells = []
                    for cell in row:
                        if cell.tag.rsplit("}", 1)[-1] != "c":
                            continue
                        col = cls._column_number(cell.attrib.get("r", ""))
                        raw, shared_id = cls._read_cell_raw(cell)
                        if shared_id is not None:
                            shared_ids.add(shared_id)
                        cells.append((col, raw, shared_id))
                    snapshots.append((number, cells))
                    row.clear()
            shared = cls._shared_strings(archive, shared_ids)
        for number, cells in snapshots:
            values: list[str] = []
            resolved: dict[int, str] = {}
            for col, raw, shared_id in cells:
                value = shared.get(shared_id) if shared_id is not None else raw
                normalized = DatasetValidator.normalize_column(value)
                resolved[col] = normalized
                if value is not None:
                    values.append(normalized)
            if "IDPEL" in values or "LOCATIONCODE" in values:
                return number, resolved
        return 0, {}

    @classmethod
    def _shared_strings(cls, archive: zipfile.ZipFile, needed: set[int]) -> dict[int, str]:
        if not needed or "xl/sharedStrings.xml" not in archive.namelist():
            return {}
        result: dict[int, str] = {}
        current = -1
        with archive.open("xl/sharedStrings.xml") as source:
            for _event, elem in ET.iterparse(source, events=("end",)):
                if elem.tag.rsplit("}", 1)[-1] != "si":
                    continue
                current += 1
                if current in needed:
                    result[current] = "".join(t.text or "" for t in elem.iter() if t.tag.rsplit("}", 1)[-1] == "t")
                    if len(result) == len(needed):
                        elem.clear()
                        break
                elem.clear()
        return result

    @classmethod
    def _scan_month_cells(cls, archive: zipfile.ZipFile, sheet_path: str, header: int, targets: dict[int, str], shared: dict[int, str], collect_shared: bool) -> tuple[set[str], set[int], int]:
        target_cols = set(targets)
        thbl_key = DatasetValidator.normalize_column("THBL")
        thblrek_key = DatasetValidator.normalize_column("THBLREK")
        date_key = DatasetValidator.normalize_column("DLPD_TGLBACA")
        months: set[str] = set()
        shared_ids: set[int] = set()
        rows = 0
        started = time.monotonic()
        last_log = started
        with archive.open(sheet_path) as source:
            for _event, row in ET.iterparse(source, events=("end",)):
                if row.tag.rsplit("}", 1)[-1] != "row":
                    continue
                rows += 1
                if rows <= header:
                    row.clear()
                    continue
                thbl = thblrek = detail_date = None
                unresolved = False
                for cell in row:
                    if cell.tag.rsplit("}", 1)[-1] != "c":
                        continue
                    col = cls._column_number(cell.attrib.get("r", ""))
                    if col not in target_cols:
                        continue
                    raw, shared_id = cls._read_cell_raw(cell)
                    value = shared.get(shared_id) if shared_id is not None else raw
                    if shared_id is not None and value is None:
                        shared_ids.add(shared_id)
                        unresolved = True
                    key = targets[col]
                    if key == thbl_key:
                        thbl = value
                    elif key == thblrek_key:
                        thblrek = value
                    elif key == date_key:
                        detail_date = value
                for value in (thbl, thblrek):
                    month = cls._normalize_month(value)
                    if month:
                        months.add(month)
                if not thbl and not thblrek and not unresolved:
                    parsed = cls._parse_date(detail_date)
                    if parsed is not None:
                        months.add(parsed.strftime("%Y%m"))
                now = time.monotonic()
                if now - last_log >= 10:
                    logger.info("DLPD MONTH SCAN PROGRESS | ROWS=%s | FILE=%s | MONTHS=%s", max(0, rows - header), Path(archive.filename or "").name, sorted(months))
                    last_log = now
                row.clear()
        return months, shared_ids, max(0, rows - header)

    @classmethod
    def _read_dlpd_months(cls, filepath: Path, dataset: str, sheet_name: str) -> list[str]:
        """Resolve DLPD months with calamine, avoiding XML shared-string stalls."""
        try:
            from python_calamine import CalamineWorkbook
        except ImportError:
            CalamineWorkbook = None

        if CalamineWorkbook is not None:
            workbook = None
            try:
                logger.info(
                    "DLPD MONTH SCAN START | FILE=%s | ENGINE=calamine",
                    filepath.name,
                )
                workbook = CalamineWorkbook.from_path(filepath)
                normalized_sheets = {
                    str(name).strip().upper(): name
                    for name in workbook.sheet_names
                }
                selected = normalized_sheets.get(
                    str(sheet_name).strip().upper(),
                    sheet_name,
                )
                worksheet = workbook.get_sheet_by_name(selected)
                header_found = False
                targets: dict[int, str] = {}
                months: set[str] = set()
                rows = 0
                last_log = time.monotonic()

                for row in worksheet.iter_rows():
                    if not header_found:
                        normalized = [
                            DatasetValidator.normalize_column(value)
                            for value in row
                        ]
                        values = {value for value in normalized if value}
                        if (
                            "IDPEL" in values
                            and values.intersection(cls.DLPD_MONTH_COLUMNS)
                        ):
                            targets = {
                                index: value
                                for index, value in enumerate(normalized)
                                if value in cls.DLPD_MONTH_COLUMNS
                            }
                            header_found = True
                            logger.info(
                                "DLPD MONTH HEADER FOUND | FILE=%s | TARGETS=%s",
                                filepath.name,
                                [targets[index] for index in sorted(targets)],
                            )
                        continue

                    rows += 1
                    values = {
                        key: row[index] if index < len(row) else None
                        for index, key in targets.items()
                    }
                    thbl = values.get(DatasetValidator.normalize_column("THBL"))
                    thblrek = values.get(DatasetValidator.normalize_column("THBLREK"))
                    detail_date = values.get(
                        DatasetValidator.normalize_column("DLPD_TGLBACA")
                    )

                    for value in (thbl, thblrek):
                        month = cls._normalize_month(value)
                        if month:
                            months.add(month)
                    if thbl is None and thblrek is None:
                        parsed = cls._parse_date(detail_date)
                        if parsed is not None:
                            months.add(parsed.strftime("%Y%m"))

                    now = time.monotonic()
                    if now - last_log >= 10:
                        logger.info(
                            "DLPD MONTH SCAN PROGRESS | ROWS=%s | FILE=%s | MONTHS=%s",
                            rows,
                            filepath.name,
                            sorted(months),
                        )
                        last_log = now

                if not header_found:
                    raise ValueError(
                        f"Unable to locate DLPD header row in '{filepath.name}'."
                    )
                logger.info(
                    "DLPD MONTH SCAN COMPLETE | FILE=%s | ROWS=%s | MONTHS=%s | ENGINE=calamine",
                    filepath.name,
                    rows,
                    sorted(months),
                )
                return sorted(months)
            finally:
                if workbook is not None:
                    try:
                        workbook.close()
                    except Exception:
                        logger.exception(
                            "Failed to close DLPD month resolver workbook: %s",
                            filepath,
                        )

        logger.warning(
            "python-calamine unavailable; using legacy XML DLPD month scan | FILE=%s",
            filepath.name,
        )
        del dataset
        header, targets = cls._read_header(filepath, sheet_name)
        targets = {
            index: value
            for index, value in targets.items()
            if value in cls.DLPD_MONTH_COLUMNS
        }
        if not targets:
            logger.warning("DLPD MONTH COLUMNS NOT FOUND | FILE=%s", filepath.name)
            return []
        with zipfile.ZipFile(filepath, "r") as archive:
            sheet_path = cls._workbook_sheet_map(archive)[sheet_name]
            months, shared_ids, rows = cls._scan_month_cells(
                archive, sheet_path, header, targets, {}, True
            )
            if shared_ids:
                shared = cls._shared_strings(archive, shared_ids)
                months, _unused, rows = cls._scan_month_cells(
                    archive, sheet_path, header, targets, shared, False
                )
        logger.info(
            "DLPD MONTH SCAN COMPLETE | FILE=%s | ROWS=%s | MONTHS=%s | ENGINE=xml",
            filepath.name,
            rows,
            sorted(months),
        )
        return sorted(months)

    @classmethod
    def _read_first_month(cls, filepath: Path, dataset: str, sheet_name: str, column_name: str) -> str | None:
        header = DatasetValidator.detect_header_row(filepath=filepath, sheet_name=sheet_name)
        dataframe = pd.read_excel(filepath, sheet_name=sheet_name, header=header, usecols=lambda c: DatasetValidator.normalize_column(c) == DatasetValidator.normalize_column(column_name))
        for value in dataframe.iloc[:, 0].tolist() if not dataframe.empty else []:
            month = cls._normalize_month(value)
            if month:
                return month
        return None

    @classmethod
    def resolve_months(cls, filepath: Path, dataset: str | None = None) -> list[str]:
        filepath = Path(filepath)
        if cls.is_coordinate_master(filepath):
            return []
        if dataset is None:
            dataset = FileDetector.detect(filepath)
        if dataset == FileDetector.UNKNOWN:
            raise ValueError(f"Unable to determine dataset type for month resolution: {filepath.name}")
        if dataset in cls.DLPD_DATASETS:
            sheet_name = cls._resolve_sheet_name_lightweight(filepath, dataset)
            return cls._read_dlpd_months(filepath, dataset, sheet_name)
        sheet_name = DatasetValidator.get_sheet_name(filepath, dataset)
        column_name = {"ANEV": "READ_DATE", "PENGECEKAN": "WAKTU_PERIKSA", "CUSTOMER_LOCATION": "MONTH"}.get(dataset, "MONTH")
        month = cls._read_first_month(filepath, dataset, sheet_name, column_name)
        return [month] if month else []
