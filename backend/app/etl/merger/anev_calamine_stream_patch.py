"""True low-memory ANEV XLSX reader.

ANEV files can be very large. Do not use openpyxl, pandas ExcelFile, or
CalamineWorkbook for the full workbook because those approaches can retain a
large workbook representation in RAM. XLSX is a ZIP of XML files, so stream
sheet XML with iterparse and only materialize a small row batch at a time.
"""
from __future__ import annotations

import gc
import logging
import os
import posixpath
import re
import zipfile
from pathlib import Path
from xml.etree.ElementTree import iterparse

import pandas as pd

logger = logging.getLogger(__name__)
_INSTALLED = False
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_PACKAGE_REL_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"

# Production ANEV exports are not perfectly uniform. Keep a permissive
# signature set and only use it to locate the real table header; the file has
# already been classified as ANEV before this streaming reader is selected.
_HEADER_ALIASES = {
    "LOCATIONCODE": "LOCATION_CODE",
    "LOCATION": "LOCATION_CODE",
    "KODELOKASI": "LOCATION_CODE",
    "KODELOK": "LOCATION_CODE",
    "READDATE": "READ_DATE",
    "READDATE": "READ_DATE",
    "TANGGALBACA": "READ_DATE",
    "TGLBACA": "READ_DATE",
    "TANGGALREAD": "READ_DATE",
    "SUSPECTNAME": "SUSPECT_NAME",
    "NAMASUSPECT": "SUSPECT_NAME",
    "NAMA": "SUSPECT_NAME",
    "IDPEL": "IDPEL",
    "IDPELANGGAN": "IDPEL",
    "NOMORIDPEL": "IDPEL",
}


def _normalize(value: object) -> str:
    return str(value or "").replace("\n", " ").replace("\r", " ").strip().upper()


def _key(value: object) -> str:
    return "".join(ch for ch in _normalize(value) if ch.isalnum())


def _canonical_header(value: object, index: int) -> str:
    raw = _normalize(value)
    key = _key(raw)
    name = _HEADER_ALIASES.get(key)
    if name:
        return name
    name = re.sub(r"\s+", "_", raw)
    name = re.sub(r"[^A-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or f"UNNAMED_{index}"


def _header_score(row: list[object]) -> tuple[int, int, int]:
    """Return a stable score for locating a real tabular header row."""
    values = [value for value in row if _normalize(value)]
    if not values:
        return (0, 0, 0)
    keys = {_key(value) for value in values}
    signature_hits = len(keys & set(_HEADER_ALIASES))
    # Metadata/title rows are usually one or two cells; wide rows are a strong
    # fallback when a customer export uses an unfamiliar ANEV schema.
    text_cells = sum(not str(value).strip().replace(".", "", 1).isdigit() for value in values)
    return (signature_hits, min(len(values), 64), text_cells)


def _is_header(row: list[object]) -> bool:
    signature_hits, width, _ = _header_score(row)
    # Accept known ANEV schema immediately, or a sufficiently wide row when
    # the filename has already classified the workbook as ANEV.
    return signature_hits >= 1 or width >= 4


def _col_index(ref: str) -> int:
    letters = re.match(r"[A-Za-z]+", ref or "")
    if not letters:
        return 0
    n = 0
    for ch in letters.group(0).upper():
        n = n * 26 + ord(ch) - 64
    return n - 1


def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        with zf.open("xl/sharedStrings.xml") as fh:
            strings: list[str] = []
            for _, elem in iterparse(fh, events=("end",)):
                if elem.tag != _NS + "si":
                    continue
                strings.append("".join(t.text or "" for t in elem.iter(_NS + "t")))
                elem.clear()
            return strings
    except KeyError:
        return []


def _workbook_sheet_path(zf: zipfile.ZipFile) -> str:
    target_rid = None
    target_name = None
    fallback_rid = None
    fallback_name = None
    with zf.open("xl/workbook.xml") as fh:
        for _, elem in iterparse(fh, events=("end",)):
            if elem.tag != _NS + "sheet":
                continue
            name = elem.attrib.get("name", "")
            rid = elem.attrib.get(_REL_NS + "id")
            if fallback_rid is None:
                fallback_rid, fallback_name = rid, name
            normalized = _key(name)
            if normalized in {"ANEV", "ANNEV", "SHEET1"}:
                target_name = name
                target_rid = rid
                elem.clear()
                break
            elem.clear()

    target_rid = target_rid or fallback_rid
    target_name = target_name or fallback_name
    if not target_rid:
        raise ValueError("Unable to resolve ANEV worksheet")

    with zf.open("xl/_rels/workbook.xml.rels") as fh:
        for _, elem in iterparse(fh, events=("end",)):
            if elem.tag == _PACKAGE_REL_NS + "Relationship" and elem.attrib.get("Id") == target_rid:
                target = elem.attrib.get("Target", "")
                target = posixpath.normpath(posixpath.join("xl", target))
                if target.startswith("../"):
                    target = target[3:]
                return target
    raise ValueError(f"Unable to resolve worksheet relationship for {target_name}")


def _cell_value(cell, shared: list[str]):
    typ = cell.attrib.get("t")
    if typ == "inlineStr":
        return "".join(t.text or "" for t in cell.iter(_NS + "t"))
    v = cell.find(_NS + "v")
    if v is None:
        return None
    value = v.text
    if value is None:
        return None
    if typ == "s":
        try:
            return shared[int(value)]
        except (ValueError, IndexError):
            return value
    if typ == "b":
        return value == "1"
    return value


def _iter_rows_xml(path: Path):
    with zipfile.ZipFile(path, "r") as zf:
        shared = _shared_strings(zf)
        sheet_path = _workbook_sheet_path(zf)
        with zf.open(sheet_path) as fh:
            for _, row in iterparse(fh, events=("end",)):
                if row.tag != _NS + "row":
                    continue
                values: list[object] = []
                for cell in row.findall(_NS + "c"):
                    idx = _col_index(cell.attrib.get("r", ""))
                    if idx >= len(values):
                        values.extend([None] * (idx + 1 - len(values)))
                    values[idx] = _cell_value(cell, shared)
                yield values
                row.clear()


def _find_header(rows, scan_limit: int = 200) -> list[object] | None:
    """Find the strongest candidate in the opening rows.

    Do not stop at a decorative title row. Prefer schema signatures, otherwise
    use the widest text-like row because this reader is only invoked for files
    already detected as ANEV.
    """
    best: list[object] | None = None
    best_score = (0, 0, 0)
    for _ in range(scan_limit):
        try:
            row = next(rows)
        except StopIteration:
            break
        score = _header_score(row)
        if score > best_score:
            best, best_score = row, score
        if score[0] >= 2:
            break
    return best if best_score[0] >= 1 or best_score[1] >= 4 else None


def iter_chunks(path: Path, chunk_rows: int):
    """Yield bounded DataFrames without retaining the workbook in memory."""
    rows = _iter_rows_xml(Path(path))
    header = _find_header(rows)
    if header is None:
        raise ValueError(
            f"Unable to locate a tabular ANEV header in {path.name}; "
            "scanned the first 200 rows."
        )

    columns = [_canonical_header(value, index) for index, value in enumerate(header)]
    seen: dict[str, int] = {}
    for index, column in enumerate(columns):
        count = seen.get(column, 0)
        seen[column] = count + 1
        if count:
            columns[index] = f"{column}_{count}"

    batch: list[list[object]] = []
    for row in rows:
        if not row or not any(_normalize(value) for value in row):
            continue
        if len(row) < len(columns):
            row.extend([None] * (len(columns) - len(row)))
        elif len(row) > len(columns):
            row = row[: len(columns)]
        batch.append(row)
        if len(batch) >= chunk_rows:
            yield pd.DataFrame(batch, columns=columns)
            batch = []
    if batch:
        yield pd.DataFrame(batch, columns=columns)


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    import app.etl.merger.streaming_dlpd_publish_guard as guard
    original_chunks = guard._chunks

    def safe_chunks(path: Path, dataset: str, n: int):
        if dataset != "ANEV":
            yield from original_chunks(path, dataset, n)
            return
        configured = int(os.getenv("ANEV_STREAM_CHUNK_ROWS", str(n or 1000)))
        size = min(2000, max(250, configured))
        logger.info("XML STREAM ANEV READER | %s | chunk_rows=%s", path.name, size)
        yield from iter_chunks(Path(path), size)

    guard._chunks = safe_chunks
    _INSTALLED = True
    logger.info("Installed resilient XML-streaming ANEV reader (schema-aware header fallback).")
