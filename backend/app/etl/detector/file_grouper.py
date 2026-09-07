from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

from app.core.constants import RAW_UPLOAD
from app.etl.detector.detector import FileDetector
from app.etl.detector.month_resolver import MonthResolver


class FileGrouper:
    """Group uploaded files by dataset and business month."""

    KNOWN_DATASETS = {
        FileDetector.ANEV,
        FileDetector.DLPD_PASCABAYAR,
        FileDetector.DLPD_PRABAYAR,
        FileDetector.PENGECEKAN,
        FileDetector.CUSTOMER_LOCATION,
    }

    @staticmethod
    def _normalize_dataset(value) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
        aliases = {
            "DLPD_PRA": FileDetector.DLPD_PRABAYAR,
            "DLPD_PRABAYAR": FileDetector.DLPD_PRABAYAR,
            "PRABAYAR": FileDetector.DLPD_PRABAYAR,
            "DLPD_PASCA": FileDetector.DLPD_PASCABAYAR,
            "DLPD_PASCABAYAR": FileDetector.DLPD_PASCABAYAR,
            "PASCABAYAR": FileDetector.DLPD_PASCABAYAR,
            "PENGECEK": FileDetector.PENGECEKAN,
            "PENGECEKAN": FileDetector.PENGECEKAN,
            "CUSTOMER_LOCATION": FileDetector.CUSTOMER_LOCATION,
            "CUSTOMERLOCATION": FileDetector.CUSTOMER_LOCATION,
            "DIL": FileDetector.CUSTOMER_LOCATION,
        }
        return aliases.get(normalized, normalized or None)

    @staticmethod
    def _infer_anev_month(filename: str) -> str | None:
        """Infer YYYYMM from an ANEV filename when possible."""
        name = Path(filename).name

        # Common forms: YYYYMMDD, YYYY-MM-DD, YYYY_MM_DD, YYYYMM.
        patterns = (
            r"(20\d{2})(0[1-9]|1[0-2])\d{2}",
            r"(20\d{2})[-_/](0[1-9]|1[0-2])[-_/]\d{2}",
            r"(20\d{2})(0[1-9]|1[0-2])(?!\d)",
        )

        for pattern in patterns:
            match = re.search(pattern, name)
            if match:
                return f"{match.group(1)}{match.group(2)}"

        return None

    @staticmethod
    def _find_uploaded_file(filename: str) -> Path | None:
        try:
            candidates = [
                path
                for path in RAW_UPLOAD.glob(f"*/{Path(filename).name}")
                if path.is_file()
            ]
        except Exception:
            return None
        if not candidates:
            return None
        return max(candidates, key=lambda path: path.stat().st_mtime_ns)

    @classmethod
    def _resolve_dataset(cls, file: dict, job_folder: Path | None = None) -> str | None:
        filename = file.get("filename") or file.get("name")
        manifest_dataset = cls._normalize_dataset(file.get("dataset"))

        if filename and job_folder is None:
            record_job_id = str(file.get("job_id") or "").strip()
            if record_job_id:
                candidate_folder = RAW_UPLOAD / record_job_id
                candidate = candidate_folder / str(filename)
                if candidate.exists() and candidate.is_file():
                    try:
                        detected = FileDetector.detect(candidate)
                    except Exception:
                        detected = FileDetector.UNKNOWN
                    if detected in cls.KNOWN_DATASETS:
                        return detected
                    return manifest_dataset if manifest_dataset in cls.KNOWN_DATASETS else None

        if filename and job_folder is not None:
            candidate = job_folder / str(filename)
            if candidate.exists() and candidate.is_file():
                try:
                    detected = FileDetector.detect(candidate)
                except Exception:
                    detected = FileDetector.UNKNOWN
                if detected in cls.KNOWN_DATASETS:
                    return detected
                return manifest_dataset if manifest_dataset in cls.KNOWN_DATASETS else None

        if filename:
            try:
                detected = FileDetector.detect(Path(str(filename)))
            except Exception:
                detected = FileDetector.UNKNOWN
            if detected in cls.KNOWN_DATASETS:
                return detected

        if filename:
            actual_path = cls._find_uploaded_file(str(filename))
            if actual_path is not None:
                try:
                    detected = FileDetector.detect(actual_path)
                except Exception:
                    detected = FileDetector.UNKNOWN
                if detected in cls.KNOWN_DATASETS:
                    return detected

        if manifest_dataset in cls.KNOWN_DATASETS:
            return manifest_dataset
        return None

    @classmethod
    def _resolve_anev_month(cls, filename: str, job_folder: Path | None = None) -> str | None:
        """Resolve one ANEV file to its business month.

        Filename inference is the cheap path. If the filename does not carry
        a usable date/month, inspect only the ANEV READ_DATE column through
        MonthResolver. This guarantees ANEV never falls back to ANEV/None
        when the workbook itself contains a resolvable month.
        """
        month = cls._infer_anev_month(filename)
        if month:
            return month

        path = None
        if job_folder is not None:
            candidate = job_folder / Path(filename).name
            if candidate.exists() and candidate.is_file():
                path = candidate

        if path is None:
            path = cls._find_uploaded_file(filename)

        if path is None:
            return None

        try:
            months = MonthResolver.resolve_months(path, dataset=FileDetector.ANEV)
        except Exception:
            # Grouping must not crash merely because a single ANEV workbook
            # cannot expose a month. The ETL layer will report the detailed
            # workbook error later.
            return None

        normalized = sorted({str(month).strip() for month in months if month})
        if len(normalized) == 1:
            return normalized[0]

        # A single ANEV file containing multiple months cannot safely be
        # represented by one group key. Returning None makes that condition
        # visible to the orchestrator instead of silently assigning the wrong
        # month.
        return None

    @classmethod
    def group(cls, files: list[dict], job_folder: Path | None = None) -> dict[tuple[str | None, str | None], list[dict]]:
        grouped: DefaultDict[tuple[str | None, str | None], list[dict]] = defaultdict(list)
        unknown_files: list[str] = []

        for file in files:
            record = dict(file)
            filename = record.get("filename") or record.get("name")
            dataset = cls._resolve_dataset(record, job_folder=job_folder)
            month = record.get("month")

            if dataset == FileDetector.ANEV and not month and filename:
                month = cls._resolve_anev_month(str(filename), job_folder=job_folder)
                if month:
                    record["month"] = month

            if dataset is None:
                unknown_files.append(str(filename or "<unknown>"))
                continue

            record["dataset"] = dataset
            grouped[(dataset, month)].append(record)

        if unknown_files:
            raise ValueError(
                "Unable to detect dataset for uploaded file(s): "
                + ", ".join(unknown_files)
                + ". Use a supported PLN dataset or upload a valid Excel workbook."
            )
        return dict(grouped)
