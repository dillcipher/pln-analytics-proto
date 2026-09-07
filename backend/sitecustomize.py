"""Runtime guard for Excel files assembled from chunked uploads.

Python imports ``sitecustomize`` automatically when this directory is on
sys.path.  The guard is intentionally narrow: it only affects Excel files
under the raw incoming upload tree.  It waits until the file is stable and
has a valid ZIP container before pandas is allowed to open it.
"""

from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path


try:
    import pandas as _pd

    _OriginalExcelFile = _pd.ExcelFile

    _INCOMING_MARKERS = (
        os.path.normpath("/app/data/raw/incoming"),
        os.path.normpath("data/raw/incoming"),
    )

    def _is_incoming_excel(path) -> bool:
        try:
            resolved = os.path.abspath(os.fspath(path))
        except (TypeError, ValueError, OSError):
            return False

        return any(
            resolved == marker
            or resolved.startswith(marker + os.sep)
            for marker in _INCOMING_MARKERS
        )

    def _wait_for_complete_excel(path) -> None:
        """Wait for a chunk-assembled XLSX/XLSM to become readable."""
        try:
            suffix = Path(path).suffix.lower()
        except (TypeError, ValueError, OSError):
            return

        if suffix not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            return

        if not _is_incoming_excel(path):
            return

        deadline = time.monotonic() + float(
            os.getenv("EXCEL_ASSEMBLY_WAIT_SECONDS", "180")
        )
        previous = None
        stable_since = None

        while time.monotonic() < deadline:
            try:
                stat = os.stat(path)
                size = stat.st_size
                mtime_ns = stat.st_mtime_ns
            except FileNotFoundError:
                time.sleep(0.5)
                continue
            except OSError:
                time.sleep(0.5)
                continue

            current = (size, mtime_ns)
            if current != previous:
                previous = current
                stable_since = time.monotonic()
                time.sleep(0.5)
                continue

            if stable_since is None or time.monotonic() - stable_since < 0.75:
                time.sleep(0.25)
                continue

            try:
                with zipfile.ZipFile(path, "r") as archive:
                    bad_member = archive.testzip()
                if bad_member is None:
                    return
            except (zipfile.BadZipFile, OSError):
                pass

            time.sleep(0.75)

        raise RuntimeError(
            f"Excel assembly did not become a valid workbook within "
            f"{os.getenv('EXCEL_ASSEMBLY_WAIT_SECONDS', '180')}s: {path}"
        )

    def _SafeExcelFile(path_or_buffer, *args, **kwargs):
        if isinstance(path_or_buffer, (str, os.PathLike)):
            _wait_for_complete_excel(path_or_buffer)
        return _OriginalExcelFile(path_or_buffer, *args, **kwargs)

    _pd.ExcelFile = _SafeExcelFile

except Exception as _exc:
    print(f"WARNING: Excel assembly guard unavailable: {_exc!r}")


# ---------------------------------------------------------------------------
# Production ETL memory guard
# ---------------------------------------------------------------------------
# These streaming implementations already exist in the repository, but
# previously were only patch modules and were never installed.  Activate them
# automatically so the production Drive -> ETL path does not fall back to
# full-workbook pandas reads for DLPD files.
try:
    from app.etl.detector.streaming_month_resolver_patch import (
        install_streaming_month_resolver_patch,
    )
    from app.etl.merger.streaming_dlpd_merger_patch import (
        install_streaming_dlpd_merger_patch,
    )

    install_streaming_month_resolver_patch()
    install_streaming_dlpd_merger_patch()
    print("INFO: Streaming DLPD ETL patches installed")
except Exception as _etl_patch_exc:
    # Never make the API fail to boot because the optional optimization could
    # not be installed. The error is explicit so deployment logs reveal it.
    print(f"WARNING: Streaming DLPD ETL patches unavailable: {_etl_patch_exc!r}")
