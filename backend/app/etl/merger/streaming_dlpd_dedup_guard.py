"""Correct DLPD deduplication for streaming ingestion.

The first streaming implementation kept a global IDPEL-only seen set. That
is unsafe for DLPD because the same customer legitimately appears again in a
different business month. This guard scopes the persistent dedup key to the
row's business-period signal while retaining first-row-wins behavior.
"""

from __future__ import annotations

import logging

import pandas as pd

import app.etl.merger.streaming_dlpd_merger_patch as streaming
from app.etl.merger.monthly_merger import MonthlyMerger

logger = logging.getLogger(__name__)
_INSTALLED = False


def _month_scope(frame: pd.DataFrame) -> pd.Series:
    """Build a stable business-month scope from raw DLPD source columns."""
    scope = pd.Series("", index=frame.index, dtype="object")

    if "DLPD_TGLBACA" in frame.columns:
        parsed = pd.to_datetime(frame["DLPD_TGLBACA"], errors="coerce")
        scope = parsed.dt.strftime("%Y%m").fillna("")

    if "THBLREK" in frame.columns:
        thblrek = (
            frame["THBLREK"]
            .apply(lambda value: str(value).strip() if pd.notna(value) else "")
        )
        scope = scope.mask(scope.eq(""), thblrek)

    if "THBL" in frame.columns:
        thbl = (
            frame["THBL"]
            .apply(lambda value: str(value).strip() if pd.notna(value) else "")
        )
        scope = scope.mask(scope.eq(""), thbl)

    return scope.fillna("").astype(str).str.strip()


def _scoped_deduplicate(frame: pd.DataFrame, seen_db) -> pd.DataFrame:
    if frame.empty or "IDPEL" not in frame.columns:
        return frame

    frame = frame.copy()
    frame["IDPEL"] = MonthlyMerger._normalize_idpel_series(frame["IDPEL"])
    frame = frame.loc[frame["IDPEL"].ne("")].copy()
    if frame.empty:
        return frame

    # Preserve the original transformer contract within each streaming chunk.
    frame = frame.loc[~frame["IDPEL"].duplicated(keep="first")].copy()
    if frame.empty:
        return frame

    scope = _month_scope(frame)
    keys = frame["IDPEL"].astype(str) + "|" + scope
    keys = keys.astype(str)

    seen_db.execute(
        "CREATE TABLE IF NOT EXISTS seen_scoped (scope_key TEXT PRIMARY KEY)"
    )
    seen_db.execute("DELETE FROM batch_ids")
    seen_db.executemany(
        "INSERT OR IGNORE INTO batch_ids(idpel) VALUES (?)",
        ((value,) for value in keys.tolist()),
    )

    # batch_ids is only used as a temporary carrier by the original module;
    # its column name is retained for compatibility. Recreate the scoped table
    # lookup with a direct VALUES-style insert to avoid keeping a Python-wide set.
    seen_db.execute("CREATE TEMP TABLE IF NOT EXISTS batch_scopes (scope_key TEXT PRIMARY KEY)")
    seen_db.execute("DELETE FROM batch_scopes")
    seen_db.executemany(
        "INSERT OR IGNORE INTO batch_scopes(scope_key) VALUES (?)",
        ((value,) for value in keys.tolist()),
    )

    new_rows = seen_db.execute(
        """
        SELECT b.scope_key
        FROM batch_scopes AS b
        LEFT JOIN seen_scoped AS s ON s.scope_key = b.scope_key
        WHERE s.scope_key IS NULL
        """
    ).fetchall()
    new_keys = {row[0] for row in new_rows}
    if not new_keys:
        return frame.iloc[0:0].copy()

    seen_db.executemany(
        "INSERT OR IGNORE INTO seen_scoped(scope_key) VALUES (?)",
        ((value,) for value in new_keys),
    )
    seen_db.commit()

    return frame.loc[keys.isin(new_keys)].copy()


def install_streaming_dlpd_dedup_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    streaming._deduplicate_first_idpel = _scoped_deduplicate
    _INSTALLED = True
    logger.info("Installed month-scoped streaming DLPD deduplication guard.")
