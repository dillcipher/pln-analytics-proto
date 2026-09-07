"""Correct DLPD transformation ordering and low-memory ingestion wiring.

The legacy transformer removed duplicate IDPEL values before resolving MONTH.
That is unsafe when one workbook contains the same customer in several
business months: later months were silently discarded. This patch preserves
the existing business rules but performs deduplication after MONTH is known,
using (IDPEL, MONTH) as the natural partition key.

IMPORTANT (root-caused live 2026-09-03 against GitHub Actions run #17, the
first run ever to reach this code path against the real "DLPD Tidak beli
Token.xlsx" production file): this module installs itself over
`DLPDTransformer.transform` via `DLPDTransformer.transform = _transform`
below -- app.main installs it (see its patch-installing side effects) before
ANY real ETL run, including every GitHub Actions run. That means THIS
`_transform` function, not the one in dlpd_transformer.py, is what actually
runs in production and in every GitHub Actions run.

`dlpd_transformer.py`'s own `transform()` was fixed three separate times
(see its docstring/comments, commits around 26a09ad/9b1fd57/a58b8a7) to (a)
call `_alias_period_columns()` before anything else, and (b) resolve MONTH
*before* injecting blank THBL/THBLREK placeholder columns -- injecting the
placeholders first makes `_resolve_month_per_row()`'s fallback chain see a
synthetic blank THBLREK as "present" and return "" for every row instead of
falling through to real THBL data. This `_transform` function was a
completely separate, hand-duplicated copy of that same logic that never
received any of those three fixes -- it still aliased nothing and injected
THBL/THBLREK placeholders *before* calling `_resolve_month_per_row()`,
silently reintroducing the exact "produced no monthly rows" bug. Every local
verification of the dlpd_transformer.py fix (including the existing test
suite in tests/test_dlpd_transformer_month_resolution.py) called
`DLPDTransformer().transform(df)` directly without ever installing this
patch, so none of it ever exercised the code path GitHub Actions actually
runs -- which is exactly why run #17 quarantined DLPD_PRABAYAR again despite
that fix being live on `main`.

This `_transform` now mirrors dlpd_transformer.py's fixed ordering exactly
(alias periods -> resolve MONTH -> only then ensure THBL/THBLREK exist as
columns), and keeps this patch's one deliberate behavioural difference: no
`remove_duplicates()` call before MONTH is known, with (IDPEL, MONTH)
deduplication happening afterward instead.
"""

from __future__ import annotations

from app.etl.transformers.dlpd_transformer import DLPDTransformer

_INSTALLED = False


def _transform(self, dataframe):
    dataframe = self.normalize_columns(dataframe)

    # Must run before the THBL/THBLREK placeholder-injection block below --
    # see the module docstring above for the full story.
    dataframe = self._alias_period_columns(dataframe)

    dataframe = self.clean_idpel(dataframe)
    # Deliberately NO remove_duplicates() call here -- that's the exact bug
    # this patch exists to fix (see module docstring). Dedup happens below,
    # by (IDPEL, MONTH), only after MONTH is resolved.

    dataframe = self.clean_strings(dataframe, self.STRING_COLUMNS)
    dataframe = self.clean_numeric(dataframe, self.NUMERIC_COLUMNS)
    dataframe = self.clean_dates(dataframe, self.DATE_COLUMNS)

    # MONTH must be resolved BEFORE THBL/THBLREK placeholders are injected
    # below -- see the module docstring above.
    dataframe["MONTH"] = self._resolve_month_per_row(dataframe)
    dataframe["MONTH"] = (
        dataframe["MONTH"]
        .apply(self._normalize_month_value)
        .fillna("")
        .astype(str)
        .str.strip()
    )

    # Runs AFTER MONTH resolution -- only guarantees THBL/THBLREK exist as
    # columns for downstream schema consistency; must never influence which
    # column MONTH was actually resolved from.
    if "THBL" not in dataframe.columns:
        dataframe["THBL"] = ""
    if "THBLREK" not in dataframe.columns:
        dataframe["THBLREK"] = ""

    dataframe["THBL"] = dataframe["THBL"].fillna("").astype(str).str.strip()
    dataframe["THBLREK"] = dataframe["THBLREK"].fillna("").astype(str).str.strip()

    if "DATASET" not in dataframe.columns:
        dataframe["DATASET"] = "DLPD"
    dataframe["DATASET"] = (
        dataframe["DATASET"].fillna("DLPD").astype(str).str.strip()
    )

    # A customer may legitimately occur once in each business month.
    # Deduplicate only inside the same month, after MONTH is resolved.
    if "IDPEL" in dataframe.columns and "MONTH" in dataframe.columns:
        before = len(dataframe)
        dataframe = dataframe.drop_duplicates(
            subset=["IDPEL", "MONTH"],
            keep="first",
        ).copy()
        removed = before - len(dataframe)
        if removed:
            import logging
            logging.getLogger(__name__).info(
                "DLPD duplicate rows removed after month resolution: %s",
                removed,
            )

    return dataframe


def install_dlpd_transformer_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    DLPDTransformer.transform = _transform

    # The DLPD merger already processes bounded pandas chunks, but its legacy
    # workbook iterator used openpyxl directly. Replace only that iterator
    # with the Rust-backed calamine implementation; all transformation,
    # deduplication, coordinate enrichment, and publishing rules remain in
    # the existing merger.
    from app.etl.merger.dlpd_calamine_reader_patch import (
        install_dlpd_calamine_reader_patch,
    )

    install_dlpd_calamine_reader_patch()
    _INSTALLED = True
