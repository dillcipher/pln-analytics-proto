"""Regression tests for DLPDTransformer.transform()'s per-row MONTH
resolution.

Root-caused live 2026-09-03 against the real production file "DLPD Tidak
beli Token.xlsx" (the DLPD_PRABAYAR source, 176,127 rows), reproduced
locally by staging the actual workbook and running the transformer on it
directly -- entirely outside GitHub Actions/Supabase, which were blocked
by an unrelated Supabase Storage quota outage at the time.

The file's real schema has a populated THBL column (all rows: 202606) but
does NOT have THBLREK or DLPD_TGLBACA columns at all -- that's a normal,
legitimate shape for this dataset, not a data quality problem.

Before the fix, `transform()` injected blank "" THBL/THBLREK placeholder
columns (the "ENSURE SOURCE PERIOD COLUMNS EXIST" block) *before* calling
`_resolve_month_per_row()`. That function's fallback chain (used because
THBLREK/DLPD_TGLBACA aren't part of the "full business-rule" column set)
checks THBLREK before THBL -- so it silently picked up the synthetic
blank THBLREK placeholder instead of falling through to the real THBL
data, and MONTH resolved to "" for every single row. This is exactly the
"DLPD_PRABAYAR ... produced no monthly rows" quarantine seen identically
across GitHub Actions runs #8, #9, and #10, with neither the "DLPD PERIOD
COLUMN ALIASED" nor "DLPD MONTH UNRESOLVABLE" log line ever firing --
because neither of those code paths was the one actually taken.

The fix reorders `transform()` to resolve MONTH *before* the placeholder
columns are injected, so the fallback chain sees the dataframe's true
column shape.
"""

import pandas as pd

from app.etl.transformers.dlpd_transformer import DLPDTransformer


def test_month_resolves_from_thbl_when_thblrek_and_tglbaca_are_absent():
    """Regression coverage note (added 2026-09-03 after GitHub Actions run
    #17): this test and the ones below exercise DLPDTransformer.transform()
    directly, WITHOUT installing app.etl.detector.dlpd_transformer_patch's
    monkeypatch. That patch replaces DLPDTransformer.transform wholesale in
    every real run (app.main installs it before any ETL executes), and for
    a long time it silently carried its own stale, unfixed duplicate of
    this exact ordering bug -- so passing tests here gave false confidence
    while run #17 quarantined DLPD_PRABAYAR again with the identical
    symptom. See test_dlpd_transformer_patch_month_resolution.py for the
    equivalent coverage against the ACTUAL patched code path."""
    """The exact shape of the real DLPD_PRABAYAR source file: THBL is
    populated, THBLREK and DLPD_TGLBACA don't exist as columns at all."""

    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573", "171002466418", "171002470000"],
            "THBL": [202606, 202606, 202606],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == ["202606", "202606", "202606"]
    assert (result["MONTH"] == "").sum() == 0


def test_month_resolves_from_thbl_as_string_too():
    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573"],
            "THBL": ["202606"],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == ["202606"]


def test_full_business_rule_path_still_resolves_via_tglbaca_when_all_three_present():
    """When THBL/THBLREK/DLPD_TGLBACA are ALL genuinely present, the
    "full business-rule path" (_resolve_row_month, driven by
    DLPD_TGLBACA) must still take priority -- this reorder must not
    regress that path."""

    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573"],
            "THBL": [202605],
            "THBLREK": [202606],
            "DLPD_TGLBACA": pd.to_datetime(["2026-06-15"]),
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == ["202606"]


def test_month_blank_and_placeholder_columns_still_created_when_nothing_resolvable():
    """A row with none of THBL/THBLREK/DLPD_TGLBACA/MONTH populated still
    ends up with a blank MONTH (never crashes), and THBL/THBLREK still
    exist as columns afterward for downstream schema consistency."""

    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573"],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == [""]
    assert "THBL" in result.columns
    assert "THBLREK" in result.columns
    assert list(result["THBL"]) == [""]
    assert list(result["THBLREK"]) == [""]
