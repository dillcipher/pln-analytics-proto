"""Regression tests for the ACTUAL code path GitHub Actions / production run:
DLPDTransformer.transform() after app.etl.detector.dlpd_transformer_patch's
monkeypatch has been installed.

Root-caused live 2026-09-03 against GitHub Actions run #17 (job
JOB_20260903_152610_DRIVE_428dd052, the first run to ever reach this code
path with real Drive credentials against the real "DLPD Tidak beli
Token.xlsx" production file, 176,127 rows): DLPD_PRABAYAR was quarantined
again with the exact same "produced no monthly rows" error that
tests/test_dlpd_transformer_month_resolution.py's fix was supposed to have
closed out.

The reason: app.main installs dlpd_transformer_patch.install_dlpd_transformer_patch()
before any real ETL run, which replaces DLPDTransformer.transform wholesale
with a separate, hand-duplicated copy of the transform logic
(`_transform` in dlpd_transformer_patch.py). That copy never received any of
the ordering fixes applied to dlpd_transformer.py's own transform() method
-- it still injected blank THBL/THBLREK placeholder columns *before*
resolving MONTH, so MONTH silently resolved to "" for every row via the
THBLREK fallback branch, exactly like the original (already-fixed) bug.
Every test in test_dlpd_transformer_month_resolution.py calls
`DLPDTransformer().transform(df)` directly without installing this patch,
so none of them ever exercised the code path production actually runs --
which is exactly how this regression went undetected until a real
GitHub Actions run hit it.

These tests install the patch first (mirroring app.main's own startup
sequence) so they exercise the exact method object that runs in production.
"""

import pandas as pd

from app.etl.detector.dlpd_transformer_patch import install_dlpd_transformer_patch
from app.etl.transformers.dlpd_transformer import DLPDTransformer

install_dlpd_transformer_patch()


def test_patched_month_resolves_from_thbl_when_thblrek_and_tglbaca_are_absent():
    """The exact shape of the real DLPD_PRABAYAR source file ("DLPD Tidak
    beli Token.xlsx"): THBL is populated for every row, THBLREK and
    DLPD_TGLBACA don't exist as columns at all. Before this fix, the
    patched transform() resolved MONTH to "" for every single row here."""

    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573", "171002466418", "171002470000"],
            "THBL": [202606, 202606, 202606],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == ["202606", "202606", "202606"]
    assert (result["MONTH"] == "").sum() == 0


def test_patched_month_resolves_from_thbl_as_string_too():
    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573"],
            "THBL": ["202606"],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert list(result["MONTH"]) == ["202606"]


def test_patched_full_business_rule_path_still_resolves_via_tglbaca_when_all_three_present():
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


def test_patched_same_idpel_across_two_months_is_kept_not_deduplicated_away():
    """The one behaviour this patch deliberately adds over the base
    transform(): the same IDPEL appearing in two different resolved months
    must survive (deduplication is by (IDPEL, MONTH), not IDPEL alone)."""

    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573", "171002463573"],
            "THBL": [202605, 202606],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert sorted(result["MONTH"]) == ["202605", "202606"]
    assert len(result) == 2


def test_patched_duplicate_idpel_within_same_month_is_deduplicated():
    dataframe = pd.DataFrame(
        {
            "IDPEL": ["171002463573", "171002463573"],
            "THBL": [202606, 202606],
        }
    )

    result = DLPDTransformer().transform(dataframe)

    assert len(result) == 1
    assert list(result["MONTH"]) == ["202606"]
