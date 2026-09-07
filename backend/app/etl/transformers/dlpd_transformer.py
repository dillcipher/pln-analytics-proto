from __future__ import annotations

import logging
import re

import pandas as pd

from app.etl.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class DLPDTransformer(BaseTransformer):
    """
    DLPD dataset transformer.

    MONTH is resolved per row.

    Business rule
    -------------
    A DLPD record contains a period bounded by THBL and THBLREK.
    The detailed inspection/reading date (DLPD_TGLBACA) is used
    to determine the actual business month for that row.

    Example:

        THBL        = 202605
        THBLREK     = 202606
        DLPD_TGLBACA = 2026-06-15

    Result:

        MONTH = 202606

    THBL and THBLREK remain untouched as source/reference fields.
    """

    # ==========================================================
    # STRING COLUMNS
    # ==========================================================

    STRING_COLUMNS = [
        "IDPEL",
        "NAMA",
        "ALAMAT",
        "NOBANG",
        "KETNOBANG",
        "KDGARDU",
        "NAMAGARDU",
        "KDDK",
        "UNITUPI",
        "UNITAP",
        "UNITUP",
        "TARIF",
        "SEGMENT",
        "KDPT",
        "KDPT_2",
        "THBL",
        "THBLREK",
        "MONTH",
        "DATASET",

        "DLPD",
        "DLPD_LM",
        "DLPD_FKM",
        "DLPD_KVARH",
        "DLPD_3BLN",
        "DLPD_JNSMUTASI",
    ]

    # ==========================================================
    # NUMERIC COLUMNS
    # ==========================================================

    NUMERIC_COLUMNS = [
        "DAYA",
        "RPPTL",
        "RPTB",
        "RPPPN",
        "RPBPJU",
        "RPBK1",
        "RPBK2",
        "RPBK3",
        "RPTAG",
        "KWHLWBP",
        "KWHWBP",
        "BLOK3",
    ]

    # ==========================================================
    # DATE COLUMNS
    # ==========================================================

    DATE_COLUMNS = [
        "DLPD_TGLBACA",
        "TGLCABUTPASANG",
    ]

    # ==========================================================
    # MONTH HELPERS
    # ==========================================================

    @staticmethod
    def _normalize_month_value(
        value,
    ) -> str | None:
        """
        Normalize a month-like value to YYYYMM.

        Supported examples:

            202606
            "202606"
            "2026-06"
            "2026/06"
            datetime(2026, 6, 1)
            "2026-06-15"

        Returns None when the value cannot safely be interpreted
        as a valid YYYYMM value.
        """

        if value is None:
            return None

        if pd.isna(value):
            return None

        # ------------------------------------------------------
        # Datetime
        # ------------------------------------------------------

        if isinstance(
            value,
            (
                pd.Timestamp,
                pd.DatetimeIndex,
            ),
        ):
            try:
                if isinstance(value, pd.Timestamp):
                    return value.strftime("%Y%m")
            except Exception:
                return None

        # ------------------------------------------------------
        # Numeric YYYYMM
        # ------------------------------------------------------

        if isinstance(value, (int, float)):
            try:
                if pd.isna(value):
                    return None

                numeric = int(value)

                text = str(numeric)

                if re.fullmatch(
                    r"20\d{4}",
                    text,
                ):
                    month = int(text[4:6])

                    if 1 <= month <= 12:
                        return text

            except Exception:
                pass

        # ------------------------------------------------------
        # String
        # ------------------------------------------------------

        text = str(value).strip()

        if not text:
            return None

        # Direct YYYYMM
        match = re.search(
            r"(20\d{2})(0[1-9]|1[0-2])",
            text,
        )

        if match:
            return (
                f"{match.group(1)}"
                f"{match.group(2)}"
            )

        # Date-like value
        try:
            parsed = pd.to_datetime(
                text,
                errors="coerce",
            )

            if not pd.isna(parsed):
                return parsed.strftime("%Y%m")

        except Exception:
            pass

        return None

    @classmethod
    def _month_start(
        cls,
        value,
    ) -> pd.Timestamp | None:
        """
        Convert THBL/THBLREK-like value to the first day
        of its month.
        """

        month = cls._normalize_month_value(value)

        if not month:
            return None

        try:
            return pd.Timestamp(
                year=int(month[:4]),
                month=int(month[4:6]),
                day=1,
            )

        except Exception:
            return None

    @staticmethod
    def _month_end(
        month_start: pd.Timestamp | None,
    ) -> pd.Timestamp | None:
        """
        Return the last moment of the month represented by
        month_start.
        """

        if month_start is None:
            return None

        try:
            return (
                month_start
                + pd.offsets.MonthEnd(1)
                + pd.Timedelta(days=1)
                - pd.Timedelta(microseconds=1)
            )

        except Exception:
            return None

    @classmethod
    def _resolve_row_month(
        cls,
        row: pd.Series,
    ) -> str | None:
        """
        Resolve MONTH for a single DLPD row.

        Primary rule
        ------------
        Use DLPD_TGLBACA as the detailed date.

        THBL and THBLREK define the valid period boundary.

        If DLPD_TGLBACA falls inside the THBL/THBLREK interval,
        its YYYYMM becomes MONTH.

        Fallback
        --------
        If the detailed date is unavailable or cannot be placed
        inside the interval, use the existing MONTH value if it
        is valid.

        As a final fallback, use THBLREK and then THBL.

        This prevents MONTH from becoming blank for otherwise
        usable DLPD records.
        """

        # ------------------------------------------------------
        # Read source values
        # ------------------------------------------------------

        thbl = row.get("THBL")

        thblrek = row.get("THBLREK")

        detail_date = row.get(
            "DLPD_TGLBACA",
        )

        existing_month = row.get(
            "MONTH",
        )

        # ------------------------------------------------------
        # Normalize boundaries
        # ------------------------------------------------------

        thbl_start = cls._month_start(
            thbl,
        )

        thblrek_start = cls._month_start(
            thblrek,
        )

        # ------------------------------------------------------
        # Detailed date
        # ------------------------------------------------------

        parsed_date = pd.to_datetime(
            detail_date,
            errors="coerce",
        )

        if not pd.isna(parsed_date):

            # --------------------------------------------------
            # If both THBL and THBLREK exist, construct the
            # inclusive period between the two month boundaries.
            # --------------------------------------------------

            if (
                thbl_start is not None
                and thblrek_start is not None
            ):
                period_start = min(
                    thbl_start,
                    thblrek_start,
                )

                period_end = cls._month_end(
                    max(
                        thbl_start,
                        thblrek_start,
                    ),
                )

                if (
                    period_end is not None
                    and period_start
                    <= parsed_date
                    <= period_end
                ):
                    return parsed_date.strftime(
                        "%Y%m",
                    )

            # --------------------------------------------------
            # If only one boundary is available, the detailed
            # date itself remains the best row-level month.
            # --------------------------------------------------

            elif (
                thbl_start is not None
                or thblrek_start is not None
            ):
                return parsed_date.strftime(
                    "%Y%m",
                )

        # ------------------------------------------------------
        # Existing MONTH fallback
        # ------------------------------------------------------

        normalized_existing = (
            cls._normalize_month_value(
                existing_month,
            )
        )

        if normalized_existing:
            return normalized_existing

        # ------------------------------------------------------
        # THBLREK fallback
        # ------------------------------------------------------

        normalized_thblrek = (
            cls._normalize_month_value(
                thblrek,
            )
        )

        if normalized_thblrek:
            return normalized_thblrek

        # ------------------------------------------------------
        # THBL fallback
        # ------------------------------------------------------

        normalized_thbl = (
            cls._normalize_month_value(
                thbl,
            )
        )

        if normalized_thbl:
            return normalized_thbl

        return None

    @staticmethod
    def _canonicalize_column_name(name: object) -> str:
        """
        Collapse a header cell to an A-Z0-9-only token so cosmetic
        differences between source files (a stray space, underscore,
        or other punctuation in the header text) do not change whether
        a period column is recognized.

        Mirrors app.etl.validator.validator.DatasetValidator.normalize_column
        -- which is what upload-time validation and the DLPD month-resolver
        scan (dlpd_xml_month_resolver_safe.py) already use to recognize
        THBL/THBLREK/DLPD_TGLBACA in a source file.
        """

        return re.sub(
            r"[^A-Z0-9]",
            "",
            str(name).upper(),
        )

    @classmethod
    def _alias_period_columns(
        cls,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Rename any column whose canonical form matches THBL, THBLREK,
        DLPD_TGLBACA, or MONTH to that exact literal name.

        Root-caused live 2026-09-02: _resolve_row_month() and the
        required-columns check below both look up these columns by
        an EXACT, case-sensitive literal name (only .strip().upper()
        is applied upstream, unlike the lenient punctuation-stripping
        match the validator/resolver use). For "DLPD Tidak beli
        Token.xlsx", the real header text for these columns differed
        from the exact literal names by some stray whitespace/
        punctuation the validator's lenient matcher tolerates but this
        exact-match lookup did not -- so every one of the file's
        176,127 rows resolved to a blank MONTH, _process_all_dlpds()
        staged zero output files, and the whole DLPD_PRABAYAR group was
        quarantined with "produced no monthly rows", silently leaving
        whatever was previously published in place. This alias step
        makes the lookup tolerant of the same cosmetic differences the
        validator already tolerates, without touching the broader
        column-normalization used elsewhere (which deliberately
        preserves semantic underscores, e.g. KOORDINAT_X).
        """

        targets = (
            "THBL",
            "THBLREK",
            "DLPD_TGLBACA",
            "MONTH",
        )
        canonical_targets = {
            cls._canonicalize_column_name(target): target
            for target in targets
        }

        existing = set(dataframe.columns)
        rename_map: dict[object, str] = {}
        for column in dataframe.columns:
            if column in targets:
                continue
            target = canonical_targets.get(
                cls._canonicalize_column_name(column),
            )
            if target is not None and target not in existing:
                rename_map[column] = target
                existing.add(target)

        if rename_map:
            logger.info(
                "DLPD PERIOD COLUMN ALIASED | %s",
                rename_map,
            )
            dataframe = dataframe.rename(columns=rename_map)

        return dataframe

    @classmethod
    def _resolve_month_per_row(
        cls,
        dataframe: pd.DataFrame,
    ) -> pd.Series:
        """
        Resolve MONTH independently for every DLPD record.
        """

        dataframe = cls._alias_period_columns(
            dataframe,
        )

        required_columns = {
            "THBL",
            "THBLREK",
            "DLPD_TGLBACA",
        }

        available = set(
            dataframe.columns,
        )

        # ------------------------------------------------------
        # Full business-rule path
        # ------------------------------------------------------

        if required_columns.issubset(
            available,
        ):
            return dataframe.apply(
                cls._resolve_row_month,
                axis=1,
            )

        # ------------------------------------------------------
        # Graceful fallback if older DLPD files do not contain
        # all source columns.
        # ------------------------------------------------------

        if "MONTH" in dataframe.columns:
            return dataframe["MONTH"].apply(
                cls._normalize_month_value,
            )

        if "THBLREK" in dataframe.columns:
            return dataframe["THBLREK"].apply(
                cls._normalize_month_value,
            )

        if "THBL" in dataframe.columns:
            return dataframe["THBL"].apply(
                cls._normalize_month_value,
            )

        logger.warning(
            "DLPD MONTH UNRESOLVABLE | none of THBL/THBLREK/DLPD_TGLBACA/"
            "MONTH found even after alias matching | rows=%s | columns=%s",
            len(dataframe),
            list(dataframe.columns),
        )

        return pd.Series(
            [None] * len(dataframe),
            index=dataframe.index,
            dtype="object",
        )

    # ==========================================================
    # TRANSFORM
    # ==========================================================

    def transform(
        self,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:

        dataframe = self.normalize_columns(
            dataframe,
        )

        # Alias any THBL/THBLREK/DLPD_TGLBACA/MONTH column whose name only
        # differs from the canonical literal by punctuation/whitespace to
        # that exact literal name.
        #
        # MUST run here, before the "ENSURE SOURCE PERIOD COLUMNS EXIST"
        # block below -- root-caused live 2026-09-02 (second occurrence):
        # that block unconditionally injects blank "THBL"/"THBLREK"
        # columns whenever they are not already present under those exact
        # names. When this alias step ran later, inside
        # _resolve_month_per_row() (called further down, after that block
        # already ran), _alias_period_columns()'s own "don't clobber an
        # existing column" guard (`target not in existing`) saw the
        # synthetic blank THBL/THBLREK placeholders as already "existing"
        # and refused to rename the real header column onto them -- so
        # the real THBL/THBLREK data (under a cosmetically different
        # header name) never got picked up, required_columns.issubset()
        # still passed (using the blank placeholders), and every row
        # silently resolved to a blank MONTH via the THBLREK fallback
        # branch (which returned "" for all 176,127 rows without logging
        # anything, since it never reached the "MONTH UNRESOLVABLE"
        # warning). Confirmed via GitHub Actions run #9 (commit 99d3e36):
        # DLPD_PRABAYAR was quarantined again with the exact same
        # "produced no monthly rows" error, and NEITHER "DLPD PERIOD
        # COLUMN ALIASED" NOR "DLPD MONTH UNRESOLVABLE" appeared anywhere
        # in the 8.2MB raw log -- proving _alias_period_columns() never
        # renamed anything and the unresolvable-warning branch was never
        # reached either. Aliasing here, before any placeholder columns
        # exist, lets the real header rename onto the canonical name
        # cleanly.
        dataframe = self._alias_period_columns(
            dataframe,
        )

        dataframe = self.clean_idpel(
            dataframe,
        )

        dataframe = self.remove_duplicates(
            dataframe,
        )

        dataframe = self.clean_strings(
            dataframe,
            self.STRING_COLUMNS,
        )

        dataframe = self.clean_numeric(
            dataframe,
            self.NUMERIC_COLUMNS,
        )

        dataframe = self.clean_dates(
            dataframe,
            self.DATE_COLUMNS,
        )

        # ======================================================
        # MONTH
        #
        # IMPORTANT:
        #
        # Do NOT take MONTH from the first Excel row.
        #
        # Resolve it independently for every record using:
        #
        #     THBL
        #       ↓
        #   DLPD_TGLBACA
        #       ↓
        #     THBLREK
        #
        # MUST run here, BEFORE the "ENSURE SOURCE PERIOD COLUMNS EXIST"
        # block below -- root-caused live 2026-09-03 (third occurrence,
        # against the real "DLPD Tidak beli Token.xlsx" production file,
        # reproduced locally outside GitHub Actions/Supabase entirely
        # after the Supabase outage blocked every run #11-#13 attempt):
        # that block unconditionally injects a blank "" THBLREK column
        # whenever the source file doesn't have one under that exact
        # name -- which is the normal, legitimate case for this dataset:
        # its real per-row period lives in THBL only (verified: THBL was
        # 202606 for all 176,127 rows, THBLREK/DLPD_TGLBACA were never
        # present in the source at all). When MONTH resolution ran AFTER
        # that injection, _resolve_month_per_row()'s fallback chain saw
        # THBLREK as "present" (the synthetic blank placeholder) and
        # returned it -- via the MONTH -> THBLREK -> THBL check order --
        # before ever reaching the real THBL data, so
        # _normalize_month_value("") returned None for every row and
        # MONTH silently blanked out for the entire file (no "DLPD PERIOD
        # COLUMN ALIASED" or "DLPD MONTH UNRESOLVABLE" log line either,
        # since neither of those code paths were the one that fired).
        # Resolving MONTH first, while the dataframe still reflects
        # exactly which period columns the source file actually has,
        # fixes this: the fallback now correctly falls through to THBL.
        dataframe["MONTH"] = (
            self._resolve_month_per_row(
                dataframe,
            )
        )

        dataframe["MONTH"] = (
            dataframe["MONTH"]
            .apply(
                self._normalize_month_value,
            )
            .fillna("")
            .astype(str)
            .str.strip()
        )

        # ======================================================
        # ENSURE SOURCE PERIOD COLUMNS EXIST
        #
        # Runs AFTER MONTH resolution above (see comment there) -- this
        # only guarantees THBL/THBLREK exist as columns for downstream
        # schema consistency (e.g. concatenating monthly parquet output
        # across files that do vs. don't have THBLREK); it must never be
        # allowed to influence which column MONTH was actually resolved
        # from.
        # ======================================================

        if "THBL" not in dataframe.columns:
            dataframe["THBL"] = ""

        if "THBLREK" not in dataframe.columns:
            dataframe["THBLREK"] = ""

        dataframe["THBL"] = (
            dataframe["THBL"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        dataframe["THBLREK"] = (
            dataframe["THBLREK"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        # ======================================================
        # DATASET
        # ======================================================

        if "DATASET" not in dataframe.columns:
            dataframe["DATASET"] = "DLPD"

        dataframe["DATASET"] = (
            dataframe["DATASET"]
            .fillna("DLPD")
            .astype(str)
            .str.strip()
        )

        # ======================================================
        # FINAL COLUMN ORDER / RESULT
        # ======================================================

        return dataframe