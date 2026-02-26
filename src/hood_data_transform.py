"""
HOOD Data Transformation Script
================================
Loads raw SEC XBRL companyfacts CSVs, cleans them, and outputs model-ready CSVs
in time-series format (rows = line items, cols = quarters).

Purpose:
  Transform unstructured SEC XBRL extracted data into clean, normalized financial
  statements suitable for model building and analysis. This is the first step in
  the HOOD financial data pipeline.

Inputs (data/ at repo root):
  HOOD_companyfacts_IS_10Q.csv     Raw Income Statement data from SEC extraction
  HOOD_companyfacts_BS_10Q.csv     Raw Balance Sheet data from SEC extraction
  HOOD_companyfacts_CF_10Q.csv     Raw Cash Flow data from SEC extraction

Outputs (data/ at repo root):
  model_income_statement.csv       Cleaned, pivoted Income Statement (rows=items, cols=quarters)
  model_balance_sheet.csv          Cleaned, pivoted Balance Sheet (rows=items, cols=quarters)
  model_cash_flow.csv              Cleaned, pivoted Cash Flow (rows=items, cols=quarters)
  manifest.json                    Period alignment metadata consumed by build_excel_model.py
                                   and validate steps to ensure column consistency

Run:
  python -m src.hood_data_transform
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_DIR, TICKER

DATA_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Map input file keys to their paths. These CSVs are produced by the upstream
# SEC XBRL extraction pipeline and must exist before transformation can proceed.
INPUT_FILES: dict[str, Path] = {
    "IS": DATA_DIR / f"{TICKER}_companyfacts_IS_10Q.csv",
    "BS": DATA_DIR / f"{TICKER}_companyfacts_BS_10Q.csv",
    "CF": DATA_DIR / f"{TICKER}_companyfacts_CF_10Q.csv",
}

# Output file paths. Each statement type produces its own CSV with cleaned,
# pivoted data ready for downstream model building.
OUTPUT_FILES: dict[str, Path] = {
    "IS": DATA_DIR / "model_income_statement.csv",
    "BS": DATA_DIR / "model_balance_sheet.csv",
    "CF": DATA_DIR / "model_cash_flow.csv",
}

# Map raw "Line Item" labels from SEC extraction → canonical display names.
# This standardizes line item names across different SEC filing formats and
# XBRL tag variations. Keys are raw labels as they appear in input CSVs;
# values are the clean names used in output and downstream analysis.
LABEL_MAP: dict[str, str] = {
    # Income Statement — revenue segments (from XLSX note schedule)
    "Transaction-based Revenue":     "Transaction-based Revenue",
    "Net Interest Revenue":          "Net Interest Revenue",
    "Other Revenue":                 "Other Revenue",
    # Income Statement — totals & costs
    "Total Revenue":                 "Total Revenue",
    "Operating Expenses":            "Operating Expenses",
    "Stock-Based Compensation":      "Stock-Based Compensation",
    "Net Income":                    "Net Income",
    # Balance Sheet
    "Cash & Equivalents":            "Cash & Cash Equivalents",
    "Restricted Cash":               "Restricted Cash",
    "Receivables":                   "Receivables",
    "Payables":                      "Payables",
    "Total Debt (Proxy)":            "Total Debt",
    "Total Equity":                  "Stockholders' Equity",
    # Cash Flow
    "Cash From Operations (CFO)":    "Cash from Operations",
    "Capex (Productive Assets)":     "Capital Expenditures",
    "Free Cash Flow":                "Free Cash Flow",
}

# Desired presentation order per statement type (after label mapping).
# This defines the canonical row order for output CSVs, making them readable
# and suitable for presentation/analysis. Rows not in this list appear at the end.
ROW_ORDER: dict[str, list[str]] = {
    "IS": [
        "Transaction-based Revenue",
        "Net Interest Revenue",
        "Other Revenue",
        "Total Revenue",
        "Operating Expenses",
        "Stock-Based Compensation",
        "Net Income",
    ],
    "BS": [
        "Cash & Cash Equivalents",
        "Restricted Cash",
        "Receivables",
        "Payables",
        "Total Debt",
        "Stockholders' Equity",
    ],
    "CF": [
        "Net Income",
        "Stock-Based Compensation",
        "Cash from Operations",
        "Capital Expenditures",
        "Free Cash Flow",
    ],
}

# Balance Sheet items are period-end snapshots, not quarterly flows like IS/CF.
# Forward-fill sparse quarters (e.g., when a line item is missing for Q2 but
# present in Q1/Q3) to propagate the most recent known value forward.
# This assumption is appropriate for BS (steady-state accounts) but NOT for
# IS/CF (which measure period activity and require actual reported values).
FFILL_STATEMENTS: set[str] = {"BS"}


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------

def quarter_label(dt: pd.Timestamp) -> str:
    """
    Convert a date to a quarter label string.

    Args:
        dt: A pandas Timestamp (e.g., end-of-quarter date like 2025-09-30).

    Returns:
        A formatted string like 'Q3 2025' representing the quarter and year.

    Example:
        quarter_label(pd.Timestamp('2025-09-30')) → 'Q3 2025'
    """
    q = (dt.month - 1) // 3 + 1
    return f"Q{q} {dt.year}"


def _validate(df: pd.DataFrame, stmt: str, filepath: str) -> None:
    """
    Run data-quality checks and emit warnings for known data contamination patterns.

    This function inspects the transformed DataFrame for signs that upstream
    extraction may have captured YTD cumulative data instead of true quarterly
    figures, all-zero rows from XBRL tag mismatches, or missing Q4 data due to
    10-Q only filtering. Issues are logged as warnings but do NOT prevent
    transformation completion.

    Args:
        df: The pivoted DataFrame to validate (rows = line items, cols = quarters).
        stmt: Statement type key ("IS", "BS", or "CF") for logging context.
        filepath: Source file path, used in log messages for traceability.

    Returns:
        None. Modifies only the logger; does not modify df.

    Technical Notes:
        - Monotonic non-decrease detection: YTD cumulation (Q3 = sum of Q1,Q2,Q3)
          produces monotonically non-decreasing sequences that exceed typical
          quarterly growth (>50%). This heuristic checks for last/first ratio > 2×
          to distinguish genuine growth from YTD cumulation.
        - Q4 detection: Absence of Q4 across all years signals 10-Q-only extraction.
          Recommend including 10-K filings for complete annual data.
        - All-zero rows: Suggest XBRL tag resolution issues (tag not found in filing).
    """
    issues: list[str] = []

    # 1. Detect monotonically non-decreasing rows within any calendar year
    #    (strong signal that values are YTD cumulative, not true quarterly).
    for row_label in df.index:
        row = pd.to_numeric(df.loc[row_label], errors="coerce")
        row = row.dropna()
        if row.empty:
            continue
        # Group by year embedded in column name (format "Qx YYYY")
        by_year: dict[str, list[float]] = {}
        for col, val in row.items():
            parts = str(col).split()
            if len(parts) == 2:
                by_year.setdefault(parts[1], []).append(val)
        for yr, vals in by_year.items():
            # Require monotonic non-decrease AND last value > 2× first to distinguish
            # genuine quarterly growth (rarely >50% within a year) from YTD cumulation
            # (Q3 YTD is ~3× Q1 for an evenly distributed metric).
            if (
                len(vals) >= 2
                and all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1))
                and abs(vals[0]) > 0
                and abs(vals[-1]) / abs(vals[0]) > 2.0
            ):
                issues.append(
                    f"  [{stmt}] '{row_label}' values in {yr} are monotonically "
                    f"non-decreasing and last/first ratio exceeds 2× "
                    f"{[round(v, 1) for v in vals]} — likely YTD cumulative."
                )

    # 2. Flag if Q4 (December year-end) is absent from every year in the data
    col_strs = list(df.columns)
    q4_present = any("Q4" in c for c in col_strs)
    if not q4_present and len(col_strs) >= 4:
        issues.append(
            f"  [{stmt}] No Q4 periods detected. All fiscal-year Dec-31 data is missing. "
            "The extraction pipeline filters to 10-Q only; include 10-K to recover Q4."
        )

    # 3. Flag all-zero rows (common when XBRL tags don't match)
    for row_label in df.index:
        row = pd.to_numeric(df.loc[row_label], errors="coerce").fillna(0)
        if (row == 0).all():
            issues.append(
                f"  [{stmt}] '{row_label}' is zero across all periods — "
                "verify XBRL tag resolution."
            )

    if issues:
        logger.warning("  VALIDATION WARNINGS (%s):", filepath)
        for msg in issues:
            logger.warning("%s", msg)


def load_and_transform(filepath: str, stmt: str) -> pd.DataFrame:
    """
    Load a raw companyfacts CSV and return a clean, pivoted DataFrame.

    This function orchestrates the full transformation pipeline:
    label normalization, date parsing, numeric coercion, scaling, forward-fill
    (for Balance Sheets), row reordering, and quarter-label formatting.

    Args:
        filepath: Path to the raw SEC companyfacts CSV (e.g., HOOD_companyfacts_IS_10Q.csv).
        stmt: Statement type key ("IS", "BS", "CF") used to look up row order and
              forward-fill settings.

    Returns:
        A pandas DataFrame with shape (num_line_items, num_quarters) where:
        - Index: Clean line item names (e.g., "Total Revenue")
        - Columns: Quarter labels (e.g., "Q3 2025")
        - Values: Numeric amounts in millions (scaled from raw dollars)

    Raises:
        FileNotFoundError: If filepath does not exist.
        KeyError: If stmt is not in ROW_ORDER or FFILL_STATEMENTS configuration.
        ValueError: From downstream _check_data_quality if >20% of values are zero/NaN.

    Transformation Steps:
      1. Read raw CSV with line items as index.
      2. Rename line items via LABEL_MAP (standardization).
      3. Parse date columns and sort chronologically (oldest → newest).
      4. Coerce all values to numeric (blanks, dashes → NaN).
      5. Scale raw dollars → millions and round to 1 decimal.
      6. Forward-fill Balance Sheet snapshots (BS only, see FFILL_STATEMENTS).
      7. Reorder rows to canonical presentation order; preserve unexpected rows at end.
      8. Convert date columns to human-readable quarter labels.
      9. Validate data quality and emit warnings for known issues.

    Example:
        df = load_and_transform('data/HOOD_companyfacts_IS_10Q.csv', 'IS')
        # Result: 7 rows (Income Statement line items) × 8 cols (quarters)
    """
    try:
        df = pd.read_csv(filepath, index_col=0)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Input file not found: {filepath}\n"
            "Run:  python -m src.hood_sec_extract_v3 --ticker HOOD --quarters 10"
        ) from None

    # 1. Rename index using label map for standardization
    df.index = df.index.map(lambda x: LABEL_MAP.get(x, x))

    # 2. Parse & sort date columns ascending (oldest quarter on left)
    df.columns = pd.to_datetime(df.columns, errors="coerce")
    df = df.loc[:, df.columns.notna()]   # drop any unparseable column headers
    df = df.sort_index(axis=1)           # oldest quarter on left

    # 3. Coerce to numeric (blanks, dashes, non-numeric → NaN)
    df = df.apply(pd.to_numeric, errors="coerce")

    # 4. Scale raw dollars → millions, round to 1 decimal
    #    (SEC XBRL data is typically in raw dollars; our model operates in millions)
    df = (df / 1_000_000).round(1)

    # 5. Forward-fill sparse quarters (Balance Sheet only).
    #    Balance Sheet accounts are snapshots; if Q2 is missing but Q1 has a value,
    #    assume Q2 carried the same amount forward. This does NOT apply to Income
    #    Statement or Cash Flow, which measure period activity and require actual
    #    reported values (forward-filling would overstate actual period performance).
    if stmt in FFILL_STATEMENTS:
        df = df.ffill(axis=1)

    # 6. Reorder rows to canonical presentation order.
    #    Desired items appear first (in defined order), then any unexpected rows
    #    (likely new data or unmapped items) appear at the end.
    desired = [r for r in ROW_ORDER[stmt] if r in df.index]
    extra   = [r for r in df.index if r not in desired]
    df = df.loc[desired + extra]

    # 7. Human-readable quarter labels (e.g., "2025-09-30" → "Q3 2025")
    df.columns = [quarter_label(c) for c in df.columns]
    df.index.name = "Line Item ($M)"

    # 8. Validate data quality and emit warnings
    _validate(df, stmt, filepath)
    return df


# ---------------------------------------------------------------------------
# Data quality gate
# ---------------------------------------------------------------------------

def _check_data_quality(df: pd.DataFrame, name: str) -> None:
    """
    Enforce data-quality thresholds and fail loudly if violations are detected.

    This is a hard gate: if more than 20% of cells are zero or NaN after
    transformation, the pipeline fails. This threshold is conservative to catch
    upstream extraction issues (e.g., missing files, XBRL tag failures, malformed
    data) before proceeding to model building.

    Args:
        df: The transformed DataFrame to validate.
        name: Display name for logging (e.g., "Income Statement").

    Returns:
        None. Logs success; raises ValueError on failure.

    Raises:
        ValueError: If DataFrame is empty or if zero/NaN percentage exceeds 20%.
              Message includes actual percentage and suggests checking extraction step.

    Threshold Justification:
        - 20% allows for reasonable sparsity (e.g., new line items or discontinued
          operations), but catches catastrophic failures (50%+ sparsity).
        - Adjust threshold if upstream data quality improves or tolerance increases.
    """
    if df.empty:
        raise ValueError(f"{name}: DataFrame is empty after transform")
    total = df.size
    bad = (df.isna() | (df == 0)).sum().sum()
    bad_pct = bad / total
    if bad_pct > 0.20:
        raise ValueError(
            f"{name}: {bad_pct:.0%} of values are zero/NaN "
            f"(threshold 20%) — check extraction step"
        )
    logger.info("  [OK] %s: data quality %d%% filled", name, round(100 * (1 - bad_pct)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full HOOD data transformation pipeline.

    Workflow:
      1. Configure logging (level from LOG_LEVEL env var, defaults to INFO).
      2. Verify all input files exist; fail fast if any are missing.
      3. Load and transform each statement type (IS, BS, CF) independently.
      4. Validate data quality for each transformed statement.
      5. Write cleaned CSVs to output directory.
      6. Generate manifest.json with period alignment metadata (for downstream
         build_excel_model.py to verify column consistency without hardcoding).
      7. Log completion and next steps.

    Environment Variables:
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR). Defaults to INFO.

    Raises:
        RuntimeError: If any INPUT_FILES do not exist at runtime.
        ValueError: If data-quality checks fail for any statement.
    """
    logger.info("%s Data Transformation", TICKER)
    logger.info("=" * 44)

    # Fail fast if input files are missing; pipeline cannot proceed.
    missing = [str(f) for f in INPUT_FILES.values() if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(
            "Missing input file(s):\n"
            + "\n".join(f"  - {f}" for f in missing)
            + "\n\nThese CSVs are produced by the SEC extraction step.\n"
            "Run:  python -m src.hood_sec_extract_v3 --ticker HOOD --quarters 10"
        )

    _names = {"IS": "Income Statement", "BS": "Balance Sheet", "CF": "Cash Flow"}

    # Load and transform each statement type.
    dfs: dict[str, pd.DataFrame] = {}
    for key in ("IS", "BS", "CF"):
        src = INPUT_FILES[key]
        dfs[key] = load_and_transform(str(src), key)

    df_is = dfs["IS"]
    df_bs = dfs["BS"]
    df_cf = dfs["CF"]

    # Apply data-quality gate: fail if any statement has >20% sparsity.
    _check_data_quality(df_is, "Income Statement")
    _check_data_quality(df_bs, "Balance Sheet")
    _check_data_quality(df_cf, "Cash Flow")

    # Write cleaned CSVs to data/ directory.
    for key, df in (("IS", df_is), ("BS", df_bs), ("CF", df_cf)):
        src = INPUT_FILES[key]
        dst = OUTPUT_FILES[key]

        df.to_csv(dst)

        rows, cols = df.shape
        logger.info("  [%s]  %s", key, src)
        logger.info("         → %s  (%d rows × %d quarters)", dst, rows, cols)
        logger.info("")

    # Write period-alignment manifest so downstream build_excel_model.py and
    # validate steps can verify column counts without hardcoding them.
    # This is critical for catching misalignment issues early.
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "statements": {
            key: {
                "periods": list(df.columns),
                "count":   len(df.columns),
                "first":   df.columns[0]  if len(df.columns) else None,
                "last":    df.columns[-1] if len(df.columns) else None,
            }
            for key, df in (("IS", df_is), ("BS", df_bs), ("CF", df_cf))
        },
    }
    manifest_path = DATA_DIR / "manifest.json"
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)
    logger.info("  [manifest] written → %s", manifest_path)
    logger.info("")
    logger.info("Done. Pass model CSVs to build_excel_model.py to generate the workbook.")


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(message)s",
    )
    main()
