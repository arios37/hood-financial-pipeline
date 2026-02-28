"""
config.py — Shared layout constants for the HOOD three-statement Excel model.

This module serves as the single source of truth for all Excel row/column
positions, sheet names, and directory paths used across the pipeline.

Why this exists:
    Centralising layout constants here prevents silent breakages when a row
    is added to a sheet — every module that references row indices picks up
    the change automatically instead of carrying its own hardcoded copy.

Usage:
    Import this module in build_excel_model.py, validate_model.py, and any
    future scripts that need to reference Excel row/column positions or sheet
    names.  Changes here propagate everywhere.

See Also:
    - build_excel_model.py: Consumes these constants to write forecast formulas.
    - validate_model.py:    Consumes these constants to verify structural integrity.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Ticker
# ---------------------------------------------------------------------------
# Single source of truth for the company ticker used across the pipeline.
# Change this value to run the pipeline for a different company.
TICKER = "HOOD"

# ---------------------------------------------------------------------------
# Repo paths
# ---------------------------------------------------------------------------
# parents[1] navigates from config/ → Financial_Pipeline/ (the project root).
# All data and output paths are derived from this anchor so the project stays
# portable across machines and CI environments.
REPO_ROOT  = Path(__file__).resolve().parents[1]
DATA_DIR   = REPO_ROOT / "data"
OUTPUT_DIR = REPO_ROOT / "output"

# ---------------------------------------------------------------------------
# Sheet names
# ---------------------------------------------------------------------------
# Canonical order of the seven sheets in the Excel workbook.
# validate_model.py asserts all seven are present after each build.
EXPECTED_SHEETS = [
    "Assumptions",
    "Income Statement",
    "Balance Sheet",
    "Cash Flow",
    "Valuation",
    "Sensitivity Analysis",
    "Model Guide",
]

# ---------------------------------------------------------------------------
# Column layout
# ---------------------------------------------------------------------------
# Excel columns are 1-based: A = 1, B = 2, etc.

LABEL_COL      = 1    # column A — row labels (line-item names)
HIST_COL_START = 2    # column B — first historical period (oldest quarter)

# Historical period counts — INFORMATIONAL DEFAULTS ONLY.
# build_excel_model.py derives the actual column positions dynamically from
# len(df_*.columns) at runtime.  These constants are used only as fallbacks
# in validate_model.py (overridden by _actual_fcst_col() dynamic scan).
# Update these if the extraction produces a consistently different quarter count.
NUM_IS_HIST    = 14   # Income Statement: 14 quarters (Q3-2021 … Q4-2024)
NUM_BS_CF_HIST = 10   # Balance Sheet / Cash Flow: 10 quarters

# Forecast horizon — fixed 4-year forward projection window.
NUM_FCST_COLS = 4
FCST_YEARS    = ["FY2026E", "FY2027E", "FY2028E", "FY2029E"]

# Derived: first forecast column on each statement (1-based column index).
# These are STATIC FALLBACKS — the builder and validator use dynamic detection
# to find the actual "FY2026E" header at runtime, because the column count can
# shift when new quarters are extracted from SEC filings.
IS_FCST_COL_START  = HIST_COL_START + NUM_IS_HIST       # col P (16)
BS_FCST_COL_START  = HIST_COL_START + NUM_BS_CF_HIST    # col L (12)
CF_FCST_COL_START  = HIST_COL_START + NUM_BS_CF_HIST    # col L (12)

# Alias kept for validate_model.py backward compatibility
IS_FCST_COL = IS_FCST_COL_START

# ---------------------------------------------------------------------------
# Income Statement row indices
# ---------------------------------------------------------------------------
# Maps human-readable labels to their 1-based row number on the
# "Income Statement" worksheet.  Rows prefixed with "sep_" are visual
# separators (thin borders); they hold no data.

IS_ROW: dict[str, int] = {
    "title":                 1,
    "unit_note":             2,
    "col_headers":           3,
    "sep_top":               4,
    "Txn Revenue":           5,   # Transaction-based Revenue (segment)
    "NI Revenue":            6,   # Net Interest Revenue (segment)
    "Other Revenue":         7,   # Other Revenue (segment)
    "Total Revenue":         8,
    "Cost of Revenue":       9,   # COGS: transaction rebates, clearing, execution
    "Gross Profit":         10,   # Total Revenue − Cost of Revenue
    "sep_costs":            11,
    "Operating Expenses":   12,   # shown net of SBC
    "SBC":                  13,
    "Total Costs":          14,
    "sep_oi":               15,
    "Operating Income":     16,
    "Tax Provision":        17,
    "Other Income":         18,   # non-operating items (interest income, crypto gains)
    "sep_ni":               19,
    "Net Income":           20,
    "EPS":                  21,   # diluted EPS (forecast only)
    "sep_margin":           22,
    "margin_header":        23,
    "Gross Margin":         24,   # Gross Profit / Revenue
    "Op Margin":            25,
    "Net Margin":           26,
    "FCF Margin":           27,   # FCF / Revenue (forecast only)
    "SBC Pct":              28,   # SBC as % of Revenue
    "OpEx Pct":             29,   # Op. Exp. (ex. SBC) as % of Revenue
    "sep_ltm":              30,   # thin separator before LTM memo
    "LTM Revenue":          31,   # memo: trailing-12-month revenue
    "Implied Growth":       32,   # implied revenue growth %
}

# ---------------------------------------------------------------------------
# Balance Sheet row indices
# ---------------------------------------------------------------------------
# Maps BS line items to their 1-based row positions.
# Includes an asset/liability partial-check block (rows 10-14) and a
# credit metrics memo section (rows 18-22).

BS_ROW: dict[str, int] = {
    "Cash":            4,
    "Restricted Cash": 5,
    "Receivables":     6,
    "Payables":        7,
    "Total Debt":      8,
    "Equity":          9,
    "sep_check":      10,
    "check_header":   11,
    "Partial Assets": 12,   # Cash + RestrictedCash + Receivables
    "Partial LE":     13,   # Payables + Debt + Equity
    "Check":          14,   # Partial Assets − Partial L&E (should → 0)
    "debt_note":      16,   # footnote explaining debt proxy composition
    "sep_credit":     18,
    "credit_header":  19,
    "EBITDA":         20,   # rolling LTM Adjusted EBITDA (OI + SBC + D&A)
    "Debt_EBITDA":    21,   # Total Debt / EBITDA
    "NetDebt_EBITDA": 22,   # (Debt − Cash) / EBITDA
}

# ---------------------------------------------------------------------------
# Cash Flow row indices
# ---------------------------------------------------------------------------
# Maps CF line items to their 1-based row positions.
# Includes a cash-bridge reconciliation block (rows 10-14) that verifies
# the three-statement model is closed (FCF − ΔCash − ΔDebt = 0).

CF_ROW: dict[str, int] = {
    "Net Income":     4,
    "SBC":            5,
    "DA":             6,    # D&A add-back (historical from XBRL + forecast)
    "CFO":            7,
    "Capex":          8,
    "FCF":            9,
    "sep_bridge":    10,
    "bridge_header": 11,
    "Delta Cash":    12,    # ΔCash = Cash_t − Cash_t-1 (from BS)
    "Delta Debt":    13,    # ΔDebt = Debt_t − Debt_t-1 (from BS)
    "Bridge Check":  14,    # residual; should = 0 in closed forecast
}
