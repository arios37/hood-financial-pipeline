"""
config package — Re-exports public constants from config.config.

Why this exists:
    Allows other modules to write ``from config import DATA_DIR`` instead
    of ``from config.config import DATA_DIR``, keeping import statements
    clean and backward-compatible with the original flat-module layout.
"""

from config.config import (  # noqa: F401
    TICKER,
    REPO_ROOT,
    DATA_DIR,
    OUTPUT_DIR,
    EXPECTED_SHEETS,
    LABEL_COL,
    HIST_COL_START,
    NUM_IS_HIST,
    NUM_BS_CF_HIST,
    NUM_FCST_COLS,
    FCST_YEARS,
    IS_FCST_COL_START,
    BS_FCST_COL_START,
    CF_FCST_COL_START,
    IS_FCST_COL,
    IS_ROW,
    BS_ROW,
    CF_ROW,
)

__all__ = [
    "TICKER",
    "REPO_ROOT",
    "DATA_DIR",
    "OUTPUT_DIR",
    "EXPECTED_SHEETS",
    "LABEL_COL",
    "HIST_COL_START",
    "NUM_IS_HIST",
    "NUM_BS_CF_HIST",
    "NUM_FCST_COLS",
    "FCST_YEARS",
    "IS_FCST_COL_START",
    "BS_FCST_COL_START",
    "CF_FCST_COL_START",
    "IS_FCST_COL",
    "IS_ROW",
    "BS_ROW",
    "CF_ROW",
]
