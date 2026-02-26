"""
Core pipeline orchestration.

Sequences the four stages of the HOOD financial pipeline:
  1. Extract   — Pull SEC EDGAR filings and write raw XBRL CSVs to data/.
  2. Transform — Clean raw CSVs into model-ready time-series format.
  3. Build     — Generate the seven-sheet Excel financial model.
  4. Validate  — Run 27 integrity checks on the output workbook and CSVs.

Why this exists:
    Decouples the execution order from main.py's CLI layer so that other
    entry points (tests, notebooks, CI scripts) can call ``run_pipeline()``
    directly without parsing command-line arguments.

See Also:
    - main.py:  CLI wrapper that parses ``--skip-extract`` and calls this module.
    - Makefile: Provides individual ``make extract``, ``make transform``, etc. targets.
"""

from __future__ import annotations

import logging
import sys

# -- Stage imports --
# Each stage exposes a ``main()`` function that can be called standalone.
# We alias them here for readability within the orchestration loop.
from src.hood_sec_extract_v3 import main as extract
from src.hood_data_transform import main as transform
from src.build_excel_model import main as build_model
from src.validate_model import main as validate

logger = logging.getLogger(__name__)


def run_pipeline(skip_extract: bool = False) -> None:
    """Execute the full financial data pipeline.

    Runs each stage in order and propagates failures immediately so that
    downstream stages don't operate on stale or corrupt data.

    Args:
        skip_extract: If True, skip the SEC extraction step and start
                      from the transform stage.  Useful when raw CSVs
                      are already present in data/ (e.g. during local
                      development or CI integration tests).

    Raises:
        SystemExit: Re-raised from any stage that exits with a non-zero code.
        Exception:  Re-raised from any stage that throws an unhandled error.
    """
    # -- Build the ordered list of pipeline stages --
    steps: list[tuple[str, callable]] = []

    if not skip_extract:
        steps.append(("Extract SEC data", extract))

    steps.extend([
        ("Transform data", transform),
        ("Build Excel model", build_model),
        ("Validate model", validate),
    ])

    # -- Isolate sys.argv --
    # Several stages use argparse internally.  Without this guard,
    # flags meant for main.py (e.g. --skip-extract) would cause argparse
    # errors inside individual scripts like build_excel_model.py.
    saved_argv = sys.argv
    sys.argv = [sys.argv[0]]

    # -- Execute each stage sequentially --
    for name, step_fn in steps:
        logger.info("── %s ──", name)
        try:
            step_fn()
        except SystemExit as exc:
            # Non-zero exit means the stage intentionally signalled failure
            if exc.code and exc.code != 0:
                logger.error("%s failed (exit %s).", name, exc.code)
                sys.exit(exc.code)
        except Exception:
            logger.exception("%s failed.", name)
            raise

    # -- Restore original argv and report success --
    sys.argv = saved_argv
    logger.info("Pipeline complete.")
