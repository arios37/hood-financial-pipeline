"""
HOOD Financial Pipeline — Entry Point

Orchestrates the full SEC → Excel pipeline through a single CLI command.
Each stage is modular and can also be run independently via ``make``.

Usage:
    python main.py                  # full pipeline (extract → transform → model → validate)
    python main.py --skip-extract   # skip SEC extraction (use existing raw CSVs)

See Also:
    - Makefile:          Individual ``make extract``, ``make transform``, etc. targets.
    - src/pipeline.py:   Core orchestration logic that sequences the stages.
"""

import argparse
import logging
import sys

from src.pipeline import run_pipeline


def main():
    """Parse CLI arguments, configure logging, and launch the pipeline."""

    # -- CLI argument parsing --
    parser = argparse.ArgumentParser(description="HOOD Financial Pipeline")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip the SEC extraction step (use existing raw CSVs in data/)",
    )
    args = parser.parse_args()

    # -- Logging setup --
    # Uses a simple timestamp + level format suitable for local development;
    # CI environments can override via the LOG_LEVEL env variable.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting HOOD Financial Pipeline...")

    # -- Pipeline execution --
    try:
        run_pipeline(skip_extract=args.skip_extract)
        logger.info("Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
