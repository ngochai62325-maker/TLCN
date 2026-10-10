#!/usr/bin/env python3
"""Executable Spark Job for World Bank Pink Sheet Silver Transformation."""

import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("job_worldbank")

from silver.transformers.worldbank_transformer import WorldBankTransformer


def main():
    parser = argparse.ArgumentParser(description="Run World Bank Pink Sheet Silver Transformation")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="Load mode (full or incremental)")
    parser.add_argument("--run-id", type=str, default=None, help="Filter by specific bronze _ingestion_run_id")
    args = parser.parse_args()

    logger.info(f"Starting World Bank Pink Sheet Silver Job in mode={args.mode}")
    transformer = WorldBankTransformer()
    result = transformer.execute(mode=args.mode, run_id=args.run_id)
    logger.info(f"World Bank Pink Sheet Silver Job completed successfully: {result}")


if __name__ == "__main__":
    main()
