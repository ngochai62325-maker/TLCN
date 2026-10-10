#!/usr/bin/env python3
"""Executable Spark Job for USDA Rice PSD Silver Transformation."""

import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("job_usda_psd")

from silver.transformers.usda_psd_transformer import UsdaPsdTransformer


def main():
    parser = argparse.ArgumentParser(description="Run USDA Rice PSD Silver Transformation")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="Load mode (full or incremental)")
    parser.add_argument("--run-id", type=str, default=None, help="Filter by specific bronze _ingestion_run_id")
    args = parser.parse_args()

    logger.info(f"Starting USDA Rice PSD Silver Job in mode={args.mode}")
    transformer = UsdaPsdTransformer()
    result = transformer.execute(mode=args.mode, run_id=args.run_id)
    logger.info(f"USDA Rice PSD Silver Job completed successfully: {result}")


if __name__ == "__main__":
    main()
