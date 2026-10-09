#!/usr/bin/env python3
"""Executable Spark Job for USDA Export Price Silver Transformation."""

import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("job_usda_export_price")

from silver.transformers.usda_export_price_transformer import UsdaExportPriceTransformer


def main():
    parser = argparse.ArgumentParser(description="Run USDA Export Price Silver Transformation")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="Load mode (full or incremental)")
    parser.add_argument("--run-id", type=str, default=None, help="Filter by specific bronze _ingestion_run_id")
    args = parser.parse_args()

    logger.info(f"Starting USDA Export Price Silver Job in mode={args.mode}")
    transformer = UsdaExportPriceTransformer()
    result = transformer.execute(mode=args.mode, run_id=args.run_id)
    logger.info(f"USDA Export Price Silver Job completed successfully: {result}")


if __name__ == "__main__":
    main()
