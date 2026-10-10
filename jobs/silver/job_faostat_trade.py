#!/usr/bin/env python3
"""Executable Spark Job for FAOSTAT Trade Matrix Silver Transformation."""

import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("job_faostat_trade")

from silver.transformers.faostat_trade_transformer import FaostatTradeTransformer


def main():
    parser = argparse.ArgumentParser(description="Run FAOSTAT Trade Matrix Silver Transformation")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="Load mode (full or incremental)")
    parser.add_argument("--run-id", type=str, default=None, help="Filter by specific bronze _ingestion_run_id")
    args = parser.parse_args()

    logger.info(f"Starting FAOSTAT Trade Silver Job in mode={args.mode}")
    transformer = FaostatTradeTransformer()
    result = transformer.execute(mode=args.mode, run_id=args.run_id)
    logger.info(f"FAOSTAT Trade Silver Job completed successfully: {result}")


if __name__ == "__main__":
    main()
