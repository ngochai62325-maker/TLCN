#!/usr/bin/env python3
"""Unified CLI Runner for Silver Layer Transformations."""

import sys
import argparse
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("run_all_silver")

from silver.transformers.faostat_trade_transformer import FaostatTradeTransformer
from silver.transformers.usda_psd_transformer import UsdaPsdTransformer
from silver.transformers.usda_export_price_transformer import UsdaExportPriceTransformer
from silver.transformers.worldbank_transformer import WorldBankTransformer

REGISTRY = {
    "faostat_trade": FaostatTradeTransformer,
    "usda_psd": UsdaPsdTransformer,
    "usda_export_price": UsdaExportPriceTransformer,
    "worldbank_pinksheet": WorldBankTransformer,
}


def run_pipeline(dataset: str = "all", mode: str = "full", run_id: str = None) -> Dict[str, Any]:
    targets = [dataset] if dataset != "all" else list(REGISTRY.keys())
    results = {}

    for target in targets:
        if target not in REGISTRY:
            raise ValueError(f"Unknown dataset '{target}'. Available: {list(REGISTRY.keys())}")

        logger.info(f"=== Starting Silver Transformation: {target} (mode: {mode}) ===")
        transformer_cls = REGISTRY[target]
        transformer = transformer_cls()
        res = transformer.execute(mode=mode, run_id=run_id)
        results[target] = res
        logger.info(f"=== Completed Silver Transformation: {target} -> {res} ===\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="Run Silver Layer Transformations")
    parser.add_argument("--dataset", choices=["all", "faostat_trade", "usda_psd", "usda_export_price", "worldbank_pinksheet"], default="all")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    results = run_pipeline(dataset=args.dataset, mode=args.mode, run_id=args.run_id)
    logger.info(f"All requested Silver transformations finished: {results}")


if __name__ == "__main__":
    main()
