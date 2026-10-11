#!/usr/bin/env python3
"""Read-only Hoang source preview. No Iceberg, quarantine or metadata writes."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pyspark.sql import SparkSession, functions as F
from ingestion.storage.bronze_writer import sanitize_column_name
from silver.core.spark_session import get_spark_session
from silver.engine.merge import IcebergMergeEngine
from silver.transformers.faostat_production import FaostatProductionTransformer
from silver.transformers.faostat_monthly_price import FaostatMonthlyPriceTransformer
from silver.transformers.faostat_supply_utilization import FaostatSupplyUtilizationTransformer
from silver.transformers.nso_vietnam import NsoVietnamTransformer


REGISTRY = {"faostat_production": FaostatProductionTransformer,
            "faostat_monthly_price": FaostatMonthlyPriceTransformer,
            "faostat_supply_utilization": FaostatSupplyUtilizationTransformer,
            "nso_vietnam": NsoVietnamTransformer}


def preview(spark, dataset, bronze):
    transformer = REGISTRY[dataset]()
    input_rows = bronze.count()
    transformed = transformer.transform(bronze).cache()
    try:
        observation_rows = transformed.count()
        valid, quarantine = transformer.validate(transformed)
        valid_rows, quarantine_rows = valid.count(), quarantine.count()
        keys = transformer.business_keys if dataset == "nso_vietnam" else transformer.contract.business_key
        dedup_rows = IcebergMergeEngine(spark).deduplicate(valid, keys).count()
        errors = quarantine.select(F.explode("dq_errors").alias("error")).groupBy("error.rule_id").count()
        # Bounded collection: aggregate counters only, never source observations.
        rules = {r["rule_id"]: r["count"] for r in errors.collect()}
        result = {"dataset": dataset, "bronze_input_rows": input_rows,
                  "observation_rows": observation_rows, "valid_rows": valid_rows,
                  "quarantine_rows": quarantine_rows, "deduplicated_rows": valid_rows - dedup_rows,
                  "output_rows": dedup_rows, "intentionally_excluded_rows": 0,
                  "expansion_rows": observation_rows - input_rows,
                  "dq_errors": rules, "writes_performed": False}
        assert observation_rows == dedup_rows + quarantine_rows + valid_rows - dedup_rows
        return result
    finally:
        transformed.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=["all", *REGISTRY])
    parser.add_argument("--csv", help="Optional local FAOSTAT CSV; otherwise reads Bronze Iceberg")
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument("--watermark", type=datetime.fromisoformat)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.mode == "incremental" and not (args.watermark or args.run_id):
        parser.error("incremental preview requires --watermark or --run-id")
    if args.csv and (args.mode != "full" or args.dataset in ("all", "nso_vietnam")):
        parser.error("--csv supports full FAOSTAT source preview only")
    spark = (SparkSession.builder.master("local[1]").appName("HoangReadOnlyPreview")
             .config("spark.ui.enabled", "false").getOrCreate()) if args.csv else get_spark_session("HoangReadOnlyPreview")
    try:
        results = []
        targets = list(REGISTRY) if args.dataset == "all" else [args.dataset]
        for dataset in targets:
            if args.csv:
                bronze = spark.read.option("header", True).csv(args.csv)
                bronze = bronze.toDF(*[sanitize_column_name(c) for c in bronze.columns])
            else:
                bronze = spark.table(f"iceberg.bronze.{dataset}")
                if args.mode == "incremental":
                    if args.watermark:
                        bronze = bronze.filter(F.col("_ingestion_timestamp") > F.lit(args.watermark))
                    else:
                        bronze = bronze.filter(F.col("_ingestion_run_id") == args.run_id)
            try:
                results.append(preview(spark, dataset, bronze))
            except ValueError as exc:
                # Preserve the schema failure in the report and return nonzero;
                # other sources can still be inspected in an all-source audit.
                results.append({"dataset": dataset, "status": "BLOCKED",
                                "error": str(exc), "writes_performed": False})
        print(json.dumps(results if args.dataset == "all" else results[0], ensure_ascii=False))
        if any(result.get("status") == "BLOCKED" for result in results):
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
