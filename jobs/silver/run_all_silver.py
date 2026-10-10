#!/usr/bin/env python3
"""Run or preview registered Silver sources through the shared framework."""
import argparse
from datetime import datetime
import json
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from silver.core.spark_session import get_spark_session
from silver.framework import SilverTransformationFramework, TRANSFORMERS, source_transformer

REGISTRY = TRANSFORMERS
ALIASES = {"usda_export_price": "usda_rice_yearbook"}


def run_pipeline(dataset="all", mode="full", run_id=None, watermark=None, preview=False,
                 pipeline_run_id=None, spark=None):
    dataset = ALIASES.get(dataset, dataset)
    targets = list(REGISTRY) if dataset == "all" else [dataset]
    if any(target not in REGISTRY for target in targets):
        raise ValueError(f"Unknown dataset: {dataset}")
    if mode == "incremental" and watermark is None and not run_id:
        raise ValueError("Incremental execution requires watermark or Bronze run ID")
    owned_session = spark is None
    spark = spark or get_spark_session("SilverPipeline")
    framework = SilverTransformationFramework(spark)
    results = {}
    pipeline_run_id = pipeline_run_id or uuid4().hex
    try:
        # Refuse a partially writing 'all' batch while a contract is unresolved.
        if not preview:
            for target in targets:
                framework.contract_loader.load_contract(target).require_ready()
                if framework.contract_loader.load_contract(target).silver_output in ("", "UNKNOWN"):
                    raise ValueError(f"{target}: pending contract; preview all or execute an approved source individually")
        for target in targets:
            try:
                if preview:
                    bronze = framework.read_input(f"iceberg.bronze.{target}", mode, watermark, run_id)
                    contract, cached, valid, quarantine, report = framework.prepare(target, bronze, allow_unapproved=True)
                    try:
                        errors = quarantine.select(F.explode("dq_errors").alias("error")).groupBy("error.rule_id").count()
                        report["dq_errors"] = {r["rule_id"]: r["count"] for r in errors.collect()}
                        report["warning_count"] = valid.filter(F.size("dq_warnings") > 0).count() if "dq_warnings" in valid.columns else 0
                        report["null_values"] = {c: valid.filter(F.col(c).isNull()).count()
                                                 for c in ("value", "price", "price_fob", "price_value") if c in valid.columns}
                        report["status"] = "PREVIEW_VALIDATED" if contract.status == "READY" else "PREVIEW_NEEDS_APPROVAL"
                        report["output_schema"] = valid.schema.jsonValue()
                        snapshot = bronze.select("_bronze_iceberg_snapshot_id").first()
                        report["bronze_iceberg_snapshot_id"] = snapshot[0] if snapshot else None
                        results[target] = report
                    finally:
                        cached.unpersist()
                else:
                    results[target] = framework.run(target, pipeline_run_id, mode, watermark, run_id)
            except Exception as exc:
                if not preview:
                    raise
                results[target] = {"status": "BLOCKED", "error": str(exc), "writes_performed": False}
        return results
    finally:
        if owned_session:
            spark.stop()


def main(default_dataset="all"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["all", *REGISTRY, *ALIASES], default=default_dataset)
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--run-id", help="Bronze ingestion run ID for filtering")
    parser.add_argument("--pipeline-run-id", help="Stable orchestration ID for retries")
    parser.add_argument("--watermark", type=datetime.fromisoformat)
    parser.add_argument("--preview", action="store_true", help="Read-only; never creates Silver or quarantine")
    parser.add_argument("--output", help="Optional local JSON report path")
    args = parser.parse_args()
    results = run_pipeline(args.dataset, args.mode, args.run_id, args.watermark, args.preview, args.pipeline_run_id)
    payload = json.dumps(results, ensure_ascii=False, default=str)
    print("SILVER_RESULT_JSON=" + payload)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    if any(r.get("status") == "BLOCKED" for r in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
