"""Run, audit and replay all READY sources against the configured Iceberg catalog.

Reads Bronze without modifying ingestion metadata. Writes only the contract
Silver targets and quarantine. Output is reproducible JSON acceptance evidence.
"""
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

from run_all_silver import ROOT, datasets
from silver.audit import audit_tables, check_bronze
from silver.core.spark_session import get_spark_session
from silver.framework import SilverTransformationFramework
from pyspark.sql import functions as F


def table_state(spark, contract):
    df = spark.table(contract.silver_output)
    snapshots = spark.table(contract.silver_output + ".snapshots")
    latest = snapshots.orderBy(F.col("committed_at").desc(), F.col("snapshot_id").desc()).first()
    # The sum is a cheap replay check across all rows, including processing time.
    checksum = df.select(F.xxhash64(*df.columns).cast("decimal(38,0)").alias("h")).agg(F.sum("h")).first()[0]
    return {"rows": df.count(), "snapshot_id": latest.snapshot_id if latest else None,
            "row_checksum": str(checksum), "schema": df.schema.jsonValue()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--dataset", default="all")
    parser.add_argument("--audit-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    contracts = [c for k, c in datasets(ROOT / "contracts/silver").items()
                 if c.status == "READY" and (args.dataset == "all" or k == args.dataset)]
    if not contracts:
        raise ValueError("No READY datasets selected")
    spark = get_spark_session("SilverEndToEndValidation")
    spark.sparkContext.setLogLevel("WARN")
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "status": "RUNNING", "datasets": {}}
    try:
        report["bronze_readiness"] = check_bronze(spark, contracts)
        framework = SilverTransformationFramework(spark, str(ROOT / "contracts/silver"))
        for contract in contracts:
            source = contract.source_id
            bronze_before = spark.table(contract.bronze_input + ".snapshots").orderBy(F.col("committed_at").desc()).first()
            if args.audit_only:
                metrics = None
            else:
                metrics = framework.run(source, "silver-e2e-first")
            first = table_state(spark, contract)
            audit = audit_tables(spark, [contract])
            if not args.audit_only:
                replay = framework.run(source, "silver-e2e-replay")
                second = table_state(spark, contract)
                if first != second:
                    report["replay_difference"] = {"dataset": source, "first": first, "second": second}
                    raise ValueError(f"Replay changed Silver rows/schema/snapshot: {source}")
                if metrics != dict(replay, pipeline_run_id=metrics["pipeline_run_id"]):
                    raise ValueError(f"Replay metrics differ: {source}")
            bronze_after = spark.table(contract.bronze_input + ".snapshots").orderBy(F.col("committed_at").desc()).first()
            if bronze_before.snapshot_id != bronze_after.snapshot_id:
                raise ValueError(f"Bronze changed during acceptance: {source}; rerun against stable input")
            record = {"status": "PASSED", "metrics": metrics, "silver": first,
                      "bronze_snapshot_id": bronze_before.snapshot_id, "audit": audit,
                      "replay_verified": not args.audit_only}
            report["datasets"][source] = record
            print("E2E_DATASET=" + json.dumps({"dataset": source, "status": "PASSED", "metrics": metrics}), flush=True)
        report["quarantine_counts"] = {r.source_dataset: r["count"] for r in spark.table("iceberg.silver.dead_letters").groupBy("source_dataset").count().collect()}
        report["status"] = "PASSED"
    except Exception as exc:
        report["status"] = "FAILED"
        report["error"] = str(exc)
        raise
    finally:
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
        spark.stop()
    print("E2E_RESULT=" + json.dumps({"status": report["status"], "datasets": len(report["datasets"])}), flush=True)


if __name__ == "__main__":
    main()
