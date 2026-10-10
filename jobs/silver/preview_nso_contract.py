"""Evaluate the E1 draft using a checksummed export of the pinned real snapshot.

The export is made by profile_nso_contract.py through a read-only Iceberg scan.
Run in a network-disabled Spark container; this job has no catalog writer calls.
"""
import argparse
import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from silver.framework import SilverTransformationFramework
from silver.transformers.nso_vietnam import NsoVietnamTransformer


def evaluate(spark, parquet, profile):
    source = json.loads(Path(profile).read_text(encoding="utf-8"))
    checksum = hashlib.sha256(Path(parquet).read_bytes()).hexdigest()
    if checksum != source["local_parquet_sha256"] or source["snapshot"] != 3499764399995403202:
        raise ValueError("STOP: pinned NSO export identity mismatch")
    bronze = spark.read.parquet(parquet)
    if bronze.count() != source["rows"] or len(bronze.columns) != 49:
        raise ValueError("STOP: pinned export row/schema mismatch")
    bronze = bronze.withColumn("_bronze_iceberg_snapshot_id", F.lit(source["snapshot"]).cast("long"))
    framework = SilverTransformationFramework(spark)
    contract, cached, valid, quarantine, report = framework.prepare("nso_vietnam", bronze, allow_unapproved=True)
    try:
        report.update(status="PREVIEW_NEEDS_APPROVAL", bronze_snapshot_id=source["snapshot"],
            local_export_sha256=checksum, network_disabled=True, writes_shared_data=False)
        repository = Path(__file__).resolve().parents[2]
        inputs = ("src/silver/transformers/nso_vietnam.py", "src/silver/mappings/nso_geography.py",
            "src/silver/framework.py", "src/silver/contract/loader.py", "contracts/silver/nso_vietnam.yaml",
            "config/silver/nso_source_tables.json", "config/silver/nso_geography_candidates.json",
            "config/silver/nso_geography_reviewed.json")
        report["implementation_sha256"] = {path:hashlib.sha256((repository / path).read_bytes()).hexdigest() for path in inputs}
        report["dq_errors"] = {r[0]: r[1] for r in quarantine.select(F.explode("dq_errors").alias("error"))
            .groupBy("error.rule_id").count().collect()}
        report["by_source"] = [r.asDict() for r in cached.groupBy("source_table", "statistic_kind", "season", "measure")
            .agg(F.count("*").alias("observations"), F.sum(F.col("value_source").isNotNull().cast("int")).alias("numeric_cells"),
                 F.sum((F.col("missing_kind") == "source_marker").cast("int")).alias("missing_markers"))
            .orderBy("source_table", "statistic_kind", "season", "measure").collect()]
        report["missing_kinds"] = {r[0]: r[1] for r in cached.groupBy("missing_kind").count().collect()}
        report["provisional_observations"] = cached.filter("is_provisional").count()
        report["gold_ready_count"] = valid.filter("gold_ready").count()
        report["warning_count"] = valid.filter(F.size("dq_warnings") > 0).count()
        report["source_checksum_count"] = cached.select("_source_checksum").distinct().count()
        report["duplicate_key_count"] = cached.count() - cached.select(*contract.business_key).distinct().count()
        report["output_schema"] = valid.schema.jsonValue()
        report["all_source_null_record_audit"] = [json.loads(row) for row in
            NsoVietnamTransformer().excluded_records(bronze).toJSON().collect()]
        return report
    finally:
        cached.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = (SparkSession.builder.master("local[1]").appName("NSOReadOnlyE1")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = evaluate(spark, args.parquet, args.profile)
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        print("NSO_E1_PREVIEW=" + json.dumps({k:report[k] for k in ("status", "bronze_count", "observation_count", "eligible_observation_count", "valid_count", "quarantine_count", "excluded_count")}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
