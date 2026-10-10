"""Production-only, read-only Spark preflight against the reviewed C2 preview."""
import argparse
import hashlib
import json
from pathlib import Path

from pyspark.sql import functions as F
from silver.core.spark_session import get_spark_session
from silver.framework import SilverTransformationFramework, source_transformer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    approved = json.loads(Path(args.approved).read_text(encoding="utf-8"))
    spark = get_spark_session("C2ProductionReadOnlyPreflight")
    report = {"status":"BLOCKED", "writes_performed":False, "checks":{}}
    try:
        framework = SilverTransformationFramework(spark)
        transformer = source_transformer("faostat_production", spark)
        bronze = framework.read_input("iceberg.bronze.faostat_production")
        contract, cached, valid, quarantine, counts = framework.prepare("faostat_production", bronze, transformer)
        try:
            report.update(counts)
            report["runtime"] = {"spark":spark.version,"master":spark.sparkContext.master,
                "driver_memory":spark.sparkContext.getConf().get("spark.driver.memory"),
                "shuffle_partitions":spark.conf.get("spark.sql.shuffle.partitions"),
                "application_id":spark.sparkContext.applicationId}
            report["schema"] = valid.schema.jsonValue()
            report["source_rule_ids"] = [r["rule_id"] for r in transformer.quality_rules()]
            report["contract_rule_ids"] = [r["rule_id"] for r in contract.data_quality_rules]
            report["framework_rule_ids"] = ["CONTRACT_CAST","CONTRACT_REQUIRED","LINEAGE_REQUIRED","LINEAGE_REFERENCE"]
            report["warnings"] = valid.filter(F.size("dq_warnings") > 0).count()
            report["null_values"] = valid.filter("value IS NULL").count()
            report["source_identity"] = [r.asDict() for r in bronze.groupBy("_source_file","_source_checksum","_source_snapshot_id","_ingestion_run_id").count()]
            snapshots = [r[0] for r in bronze.select("_bronze_iceberg_snapshot_id").distinct().collect()]
            report["snapshots"] = snapshots
            checks = report["checks"]
            checks["snapshot_pinned"] = snapshots == [approved["snapshot"]]
            checks["runtime_limits"] = report["runtime"]["master"] == "local[2]" and report["runtime"]["driver_memory"] == "2g" and report["runtime"]["shuffle_partitions"] == "4"
            checks["physical_schema_matches_preview"] = report["schema"] == approved["preview"]["output_schema"]
            for name in ("bronze_count","observation_count","valid_count","quarantine_count","dedup_count","excluded_count","deduplicated_count"):
                checks[name] = counts[name] == approved["preview"][name]
            checks["warnings_match_preview"] = report["warnings"] == approved["preview"]["warning_count"]
            checks["missing_match_preview"] = report["null_values"] == approved["preview"]["null_values"]["value"]
            hashes = {}
            for path, expected in approved["current_code_sha256"].items():
                if Path(path).is_file():
                    hashes[path] = hashlib.sha256(Path(path).read_bytes()).hexdigest()
                    checks["code:"+path] = hashes[path] == expected
            report["code_sha256"] = hashes
            report["status"] = "PASS" if all(checks.values()) else "BLOCKED"
            report["mismatches"] = [k for k,v in checks.items() if not v]
        finally:
            cached.unpersist()
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
        print("C2_SPARK_PREFLIGHT="+json.dumps({k:report.get(k) for k in ("status","bronze_count","valid_count","quarantine_count","runtime","mismatches","error")}))
        spark.stop()
    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
