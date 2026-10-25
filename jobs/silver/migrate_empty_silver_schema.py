"""Reconcile EMPTY legacy Silver schemas without dropping tables.

Dry run by default. Existing NTZ fields are retained under a legacy name;
new UTC timestamp fields are added. Nonempty tables require a separate data
migration and are deliberately refused by this utility.
Obsolete required diagnostics are made nullable while retaining their fields.
"""
import argparse
import json
from pathlib import Path
from run_all_silver import ROOT, datasets
from silver.core.spark_session import get_spark_session
from silver.engine.merge import quote, table_identifier
from silver.framework import AUDIT_COLUMNS
from pyspark.sql import types as T


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    contract = datasets(ROOT / "contracts/silver")[args.dataset]
    contract.validate_executable()
    spark = get_spark_session("EmptySilverSchemaMigration")
    spark.sparkContext.setLogLevel("WARN")
    report = {"dataset": args.dataset, "table": contract.silver_output, "executed": args.execute, "actions": []}
    try:
        df = spark.table(contract.silver_output)
        if not df.isEmpty():
            raise ValueError("Refusing schema migration of a nonempty Silver table")
        fields = {f.name: f.dataType for f in df.schema}
        published = {c["name"] for c in contract.columns} | set(AUDIT_COLUMNS) | {"_dq_warnings", "_silver_processed_at"}
        for field in df.schema:
            if not field.nullable and field.name not in published and (field.name.startswith("_") or field.name == "dq_warnings"):
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ALTER COLUMN {quote(field.name)} DROP NOT NULL")
        # Only the audit UTC timestamps have unambiguous target semantics.
        for column in ["_ingestion_timestamp", "_silver_processed_at"]:
            legacy = column + "_legacy_ntz"
            if isinstance(fields.get(column), T.TimestampNTZType):
                if legacy in fields:
                    raise ValueError(f"Legacy column already exists: {legacy}; review previous migration")
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} RENAME COLUMN {quote(column)} TO {quote(legacy)}")
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ADD COLUMN {quote(column)} TIMESTAMP")
            elif column not in fields and legacy in fields:
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ADD COLUMN {quote(column)} TIMESTAMP")
        if args.execute:
            for action in report["actions"]:
                # Recheck immediately before each metadata change.
                if not spark.table(contract.silver_output).isEmpty():
                    raise ValueError("Concurrent writer detected; stop and review migration")
                spark.sql(action)
        report["status"] = "MIGRATED" if args.execute and report["actions"] else "NOOP" if not report["actions"] else "PLANNED"
        if args.output_json:
            args.output_json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print("MIGRATION_RESULT=" + json.dumps(report))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
