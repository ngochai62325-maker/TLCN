#!/usr/bin/env python3
"""Unified CLI entrypoint for the Gold layer (stages, preflight, recovery, trade validation, and view queries)."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


class _Checks:
    def __init__(self):
        self.results = []

    def record(self, rule, scope, description, expected, actual, ok=True):
        if not ok or expected != actual:
            raise ValueError(f"{rule} {scope}: {description}")
        self.results.append({"rule_id": rule, "scope": scope, "status": "ACCEPT"})


def _load_readonly_catalog(name):
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        name,
        type="rest",
        uri=os.environ["ICEBERG_REST_URI"],
        warehouse="s3://warehouse",
        **{
            "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"),
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": "us-east-1",
        },
    )


def _run_recover(from_batch, owner_run_id):
    from gold.admission import writer_guard, success_context, batch_context
    from gold.publication import Publication
    from gold.recovery import recovered_state
    from gold.spec import load_spec
    from gold.preflight import validate_gold_schema

    with writer_guard() as guard:
        store = Publication(guard=guard)
        active = store.get("active.json")
        if not active or active["batch"] != from_batch or active["status"] != "FAILED_NOT_PUBLISHED":
            raise ValueError("Only the active failed batch may enter recovery")
        previous = store.get("working/" + from_batch + ".json")
        context = batch_context(success_context())
        catalog = _load_readonly_catalog("gold_recovery_readonly")
        actual = {}
        for name, spec in load_spec().items():
            table = catalog.load_table("gold_local." + name)
            validate_gold_schema(table, spec)
            if table.metadata.properties.get("gold.owner") != "tlcn-local" or table.metadata.properties.get("gold.model") != context["model"]:
                raise ValueError("Unowned recovery table")
            snap = table.current_snapshot()
            actual[name] = {
                "uuid": str(table.metadata.table_uuid),
                "snapshot_id": str(snap.snapshot_id),
                "metadata_location": table.metadata_location,
                "rows": int(snap.summary.additional_properties["total-records"]),
                "data_files": sorted(str(t.file.file_path) for t in table.scan(snapshot_id=snap.snapshot_id).plan_files()),
            }
        recovered = recovered_state(previous, context, owner_run_id, actual)
        store.save(recovered)
        print(json.dumps({"status": recovered["status"], "batch": recovered["batch"], "from_batch": from_batch, "tables_verified": len(actual), "table_writes": 0}))


def _run_validate_trade(year=None, bucket=None, explain=False):
    from gold.admission import writer_guard, success_context, batch_context, runtime_resources
    from gold.publication import Publication
    from gold.recovery import recovered_state
    from gold.trade_validation import validate_trade, partition_sql, resource_guard
    from gold.views import trino_result
    from gold.preflight import validate_gold_schema
    from gold.spec import load_spec

    runtime_resources()
    with writer_guard() as guard:
        store = Publication(guard=guard)
        active = store.get("active.json")
        state = store.get("working/" + active["batch"] + ".json")
        context = batch_context(success_context())
        catalog = _load_readonly_catalog("gold_trade_readonly")
        for pin in context["inputs"].values():
            table = catalog.load_table(pin["table"].removeprefix("iceberg."))
            if str(table.metadata.table_uuid) != pin["uuid"] or str(table.current_snapshot().snapshot_id) != pin["snapshot_id"]:
                raise ValueError("Silver moved beyond successful pinned proof")
        actual = {}
        for name, spec in load_spec().items():
            table = catalog.load_table("gold_local." + name)
            validate_gold_schema(table, spec)
            snap = table.current_snapshot()
            actual[name] = {
                "uuid": str(table.metadata.table_uuid),
                "snapshot_id": str(snap.snapshot_id),
                "metadata_location": table.metadata_location,
                "rows": int(snap.summary.additional_properties["total-records"]),
                "data_files": sorted(str(t.file.file_path) for t in table.scan(snapshot_id=snap.snapshot_id).plan_files()),
            }
            ownership = table.metadata.properties
            if ownership.get("gold.owner") != state["owner"] or ownership.get("gold.model") != state["model"]:
                raise ValueError("Trade validation found unowned output")
        recovered_state(state, context, "read_only_trade_validation", actual)
        if explain:
            if year is None or bucket is None:
                raise ValueError("EXPLAIN requires --year and --bucket")
            resource_guard()
            result = trino_result("EXPLAIN (TYPE DISTRIBUTED) " + partition_sql(state["outputs"], year, bucket))
            print(result["rows"][0][0])
        else:
            result = validate_trade(state["outputs"], _Checks(), store, context, year, bucket)
            print("TRADE_RESULT=" + json.dumps(result))


def main():
    from gold.facts import BUILD_ORDER
    from gold.pipeline import ACTIONS, execute
    from gold.views import VIEW_NAMES

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=ACTIONS, help="Gold pipeline stage to execute")
    parser.add_argument("--owner-run-id", help="Run ownership identifier")
    parser.add_argument("--silver-run-id", help="Optional pinned Silver run ID")
    parser.add_argument("--source", choices=BUILD_ORDER, help="Source name when --action=fact")
    parser.add_argument("--preflight", action="store_true", help="Read-only static/physical baseline check (no Spark)")
    parser.add_argument("--recover", action="store_true", help="Admit a new code fingerprint after failed read-only validation")
    parser.add_argument("--from-batch", help="Failed batch ID when using --recover")
    parser.add_argument("--validate-trade", action="store_true", help="Read-only Trade semantic validation/checkpointing")
    parser.add_argument("--year", type=int, help="Optional reporting year for --validate-trade or --query")
    parser.add_argument("--bucket", type=int, choices=range(4), help="Optional bucket (0..3) for --validate-trade")
    parser.add_argument("--explain", action="store_true", help="Print Trino EXPLAIN plan for --validate-trade")
    parser.add_argument("--query", choices=VIEW_NAMES, help="Query a published Gold semantic view via Trino")
    args = parser.parse_args()

    if args.preflight:
        from gold.preflight import inspect
        report = inspect()
        print(json.dumps(report, indent=2, ensure_ascii=False))
        raise SystemExit(0 if report["status"] == "READY" else 2)

    if args.query:
        from gold.consumer import query
        print(json.dumps(query(args.query, args.year), ensure_ascii=False, indent=2))
        return

    if args.recover:
        if not args.from_batch or not args.owner_run_id:
            parser.error("--recover requires --from-batch and --owner-run-id")
        _run_recover(args.from_batch, args.owner_run_id)
        return

    if args.validate_trade:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        _run_validate_trade(args.year, args.bucket, args.explain)
        return

    if not args.action or not args.owner_run_id:
        parser.error("Stage execution requires --action and --owner-run-id")

    from silver.core.spark_session import get_spark_session
    from gold.admission import runtime_resources, success_context
    from gold.views import require_semantic_definitions

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Cheap capability checks precede JVM allocation and every catalog/object write.
    runtime_resources()
    success_context(args.silver_run_id)
    require_semantic_definitions()
    spark = get_spark_session("TLCNGold-" + args.action)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    try:
        result = execute(spark, args.action, args.owner_run_id, args.silver_run_id, args.source)
        print("GOLD_RESULT=" + json.dumps(result, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

