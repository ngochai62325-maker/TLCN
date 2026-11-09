#!/usr/bin/env python3
"""Read-only actual Trade validation/checkpointing; never edits batch pointers."""
import argparse
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


class Checks:
    def __init__(self):
        self.results = []

    def record(self, rule, scope, description, expected, actual, ok=True):
        if not ok or expected != actual:
            raise ValueError(f"{rule} {scope}: {description}")
        self.results.append({"rule_id": rule, "scope": scope, "status": "ACCEPT"})


def main():
    from gold.admission import writer_guard, success_context, batch_context, runtime_resources
    from gold.publication import Publication
    from gold.recovery import recovered_state
    from gold.trade_validation import validate_trade, partition_sql, resource_guard
    from gold.views import trino_result
    from gold.preflight import validate_gold_schema
    from gold.spec import load_spec
    import os
    from pyiceberg.catalog import load_catalog
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int)
    parser.add_argument("--bucket", type=int, choices=range(4))
    parser.add_argument("--explain", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    runtime_resources()
    with writer_guard() as guard:
        store = Publication(guard=guard)
        active = store.get("active.json")
        state = store.get("working/" + active["batch"] + ".json")
        context = batch_context(success_context())
        catalog = load_catalog("gold_trade_readonly", **{"type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse",
            "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"), "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1"})
        for pin in context["inputs"].values():
            table = catalog.load_table(pin["table"].removeprefix("iceberg."))
            if str(table.metadata.table_uuid) != pin["uuid"] or str(table.current_snapshot().snapshot_id) != pin["snapshot_id"]:
                raise ValueError("Silver moved beyond successful pinned proof")
        actual = {}
        for name, spec in load_spec().items():
            table = catalog.load_table("gold_local." + name)
            validate_gold_schema(table, spec)
            snap = table.current_snapshot()
            actual[name] = {"uuid": str(table.metadata.table_uuid), "snapshot_id": str(snap.snapshot_id), "metadata_location": table.metadata_location,
                "rows": int(snap.summary.additional_properties["total-records"]), "data_files": sorted(str(t.file.file_path) for t in table.scan(snapshot_id=snap.snapshot_id).plan_files())}
            ownership = table.metadata.properties
            if ownership.get("gold.owner") != state["owner"] or ownership.get("gold.model") != state["model"]:
                raise ValueError("Trade validation found unowned output")
        # Reuse recovery's strict read-only admission; DO NOT save its proposed state.
        recovered_state(state, context, "read_only_trade_validation", actual)
        if args.explain:
            if args.year is None or args.bucket is None:
                parser.error("EXPLAIN requires --year and --bucket")
            resource_guard()
            result = trino_result("EXPLAIN (TYPE DISTRIBUTED) " + partition_sql(state["outputs"], args.year, args.bucket))
            print(result["rows"][0][0])
        else:
            result = validate_trade(state["outputs"], Checks(), store, context, args.year, args.bucket)
            print("TRADE_RESULT=" + json.dumps(result))


if __name__ == "__main__":
    main()
