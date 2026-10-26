#!/usr/bin/env python3
"""Admit a new code fingerprint after failed read-only semantic validation.

Requires identical source/reference pins and independent exact readback of all
17 old output UUID/snapshot/metadata/file sets. No table writes or reset.
"""
import argparse
import os
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from gold.admission import writer_guard, success_context, batch_context
from gold.publication import Publication
from gold.recovery import recovered_state
from gold.spec import load_spec
from gold.preflight import validate_gold_schema


def main():
    from pyiceberg.catalog import load_catalog
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-batch", required=True)
    parser.add_argument("--owner-run-id", required=True)
    args = parser.parse_args()
    with writer_guard() as guard:
        store = Publication(guard=guard)
        active = store.get("active.json")
        if not active or active["batch"] != args.from_batch or active["status"] != "FAILED_NOT_PUBLISHED":
            raise ValueError("Only the active failed batch may enter recovery")
        previous = store.get("working/" + args.from_batch + ".json")
        context = batch_context(success_context())
        catalog = load_catalog("gold_recovery_readonly", **{"type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse", "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"), "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1"})
        actual = {}
        for name, spec in load_spec().items():
            table = catalog.load_table("gold_local." + name)
            validate_gold_schema(table, spec)
            if table.metadata.properties.get("gold.owner") != "tlcn-local" or table.metadata.properties.get("gold.model") != context["model"]:
                raise ValueError("Unowned recovery table")
            snap = table.current_snapshot()
            actual[name] = {"uuid": str(table.metadata.table_uuid), "snapshot_id": str(snap.snapshot_id), "metadata_location": table.metadata_location,
                            "rows": int(snap.summary.additional_properties["total-records"]), "data_files": sorted(str(t.file.file_path) for t in table.scan(snapshot_id=snap.snapshot_id).plan_files())}
        recovered = recovered_state(previous, context, args.owner_run_id, actual)
        store.save(recovered)
        print(json.dumps({"status": recovered["status"], "batch": recovered["batch"], "from_batch": args.from_batch, "tables_verified": len(actual), "table_writes": 0}))


if __name__ == "__main__":
    main()
