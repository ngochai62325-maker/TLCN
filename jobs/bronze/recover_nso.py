"""Plan/test NSO repair; shared execute/rollback require separate user approval."""
import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from ingestion.recovery.nso import atomic_recover, digest_rows, prepare_recovery
from ingestion.storage.bronze_writer import BronzeIcebergWriter


def load_inputs(writer, local_dir):
    from ingestion.storage.minio_storage import MinioStorage
    table = writer.get_iceberg_catalog().load_table("bronze.nso_vietnam")
    snapshot = table.current_snapshot().snapshot_id
    baseline = table.scan(snapshot_id=snapshot).to_arrow().to_pandas()
    storage = MinioStorage()
    candidates = [obj for obj in storage.list_objects("bronze", "raw/nso_vietnam/") if obj["Key"].endswith("/manifest.json")]
    # A new raw run requires a reviewed source-selection plan, never pick by filename order.
    if len(candidates) != 1:
        raise ValueError("Expected one NSO raw manifest; review multiple runs before recovery")
    manifest_key = candidates[0]["Key"]
    manifest = json.loads(storage.s3_client.get_object(Bucket="bronze", Key=manifest_key)["Body"].read())
    uri = manifest["artifact_info"]["uri"]
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    archive = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    recovered, report = prepare_recovery(archive, baseline, table.schema(), local_dir,
                                          manifest["artifact_info"]["checksum"])
    report.update(current_snapshot_id=snapshot, target="iceberg.bronze.nso_vietnam", archive_uri=uri,
                  manifest_uri=f"s3://bronze/{manifest_key}", manifest=manifest,
                  schema_id_before=table.schema().schema_id,
                  source_metadata_txt={"location": str(Path(local_dir) / "metadata.txt"),
                      "sha256": hashlib.sha256((Path(local_dir) / "metadata.txt").read_bytes()).hexdigest(),
                      "included_in_zip": False})
    import pyiceberg
    import pyarrow
    report["runtime"] = {"pyiceberg": pyiceberg.__version__, "pyarrow": pyarrow.__version__,
                         "table_format_version": table.metadata.format_version,
                         "native_transaction_available": hasattr(table, "transaction"),
                         "native_rollback_available": hasattr(table.manage_snapshots(), "rollback_to_snapshot")}
    return table, baseline, recovered, report


def metadata_repository():
    """Use supplied local credentials without logging them or rewriting service settings."""
    from ingestion.storage.metadata_repository import MetadataRepository
    from psycopg2.extensions import make_dsn
    if os.environ.get("INGESTION_DB_URL"):
        return MetadataRepository()
    return MetadataRepository(make_dsn(host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"), user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname=os.environ.get("POSTGRES_DB", "airflow")))


def isolated_recovery(baseline, recovered, schema, report):
    import pyiceberg
    import pyarrow
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.types import StringType
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="nso-recovery-") as root:
        path = Path(root)
        catalog = SqlCatalog("isolated", uri=f"sqlite:///{(path / 'catalog.db').as_posix()}", warehouse=str(path))
        catalog.create_namespace("isolated")
        table = catalog.create_table("isolated.nso_vietnam", schema=schema)
        writer = BronzeIcebergWriter()
        table.append(writer._dataframe_to_arrow(baseline, schema))
        first = table.current_snapshot().snapshot_id
        approved = {c: "string" for c in report["schema_additions"]}
        def fail():
            raise RuntimeError("Injected pre-commit failure")
        try:
            atomic_recover(table, recovered, approved, first, before_commit=fail)
        except RuntimeError as exc:
            if str(exc) != "Injected pre-commit failure":
                raise
        else:
            raise AssertionError("Injected failure did not stop the transaction")
        table.refresh()
        assert table.current_snapshot().snapshot_id == first
        assert len(table.schema().fields) == len(schema.fields)
        assert digest_rows(table.scan().to_arrow().to_pandas()[list(baseline)]) == digest_rows(baseline)
        result = atomic_recover(table, recovered, approved, first)
        second = atomic_recover(table, recovered, approved, first)
        assert second["status"] == "NOOP" and second["snapshot_after"] == result["snapshot_after"]
        retained_snapshots = len(table.metadata.snapshots)
        table.manage_snapshots().rollback_to_snapshot(first).commit()
        table.refresh()
        rolled = table.scan().to_arrow().to_pandas()
        assert digest_rows(rolled[list(baseline)]) == digest_rows(baseline)
        assert len(table.metadata.snapshots) == retained_snapshots
        # Snapshot rollback retains the added nullable schema; no DROP COLUMN.
        assert all(isinstance(table.schema().find_field(c).field_type, StringType) for c in approved)
        report["isolated"] = {"status": "PASSED", "engine": "PyIceberg SQLite/filesystem",
            "pyiceberg": pyiceberg.__version__, "pyarrow": pyarrow.__version__,
            "network_required_for_writes": False, "recovery": result, "rerun": second,
            "pre_commit_failure_kept_baseline": True, "rollback_projection_matches": True,
            "rollback_retains_added_nullable_schema": True, "snapshot_history_retained": True,
            "elapsed_seconds": round(time.monotonic() - started, 3)}
        catalog.engine.dispose()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["plan", "isolated", "execute", "rollback"], default="plan")
    parser.add_argument("--env-file")
    parser.add_argument("--local-dir", default="data/raw/nso")
    parser.add_argument("--plan", help="Reviewed read-only evidence JSON, mandatory for execute")
    parser.add_argument("--offline-archive", help="Verified local ZIP copy; isolated mode only")
    parser.add_argument("--offline-baseline", help="Pinned baseline Parquet copy; isolated mode only")
    parser.add_argument("--offline-plan", help="Read-only evidence describing the pinned local copies")
    parser.add_argument("--output", default="docs/silver/nso_recovery_evidence.json")
    parser.add_argument("--from-snapshot", type=int, help="Expected current snapshot for explicit rollback")
    parser.add_argument("--to-snapshot", type=int, help="Retained baseline snapshot for explicit rollback")
    args = parser.parse_args()
    if args.env_file:
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    output = Path(args.output)
    if args.mode == "execute" and not args.plan:
        parser.error("execute requires the separately approved --plan")
    offline = any((args.offline_archive, args.offline_baseline, args.offline_plan))
    if offline and (args.mode != "isolated" or not all((args.offline_archive, args.offline_baseline, args.offline_plan))):
        parser.error("offline inputs require isolated mode and all three offline arguments")
    if args.mode == "rollback" and (not args.from_snapshot or not args.to_snapshot):
        parser.error("rollback requires --from-snapshot and --to-snapshot")
    writer = BronzeIcebergWriter(trino_port=8080)
    lock = metadata_repository().source_lock("nso_vietnam") if args.mode in ("execute", "rollback") else nullcontext()
    with lock:
        execution_checks = None
        if args.mode == "execute":
            from audit_nso_b4 import execution_state, require_idle
            execution_checks = execution_state(metadata_repository())
            require_idle(execution_checks)
            if len(execution_checks["nso_advisory_locks"]) != 1 or not execution_checks["nso_advisory_locks"][0][1]:
                raise ValueError("Expected one granted NSO source lock during recovery")
        if args.mode == "rollback":
            table = writer.get_iceberg_catalog().load_table("bronze.nso_vietnam")
            if table.current_snapshot().snapshot_id != args.from_snapshot:
                raise ValueError("Rollback current snapshot differs; review concurrent changes")
            table.manage_snapshots().rollback_to_snapshot(args.to_snapshot).commit()
            table.refresh()
            report = {"mode": "rollback", "writes_shared_data": True,
                      "snapshot_after": table.current_snapshot().snapshot_id}
        else:
            if offline:
                import pyarrow.parquet as pq
                from pyiceberg.schema import Schema
                plan = json.loads(Path(args.offline_plan).read_text(encoding="utf-8"))
                schema = Schema.model_validate(plan["schema_before"])
                archive = Path(args.offline_archive).read_bytes()
                if hashlib.sha256(archive).hexdigest() != plan["archive_sha256"]:
                    raise ValueError("Offline ZIP copy differs from the verified raw archive")
                baseline = pq.read_table(args.offline_baseline).to_pandas()
                recovered, report = prepare_recovery(archive, baseline, schema, args.local_dir, plan["directory_content_sha256"])
                report["offline_verified_copies"] = True
            else:
                table, baseline, recovered, report = load_inputs(writer, args.local_dir)
                schema = table.schema()
            if args.mode == "isolated":
                report = isolated_recovery(baseline, recovered, schema, report)
            elif args.mode == "execute":
                from audit_nso_b4 import compare_approved
                plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
                if report["archive_sha256"] != plan["archive_sha256"] or report["directory_content_sha256"] != plan["directory_content_sha256"]:
                    raise ValueError("Raw archive changed since the reviewed plan")
                if plan.get("isolated", {}).get("status") != "PASSED":
                    raise ValueError("Reviewed plan must contain successful isolated evidence")
                for key in ("physical_rows", "matrix_nonnull_cells", "numeric_cells", "missing_marker_cells", "replay_identity_sha256"):
                    if report[key] != plan[key]:
                        raise ValueError(f"Recovery coverage/identity changed: {key}")
                if report["current_snapshot_id"] == plan["current_snapshot_id"] and report["schema_before"] != plan["schema_before"]:
                    raise ValueError("Schema changed since review")
                report["approved_plan_checks"] = compare_approved(report, plan,
                    allow_recovered=report["current_snapshot_id"] != plan["current_snapshot_id"])
                report["source_lock_acquired"] = True
                report["execution_state_under_lock"] = execution_checks
                report["commit_intent_at"] = datetime.now(timezone.utc).isoformat()
                # A timeout/post-commit exception cannot be reported as "no writes".
                report["status"] = "COMMIT_INTENT"
                report["writes_shared_data"] = "UNKNOWN_UNTIL_VALIDATED"
                output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
                report["shared_recovery"] = atomic_recover(table, recovered,
                    {c: "string" for c in plan["schema_additions"]}, plan["current_snapshot_id"],
                    repair_id="nso-" + plan["archive_sha256"][:16])
                report["writes_shared_data"] = report["shared_recovery"]["status"] != "NOOP"
                report["status"] = report["shared_recovery"]["status"]
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"mode": args.mode, "rows": report.get("physical_rows"),
        "writes_shared_data": report["writes_shared_data"], "output": str(output)}))


if __name__ == "__main__":
    main()
