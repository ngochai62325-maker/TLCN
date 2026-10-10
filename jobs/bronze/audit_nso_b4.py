"""Read-only B4 evidence: catalog, raw inventory, PostgreSQL and full Trino rows."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

from ingestion.recovery.nso import digest_rows
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.minio_storage import MinioStorage


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def execution_state(repo):
    """SELECT only; the recovery caller separately holds the NSO advisory lock."""
    result = {}
    conn = repo._get_connection()
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            for name in ("ingestion_runs", "source_snapshots", "source_watermarks",
                         "ingestion_checkpoints", "dead_letter_records"):
                cur.execute(f"SELECT row_to_json(t) FROM ingestion.{name} t ORDER BY row_to_json(t)::text")
                rows = [r[0] for r in cur.fetchall()]
                result[name] = {"rows": len(rows), "sha256": fingerprint(rows)}
                if name == "ingestion_runs":
                    result["active_nso_runs"] = [r for r in rows if r["source_id"] == "nso_vietnam"
                        and (r["status"] not in ("SUCCESS", "SKIPPED", "FAILED") or r["completed_at"] is None)]
            cur.execute("SELECT dag_id, is_paused FROM dag WHERE dag_id IN "
                        "('rice_lakehouse_ingestion', 'rice_lakehouse_silver_pipeline') ORDER BY dag_id")
            result["dags"] = [list(r) for r in cur.fetchall()]
            cur.execute("SELECT dag_id, task_id, run_id, state FROM task_instance WHERE "
                        "state IN ('running','queued','scheduled','up_for_retry','deferred','restarting')")
            result["active_tasks"] = [list(r) for r in cur.fetchall()]
            key = repo._derive_lock_key("nso_vietnam") & ((1 << 64) - 1)
            cur.execute("SELECT pid, granted FROM pg_locks WHERE locktype='advisory' AND "
                        "classid::bigint=%s AND objid::bigint=%s AND objsubid=1", (key >> 32, key & 0xffffffff))
            result["nso_advisory_locks"] = [list(r) for r in cur.fetchall()]
    finally:
        conn.rollback()
        conn.close()
    return result


def require_idle(state):
    if state["active_nso_runs"] or state["active_tasks"]:
        raise ValueError("STOP: active ingestion/replay/orchestration; no shared recovery allowed")
    if len(state["dags"]) != 2 or not all(row[1] for row in state["dags"]):
        raise ValueError("STOP: expected DAGs must remain paused for approved B4 window")


def compare_approved(report, plan, allow_recovered=False):
    """Check all raw/file/lineage identities; only an exact repaired retry may differ."""
    keys = ("archive_sha256", "directory_content_sha256", "archive_members", "physical_rows",
            "matrix_nonnull_cells", "numeric_cells", "missing_marker_cells", "replay_identity_sha256",
            "manifest", "archive_uri", "manifest_uri", "schema_fields_after")
    checks = {key: report[key] == plan[key] for key in keys}
    for key in ("file", "sha256", "rows", "lineage", "matrix_nonnull_cells", "numeric_cells",
                "missing_marker_cells", "missing_markers"):
        checks["files." + key] = [f[key] for f in report["files"]] == [f[key] for f in plan["files"]]
    checks["metadata_txt.sha256"] = report["source_metadata_txt"]["sha256"] == plan["source_metadata_txt"]["sha256"]
    if not allow_recovered:
        for key in ("current_snapshot_id", "schema_id_before", "schema_before", "schema_additions"):
            checks[key] = report[key] == plan[key]
    else:
        original = {f["name"]: f for f in plan["schema_before"]["fields"]}
        current = {f["name"]: f for f in report["schema_before"]["fields"]}
        checks["retained_field_ids_types"] = all(current.get(name) == field for name, field in original.items())
        checks["exact_recovered_schema"] = set(current) == set(original) | set(plan["schema_additions"])
        checks["nullable_string_additions"] = all(current.get(name, {}).get("type") == "string"
            and current.get(name, {}).get("required") is False for name in plan["schema_additions"])
    if not all(checks.values()):
        raise ValueError("STOP: approved plan drift: " + ", ".join(k for k, v in checks.items() if not v))
    return checks


def catalog_inventory(catalog):
    result = {}
    for namespace in catalog.list_namespaces():
        for identifier in catalog.list_tables(namespace):
            table = catalog.load_table(identifier)
            result[".".join(identifier)] = {"snapshot": table.metadata.current_snapshot_id,
                "schema": table.schema().model_dump(mode="json"),
                "snapshots": [s.model_dump(mode="json", by_alias=True) for s in table.metadata.snapshots]}
    return result


def data_files(table, snapshot=None):
    return sorted([{"path": task.file.file_path, "rows": task.file.record_count,
                    "bytes": task.file.file_size_in_bytes} for task in table.scan(snapshot_id=snapshot).plan_files()],
                  key=lambda item: item["path"])


def audit(args):
    from recover_nso import load_inputs, metadata_repository
    writer = BronzeIcebergWriter(trino_port=8080)
    table, frame, recovered, report = load_inputs(writer, args.local_dir)
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    after = args.phase == "after"
    checks = compare_approved(report, plan, allow_recovered=after)
    state = execution_state(metadata_repository())
    require_idle(state)
    if state["nso_advisory_locks"]:
        raise ValueError("STOP: NSO advisory lock already held outside recovery process")
    report["approved_plan_checks"] = checks
    report["execution_state"] = state
    report["catalog_inventory"] = catalog_inventory(writer.get_iceberg_catalog())
    report["data_files"] = data_files(table)
    report["metadata_location"] = table.metadata_location
    storage = MinioStorage()
    report["raw_objects"] = json.loads(json.dumps(
        sorted(storage.list_objects("bronze", "raw/nso_vietnam/"), key=lambda obj: obj["Key"]), default=str))
    # Legacy Trino REST clients render datetime values at millisecond precision.
    # Server-side varchar preserves all six digits without changing table/session settings.
    projection = [f'CAST("{c}" AS varchar) AS "{c}"' if c == "_ingestion_timestamp" else f'"{c}"'
                  for c in frame.columns]
    columns, values = writer.execute_query("SELECT " + ", ".join(projection) + " FROM iceberg.bronze.nso_vietnam")
    trino = pd.DataFrame(values, columns=columns)
    trino["_ingestion_timestamp"] = pd.to_datetime(
        trino["_ingestion_timestamp"].str.replace(" UTC", "+00:00", regex=False), utc=True)
    report["trino"] = {"rows": len(trino), "fields": len(columns),
        "timestamp_read": "server-side varchar retains microseconds",
        "full_cell_multiset_matches_pyiceberg": digest_rows(trino[list(frame)]) == digest_rows(frame)}
    if not report["trino"]["full_cell_multiset_matches_pyiceberg"]:
        raise ValueError("STOP: Trino/PyIceberg full-row reconciliation mismatch")
    if after:
        before = json.loads(Path(args.before).read_text(encoding="utf-8"))
        commit = json.loads(Path(args.result).read_text(encoding="utf-8"))
        if commit["shared_recovery"]["snapshot_after"] != table.metadata.current_snapshot_id:
            raise ValueError("STOP: current snapshot differs from recorded commit")
        matrix = list(plan["schema_additions"])
        matrix = [c for c in matrix if c != "t_nh_th_nh_ph_"]
        cells = frame[matrix].stack().tolist()
        numeric = sum(pd.notna(pd.to_numeric(value, errors="coerce")) for value in cells)
        markers = sum(value == ".." for value in cells)
        report["post_commit_cells"] = {"nonnull": len(cells), "numeric": int(numeric), "missing_markers": markers}
        if (len(cells), numeric, markers) != (plan["matrix_nonnull_cells"], plan["numeric_cells"], plan["missing_marker_cells"]):
            raise ValueError("STOP: recovered physical matrix counts mismatch")
        if digest_rows(frame[list(recovered)]) != digest_rows(recovered):
            raise ValueError("STOP: committed rows differ from lossless raw reconstruction")
        report["raw_reconstruction_matches"] = True
        report["original_snapshot_data_files"] = data_files(table, plan["current_snapshot_id"])
        if report["original_snapshot_data_files"] != before["data_files"]:
            raise ValueError("STOP: baseline snapshot files changed")
        report["written_data_files"] = [f for f in report["data_files"] if f not in before["data_files"]]
        preserved = {}
        for name in ("ingestion_runs", "source_snapshots", "source_watermarks", "ingestion_checkpoints", "dead_letter_records"):
            preserved[name] = state[name] == before["execution_state"][name]
        preserved["raw_objects"] = report["raw_objects"] == before["raw_objects"]
        preserved["dags_paused"] = state["dags"] == before["execution_state"]["dags"]
        preserved["other_catalog_tables"] = {k:v for k,v in report["catalog_inventory"].items() if k != "bronze.nso_vietnam"} == {
            k:v for k,v in before["catalog_inventory"].items() if k != "bronze.nso_vietnam"}
        previous = before["catalog_inventory"]["bronze.nso_vietnam"]["snapshots"]
        current = report["catalog_inventory"]["bronze.nso_vietnam"]["snapshots"]
        preserved["original_snapshots"] = all(s in current for s in previous)
        report["preserved_state"] = preserved
        if not all(preserved.values()):
            raise ValueError("STOP: protected state changed: " + ", ".join(k for k,v in preserved.items() if not v))
        report["rollback_readiness"] = {"snapshot_before": plan["current_snapshot_id"],
            "snapshot_after": table.metadata.current_snapshot_id, "baseline_files_retained": True,
            "baseline_snapshot_retained": True, "nullable_schema_retained_on_rollback": True,
            "rollback_executed": False, "requires_current_snapshot_guard_and_user_decision": True}
    report["audit_status"] = "PASSED"
    report["phase"] = args.phase
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str)+"\n", encoding="utf-8")
    print(json.dumps({"phase":args.phase, "audit_status":report["audit_status"], "snapshot": report["current_snapshot_id"],
                      "rows":len(frame), "fields":len(frame.columns), "output":args.output}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--local-dir", default="/opt/airflow/data/raw/nso")
    parser.add_argument("--before")
    parser.add_argument("--result")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.phase == "after" and (not args.before or not args.result):
        parser.error("after requires --before and --result")
    audit(args)
