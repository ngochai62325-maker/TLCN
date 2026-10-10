"""Read-only Production C2 identity, catalog, raw artifact and source-lock audit."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

import requests
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bronze"))
from recover_nso import metadata_repository
from audit_nso_b4 import catalog_inventory, execution_state, data_files
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.minio_storage import MinioStorage
from silver.registry import TRANSFORMERS

SOURCE = "faostat_production"
SNAPSHOT = 3250087836768729857


def lock_rows(repo, source=SOURCE):
    key = repo._derive_lock_key(source) & ((1 << 64) - 1)
    conn = repo._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pid, granted FROM pg_locks WHERE locktype='advisory' AND "
                "classid::bigint=%s AND objid::bigint=%s AND objsubid=1", (key >> 32, key & 0xffffffff))
            return [list(row) for row in cur.fetchall()]
    finally:
        conn.close()


def audit(approved, output):
    report = {"status":"BLOCKED", "observed_at":datetime.now(timezone.utc).isoformat(),
              "shared_writes":False, "checks":{}, "errors":[]}
    def check(name, ok):
        report["checks"][name] = bool(ok)
        if not ok:
            report["errors"].append(name)
    try:
        writer = BronzeIcebergWriter(trino_host="trino", trino_port=8080)
        cat = writer.get_iceberg_catalog()
        inventory = catalog_inventory(cat)
        report["catalog_inventory"] = inventory
        report["namespaces"] = [list(n) for n in cat.list_namespaces()]
        check("target_absent", "silver.faostat_production" not in inventory)
        # Existing target is a strict stop, even if its schema happens to match.
        report["table_before"] = inventory.get("silver.faostat_production")
        table = cat.load_table("bronze.faostat_production")
        check("bronze_snapshot", table.metadata.current_snapshot_id == approved["snapshot"] == SNAPSHOT)
        report["bronze_snapshot"] = table.metadata.current_snapshot_id
        report["bronze_schema"] = table.schema().model_dump(mode="json")
        expected_schema = dict(approved["inventory"]["schema"][-1])
        # REST may omit this optional empty default; compare the same schema
        # representation without relaxing IDs, types, required flags or fields.
        expected_schema.setdefault("identifier-field-ids", [])
        report["schema_comparison_normalization"] = "Only omitted identifier-field-ids is canonicalized to []"
        check("bronze_schema", report["bronze_schema"] == expected_schema)
        report["bronze_metadata_location"] = table.metadata_location
        report["bronze_data_files"] = data_files(table, SNAPSHOT)
        frame = table.scan(snapshot_id=SNAPSHOT).to_arrow().to_pandas()
        report["bronze_rows"] = len(frame)
        check("bronze_rows", len(frame) == approved["preview"]["bronze_count"] == 22738)
        identity_cols = ["_source_file", "_source_checksum", "_source_snapshot_id", "_ingestion_run_id"]
        identities = frame.groupby(identity_cols, dropna=False).size().reset_index(name="rows").to_dict("records")
        report["source_identity"] = identities
        check("source_identity", identities == approved["inventory"]["files"])
        lineage = ["_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
                   "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id"]
        report["lineage_identity"] = frame[lineage].drop_duplicates().to_dict("records")
        check("lineage_complete", not frame[lineage].isna().any().any())
        check("source_id", set(frame["_source_id"]) == {SOURCE})
        local = yaml.safe_load(Path("contracts/silver/faostat_production.yaml").read_text(encoding="utf-8"))
        projection = {"dataset":local["dataset"], "status":local["contract_status"],
            "keys":local["grain"]["business_key"], "columns":local["schema"]["columns"], "rules":local["data_quality"]}
        check("contract_and_DQ_identical", projection == approved["contract"] and local["contract_status"] == "READY")
        report["contract_sha256"] = hashlib.sha256(Path("contracts/silver/faostat_production.yaml").read_bytes()).hexdigest()
        repo = metadata_repository()
        state = execution_state(repo)
        report["execution_state"] = state
        check("DAGs_paused", len(state["dags"]) == 2 and all(r[1] for r in state["dags"]))
        check("no_active_tasks", not state["active_tasks"])
        conn = repo._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT row_to_json(r) FROM ingestion.ingestion_runs r WHERE status NOT IN ('SUCCESS','SKIPPED','FAILED')")
                active = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT row_to_json(r) FROM ingestion.ingestion_runs r WHERE source_id=%s ORDER BY started_at", (SOURCE,))
                runs = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
        report["production_runs"] = runs
        report["active_ingestion_runs"] = active
        check("no_active_ingestion", not active)
        locks = {source:lock_rows(repo, source) for source in TRANSFORMERS}
        report["source_locks_before"] = locks
        check("no_active_source_locks", not any(locks.values()))
        check("real_run_id", len(identities) == 1 and any(r["run_id"] == identities[0]["_ingestion_run_id"] and r["status"] == "SUCCESS" for r in runs))
        storage = MinioStorage()
        raw = sorted(storage.list_objects("bronze", "raw/"), key=lambda o:o["Key"])
        report["raw_objects"] = raw
        manifest_objects = [o for o in raw if "raw/faostat_production/" in o["Key"] and o["Key"].endswith("manifest.json")]
        report["manifests"] = []
        for obj in manifest_objects:
            content = storage.s3_client.get_object(Bucket="bronze", Key=obj["Key"])["Body"].read()
            manifest = json.loads(content)
            report["manifests"].append({"uri":"s3://bronze/"+obj["Key"], "content":manifest})
        check("manifest_identity", report["manifests"] == approved["manifests"] and len(report["manifests"]) == 1)
        if len(report["manifests"]) == 1:
            artifact = report["manifests"][0]["content"]["artifact_info"]
            bucket, key = artifact["uri"].removeprefix("s3://").split("/", 1)
            body = storage.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
            report["raw_artifact"] = {"uri":artifact["uri"], "sha256":hashlib.sha256(body).hexdigest(), "bytes":len(body)}
            check("raw_artifact_checksum", report["raw_artifact"]["sha256"] == artifact["checksum"])
        sql = "SELECT count(*) AS rows, count(DISTINCT _source_checksum) AS checksums FROM iceberg.bronze.faostat_production"
        cols, rows = writer.execute_query(sql)
        report["trino_probe"] = {"sql":sql, "columns":cols, "rows":rows}
        check("trino_functional", rows == [[22738, 1]])
        response = requests.get(os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181") + "/v1/namespaces", timeout=15)
        check("REST_catalog", response.ok)
        if all(report["checks"].values()):
            with repo.source_lock(SOURCE):
                report["lock_probe"] = {"source":SOURCE, "signed_key":repo._derive_lock_key(SOURCE), "held":lock_rows(repo)}
                other = repo._get_connection()
                try:
                    with other.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_lock(%s)", (repo._derive_lock_key(SOURCE),))
                        acquired = cur.fetchone()[0]
                        if acquired:
                            cur.execute("SELECT pg_advisory_unlock(%s)", (repo._derive_lock_key(SOURCE),))
                finally:
                    other.close()
                check("source_lock_contention", not acquired and len(report["lock_probe"]["held"]) == 1 and report["lock_probe"]["held"][0][1])
            check("lock_released_after_probe", not lock_rows(repo))
        if all(report["checks"].values()):
            report["status"] = "NATIVE_PREFLIGHT_PASS_SPARK_PENDING"
    except Exception as exc:
        report["errors"].append(str(exc))
    Path(output).write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    print(json.dumps({"status":report["status"],"checks":report["checks"],"errors":report["errors"]}))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved",required=True)
    parser.add_argument("--output",required=True)
    args=parser.parse_args()
    report=audit(json.loads(Path(args.approved).read_text(encoding="utf-8")),args.output)
    if report["errors"]:
        raise SystemExit(1)
