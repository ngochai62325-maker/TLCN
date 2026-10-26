"""Admission from persisted Airflow success proof; no synthetic snapshot identities."""
import json
import os
from contextlib import contextmanager, ExitStack

from gold import MODEL_VERSION
from gold.keys import fingerprint
from gold.spec import ROOT, validate_mapping
from silver.local_profile import DATASETS, MAPPINGS, require_profile, validate_registry


@contextmanager
def writer_guard():
    """Reuse the exact existing source lock protocol; one Gold writer across APIs/CLI."""
    from ingestion.storage.metadata_repository import MetadataRepository
    if not os.environ.get("INGESTION_DB_URL"):
        raise ValueError("Gold requires explicit INGESTION_DB_URL")
    repo = MetadataRepository()
    conn = repo._get_connection()
    lock_key = repo._derive_lock_key("tlcn-local:gold_local")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another Gold writer owns the advisory lock")
        def assert_alive():
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("Gold writer lock connection lost")
        with ExitStack() as stack:
            for source in sorted(DATASETS):
                stack.enter_context(repo.source_lock(source))
            yield assert_alive
    finally:
        conn.close()  # Session advisory lock is released, including on failure.


def _read_xcom(cursor, run_id, task_id):
    cursor.execute("SELECT value FROM xcom WHERE dag_id=%s AND run_id=%s AND task_id=%s AND key='return_value' AND map_index=-1", ("rice_lakehouse_silver_pipeline", run_id, task_id))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError(f"Missing/ambiguous persisted Silver proof: {task_id}")
    value = rows[0][0]
    if isinstance(value, memoryview):
        value = value.tobytes()
    return json.loads(value)


def success_context(silver_run_id=None):
    """Read Airflow metadata with psycopg2, available in both existing images."""
    from ingestion.storage.metadata_repository import MetadataRepository
    require_profile("local_nine")
    validate_registry()
    validate_mapping()
    repo = MetadataRepository()
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dag_id,run_id,state FROM dag_run WHERE dag_id IN ('rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline') AND state IN ('running','queued')")
            if cur.fetchall():
                raise ValueError("Active Bronze/Silver run: Gold admission blocked")
            latest = {}
            for dag in ("rice_lakehouse_ingestion", "rice_lakehouse_silver_pipeline"):
                cur.execute("SELECT run_id,state FROM dag_run WHERE dag_id=%s ORDER BY execution_date DESC LIMIT 1", (dag,))
                row = cur.fetchone()
                if row is None or row[1] != "success":
                    raise ValueError(f"Latest {dag} outcome is not SUCCESS; review before Gold")
                latest[dag] = row[0]
            run_id = silver_run_id or latest["rice_lakehouse_silver_pipeline"]
            if run_id != latest["rice_lakehouse_silver_pipeline"]:
                raise ValueError("Gold admits the latest proven Silver batch only")
            signal = _read_xcom(cur, run_id, "publish_silver_signal")
            if signal.get("silver_status") != "DQ_PASSED" or signal.get("profile") != "local_nine" or set(signal.get("datasets", [])) != set(DATASETS):
                raise ValueError("Incomplete Silver success signal")
            audit = _read_xcom(cur, run_id, "silver_data_quality_audit")
            if len(audit) != 9 or any(a.get("status") != "PASSED" for a in audit):
                raise ValueError("Persisted Silver DQ did not pass all sources")
            proofs, pins = {}, {}
            for source, dataset, target in MAPPINGS:
                cur.execute("SELECT state FROM task_instance WHERE dag_id=%s AND run_id=%s AND task_id=%s AND map_index=-1", ("rice_lakehouse_silver_pipeline", run_id, "transform_" + dataset))
                row = cur.fetchone()
                if row is None or row[0] != "success":
                    raise ValueError(f"Silver task not successful: {source}")
                proof = _read_xcom(cur, run_id, "transform_" + dataset)[source]
                commit = proof.get("commit_verified", {})
                if (proof.get("status") != "SUCCESS" or proof.get("pipeline_run_id") != run_id
                        or proof.get("target_table") != f"iceberg.silver_local.{target}"
                        or not commit.get("uuid") or not str(commit.get("snapshot_id", "")).isdigit()
                        or int(commit["snapshot_id"]) <= 0):
                    raise ValueError(f"Incomplete physical Silver proof: {source}")
                proofs[source] = proof
                pins[source] = {"table": f"iceberg.silver_local.{target}", "uuid": commit["uuid"], "snapshot_id": str(commit["snapshot_id"])}
    return {"silver_run_id": run_id, "pins": pins, "proofs": proofs}


def code_fingerprint():
    paths = sorted([*(ROOT / "src/gold").glob("*.py"), *(ROOT / "jobs/gold").glob("*.py"), *(ROOT / "queries/gold").glob("*.sql"), ROOT / "dags/dag_gold_pipeline.py", ROOT / "jobs/silver/spark_api_server.py"])
    return fingerprint({str(p.relative_to(ROOT)).replace("\\", "/"): __import__("hashlib").sha256(p.read_bytes()).hexdigest() for p in paths})


def batch_context(proof):
    import hashlib
    refs = [ROOT / "config/ingestion/source_registry.yaml", ROOT / "docs/gold/dim_fact_v3.dbml", ROOT / "docs/gold/source_to_gold_mapping.csv", *(ROOT / "contracts/silver").glob("*.yaml")]
    return {"model": MODEL_VERSION, "code_fingerprint": code_fingerprint(), "reference_fingerprints": {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(refs)}, "source_scope": list(DATASETS), "inputs": proof["pins"]}


def runtime_resources():
    """Actual WSL headroom; runner is capped at 3 GiB by canonical Compose."""
    from pathlib import Path
    if not Path("/proc/meminfo").exists():
        raise RuntimeError("Run Gold only in the existing Linux runner")
    memory = {k: int(v.strip().split()[0]) for k, v in (line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())}
    available = memory["MemAvailable"]
    if available < 3 * 1024 * 1024:
        raise RuntimeError(f"Gold resource admission blocked: WSL MemAvailable={available} KiB (<3 GiB)")
    return {"wsl_available_kib": available}
