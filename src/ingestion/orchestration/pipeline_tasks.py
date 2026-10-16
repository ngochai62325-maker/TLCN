"""Enterprise Pipeline Tasks for Lakehouse Bronze Ingestion.

Implements the Airflow orchestration layer.
Delegates all application ingestion logic (extraction, validation, chunking,
idempotency, writing, watermarks) to the IngestionEngine.
"""

from __future__ import annotations

from typing import Any, Dict
import hashlib
import re
from pathlib import Path

from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.enums import IngestionStatus
from ingestion.config.registry import SourceRegistry
from ingestion.storage.metadata_repository import MetadataRepository

SILVER_RELEASE = "silver_v2_release_20261013"
RELEASE_SOURCES = ("faostat_production", "faostat_monthly_price", "faostat_supply_utilization",
                   "faostat_trade", "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan")
REGISTRY_SHA256 = "9f37db79754fd9669d0d8e1b621b4c7b2cbbe7371eb51c7ec55102bf79bacda6"

try:
    from airflow.exceptions import AirflowException, AirflowSkipException
except ImportError:
    class AirflowException(Exception):
        pass

    class AirflowSkipException(Exception):
        pass


def validate_release_registry(registry=None):
    """Fail closed on scope/config drift, both at DAG parse and before execution."""
    registry = registry or SourceRegistry()
    registry.load()
    sources = registry.get_all_sources()
    if any(source not in sources or not sources[source].enabled or sources[source].source_id != source
           for source in RELEASE_SOURCES):
        raise AirflowException("Approved release registry has missing, disabled or unexpected source IDs")
    if hashlib.sha256(Path(registry.registry_path).read_bytes()).hexdigest() != REGISTRY_SHA256:
        raise AirflowException("Approved release registry checksum drift; owner review required")
    return [sources[source] for source in RELEASE_SOURCES]


def verify_release_result(record, repository, *, profile=None):
    """A task result must match a persisted ingestion outcome, not a Bronze existence check."""
    source, run_id, checksum = (record.get(k) for k in ("source_id", "run_id", "checksum"))
    sources = RELEASE_SOURCES
    if profile:
        from silver.local_profile import BRONZE_SOURCES, require_profile
        require_profile(profile)
        sources = BRONZE_SOURCES
    if source not in sources or not run_id or not re.fullmatch(r"[0-9a-f]{64}", checksum or ""):
        raise AirflowException("Invalid release ingestion identity/checksum")
    status = record.get("status")
    if status not in ("SUCCESS", "SKIPPED"):
        raise AirflowException(f"Release source {source} did not complete: {status}")
    stored = repository.get_run(run_id)
    if not stored or any(stored.get(k) != record.get(k) for k in ("source_id", "status", "checksum")):
        raise AirflowException(f"Persisted ingestion outcome mismatch for {source}; no automatic retry")
    if status == "SUCCESS":
        if not record.get("records_extracted") or stored.get("records_extracted") != record["records_extracted"]:
            raise AirflowException(f"Release source {source} has no verified extracted records")
    else:
        if (record.get("source_metadata") or {}).get("skipped_reason") != "unchanged_snapshot":
            raise AirflowException(f"Unverified SKIPPED reason for {source}")
        snapshot = repository.find_snapshot_by_checksum(source, checksum)
        previous = repository.get_run(snapshot["run_id"]) if snapshot and snapshot.get("run_id") else None
        if not previous or previous.get("source_id") != source or previous.get("status") != "SUCCESS" or previous.get("checksum") != checksum or not previous.get("records_extracted"):
            raise AirflowException(f"Checksum SKIPPED lacks successful ingestion evidence for {source}")
    return record


def verify_bronze_batch(results):
    validate_release_registry()
    if len(results) != len(RELEASE_SOURCES) or {r.get("source_id") for r in results} != set(RELEASE_SOURCES):
        raise AirflowException("Bronze batch must contain exactly the eight approved source IDs once")
    repository = MetadataRepository()
    for record in results:
        if record.get("registry_sha256") != REGISTRY_SHA256:
            raise AirflowException("Bronze task registry identity drift")
        verify_release_result(record, repository)
    return {"status": "PASSED", "release": SILVER_RELEASE,
            "source_runs": {r["source_id"]: r["run_id"] for r in results}}


def silver_trigger_run_id(bronze_run_id, profile=None):
    if profile:
        from silver.local_profile import require_profile
        require_profile(profile)
        if not isinstance(bronze_run_id, str) or not bronze_run_id:
            raise AirflowException("Missing local Bronze batch identity")
        return "local_nine_bronze__" + hashlib.sha256(bronze_run_id.encode()).hexdigest()
    if not bronze_run_id or bronze_run_id.startswith("silver_v2_canary_"):
        raise AirflowException("Historical canary runs cannot trigger production Silver")
    return "silver_v2_bronze__" + hashlib.sha256(bronze_run_id.encode()).hexdigest()


def verify_silver_trigger_ready(bronze_run_id, session=None, profile=None):
    """Read-only admission check; TriggerDagRunOperator also rejects races/duplicates."""
    from airflow.models import DagModel, DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session
    from contextlib import nullcontext

    run_id = silver_trigger_run_id(bronze_run_id, profile=profile)
    with create_session() if session is None else nullcontext(session) as db:
        target = "rice_lakehouse_silver_pipeline"
        model = db.query(DagModel).filter(DagModel.dag_id == target).one_or_none()
        serialized = db.query(SerializedDagModel).filter(SerializedDagModel.dag_id == target).one_or_none()
        if model is None or serialized is None or not model.is_active or model.is_paused or model.has_import_errors:
            raise AirflowException("Silver DAG unavailable/paused/import error; keep batch blocked, no trigger")
        dag = serialized.dag
        writers = [t for t in dag.tasks if t.task_id.startswith("transform_")]
        expected = RELEASE_SOURCES
        routing_valid = dag.params.get("silver_release") == SILVER_RELEASE
        if profile:
            from silver.local_profile import DATASETS, NAMESPACE
            expected = DATASETS
            routing_valid = (dag.params.get("local_profile") == profile and dag.params.get("silver_release") is None
                             and dag.params.get("silver_namespace") == NAMESPACE)
        if (dag.schedule_interval is not None or {t.task_id.removeprefix("transform_") for t in writers} != set(expected)
                or any(t.retries != 0 for t in writers) or not routing_valid
                or dag.params.get("registry_sha256") != REGISTRY_SHA256):
            raise AirflowException("Silver serialized DAG routing/registry/scope/schedule/retries drift")
        if db.query(DagRun).filter(DagRun.dag_id == target, DagRun.run_id == run_id).first():
            raise AirflowException("Duplicate Silver trigger/run ID; inspect existing outcome, do not clear/reset")
        if db.query(DagRun).filter(DagRun.dag_id == target, DagRun.state.in_(["running", "queued"])).first():
            raise AirflowException("Silver already has an active run; inspect before starting another batch")
    return run_id


def run_ingestion_task(source_id: str, *, approved_release=None, profile=None, **kwargs: Any) -> Dict[str, Any]:
    """Single entry point for Airflow to execute the ingestion pipeline.
    
    This replaces the legacy 8-stage manual orchestration tasks by delegating
    the entire application lifecycle (Readiness -> Extract -> Validate -> Write -> 
    Metadata -> Checkpoint/Watermark) to the IngestionEngine.
    
    Returns the summary dictionary of the ingestion result.
    Raises AirflowException to trigger Airflow retries on FAILED status.
    Raises AirflowSkipException on NOT_READY or SKIPPED status.
    """
    if profile:
        from silver.local_profile import BRONZE_SOURCES, require_profile, validate_registry
        require_profile(profile, approved_release)
        validate_registry()
        if source_id not in BRONZE_SOURCES or kwargs.get("force_reprocess", False):
            raise AirflowException("local_nine rejects source outside scope or forced reprocessing")
    if approved_release:
        if approved_release != SILVER_RELEASE or source_id not in RELEASE_SOURCES:
            raise AirflowException("Source outside approved Silver release")
        validate_release_registry()
    engine = IngestionEngine.create_default()
    
    # Engine runs readiness checks, evaluates full vs incremental, handles locks,
    # and records commit/checkpoint outcomes. Bronze chunks are separate commits.
    result = engine.run_with_readiness_check(source_id, **kwargs)

    if profile:
        record = result.to_dict()
        if record.get("source_id") != source_id:
            raise AirflowException("Engine returned an unexpected local source identity")
        verify_release_result(record, engine.metadata_repo, profile=profile)
        proof = local_bronze_proof(record, engine.metadata_repo, engine.bronze_writer)
        return {**record, "registry_sha256": REGISTRY_SHA256, "bronze_identity": proof}
    
    if result.status == IngestionStatus.FAILED:
        # FAILED status propagates to Airflow to trigger standard retries.
        # Checkpoints ensure that a retry resumes from the failure point, rather than starting over.
        raise AirflowException(f"Ingestion failed for source {source_id}: {result.error_message}")
        
    if result.status == IngestionStatus.NOT_READY:
        if approved_release:
            raise AirflowException(f"Release source {source_id} NOT_READY; batch blocked: {result.error_message}")
        # Clean skip, doesn't mark DAG as failed
        raise AirflowSkipException(f"Source {source_id} not ready: {result.error_message}")
        
    if result.status == IngestionStatus.SKIPPED:
        if approved_release:
            record = result.to_dict()
            if record.get("source_id") != source_id:
                raise AirflowException("Engine returned an unexpected source identity")
            verify_release_result(record, engine.metadata_repo)
            return {**record, "registry_sha256": REGISTRY_SHA256}
        # Idempotent skip (e.g. source file hasn't changed since last run)
        reason = result.source_metadata.get("idempotency_reason", result.source_metadata.get("skipped_reason", "Unknown"))
        raise AirflowSkipException(f"Ingestion safely skipped for source {source_id}: {reason}")
        
    # SUCCESS or NO_NEW_DATA
    record = result.to_dict()
    if approved_release:
        if record.get("source_id") != source_id:
            raise AirflowException("Engine returned an unexpected source identity")
        verify_release_result(record, engine.metadata_repo)
        record["registry_sha256"] = REGISTRY_SHA256
    return record


def local_bronze_proof(record, repository, writer=None):
    """Pin and physically verify the committed run; metadata SUCCESS alone is insufficient."""
    from silver.local_profile import PROFILE
    from ingestion.storage.bronze_writer import BronzeIcebergWriter
    from pyiceberg.expressions import EqualTo
    import pyarrow.compute as pc

    verify_release_result(record, repository, profile=PROFILE)
    source, checksum = record["source_id"], record["checksum"]
    proved_run = record["run_id"]
    if record["status"] == "SKIPPED":
        proved_run = repository.find_snapshot_by_checksum(source, checksum)["run_id"]
    prior = repository.get_run(proved_run)
    with repository.source_lock(source):
        writer = writer or BronzeIcebergWriter()
        catalog = writer.get_iceberg_catalog()
        table = catalog.load_table("bronze." + source)
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise AirflowException("No committed local Bronze snapshot")
        rows = table.scan(snapshot_id=snapshot.snapshot_id, row_filter=EqualTo("_ingestion_run_id", proved_run)).select("_source_id").to_arrow()
        if rows.num_rows != prior["records_extracted"] or set(pc.unique(rows["_source_id"]).to_pylist()) != {source}:
            raise AirflowException("Local Bronze commit/readback does not match persisted ingestion")
        if catalog.load_table("bronze." + source).metadata_location != table.metadata_location:
            raise AirflowException("Local Bronze changed during proof; no automatic retry")
        return {"table": "iceberg.bronze." + source, "uuid": str(table.metadata.table_uuid),
                "snapshot_id": str(snapshot.snapshot_id), "schema_id": table.metadata.current_schema_id,
                "metadata_location": table.metadata_location, "rows": rows.num_rows,
                "proved_run_id": proved_run, "checksum": checksum}


def verify_local_bronze_batch(results):
    from silver.local_profile import PROFILE, BRONZE_SOURCES, validate_registry
    validate_registry()
    if len(results) != 9 or {r.get("source_id") for r in results} != set(BRONZE_SOURCES):
        raise AirflowException("Local batch must contain exactly nine source IDs once")
    repository = MetadataRepository()
    for record in results:
        if record.get("registry_sha256") != REGISTRY_SHA256:
            raise AirflowException("Local registry identity drift")
        proof = local_bronze_proof(record, repository)
        if record.get("bronze_identity") != proof:
            raise AirflowException("Local Bronze snapshot/commit identity drift after task completion")
    return {"status": "PASSED", "profile": PROFILE, "registry_sha256": REGISTRY_SHA256,
            "results": results, "bronze_inputs": {r["source_id"]: r["bronze_identity"] for r in results}}


def verify_local_silver_origin(conf, silver_run_id):
    """Reject independent/manual Silver invocations and fabricated parent batch evidence."""
    from silver.local_profile import PROFILE, require_profile
    from airflow.models import DagRun, TaskInstance, XCom
    from airflow.utils.session import create_session
    require_profile(conf.get("profile"), conf.get("release"))
    batch = conf.get("bronze_batch") or {}
    parent = batch.get("bronze_dag_run_id")
    if silver_trigger_run_id(parent, profile=PROFILE) != silver_run_id:
        raise AirflowException("Silver must originate from its deterministic local Bronze batch")
    with create_session() as db:
        run = db.query(DagRun).filter_by(dag_id="rice_lakehouse_ingestion", run_id=parent).one_or_none()
        gate = db.query(TaskInstance).filter_by(dag_id="rice_lakehouse_ingestion", run_id=parent, task_id="verify_bronze_batch").one_or_none()
        stored = XCom.get_one(dag_id="rice_lakehouse_ingestion", run_id=parent,
                              task_id="verify_bronze_batch", key="return_value", session=db)
        if run is None or run.state != "running" or gate is None or gate.state != "success" or stored != batch:
            raise AirflowException("Missing successful persisted parent Bronze gate; batch blocked")
    actual = verify_local_bronze_batch(batch.get("results", []))
    if actual["bronze_inputs"] != batch.get("bronze_inputs"):
        raise AirflowException("Local Silver input proof drift")
    return batch

