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


def verify_release_result(record, repository):
    """A task result must match a persisted ingestion outcome, not a Bronze existence check."""
    source, run_id, checksum = (record.get(k) for k in ("source_id", "run_id", "checksum"))
    if source not in RELEASE_SOURCES or not run_id or not re.fullmatch(r"[0-9a-f]{64}", checksum or ""):
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


def silver_trigger_run_id(bronze_run_id):
    if not bronze_run_id or bronze_run_id.startswith("silver_v2_canary_"):
        raise AirflowException("Historical canary runs cannot trigger production Silver")
    return "silver_v2_bronze__" + hashlib.sha256(bronze_run_id.encode()).hexdigest()


def verify_silver_trigger_ready(bronze_run_id, session=None):
    """Read-only admission check; TriggerDagRunOperator also rejects races/duplicates."""
    from airflow.models import DagModel, DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session
    from contextlib import nullcontext

    run_id = silver_trigger_run_id(bronze_run_id)
    with create_session() if session is None else nullcontext(session) as db:
        target = "rice_lakehouse_silver_pipeline"
        model = db.query(DagModel).filter(DagModel.dag_id == target).one_or_none()
        serialized = db.query(SerializedDagModel).filter(SerializedDagModel.dag_id == target).one_or_none()
        if model is None or serialized is None or not model.is_active or model.is_paused or model.has_import_errors:
            raise AirflowException("Silver DAG unavailable/paused/import error; keep batch blocked, no trigger")
        dag = serialized.dag
        writers = [t for t in dag.tasks if t.task_id.startswith("transform_")]
        if (dag.schedule_interval is not None or {t.task_id.removeprefix("transform_") for t in writers} != set(RELEASE_SOURCES)
                or any(t.retries != 0 for t in writers) or dag.params.get("silver_release") != SILVER_RELEASE
                or dag.params.get("registry_sha256") != REGISTRY_SHA256):
            raise AirflowException("Silver serialized DAG routing/registry/scope/schedule/retries drift")
        if db.query(DagRun).filter(DagRun.dag_id == target, DagRun.run_id == run_id).first():
            raise AirflowException("Duplicate Silver trigger/run ID; inspect existing outcome, do not clear/reset")
        if db.query(DagRun).filter(DagRun.dag_id == target, DagRun.state.in_(["running", "queued"])).first():
            raise AirflowException("Silver already has an active run; inspect before starting another batch")
    return run_id


def run_ingestion_task(source_id: str, *, approved_release=None, **kwargs: Any) -> Dict[str, Any]:
    """Single entry point for Airflow to execute the ingestion pipeline.
    
    This replaces the legacy 8-stage manual orchestration tasks by delegating
    the entire application lifecycle (Readiness -> Extract -> Validate -> Write -> 
    Metadata -> Checkpoint/Watermark) to the IngestionEngine.
    
    Returns the summary dictionary of the ingestion result.
    Raises AirflowException to trigger Airflow retries on FAILED status.
    Raises AirflowSkipException on NOT_READY or SKIPPED status.
    """
    if approved_release:
        if approved_release != SILVER_RELEASE or source_id not in RELEASE_SOURCES:
            raise AirflowException("Source outside approved Silver release")
        validate_release_registry()
    engine = IngestionEngine.create_default()
    
    # Engine runs readiness checks, evaluates full vs incremental, handles locks,
    # and records commit/checkpoint outcomes. Bronze chunks are separate commits.
    result = engine.run_with_readiness_check(source_id, **kwargs)
    
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

