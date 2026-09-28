"""Enterprise Pipeline Tasks for Lakehouse Bronze Ingestion.

Implements the Airflow orchestration layer.
Delegates all application ingestion logic (extraction, validation, chunking,
idempotency, writing, watermarks) to the IngestionEngine.
"""

from __future__ import annotations

from typing import Any, Dict
import logging

from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.enums import IngestionStatus

try:
    from airflow.exceptions import AirflowException, AirflowSkipException
except ImportError:
    class AirflowException(Exception):
        pass

    class AirflowSkipException(Exception):
        pass


def run_ingestion_task(source_id: str, **kwargs: Any) -> Dict[str, Any]:
    """Single entry point for Airflow to execute the ingestion pipeline.
    
    This replaces the legacy 8-stage manual orchestration tasks by delegating
    the entire application lifecycle (Readiness -> Extract -> Validate -> Write -> 
    Metadata -> Checkpoint/Watermark) to the IngestionEngine.
    
    Returns the summary dictionary of the ingestion result.
    Raises AirflowException to trigger Airflow retries on FAILED status.
    Raises AirflowSkipException on NOT_READY or SKIPPED status.
    """
    engine = IngestionEngine.create_default()
    
    # Engine runs readiness checks, evaluates full vs incremental, handles locks,
    # and performs the entire ingestion lifecycle atomically.
    result = engine.run_with_readiness_check(source_id, **kwargs)
    
    if result.status == IngestionStatus.FAILED:
        # FAILED status propagates to Airflow to trigger standard retries.
        # Checkpoints ensure that a retry resumes from the failure point, rather than starting over.
        raise AirflowException(f"Ingestion failed for source {source_id}: {result.error_message}")
        
    if result.status == IngestionStatus.NOT_READY:
        # Clean skip, doesn't mark DAG as failed
        raise AirflowSkipException(f"Source {source_id} not ready: {result.error_message}")
        
    if result.status == IngestionStatus.SKIPPED:
        # Idempotent skip (e.g. source file hasn't changed since last run)
        reason = result.source_metadata.get("idempotency_reason", result.source_metadata.get("skipped_reason", "Unknown"))
        raise AirflowSkipException(f"Ingestion safely skipped for source {source_id}: {reason}")
        
    # SUCCESS or NO_NEW_DATA
    return result.to_dict()

