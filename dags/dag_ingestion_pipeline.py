"""Airflow DAG for Rice Lakehouse Bronze Ingestion Pipeline.

Orchestrates:
1. Metadata schema initialization (PostgreSQL schema `ingestion`).
2. Per-source pipeline:
   Readiness Check → Ingestion Engine → Result Validation
3. Separation of Concerns:
   - Orchestration, retries, and scheduling handled by Airflow.
   - Core ingestion, adapters, chunking, checkpointing, and storage handled by `ingestion` package.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.empty import EmptyOperator

# Ensure src/ is on sys.path for container execution
for p in ["/opt/airflow/src", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))]:
    if p not in sys.path and os.path.exists(p):
        sys.path.insert(0, p)

from ingestion.config.registry import SourceRegistry
from ingestion.core.enums import IngestionStatus
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.readiness.source_readiness import ReadinessChecker
from ingestion.storage.metadata_repository import MetadataRepository

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rice_lakehouse_ingestion",
    default_args=default_args,
    description="Ingestion Pipeline for Vietnam Rice Market Data Lakehouse",
    schedule="0 6 15 * *",  # 15th of each month at 6 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["ingestion", "bronze", "lakehouse"],
) as dag:

    @task(task_id="initialize_metadata")
    def initialize_metadata_task():
        """Initialize PostgreSQL ingestion metadata schema and tables."""
        try:
            repo = MetadataRepository()
            repo.initialize()
            logging.info("Metadata repository tables verified / initialized successfully.")
            return True
        except Exception as e:
            logging.error(f"Failed to initialize metadata repo: {e}")
            raise AirflowException(f"Metadata init failed: {e}")

    init_task = initialize_metadata_task()

    # Load registry
    registry = SourceRegistry()
    registry.load()
    all_sources = list(registry.get_all_sources().values())

    faostat_sources = [s for s in all_sources if "faostat" in s.source_id.lower()]
    other_sources = [s for s in all_sources if "faostat" not in s.source_id.lower()]

    start_faostat = EmptyOperator(task_id="start_faostat_sources")
    start_other = EmptyOperator(task_id="start_other_sources")
    end_all = EmptyOperator(task_id="end_ingestion")

    init_task >> [start_faostat, start_other]

    def build_source_subpipeline(source_config, upstream_marker):
        source_id = source_config.source_id

        @task(task_id=f"readiness_{source_id}")
        def check_source_readiness():
            checker = ReadinessChecker()
            res = checker.check(source_config)
            if not res.ready:
                logging.warning(f"Source {source_id} is not ready: {res.reason}")
                raise AirflowSkipException(f"Source not ready: {res.reason}")
            logging.info(f"Source {source_id} readiness check passed: {res.source_metadata}")
            return True

        @task(task_id=f"ingest_{source_id}", retries=0)  # Engine handles inner retry
        def execute_ingest(is_ready: bool = True):
            if is_ready is False:
                raise AirflowSkipException("Skipped due to unready upstream")
            engine = IngestionEngine.create_default()
            result = engine.run(source_id)
            return result.to_dict()

        @task(task_id=f"validate_{source_id}")
        def validate_result(res_dict: dict = None):
            if res_dict is None:
                repo = MetadataRepository()
                res_dict = repo.get_latest_run(source_id) or {"status": "SUCCESS"}
            status = res_dict.get("status")
            error_msg = res_dict.get("error_message", "")
            logging.info(f"Source {source_id} ingestion completed with status: {status}")

            if status == IngestionStatus.SUCCESS.value:
                records = res_dict.get("records_extracted", 0)
                artifact = res_dict.get("artifact_uri")
                logging.info(f"SUCCESS: {records} records extracted -> {artifact}")
                return True
            elif status == IngestionStatus.SKIPPED.value:
                reason = (res_dict.get("source_metadata") or {}).get("skipped_reason", "idempotent")
                logging.info(f"SKIPPED: {reason}")
                return True
            elif status == IngestionStatus.NOT_READY.value:
                raise AirflowSkipException(f"Source reported NOT_READY: {error_msg}")
            else:
                raise AirflowException(f"Ingestion FAILED for {source_id}: {error_msg}")

        t_ready = check_source_readiness()
        t_ingest = execute_ingest(t_ready)
        t_validate = validate_result(t_ingest)

        upstream_marker >> t_ready
        t_validate >> end_all
        return t_validate

    for s in faostat_sources:
        build_source_subpipeline(s, start_faostat)

    for s in other_sources:
        build_source_subpipeline(s, start_other)
