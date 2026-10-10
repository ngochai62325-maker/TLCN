"""Airflow DAG for Vietnam Rice Market Data Lakehouse Bronze Ingestion.

Implements the Airflow production DAG.
Delegates all application logic to IngestionEngine via the `run_ingestion_task`.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

# Ensure src/ is on sys.path for container execution
for p in ["/opt/airflow/src", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))]:
    if p not in sys.path and os.path.exists(p):
        sys.path.insert(0, p)

from ingestion.config.registry import SourceRegistry
from ingestion.orchestration.pipeline_tasks import run_ingestion_task
from ingestion.storage.metadata_repository import MetadataRepository

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="rice_lakehouse_ingestion",
    default_args=default_args,
    description="Orchestrates the Rice Lakehouse Bronze Ingestion using IngestionEngine",
    schedule="0 6 15 * *",  # 15th of each month at 6 AM
    start_date=datetime(2026, 9, 26),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["ingestion", "bronze", "lakehouse"],
) as dag:

    @task(task_id="initialize_metadata")
    def initialize_metadata_task():
        """Initialize PostgreSQL ingestion metadata schema and tables, and Iceberg schema."""
        repo = MetadataRepository()
        repo.initialize()

        # Pre-create Iceberg Bronze schema to prevent race conditions during parallel source ingestion
        try:
            from ingestion.storage.bronze_writer import BronzeIcebergWriter
            writer = BronzeIcebergWriter()
            writer.ensure_schema()
        except Exception as e:
            logging.warning(f"Could not pre-initialize Iceberg schema: {e}")

        logging.info("PostgreSQL metadata repository and Iceberg schema verified/initialized.")
        return True

    init_task = initialize_metadata_task()

    start_all = EmptyOperator(task_id="start_lakehouse_ingestion")
    end_all = EmptyOperator(task_id="end_lakehouse_ingestion")

    init_task >> start_all

    # Load active sources from registry
    registry = SourceRegistry()
    registry.load()
    all_sources = list(registry.get_all_sources().values())

    @task(task_id="run_ingestion", retries=2, retry_delay=timedelta(seconds=30), multiple_outputs=True)
    def build_run_ingestion_task(source_id: str, **context) -> Dict[str, Any]:
        """Airflow thin wrapper executing the IngestionEngine."""
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        force = conf.get("force_reprocess", False)
        forced_sources = conf.get("force_reprocess_sources", [])
        if source_id in forced_sources:
            force = True
        return run_ingestion_task(source_id, force_reprocess=force)

    @task(task_id="run_silver", retries=2, retry_delay=timedelta(minutes=1), trigger_rule="none_failed")
    def build_run_silver_task(dataset_id: str):
        """Executes the Silver Transformation Framework using a local PySpark session connected to spark-iceberg."""
        from ingestion.storage.metadata_repository import MetadataRepository
        from airflow.exceptions import AirflowSkipException
        from dag_silver_pipeline import trigger_spark_job
        repo = MetadataRepository()
        with repo.source_lock(dataset_id):
            pending = repo.get_pending_silver_run(dataset_id)
            if not pending:
                raise AirflowSkipException(f"No pending Bronze run for {dataset_id}")
            run_id = pending["run_id"]
            result = trigger_spark_job("run_all_silver.py", "incremental", run_id, dataset_id, run_id)
            if result.get("status") != "SUCCESS":
                raise RuntimeError(f"Silver did not validate {dataset_id}")
            repo.mark_silver_status(run_id, "SUCCESS")
            return result

    # Connect all source tasks between start_all and end_all markers
    for src in all_sources:
        if src.enabled and src.extra.get("dataset_role") != "TEST_FIXTURE_ONLY":
            ingest_task = build_run_ingestion_task.override(task_id=f"ingest_{src.source_id}")(src.source_id)
            
            # Silver uses the same job/framework as the standalone Silver DAG.
            silver_task = build_run_silver_task.override(task_id=f"silver_{src.source_id}")(
                dataset_id=src.source_id
            )
            start_all >> ingest_task >> silver_task >> end_all

