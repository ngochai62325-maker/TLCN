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
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Ensure src/ is on sys.path for container execution
for p in ["/opt/airflow/src", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))]:
    if p not in sys.path and os.path.exists(p):
        sys.path.insert(0, p)

from ingestion.config.registry import SourceRegistry
from ingestion.orchestration.pipeline_tasks import (
    run_ingestion_task, validate_release_registry, verify_bronze_batch,
    verify_silver_trigger_ready, silver_trigger_run_id, SILVER_RELEASE, RELEASE_SOURCES,
)
from ingestion.storage.metadata_repository import MetadataRepository
from silver.local_profile import active_profile, validate_registry as validate_local_registry, BRONZE_SOURCES
from ingestion.orchestration.pipeline_tasks import verify_local_bronze_batch

LOCAL_PROFILE = active_profile()
RELEASE = None if LOCAL_PROFILE else os.environ.get("SILVER_ACCEPTED_RELEASE")
CANARY_ONLY = os.environ.get("SILVER_CANARY_ONLY", "0")
PRODUCTION_ENABLED = os.environ.get("SILVER_PRODUCTION_ENABLED", "0")
if RELEASE not in (None, SILVER_RELEASE):
    raise ValueError("Unapproved ingestion release scope")
if CANARY_ONLY not in ("0", "1") or (CANARY_ONLY == "1" and RELEASE != SILVER_RELEASE):
    raise ValueError("Manual canary requires the approved V2 release and SILVER_CANARY_ONLY=1")
if PRODUCTION_ENABLED not in ("0", "1") or (PRODUCTION_ENABLED == "1" and (not RELEASE or CANARY_ONLY != "0")):
    raise ValueError("Production orchestration requires approved release and canary disabled")
production = PRODUCTION_ENABLED == "1"
schedule = (os.environ.get("SILVER_BRONZE_SCHEDULE") or None) if production else None


def check_silver_before_trigger(context):
    # Recheck at execution, since the target can pause/change after the batch gate.
    verify_silver_trigger_ready(context["run_id"], **({"profile": LOCAL_PROFILE} if LOCAL_PROFILE else {}))


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0 if RELEASE or LOCAL_PROFILE else 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="rice_lakehouse_ingestion",
    default_args=default_args,
    description="Orchestrates the Rice Lakehouse Bronze Ingestion using IngestionEngine",
    schedule=schedule,  # No release schedule until the owner supplies one and enables production.
    start_date=pendulum.datetime(2026, 9, 26, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1 if LOCAL_PROFILE else 4,
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
    # Apply only at owner-approved V2 cutover; default Bronze routing stays intact.
    if RELEASE:
        all_sources = validate_release_registry(registry)
    elif LOCAL_PROFILE:
        all_sources = validate_local_registry(registry)

    @task(task_id="run_ingestion", retries=0 if RELEASE or LOCAL_PROFILE else 2, retry_delay=timedelta(seconds=30), multiple_outputs=True)
    def build_run_ingestion_task(source_id: str, **context) -> Dict[str, Any]:
        """Airflow thin wrapper executing the IngestionEngine."""
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        conf = conf or {}
        if LOCAL_PROFILE:
            if conf.get("profile", LOCAL_PROFILE) != LOCAL_PROFILE or conf.get("release"):
                raise ValueError("Local Bronze profile/release conf mismatch")
            if conf.get("force_reprocess", False) is not False or conf.get("force_reprocess_sources"):
                raise ValueError("local_nine does not force reprocess existing publications")
            return run_ingestion_task(source_id, force_reprocess=False, profile=LOCAL_PROFILE)
        if RELEASE:
            validate_release_registry()
            if conf.get("release", RELEASE) != RELEASE:
                raise ValueError("Unapproved ingestion release conf")
        if production:
            silver_trigger_run_id(context["run_id"])  # Reject historical canaries before any ingestion.
        force = conf.get("force_reprocess", False)
        forced_sources = conf.get("force_reprocess_sources", [])
        if RELEASE and (not isinstance(forced_sources, list) or set(forced_sources) - set(RELEASE_SOURCES)):
            raise ValueError("Unexpected source IDs in release ingestion conf")
        if source_id in forced_sources:
            force = True
        return run_ingestion_task(source_id, force_reprocess=force,
                                  **({"approved_release": RELEASE} if production else {}))

    # Connect all source tasks between start_all and end_all markers
    ingestions = []
    for src in all_sources:
        if src.enabled:
            ingest_task = build_run_ingestion_task.override(task_id=f"ingest_{src.source_id}")(src.source_id)
            
            start_all >> ingest_task
            ingestions.append(ingest_task)
            if not production and not LOCAL_PROFILE:
                ingest_task >> end_all

    if production or LOCAL_PROFILE:
        if LOCAL_PROFILE:
            for previous, following in zip(ingestions, ingestions[1:]):
                previous >> following  # Stop the local batch on a failed/unknown writer.
        @task(task_id="verify_bronze_batch")
        def batch_gate(results, **context):
            verified = verify_local_bronze_batch(results) if LOCAL_PROFILE else verify_bronze_batch(results)
            if LOCAL_PROFILE:
                verified["bronze_dag_run_id"] = context["run_id"]
            verified["silver_run_id"] = verify_silver_trigger_ready(context["run_id"], **({"profile": LOCAL_PROFILE} if LOCAL_PROFILE else {}))
            return verified

        verified = batch_gate(ingestions)
        trigger = TriggerDagRunOperator(
            task_id="trigger_silver_release", trigger_dag_id="rice_lakehouse_silver_pipeline",
            trigger_run_id="{{ ti.xcom_pull(task_ids='verify_bronze_batch')['silver_run_id'] }}",
            conf=({"mode": "full", "profile": LOCAL_PROFILE, "bronze_batch": verified} if LOCAL_PROFILE
                  else {"mode": "full", "release": RELEASE}), reset_dag_run=False,
            wait_for_completion=True, deferrable=False, retries=0,
            pre_execute=check_silver_before_trigger,
            execution_timeout=timedelta(hours=2), poke_interval=30,
        )
        verified >> trigger >> end_all

