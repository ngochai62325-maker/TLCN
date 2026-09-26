"""Airflow DAG for Vietnam Rice Market Data Lakehouse Bronze Ingestion.

Implements the strict enterprise 8-stage sequential fail-safe dependency graph:
    check_source 
         ↓ 
    readiness_check (Sensor/Gate) 
         ↓ 
    extract (Person 1's module) 
         ↓ 
    pre_audit (Person 2's validation) 
         ↓ 
    write_bronze (Person 2's writer) 
         ↓ 
    post_audit (Reconciliation & sanity checks) 
         ↓ 
    update_metadata (Watermark, audit catalog) 
         ↓ 
    publish (Signal downstream layers)
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
from airflow.utils.task_group import TaskGroup

# Ensure src/ is on sys.path for container execution
for p in ["/opt/airflow/src", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))]:
    if p not in sys.path and os.path.exists(p):
        sys.path.insert(0, p)

from ingestion.config.registry import SourceRegistry
from ingestion.orchestration.pipeline_tasks import (
    check_source,
    extract,
    post_audit,
    pre_audit,
    publish,
    readiness_check,
    update_metadata,
    write_bronze,
)
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
    description="Enterprise 8-Stage Fail-Safe Ingestion Pipeline for Vietnam Rice Lakehouse",
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

    # Build subpipeline function implementing the strict 8-step lifecycle
    def create_source_pipeline(source_id: str):
        with TaskGroup(group_id=f"pipeline_{source_id}", tooltip=f"8-Stage Lifecycle for {source_id}") as tg:

            @task(task_id="check_source")
            def t_check():
                return check_source(source_id)

            @task(task_id="readiness_check")
            def t_ready(source_ctx: Dict[str, Any]):
                return readiness_check(source_ctx)

            @task(task_id="extract", retries=2, retry_delay=timedelta(seconds=10))
            def t_extract(source_ctx: Dict[str, Any]):
                return extract(source_ctx)

            @task(task_id="pre_audit")
            def t_pre_audit(source_ctx: Dict[str, Any], extract_ctx: Dict[str, Any]):
                return pre_audit(source_ctx, extract_ctx)

            @task(task_id="write_bronze", retries=2, retry_delay=timedelta(seconds=15))
            def t_write(
                source_ctx: Dict[str, Any],
                extract_ctx: Dict[str, Any],
                pre_audit_ctx: Dict[str, Any],
            ):
                return write_bronze(source_ctx, extract_ctx, pre_audit_ctx)

            @task(task_id="post_audit")
            def t_post_audit(
                source_ctx: Dict[str, Any],
                extract_ctx: Dict[str, Any],
                write_ctx: Dict[str, Any],
            ):
                return post_audit(source_ctx, extract_ctx, write_ctx)

            @task(task_id="update_metadata")
            def t_meta(
                source_ctx: Dict[str, Any],
                extract_ctx: Dict[str, Any],
                write_ctx: Dict[str, Any],
                post_audit_ctx: Dict[str, Any],
            ):
                return update_metadata(source_ctx, extract_ctx, write_ctx, post_audit_ctx)

            @task(task_id="publish")
            def t_pub(
                source_ctx: Dict[str, Any],
                write_ctx: Dict[str, Any],
                post_audit_ctx: Dict[str, Any],
            ):
                return publish(source_ctx, write_ctx, post_audit_ctx)

            # Strict sequential fail-safe task dependency chain
            ctx_check = t_check()
            ctx_ready = t_ready(ctx_check)
            ctx_extract = t_extract(ctx_check)
            ctx_pre_audit = t_pre_audit(ctx_check, ctx_extract)
            ctx_write = t_write(ctx_check, ctx_extract, ctx_pre_audit)
            ctx_post_audit = t_post_audit(ctx_check, ctx_extract, ctx_write)
            ctx_meta = t_meta(ctx_check, ctx_extract, ctx_write, ctx_post_audit)
            ctx_pub = t_pub(ctx_check, ctx_write, ctx_post_audit)

            # Enforce execution ordering:
            # check_source -> readiness_check -> extract -> pre_audit -> write_bronze -> post_audit -> update_metadata -> publish
            ctx_ready >> ctx_extract
            ctx_meta >> ctx_pub

        return tg

    # Connect all source TaskGroups between start_all and end_all markers
    for src in all_sources:
        # Include enabled sources
        if src.enabled:
            source_tg = create_source_pipeline(src.source_id)
            start_all >> source_tg >> end_all
