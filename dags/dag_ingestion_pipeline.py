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

    @task(task_id="run_ingestion", retries=2, retry_delay=timedelta(seconds=30))
    def build_run_ingestion_task(source_id: str):
        """Airflow thin wrapper executing the IngestionEngine."""
        return run_ingestion_task(source_id)

    @task(task_id="run_silver", retries=2, retry_delay=timedelta(minutes=1))
    def build_run_silver_task(dataset_id: str, run_id: str):
        """Executes the Silver Transformation Framework using a local PySpark session connected to spark-iceberg."""
        from pyspark.sql import SparkSession
        from silver.framework import SilverTransformationFramework

        # Initialize SparkSession connecting to the Spark cluster Master
        spark = SparkSession.builder \
            .appName(f"silver-{dataset_id}") \
            .master("spark://spark-iceberg:7077") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "rest") \
            .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password123") \
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()
        
        try:
            logging.info(f"Starting Silver Transformation for {dataset_id}, Bronze Run ID: {run_id}")
            framework = SilverTransformationFramework(spark, contract_dir="/opt/airflow/contracts/silver")
            result = framework.run(dataset_id, run_id)
            logging.info(f"Silver Transformation result: {result}")
            return result
        except Exception as e:
            logging.error(f"Silver Transformation failed: {e}")
            raise
        finally:
            spark.stop()

    # Connect all source tasks between start_all and end_all markers
    for src in all_sources:
        if src.enabled:
            ingest_task = build_run_ingestion_task.override(task_id=f"ingest_{src.source_id}")(src.source_id)
            
            if src.source_id == "faostat_production":
                # Silver runs after Bronze. The Bronze task returns a dict with 'run_id'
                silver_task = build_run_silver_task.override(task_id=f"silver_{src.source_id}")(
                    dataset_id=src.source_id,
                    run_id=ingest_task["run_id"]
                )
                start_all >> ingest_task >> silver_task >> end_all
            else:
                start_all >> ingest_task >> end_all

