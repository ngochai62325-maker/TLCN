"""Orchestration package for Lakehouse Bronze Ingestion."""

from ingestion.orchestration.pipeline_tasks import (
    run_ingestion_task,
)

__all__ = [
    "run_ingestion_task",
]
