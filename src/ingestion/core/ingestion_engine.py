"""Main ingestion engine — the entry point for Person 3 (Airflow).

Usage::

    engine = IngestionEngine.create_default()
    result = engine.run("faostat_trade")
    # or
    result = engine.run_with_readiness_check("faostat_trade")

The engine:
1. Loads source config from the registry.
2. Resolves the adapter dynamically from ``adapter_class``.
3. Selects the correct loader (Full / Incremental).
4. Connects Bronze Iceberg writer to persist queryable tables.
5. Delegates extraction to the loader.
6. Updates watermark ONLY after incremental success.
7. Records run metadata in PostgreSQL.
8. Returns an :class:`IngestionResult` for Person 2 / Airflow.
"""

from __future__ import annotations

import importlib
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.config.registry import SourceRegistry
from ingestion.core.base_loader import BaseLoader
from ingestion.core.checkpoint import CheckpointStore, PostgresCheckpointStore
from ingestion.core.config import SourceConfig
from ingestion.core.enums import IngestionStatus, LoadStrategy
from ingestion.core.full_loader import FullLoader
from ingestion.core.incremental_loader import IncrementalLoader
from ingestion.core.result import IngestionResult
from ingestion.core.watermark import PostgresWatermarkStore, WatermarkStore
from ingestion.readiness.source_readiness import ReadinessChecker
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.error_classifier import classify_error
from ingestion.utils.logging_config import create_ingestion_logger


class IngestionEngine:
    """Orchestrates a single ingestion run for a given source.

    This class is the **only** entry point that Person 3 (Airflow) or a
    CLI needs to call.  Everything else is internal.
    """

    def __init__(
        self,
        registry: Optional[SourceRegistry] = None,
        metadata_repo: Optional[MetadataRepository] = None,
        checkpoint_store: Optional[CheckpointStore] = None,
        watermark_store: Optional[WatermarkStore] = None,
        minio_storage: Optional[MinioStorage] = None,
        bronze_writer: Optional[BronzeIcebergWriter] = None,
    ) -> None:
        self.metadata_repo = metadata_repo or MetadataRepository()
        self.registry = registry or SourceRegistry()
        self.checkpoint_store = checkpoint_store or PostgresCheckpointStore(self.metadata_repo)
        self.watermark_store = watermark_store or PostgresWatermarkStore(self.metadata_repo)
        self.minio_storage = minio_storage or MinioStorage()
        self.bronze_writer = bronze_writer or BronzeIcebergWriter()
        self.readiness_checker = ReadinessChecker()
        self.registry.load()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def create_default(cls) -> "IngestionEngine":
        """Create an engine using environment-based defaults."""
        return cls()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self, source_id: str, **kwargs: Any) -> IngestionResult:
        """Execute ingestion for *source_id*.  Main entry point."""
        now = datetime.now(timezone.utc)
        run_id = f"{source_id}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        batch_id = f"{source_id}_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:4]}"

        logger = create_ingestion_logger(source_id, run_id, batch_id)
        logger.info("Starting ingestion engine run")

        try:
            config = self.registry.get_source(source_id)

            # Record run start in metadata DB (best-effort)
            self._record_run_start(run_id, source_id, config.load_strategy.value, now)

            adapter = self._resolve_adapter(config)
            loader = self._select_loader(config, adapter)

            result = loader.execute(config, run_id, batch_id, **kwargs)

            # Watermark update — only for genuine INCREMENTAL success
            if (
                config.load_strategy == LoadStrategy.INCREMENTAL
                and result.status == IngestionStatus.SUCCESS
                and result.watermark_after is not None
            ):
                self.watermark_store.set(
                    source_id,
                    str(result.watermark_after),
                    run_id,
                    batch_id,
                    IngestionStatus.SUCCESS.value,
                )
                logger.info(
                    "Watermark updated",
                    watermark_before=result.watermark_before,
                    watermark_after=result.watermark_after,
                )

            # Record run completion
            self._record_run_end(run_id, result)
            logger.info("Ingestion engine run completed", status=result.status.value)
            return result

        except Exception as exc:
            error_type = classify_error(exc)
            logger.error(
                "Ingestion engine run failed",
                error=str(exc),
                error_type=error_type.value,
                exc_info=True,
            )
            failed_result = IngestionResult(
                source_id=source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=LoadStrategy.FULL,
                status=IngestionStatus.FAILED,
                started_at=now,
                completed_at=datetime.now(timezone.utc),
                error_message=str(exc),
                error_type=error_type.value,
            )
            self._record_run_end(run_id, failed_result)
            return failed_result

    def run_with_readiness_check(self, source_id: str, **kwargs: Any) -> IngestionResult:
        """Check source readiness first, then run."""
        config = self.registry.get_source(source_id)
        readiness = self.readiness_checker.check(config)

        if not readiness.ready:
            now = datetime.now(timezone.utc)
            run_id = f"{source_id}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
            batch_id = f"{source_id}_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:4]}"
            logger = create_ingestion_logger(source_id, run_id, batch_id)
            logger.warning("Source not ready", reason=readiness.reason)
            return IngestionResult(
                source_id=source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=config.load_strategy,
                status=IngestionStatus.NOT_READY,
                started_at=now,
                completed_at=now,
                error_message=readiness.reason,
                source_metadata=readiness.source_metadata,
            )

        return self.run(source_id, **kwargs)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_adapter(self, config: SourceConfig) -> BaseSourceAdapter:
        """Dynamically import the adapter class from its dotted path."""
        if not config.adapter_class:
            raise ValueError(f"No adapter_class configured for source '{config.source_id}'")
        module_path, class_name = config.adapter_class.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls()

    def _select_loader(self, config: SourceConfig, adapter: BaseSourceAdapter) -> BaseLoader:
        if config.load_strategy == LoadStrategy.FULL:
            return FullLoader(
                adapter=adapter,
                checkpoint_store=self.checkpoint_store,
                minio_storage=self.minio_storage,
                metadata_repo=self.metadata_repo,
                bronze_writer=self.bronze_writer,
            )
        elif config.load_strategy == LoadStrategy.INCREMENTAL:
            return IncrementalLoader(
                adapter=adapter,
                checkpoint_store=self.checkpoint_store,
                watermark_store=self.watermark_store,
                minio_storage=self.minio_storage,
                metadata_repo=self.metadata_repo,
            )
        raise ValueError(f"Unsupported load strategy: {config.load_strategy}")

    def _record_run_start(self, run_id: str, source_id: str, load_type: str, started_at: datetime) -> None:
        try:
            self.metadata_repo.create_run(run_id, source_id, load_type, started_at.isoformat())
        except Exception:
            pass  # metadata recording is best-effort; do not block ingestion

    def _record_run_end(self, run_id: str, result: IngestionResult) -> None:
        try:
            self.metadata_repo.complete_run(
                run_id=run_id,
                status=result.status.value if isinstance(result.status, IngestionStatus) else str(result.status),
                records_extracted=result.records_extracted,
                records_quarantined=result.records_quarantined,
                chunks_processed=result.chunks_processed,
                chunks_total=result.chunks_total,
                completed_at=result.completed_at.isoformat() if isinstance(result.completed_at, datetime) else result.completed_at,
                artifact_uri=result.artifact_uri,
                manifest_uri=result.manifest_uri,
                checksum=result.checksum,
                error_message=result.error_message,
                error_type=result.error_type,
                source_metadata=result.source_metadata,
            )
        except Exception:
            pass  # best-effort
