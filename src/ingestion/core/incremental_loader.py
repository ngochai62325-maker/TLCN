"""Incremental Loader — extracts only new data since the last watermark.

This loader is NOT used by any current source (all are FULL snapshot),
but it is fully functional and unit-testable with mock adapters for
future sources that genuinely support incremental semantics.

Watermark safety:
    The loader sets ``watermark_after`` in the result but does NOT
    commit the watermark.  The IngestionEngine commits the watermark
    ONLY after confirming SUCCESS.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from typing import Optional

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.base_loader import BaseLoader
from ingestion.core.checkpoint import CheckpointStore
from ingestion.core.config import SourceConfig
from ingestion.core.enums import ChunkStatus, IngestionStatus, LoadStrategy
from ingestion.core.result import IngestionResult
from ingestion.core.watermark import WatermarkStore
from ingestion.manifest.manifest import IngestionManifest
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.error_classifier import classify_error
from ingestion.utils.logging_config import create_ingestion_logger


class IncrementalLoader(BaseLoader):
    """Loader for ``load_strategy = INCREMENTAL`` sources."""

    def __init__(
        self,
        adapter: BaseSourceAdapter,
        checkpoint_store: CheckpointStore,
        watermark_store: WatermarkStore,
        minio_storage: MinioStorage,
        metadata_repo: MetadataRepository,
    ) -> None:
        self.adapter = adapter
        self.checkpoint_store = checkpoint_store
        self.watermark_store = watermark_store
        self.minio_storage = minio_storage
        self.metadata_repo = metadata_repo

    def execute(self, config: SourceConfig, run_id: str, batch_id: str, **kwargs: Any) -> IngestionResult:
        logger = create_ingestion_logger(config.source_id, run_id, batch_id)
        started_at = datetime.now(timezone.utc)

        # ── 1. Read current watermark ───────────────────────────────
        watermark_entry = self.watermark_store.get(config.source_id)
        current_watermark = watermark_entry.watermark_value if watermark_entry else None
        logger.info("Starting INCREMENTAL load", watermark_before=current_watermark)

        staging = tempfile.mkdtemp(prefix=f"ingest_{config.source_id}_")
        total_records = 0
        chunks_processed = 0
        new_watermark: Optional[str] = None

        try:
            # ── 2. Extract incremental data ─────────────────────────
            chunks_iter = self.adapter.extract_incremental(
                config, watermark=current_watermark, download_dir=staging,
            )

            for chunk in chunks_iter:
                total_records += chunk.record_count
                chunks_processed += 1

                self.checkpoint_store.save(
                    source_id=config.source_id,
                    run_id=run_id,
                    batch_id=batch_id,
                    chunk_id=chunk.chunk_id if isinstance(chunk.chunk_id, int) else int(chunk.chunk_id),
                    row_start=chunk.row_start,
                    row_end=chunk.row_end,
                    status=ChunkStatus.SUCCESS.value,
                )

                # The adapter should embed watermark info in the chunk or
                # the engine determines it from config.watermark_column.
                # For now, track the latest chunk as a simplistic approach.
                if hasattr(chunk, "data") and config.watermark_column:
                    try:
                        col = config.watermark_column
                        if col in chunk.data.columns:
                            new_watermark = str(chunk.data[col].max())
                    except Exception:
                        pass

            # ── 3. Handle no-new-data case ──────────────────────────
            if total_records == 0:
                logger.info("No new data found")
                return IngestionResult(
                    source_id=config.source_id,
                    run_id=run_id,
                    batch_id=batch_id,
                    load_type=LoadStrategy.INCREMENTAL,
                    status=IngestionStatus.NO_NEW_DATA,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    watermark_before=current_watermark,
                    watermark_after=current_watermark,
                )

            completed_at = datetime.now(timezone.utc)
            self.checkpoint_store.clear(config.source_id, run_id)

            logger.info(
                "INCREMENTAL load completed",
                records_extracted=total_records,
                watermark_before=current_watermark,
                watermark_after=new_watermark,
            )

            # NOTE: watermark_after is set but NOT committed.
            # The IngestionEngine commits watermark only after confirming success.
            return IngestionResult(
                source_id=config.source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=LoadStrategy.INCREMENTAL,
                status=IngestionStatus.SUCCESS,
                records_extracted=total_records,
                chunks_processed=chunks_processed,
                started_at=started_at,
                completed_at=completed_at,
                watermark_before=current_watermark,
                watermark_after=new_watermark or current_watermark,
            )

        except Exception as exc:
            error_type = classify_error(exc)
            logger.error(
                "INCREMENTAL load failed — watermark NOT updated",
                error=str(exc),
                error_type=error_type.value,
                exc_info=True,
            )
            return IngestionResult(
                source_id=config.source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=LoadStrategy.INCREMENTAL,
                status=IngestionStatus.FAILED,
                records_extracted=total_records,
                chunks_processed=chunks_processed,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                watermark_before=current_watermark,
                watermark_after=None,  # NOT updated on failure
                error_message=str(exc),
                error_type=error_type.value,
            )
