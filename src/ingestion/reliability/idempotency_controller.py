"""Idempotency controller for Bronze layer.

Guarantees:
1. Re-running the pipeline on the same source data produces zero duplicate records.
2. Identical batches (matching batch_id and checksum) are safely skipped or deduplicated.
3. Out-of-order batches or retries do not corrupt Lakehouse state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ingestion.storage.bronze_storage_layout import BronzeStorageLayout
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.metadata_schemas import BatchMetadata, BatchStatus
from ingestion.utils.logging_config import create_ingestion_logger


@dataclass
class IdempotencyDecision:
    """Actionable decision on whether to proceed with batch processing."""

    should_process: bool
    status: BatchStatus
    reason: str
    existing_batch: Optional[BatchMetadata] = None


class IdempotencyController:
    """Controls idempotent ingestion execution to prevent duplicate Bronze records."""

    def __init__(
        self,
        layout: Optional[BronzeStorageLayout] = None,
        metadata_repo: Optional[MetadataRepository] = None,
    ) -> None:
        self.layout = layout or BronzeStorageLayout()
        self.metadata_repo = metadata_repo
        self.logger = create_ingestion_logger("idempotency_controller")

    def evaluate_batch(
        self,
        source_id: str,
        batch_id: str,
        current_checksum: str,
        force_reprocess: bool = False,
    ) -> IdempotencyDecision:
        """Evaluate if the batch should be processed or skipped as a duplicate."""
        # 1. Check MinIO metadata directory for existing committed batch
        existing_meta = self.layout.load_batch_metadata(source_id, batch_id)

        if existing_meta:
            if existing_meta.status == BatchStatus.COMMITTED and not force_reprocess:
                # If checksum matches, exact duplicate -> safe skip
                if existing_meta.checksum == current_checksum:
                    self.logger.info(
                        "Batch already committed with identical checksum; skipping idempotently",
                        source_id=source_id,
                        batch_id=batch_id,
                        checksum=current_checksum,
                    )
                    return IdempotencyDecision(
                        should_process=False,
                        status=BatchStatus.SKIPPED_IDEMPOTENT,
                        reason=f"Batch {batch_id} already committed with matching checksum",
                        existing_batch=existing_meta,
                    )

        # 2. Check relational metadata repository if configured
        if self.metadata_repo:
            try:
                if hasattr(self.metadata_repo, "find_snapshot_by_checksum"):
                    snap = self.metadata_repo.find_snapshot_by_checksum(source_id, current_checksum)
                    if snap and snap.get("status") == "SUCCESS" and not force_reprocess:
                        return IdempotencyDecision(
                            should_process=False,
                            status=BatchStatus.SKIPPED_IDEMPOTENT,
                            reason=f"Snapshot for {source_id} already ingested in run {snap.get('run_id')}",
                        )
                elif hasattr(self.metadata_repo, "get_batch_status"):
                    repo_status = self.metadata_repo.get_batch_status(batch_id)
                    if repo_status == "COMMITTED" and not force_reprocess:
                        return IdempotencyDecision(
                            should_process=False,
                            status=BatchStatus.SKIPPED_IDEMPOTENT,
                            reason=f"Batch {batch_id} marked as COMMITTED in metadata repository",
                        )
            except Exception as e:
                self.logger.warning(f"Could not check metadata repo for batch {batch_id}: {e}")

        # Batch is new or forced reprocess -> proceed
        return IdempotencyDecision(
            should_process=True,
            status=BatchStatus.IN_PROGRESS,
            reason="New batch or reprocess requested",
            existing_batch=existing_meta,
        )

    def register_committed_batch(
        self,
        metadata: BatchMetadata,
    ) -> None:
        """Mark batch as COMMITTED and save to MinIO metadata."""
        metadata.status = BatchStatus.COMMITTED
        self.layout.save_batch_metadata(metadata)
        self.logger.info(
            "Batch successfully registered as COMMITTED",
            source_id=metadata.source_name,
            batch_id=metadata.batch_id,
            records=metadata.record_count,
        )
