"""Full Loader — extracts the complete dataset from a source.

Lifecycle::

    adapter.extract_full(config)
      → iterate DataChunks with checkpoint
      → persist chunk to Bronze Iceberg (if Bronze writer configured)
      → compute artifact checksum
      → check idempotency (same snapshot already ingested?)
      → upload raw artifact to MinIO raw landing
      → build & upload manifest
      → record snapshot in PostgreSQL metadata
      → return IngestionResult

Architecture Separation:
1. Raw Landing: Immutable raw artifact (ZIP/CSV) + manifest.json in s3://bronze/raw/
2. Bronze Iceberg: Append-only queryable Iceberg tables in iceberg.bronze with technical metadata.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.base_loader import BaseLoader
from ingestion.core.checkpoint import CheckpointStore
from ingestion.core.config import SourceConfig
from ingestion.core.enums import ChunkStatus, IngestionStatus, LoadStrategy
from ingestion.core.result import IngestionResult
from ingestion.manifest.manifest import IngestionManifest
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.error_classifier import classify_error
from ingestion.utils.hashing import compute_file_checksum
from ingestion.utils.logging_config import create_ingestion_logger


class FullLoader(BaseLoader):
    """Loader for ``load_strategy = FULL`` sources."""

    def __init__(
        self,
        adapter: BaseSourceAdapter,
        checkpoint_store: CheckpointStore,
        minio_storage: MinioStorage,
        metadata_repo: MetadataRepository,
        bronze_writer: Optional[BronzeIcebergWriter] = None,
        manifest_builder_cls: Type[IngestionManifest] = IngestionManifest,
    ) -> None:
        self.adapter = adapter
        self.checkpoint_store = checkpoint_store
        self.minio_storage = minio_storage
        self.metadata_repo = metadata_repo
        self.bronze_writer = bronze_writer
        self.manifest_cls = manifest_builder_cls

    def execute(
        self,
        config: SourceConfig,
        run_id: str,
        batch_id: str,
        resume_from_run_id: Optional[str] = None,
    ) -> IngestionResult:
        logger = create_ingestion_logger(config.source_id, run_id, batch_id)
        started_at = datetime.now(timezone.utc)

        manifest = self.manifest_cls(config.source_id, run_id, batch_id, LoadStrategy.FULL.value)
        manifest.set_timing(started_at.isoformat(), None)
        if config.endpoint:
            manifest.set_source_metadata(
                provider=config.provider,
                dataset=config.dataset,
                source_uri=config.endpoint,
            )

        # Deterministic staging directory per source and batch to preserve download across retries
        staging = os.path.join(tempfile.gettempdir(), f"ingest_{config.source_id}_{batch_id}")
        os.makedirs(staging, exist_ok=True)
        logger.info("Starting FULL load", staging_dir=staging)

        total_records = 0
        chunks_processed = 0
        chunks_total = 0
        artifact_path: Optional[str] = None

        try:
            # ── 1. Check for resume checkpoints ───────────────────────
            active_resume_run = resume_from_run_id
            if not active_resume_run:
                # Look up latest failed/incomplete run for this source
                latest_run = self.metadata_repo.get_latest_run(config.source_id)
                if latest_run and latest_run.get("status") in (
                    IngestionStatus.FAILED.value,
                    IngestionStatus.IN_PROGRESS.value,
                ):
                    active_resume_run = latest_run.get("run_id")

            last_ckpt = None
            if active_resume_run:
                last_ckpt = self.checkpoint_store.get_last_successful(config.source_id, active_resume_run)

            resume_chunk_id = last_ckpt.chunk_id if last_ckpt else None
            resume_row_start = (last_ckpt.row_end + 1) if (last_ckpt and last_ckpt.row_end is not None) else None

            if resume_chunk_id is not None:
                logger.info(
                    "Resuming extraction from previous checkpoint",
                    resume_from_run=active_resume_run,
                    resume_chunk_id=resume_chunk_id,
                    resume_row_start=resume_row_start,
                )

            # ── 1b. Up-front idempotency / snapshot check ───────────
            source_file = self._find_source_artifact(config, staging)
            initial_checksum = compute_file_checksum(source_file) if (source_file and os.path.exists(source_file)) else None

            if initial_checksum:
                existing = self._check_existing_snapshot(config.source_id, initial_checksum)
                if existing is not None and existing.get("status") == IngestionStatus.SUCCESS.value:
                    logger.info(
                        "Snapshot already ingested (idempotent skip before extraction)",
                        existing_run=existing.get("run_id"),
                        checksum=initial_checksum,
                    )
                    completed_at = datetime.now(timezone.utc)
                    return IngestionResult(
                        source_id=config.source_id,
                        run_id=run_id,
                        batch_id=batch_id,
                        load_type=LoadStrategy.FULL,
                        status=IngestionStatus.SKIPPED,
                        records_extracted=0,
                        chunks_processed=0,
                        chunks_total=0,
                        checksum=initial_checksum,
                        started_at=started_at,
                        completed_at=completed_at,
                        source_metadata={"skipped_reason": "unchanged_snapshot"},
                    )

            # Pass resume hints to adapter if supported
            extract_kwargs: Dict[str, Any] = {"download_dir": staging}
            if resume_chunk_id is not None:
                extract_kwargs["resume_chunk_id"] = resume_chunk_id
                extract_kwargs["resume_row_start"] = resume_row_start

            # ── 2. Extract data via adapter ─────────────────────────
            try:
                chunks_iter = self.adapter.extract_full(config, **extract_kwargs)
            except TypeError:
                # Fallback if adapter does not take resume kwargs
                chunks_iter = self.adapter.extract_full(config, download_dir=staging)

            # If resuming, load previously completed chunks count
            if last_ckpt:
                all_prior_ckpts = self.checkpoint_store.get_all(config.source_id, active_resume_run)
                for prior in all_prior_ckpts:
                    if prior.status == ChunkStatus.SUCCESS.value:
                        chunks_total += 1
                        chunks_processed += 1
                        prior_count = (prior.row_end - prior.row_start + 1) if (prior.row_end is not None and prior.row_start is not None) else 0
                        total_records += prior_count

            for chunk in chunks_iter:
                chunks_total += 1
                chunk_id = chunk.chunk_id if isinstance(chunk.chunk_id, int) else int(chunk.chunk_id)

                # Skip if already processed
                if resume_chunk_id is not None and chunk_id <= resume_chunk_id:
                    total_records += chunk.record_count
                    chunks_processed += 1
                    logger.info("Skipping already-processed chunk", chunk_id=chunk_id)
                    continue

                try:
                    total_records += chunk.record_count
                    chunks_processed += 1

                    # Write chunk into Bronze Iceberg table if configured
                    if self.bronze_writer and hasattr(chunk, "data") and chunk.data is not None:
                        source_file_label = os.path.basename(config.local_path or config.endpoint or config.source_id)
                        self.bronze_writer.write_chunk(
                            source_id=config.source_id,
                            chunk_df=chunk.data,
                            run_id=run_id,
                            batch_id=batch_id,
                            source_checksum=initial_checksum or chunk.checksum or "unknown",
                            source_snapshot_id=0,
                            source_file=source_file_label,
                        )

                    # Save checkpoint SUCCESS for this chunk
                    self.checkpoint_store.save(
                        source_id=config.source_id,
                        run_id=run_id,
                        batch_id=batch_id,
                        chunk_id=chunk_id,
                        row_start=chunk.row_start,
                        row_end=chunk.row_end,
                        status=ChunkStatus.SUCCESS.value,
                        checksum=chunk.checksum,
                    )

                    if chunks_processed % 50 == 0:
                        logger.info(
                            "Chunk progress",
                            chunk_id=chunk_id,
                            chunks_processed=chunks_processed,
                            total_records=total_records,
                        )

                except Exception as chunk_err:
                    self.checkpoint_store.save(
                        source_id=config.source_id,
                        run_id=run_id,
                        batch_id=batch_id,
                        chunk_id=chunk_id,
                        row_start=chunk.row_start,
                        row_end=chunk.row_end,
                        status=ChunkStatus.FAILED.value,
                    )
                    raise chunk_err

            logger.info(
                "All chunks processed",
                chunks_processed=chunks_processed,
                total_records=total_records,
            )

            # ── 3. Compute artifact checksum ────────────────────────
            artifact_path = self._find_artifact(staging, config)
            artifact_checksum = (
                initial_checksum or (compute_file_checksum(artifact_path) if artifact_path else "no-artifact")
            )
            artifact_size = os.path.getsize(artifact_path) if artifact_path else 0

            # ── 4. Idempotency / snapshot detection ─────────────────
            existing = self._check_existing_snapshot(config.source_id, artifact_checksum)
            if existing is not None:
                logger.info(
                    "Snapshot already ingested (idempotent skip)",
                    existing_run=existing.get("run_id"),
                    checksum=artifact_checksum,
                )
                completed_at = datetime.now(timezone.utc)
                return IngestionResult(
                    source_id=config.source_id,
                    run_id=run_id,
                    batch_id=batch_id,
                    load_type=LoadStrategy.FULL,
                    status=IngestionStatus.SKIPPED,
                    records_extracted=total_records,
                    chunks_processed=chunks_processed,
                    chunks_total=chunks_total,
                    checksum=artifact_checksum,
                    started_at=started_at,
                    completed_at=completed_at,
                    source_metadata={"skipped_reason": "unchanged_snapshot"},
                )

            # ── 5. Upload raw artifact to MinIO raw landing ─────────
            ingestion_date = started_at.strftime("%Y-%m-%d")
            artifact_filename = os.path.basename(artifact_path) if artifact_path else f"data.{config.format.value.lower()}"
            artifact_key = self.minio_storage.build_raw_landing_key(
                config.source_id,
                ingestion_date,
                run_id,
                artifact_filename,
            )

            artifact_uri = "no-artifact"
            if artifact_path and os.path.exists(artifact_path):
                artifact_uri = self.minio_storage.upload_file(artifact_path, "bronze", artifact_key)
                logger.info("Raw artifact uploaded", artifact_uri=artifact_uri, size=artifact_size)

            # ── 6. Build & upload manifest ──────────────────────────
            completed_at = datetime.now(timezone.utc)
            manifest.set_artifact_info(artifact_uri, config.format.value, artifact_size, artifact_checksum)
            manifest.set_metrics(total_records, chunks_processed)
            manifest.set_timing(started_at.isoformat(), completed_at.isoformat())
            manifest.set_status(IngestionStatus.SUCCESS)

            manifest_key = self.minio_storage.build_manifest_key(config.source_id, ingestion_date, run_id)
            manifest_uri = manifest.save_to_storage(self.minio_storage, "bronze", manifest_key)

            # ── 7. Record snapshot in PostgreSQL ────────────────────
            snapshot_id = self._record_snapshot(
                config.source_id,
                artifact_checksum,
                artifact_size,
                run_id,
                artifact_uri,
            )

            # Clear checkpoints on successful completion of the full run
            self.checkpoint_store.clear(config.source_id, run_id)
            if active_resume_run:
                self.checkpoint_store.clear(config.source_id, active_resume_run)

            logger.info(
                "FULL load completed successfully",
                records_extracted=total_records,
                checksum=artifact_checksum,
                artifact_uri=artifact_uri,
                manifest_uri=manifest_uri,
            )

            return IngestionResult(
                source_id=config.source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=LoadStrategy.FULL,
                status=IngestionStatus.SUCCESS,
                artifact_uri=artifact_uri,
                manifest_uri=manifest_uri,
                data_format=config.format,
                records_extracted=total_records,
                chunks_processed=chunks_processed,
                chunks_total=chunks_total,
                checksum=artifact_checksum,
                started_at=started_at,
                completed_at=completed_at,
                manifest=manifest.to_dict(),
            )

        except Exception as exc:
            error_type = classify_error(exc)
            completed_at = datetime.now(timezone.utc)
            logger.error(
                "FULL load failed",
                error=str(exc),
                error_type=error_type.value,
                chunks_processed=chunks_processed,
                total_records=total_records,
                exc_info=True,
            )

            manifest.set_status(IngestionStatus.FAILED)
            manifest.set_error(str(exc), error_type.value)
            manifest.set_timing(started_at.isoformat(), completed_at.isoformat())

            return IngestionResult(
                source_id=config.source_id,
                run_id=run_id,
                batch_id=batch_id,
                load_type=LoadStrategy.FULL,
                status=IngestionStatus.FAILED,
                records_extracted=total_records,
                chunks_processed=chunks_processed,
                chunks_total=chunks_total,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(exc),
                error_type=error_type.value,
                manifest=manifest.to_dict(),
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _find_source_artifact(self, config: SourceConfig, staging_dir: str) -> Optional[str]:
        """Locate the original raw source artifact before extraction."""
        if config.local_path and os.path.exists(config.local_path):
            if os.path.isfile(config.local_path):
                return config.local_path
            elif os.path.isdir(config.local_path):
                files = [
                    os.path.join(config.local_path, f)
                    for f in os.listdir(config.local_path)
                    if not f.startswith(".")
                ]
                if files:
                    return max(files, key=os.path.getsize)
        staged_zip = os.path.join(staging_dir, f"{config.source_id}_bulk.zip")
        if os.path.exists(staged_zip):
            return staged_zip
        return None

    def _find_artifact(self, staging_dir: str, config: SourceConfig) -> Optional[str]:
        """Locate the primary extracted or downloaded artifact."""
        for root, _dirs, files in os.walk(staging_dir):
            for f in sorted(files, key=lambda x: os.path.getsize(os.path.join(root, x)), reverse=True):
                if f.endswith((".zip", ".csv", ".xlsx", ".xls", ".parquet")):
                    return os.path.join(root, f)
        return None

    def _check_existing_snapshot(self, source_id: str, checksum: str) -> Optional[dict]:
        """Return existing snapshot dict if same checksum was already ingested successfully."""
        try:
            return self.metadata_repo.find_snapshot_by_checksum(source_id, checksum)
        except Exception:
            return None

    def _record_snapshot(
        self,
        source_id: str,
        checksum: str,
        size_bytes: int,
        run_id: str,
        source_uri: str,
    ) -> Optional[int]:
        try:
            return self.metadata_repo.save_snapshot(
                source_id=source_id,
                checksum=checksum,
                size_bytes=size_bytes,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                run_id=run_id,
                status=IngestionStatus.SUCCESS.value,
                source_uri=source_uri,
            )
        except Exception:
            return None
