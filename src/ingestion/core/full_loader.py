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
from ingestion.reliability.bronze_quality_validator import BronzeQualityValidator
from ingestion.reliability.idempotency_controller import IdempotencyController
from ingestion.reliability.quarantine_manager import QuarantineManager
from ingestion.storage.bronze_storage_layout import BronzeStorageLayout, resolve_source_group
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.metadata_schemas import (
    AuditLogEntry,
    BatchMetadata,
    BatchStatus,
    QuarantineErrorType,
)
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.error_classifier import classify_error
from ingestion.utils.hashing import compute_directory_checksum, compute_file_checksum
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
        bronze_layout: Optional[BronzeStorageLayout] = None,
        quality_validator: Optional[BronzeQualityValidator] = None,
        quarantine_manager: Optional[QuarantineManager] = None,
        idempotency_controller: Optional[IdempotencyController] = None,
    ) -> None:
        self.adapter = adapter
        self.checkpoint_store = checkpoint_store
        self.minio_storage = minio_storage
        self.metadata_repo = metadata_repo
        self.bronze_writer = bronze_writer
        self.manifest_cls = manifest_builder_cls
        self.bronze_layout = bronze_layout or BronzeStorageLayout(minio_storage=self.minio_storage)
        self.quality_validator = quality_validator or BronzeQualityValidator()
        self.quarantine_manager = quarantine_manager or QuarantineManager(layout=self.bronze_layout)
        self.idempotency_controller = idempotency_controller or IdempotencyController(
            layout=self.bronze_layout, metadata_repo=self.metadata_repo
        )

    def execute(
        self,
        config: SourceConfig,
        run_id: str,
        batch_id: str,
        resume_from_run_id: Optional[str] = None,
        force_reprocess: bool = False,
        **kwargs: Any,
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
            initial_checksum = self._compute_source_checksum(config, staging)

            if initial_checksum:
                # Check with IdempotencyController
                idempotency_decision = self.idempotency_controller.evaluate_batch(
                    source_id=config.source_id,
                    batch_id=batch_id,
                    current_checksum=initial_checksum,
                    force_reprocess=force_reprocess,
                )
                if not idempotency_decision.should_process:
                    logger.info(
                        "Batch skipped via IdempotencyController",
                        reason=idempotency_decision.reason,
                        batch_id=batch_id,
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
                        source_metadata={
                            "skipped_reason": "unchanged_snapshot",
                            "idempotency_reason": idempotency_decision.reason,
                        },
                    )

                existing = self._check_existing_snapshot(config.source_id, initial_checksum)
                if not force_reprocess and existing is not None and existing.get("status") == IngestionStatus.SUCCESS.value:
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

            # Pre-flight Quality Validation on source artifact (if local file exists)
            local_src = config.local_path or config.local_fallback
            if local_src and os.path.exists(local_src) and os.path.isfile(local_src):
                v_res = self.quality_validator.validate_file(local_src)
                if not v_res.is_valid:
                    logger.warning(
                        "Source artifact failed Bronze Quality Validation; routing to quarantine",
                        file=local_src,
                        error=v_res.error_message,
                    )
                    q_res = self.quarantine_manager.quarantine_artifact(
                        source_id=config.source_id,
                        file_path=local_src,
                        validation_result=v_res,
                        run_id=run_id,
                        batch_id=batch_id,
                    )
                    completed_at = datetime.now(timezone.utc)
                    return IngestionResult(
                        source_id=config.source_id,
                        run_id=run_id,
                        batch_id=batch_id,
                        load_type=LoadStrategy.FULL,
                        status=IngestionStatus.FAILED,
                        records_extracted=0,
                        records_quarantined=v_res.record_count,
                        error_type=v_res.error_type.value if v_res.error_type else "QUALITY_ERROR",
                        error_message=v_res.error_message,
                        started_at=started_at,
                        completed_at=completed_at,
                        artifact_uri=q_res.quarantined_uri,
                        source_metadata={"diagnostic_uri": q_res.diagnostic_uri},
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
            prior_chunk_ids: set = set()
            if last_ckpt:
                all_prior_ckpts = self.checkpoint_store.get_all(config.source_id, active_resume_run)
                for prior in all_prior_ckpts:
                    if prior.status == ChunkStatus.SUCCESS.value:
                        prior_chunk_ids.add(prior.chunk_id)
                        chunks_total += 1
                        chunks_processed += 1
                        prior_count = (prior.row_end - prior.row_start + 1) if (prior.row_end is not None and prior.row_start is not None) else 0
                        total_records += prior_count

            existing_bronze_files: set = set()
            if self.bronze_writer:
                try:
                    existing_bronze_files = set(self.bronze_writer.get_ingested_files(config.source_id))
                except Exception:
                    existing_bronze_files = set()

            for chunk in chunks_iter:
                chunk_id = chunk.chunk_id if isinstance(chunk.chunk_id, int) else int(chunk.chunk_id)

                # Skip if already processed in prior failed run
                if resume_chunk_id is not None and chunk_id <= resume_chunk_id:
                    if chunk_id not in prior_chunk_ids:
                        total_records += chunk.record_count
                        chunks_processed += 1
                        chunks_total += 1
                        prior_chunk_ids.add(chunk_id)
                    logger.info("Skipping already-processed chunk", chunk_id=chunk_id)
                    continue

                chunks_total += 1

                # File-arrival incremental skip: if chunk contains data for files already in Bronze
                if (
                    existing_bronze_files
                    and hasattr(chunk, "data")
                    and chunk.data is not None
                    and "_source_file" in chunk.data.columns
                ):
                    chunk_files = set(chunk.data["_source_file"].dropna().unique())
                    if chunk_files and chunk_files.issubset(existing_bronze_files):
                        logger.info("Skipping chunk for already-ingested file(s)", files=list(chunk_files), chunk_id=chunk_id)
                        chunks_processed += 1
                        continue

                try:
                    total_records += chunk.record_count
                    chunks_processed += 1

                    # Write chunk into Bronze Iceberg table if configured
                    if self.bronze_writer and hasattr(chunk, "data") and chunk.data is not None:
                        source_file_label = os.path.basename(config.local_path or config.endpoint or config.source_id)
                        is_dir_source = bool(config.local_path and os.path.exists(config.local_path) and os.path.isdir(config.local_path))
                        source_chk = chunk.checksum if is_dir_source else (initial_checksum or chunk.checksum or "unknown")
                        self.bronze_writer.write_chunk(
                            source_id=config.source_id,
                            chunk_df=chunk.data,
                            run_id=run_id,
                            batch_id=batch_id,
                            source_checksum=source_chk,
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

                # Archive into Bronze 5-folder storage layout: {source_group}/raw/{batch_id}/{filename}
                try:
                    self.bronze_layout.archive_raw_artifact(config.source_id, batch_id, artifact_path)
                except Exception as layout_err:
                    logger.warning(f"Could not archive to bronze raw layout: {layout_err}")

            # ── 6. Build & upload manifest ──────────────────────────
            completed_at = datetime.now(timezone.utc)
            manifest.set_artifact_info(artifact_uri, config.format.value, artifact_size, artifact_checksum)
            manifest.set_metrics(total_records, chunks_processed)
            manifest.set_timing(started_at.isoformat(), completed_at.isoformat())
            manifest.set_status(IngestionStatus.SUCCESS)

            manifest_key = self.minio_storage.build_manifest_key(config.source_id, ingestion_date, run_id)
            manifest_uri = manifest.save_to_storage(self.minio_storage, "bronze", manifest_key)

            # Save manifest into Bronze 5-folder layout: {source_group}/manifest/run_{run_id}.json
            try:
                self.bronze_layout.save_manifest(config.source_id, run_id, manifest.to_dict())
            except Exception as m_err:
                logger.warning(f"Could not save manifest to bronze layout: {m_err}")

            # ── 6b. Persist Batch Metadata & Audit Entry ─────────────
            batch_meta = BatchMetadata(
                batch_id=batch_id,
                source_name=config.source_id,
                source_group=resolve_source_group(config.source_id),
                extracted_at=started_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                record_count=total_records,
                checksum=artifact_checksum,
                file_size_bytes=artifact_size,
                status=BatchStatus.COMMITTED,
                committed_at=completed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                target_table=f"iceberg.bronze.{config.source_id}",
            )
            try:
                self.bronze_layout.save_batch_metadata(batch_meta)
                self.idempotency_controller.register_committed_batch(batch_meta)
            except Exception as meta_err:
                logger.warning(f"Could not save batch metadata: {meta_err}")

            audit_entry = AuditLogEntry(
                run_id=run_id,
                batch_id=batch_id,
                source_id=config.source_id,
                source_group=resolve_source_group(config.source_id),
                started_at=started_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                ended_at=completed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                status=BatchStatus.COMMITTED,
                records_processed=total_records,
                records_quarantined=0,
                checksum=artifact_checksum,
                duration_seconds=(completed_at - started_at).total_seconds(),
            )
            try:
                self.bronze_layout.save_audit_entry(audit_entry)
            except Exception as audit_err:
                logger.warning(f"Could not save audit entry: {audit_err}")

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

            try:
                fail_audit = AuditLogEntry(
                    run_id=run_id,
                    batch_id=batch_id,
                    source_id=config.source_id,
                    source_group=resolve_source_group(config.source_id),
                    started_at=started_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    ended_at=completed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    status=BatchStatus.FAILED,
                    records_processed=total_records,
                    records_quarantined=0,
                    error_message=str(exc),
                    duration_seconds=(completed_at - started_at).total_seconds(),
                )
                self.bronze_layout.save_audit_entry(fail_audit)
            except Exception:
                pass

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
    def _compute_source_checksum(self, config: SourceConfig, staging_dir: str) -> Optional[str]:
        """Compute the deterministic SHA-256 checksum of the source (file or directory)."""
        path = config.local_path or config.local_fallback
        if path and os.path.exists(path):
            if os.path.isdir(path):
                pattern = (config.extra.get("file_pattern") if config.extra else None) or "*"
                return compute_directory_checksum(path, pattern=pattern)
            return compute_file_checksum(path)
        source_file = self._find_source_artifact(config, staging_dir)
        if source_file and os.path.exists(source_file):
            return compute_file_checksum(source_file)
        return None

    def _find_source_artifact(self, config: SourceConfig, staging_dir: str) -> Optional[str]:
        """Locate the original raw source artifact before extraction."""
        path = config.local_path or config.local_fallback
        if path and os.path.exists(path):
            if os.path.isfile(path):
                return path
            elif os.path.isdir(path):
                # For directories, package all matching files into a deterministic zip in staging
                zip_path = os.path.join(staging_dir, f"{config.source_id}_raw.zip")
                if not os.path.exists(zip_path):
                    import glob
                    import zipfile
                    pattern = (config.extra.get("file_pattern") if config.extra else None) or "*"
                    matched_files = sorted(glob.glob(os.path.join(path, pattern)))
                    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                        for f in matched_files:
                            if os.path.isfile(f):
                                zf.write(f, arcname=os.path.basename(f))
                return zip_path
        staged_zip = os.path.join(staging_dir, f"{config.source_id}_bulk.zip")
        if os.path.exists(staged_zip):
            return staged_zip
        return None


    def _find_artifact(self, staging_dir: str, config: SourceConfig) -> Optional[str]:
        """Locate the primary raw source artifact, falling back to staging files."""
        source_art = self._find_source_artifact(config, staging_dir)
        if source_art and os.path.exists(source_art):
            return source_art
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
