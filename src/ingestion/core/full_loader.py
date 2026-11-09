"""FULL extraction with source locking, immutable Bronze chunks and durable proof.

Raw stays local. A chunk has one append commit and exact readback; a FULL run is
not a multi-chunk transaction. Interrupted runs retain data and checkpoints.
Cross-run automatic checkpoint skips are forbidden. Silver retains absent keys;
FULL extraction does not authorize source withdrawal.
"""

from __future__ import annotations

import os
import tempfile
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Type

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
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.metadata_schemas import (
    BatchMetadata,
    BatchStatus,
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
        quality_validator: Optional[BronzeQualityValidator] = None,
        quarantine_manager: Optional[QuarantineManager] = None,
        idempotency_controller: Optional[IdempotencyController] = None,
        **kwargs: Any,
    ) -> None:
        self.adapter = adapter
        self.checkpoint_store = checkpoint_store
        self.minio_storage = minio_storage
        self.metadata_repo = metadata_repo
        self.bronze_writer = bronze_writer
        self.manifest_cls = manifest_builder_cls
        self.quality_validator = quality_validator or BronzeQualityValidator()
        self.quarantine_manager = quarantine_manager or QuarantineManager(metadata_repo=self.metadata_repo)
        self.idempotency_controller = idempotency_controller or IdempotencyController(
            metadata_repo=self.metadata_repo
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
            # An explicit reprocess must read every chunk, including files
            # written by a failed run. Keep old rows as Bronze history.
            active_resume_run = None if force_reprocess else resume_from_run_id
            if not force_reprocess and not active_resume_run:
                # Look up latest failed/incomplete run for this source
                latest_run = self.metadata_repo.get_latest_run(config.source_id, exclude_run_id=run_id)
                if latest_run and latest_run.get("status") in (
                    IngestionStatus.FAILED.value,
                    IngestionStatus.IN_PROGRESS.value,
                ):
                    metadata = latest_run.get("source_metadata") or {}
                    never_wrote = metadata.get("protocol") == "foundation-20261026.1" and metadata.get("physical_write_started") is False
                    if not never_wrote:
                        active_resume_run = latest_run.get("run_id")

            last_ckpt = None
            if active_resume_run:
                last_ckpt = self.checkpoint_store.get_last_successful(config.source_id, active_resume_run)

            recovering = bool(self.bronze_writer and active_resume_run and active_resume_run != run_id)

            if self.bronze_writer:
                last_ckpt = None  # re-extract and exactly re-verify every immutable chunk
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
            # Pre-flight Quality Validation on source artifact (if local file exists)
            local_src = config.local_path or config.local_fallback
            if local_src and os.path.exists(local_src) and os.path.isfile(local_src):
                v_res = self.quality_validator.validate_file(local_src, expected_columns=config.expected_columns)
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
                        extra_details={"provider": config.provider},
                    )
                    if not q_res.is_quarantined or not q_res.diagnostic_uri:
                        raise RuntimeError("Rejected input diagnostic was not persisted")
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


            physical_run_id, physical_batch_id = run_id, batch_id
            input_context = None
            if self.bronze_writer:
                layout = self._layout_fingerprint(config)
                if recovering:
                    prior = self.metadata_repo.get_run(active_resume_run)
                    if isinstance(prior, dict) and "UNKNOWN_COMMIT" in (prior.get("error_message") or ""):
                        raise RuntimeError("RECOVERY_REQUIRED: unknown commit requires explicit catalog/content readback; automatic replay forbidden")
                    context = (prior.get("source_metadata") or {}).get("input_context") if isinstance(prior, dict) else None
                    if (not context or prior.get("source_id") != config.source_id or prior.get("status") not in ("FAILED", "IN_PROGRESS")
                            or not initial_checksum or context.get("checksum") != initial_checksum or context.get("layout") != layout):
                        raise RuntimeError("RECOVERY_REQUIRED: missing or incompatible durable source checksum/layout context; no checkpoint-only replay")
                    physical_run_id, physical_batch_id = context["physical_run_id"], context["physical_batch_id"]
                input_context = {"checksum": initial_checksum, "layout": layout,
                                 "physical_run_id": physical_run_id, "physical_batch_id": physical_batch_id}
                self.metadata_repo.save_input_context(run_id, input_context)

            if initial_checksum:
                # Check with IdempotencyController
                idempotency_decision = self.idempotency_controller.evaluate_batch(
                    source_id=config.source_id,
                    batch_id=batch_id,
                    current_checksum=initial_checksum,
                    force_reprocess=force_reprocess or recovering,
                )
                if not idempotency_decision.should_process:
                    if not self._check_existing_snapshot(config.source_id, initial_checksum):
                        raise RuntimeError("Idempotency skip lacks durable source snapshot proof")
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

                existing = None if force_reprocess or recovering else self._check_existing_snapshot(config.source_id, initial_checksum)
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

                try:
                    if self.bronze_writer and chunk.record_count != len(chunk.data):
                        raise RuntimeError("Chunk record_count differs from physical input")
                    total_records += chunk.record_count
                    chunks_processed += 1

                    # Write chunk into Bronze Iceberg table if configured
                    if self.bronze_writer and hasattr(chunk, "data") and chunk.data is not None:
                        self.metadata_repo.mark_physical_write_started(run_id)
                        source_file_label = os.path.basename(config.local_path or config.endpoint or config.source_id)
                        is_dir_source = bool(config.local_path and os.path.exists(config.local_path) and os.path.isdir(config.local_path))
                        source_chk = chunk.checksum if is_dir_source else (initial_checksum or chunk.checksum or "unknown")
                        self.bronze_writer.write_chunk(
                            source_id=config.source_id,
                            chunk_df=chunk.data,
                            run_id=physical_run_id,
                            batch_id=physical_batch_id,
                            source_checksum=source_chk,
                            chunk_id=chunk_id,
                            source_snapshot_id=None,
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
                    try:
                        self.checkpoint_store.save(
                            source_id=config.source_id, run_id=run_id, batch_id=batch_id,
                            chunk_id=chunk_id, row_start=chunk.row_start, row_end=chunk.row_end,
                            status=ChunkStatus.FAILED.value,
                        )
                    except Exception as checkpoint_error:
                        raise RuntimeError(f"{chunk_err}; FAILED checkpoint persistence also failed: {checkpoint_error}") from chunk_err
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

            if initial_checksum and self._compute_source_checksum(config, staging) != initial_checksum:
                raise RuntimeError("Source content changed during extraction; committed chunks retained, publication blocked")
            bronze_proof = self.bronze_writer.verify_run(config.source_id, physical_run_id, total_records) if self.bronze_writer else None

            # ── 4. Idempotency / snapshot detection ─────────────────
            existing = None if force_reprocess or recovering else self._check_existing_snapshot(config.source_id, artifact_checksum)
            if not force_reprocess and existing is not None and existing.get("status") == IngestionStatus.SUCCESS.value:
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

            # ── 5. Compute artifact URI (no MinIO upload – Phase 2) ─────
            # Raw files stay on disk; idempotency is owned by PostgreSQL snapshots.
            # artifact_uri is recorded as a local path reference for traceability.
            artifact_uri = f"local://{artifact_path}" if artifact_path else "no-artifact"

            # ── 6. Build manifest (in-memory only) ──────────────────────
            completed_at = datetime.now(timezone.utc)
            manifest.set_artifact_info(artifact_uri, config.format.value, artifact_size, artifact_checksum)
            manifest.set_metrics(total_records, chunks_processed)
            manifest.set_timing(started_at.isoformat(), completed_at.isoformat())
            manifest.set_status(IngestionStatus.SUCCESS)
            manifest_uri = f"local://manifest/{config.source_id}/{run_id}"

            # ── 6b. Persist Batch Metadata ─────────────
            source_group = (config.provider.lower() if getattr(config, "provider", None) else "") or "general"
            batch_meta = BatchMetadata(
                batch_id=batch_id,
                source_name=config.source_id,
                source_group=source_group,
                extracted_at=started_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                record_count=total_records,
                checksum=artifact_checksum,
                file_size_bytes=artifact_size,
                status=BatchStatus.COMMITTED,
                committed_at=completed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                target_table=f"iceberg.bronze.{config.source_id}",
                extra_properties={"run_id": run_id, "bronze_proof": bronze_proof},
            )
            try:
                self.idempotency_controller.register_committed_batch(batch_meta)
            except Exception as meta_err:
                raise RuntimeError("Committed chunk data retained; batch metadata registration failed") from meta_err

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
                source_metadata={"batch_commit": batch_meta.to_dict(), "input_context": input_context, "physical_run_id": physical_run_id},
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
    @staticmethod
    def _layout_fingerprint(config):
        layout = {k: getattr(config, k) for k in ("source_id", "adapter_class", "format", "encoding", "chunk_size", "expected_columns", "expected_schema", "extra")}
        return hashlib.sha256(json.dumps(layout, sort_keys=True, default=str).encode()).hexdigest()

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
        snapshot = self.metadata_repo.find_snapshot_by_checksum(source_id, checksum)
        if snapshot and snapshot.get("status") == "SUCCESS" and self.bronze_writer:
            prior = self.metadata_repo.get_run(snapshot["run_id"])
            if not prior or prior.get("status") != "SUCCESS" or prior.get("checksum") != checksum or prior.get("source_id") != source_id:
                raise RuntimeError("Inconsistent persisted source snapshot/run proof")
            physical_run = (prior.get("source_metadata") or {}).get("physical_run_id", snapshot["run_id"])
            self.bronze_writer.verify_run(source_id, physical_run, prior["records_extracted"])
        return snapshot

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
        except Exception as exc:
            raise RuntimeError("Committed chunk data retained; source snapshot metadata persistence failed") from exc
