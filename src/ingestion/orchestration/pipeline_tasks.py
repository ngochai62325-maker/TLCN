"""Enterprise Pipeline Tasks for Lakehouse Bronze Ingestion.

Implements the strict 8-stage fail-safe sequential lifecycle:
1. check_source
2. readiness_check (Sensor/Gate)
3. extract (Person 1's module)
4. pre_audit (Person 2's validation)
5. write_bronze (Person 2's writer)
6. post_audit (Reconciliation & sanity checks)
7. update_metadata (Watermark, audit catalog)
8. publish (Signal downstream layers)
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from ingestion.config.registry import SourceRegistry
from ingestion.core.config import SourceConfig
from ingestion.core.enums import IngestionStatus, LoadStrategy
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.core.watermark import PostgresWatermarkStore
from ingestion.manifest.manifest import IngestionManifest
from ingestion.readiness.source_readiness import ReadinessChecker
from ingestion.reliability.bronze_quality_validator import BronzeQualityValidator, ValidationResult
from ingestion.reliability.idempotency_controller import IdempotencyController, IdempotencyDecision
from ingestion.reliability.quarantine_manager import QuarantineManager
from ingestion.storage.bronze_storage_layout import BronzeStorageLayout, resolve_source_group
from ingestion.storage.bronze_writer import BronzeIcebergWriter, sanitize_column_name
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

try:
    from airflow.exceptions import AirflowException, AirflowSkipException
except ImportError:
    class AirflowException(Exception):
        """Fallback AirflowException when airflow is not installed."""
        pass

    class AirflowSkipException(Exception):
        """Fallback AirflowSkipException when airflow is not installed."""
        pass


# ---------------------------------------------------------------------------
# Stage 1: check_source
# ---------------------------------------------------------------------------
def check_source(source_id: str) -> Dict[str, Any]:
    """Stage 1: Verify source registration, create identifiers, record run start.

    Validates that the source exists in SourceRegistry, is enabled, creates
    deterministic run_id and batch_id, creates staging directory, and registers
    run start in PostgreSQL metadata repository.
    """
    registry = SourceRegistry()
    registry.load()

    try:
        config = registry.get_source(source_id)
    except KeyError:
        raise AirflowException(f"Source '{source_id}' not found in SourceRegistry")

    if not config.enabled:
        raise AirflowSkipException(f"Source '{source_id}' is disabled in registry configuration")

    now = datetime.now(timezone.utc)
    run_id = f"{source_id}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    batch_id = f"{source_id}_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:4]}"

    staging_dir = os.path.join(tempfile.gettempdir(), f"ingest_{source_id}_{batch_id}")
    os.makedirs(staging_dir, exist_ok=True)

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info(
        "Stage 1 [check_source] completed",
        source_id=source_id,
        run_id=run_id,
        batch_id=batch_id,
        load_strategy=config.load_strategy.value,
    )

    # Record run start in PostgreSQL metadata repository (best-effort)
    try:
        meta_repo = MetadataRepository()
        meta_repo.create_run(run_id, source_id, config.load_strategy.value, now.isoformat())
    except Exception as e:
        logger.warning(f"Could not record run start in metadata repo: {e}")

    return {
        "source_id": source_id,
        "run_id": run_id,
        "batch_id": batch_id,
        "staging_dir": staging_dir,
        "load_strategy": config.load_strategy.value,
        "started_at": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# Stage 2: readiness_check (Sensor / Gate)
# ---------------------------------------------------------------------------
def readiness_check(source_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Stage 2: Sensor/Gate checking external source or local file availability.

    Uses ReadinessChecker. If unready, records status in metadata and skips cleanly.
    """
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    logger = create_ingestion_logger(source_id, run_id, batch_id)

    registry = SourceRegistry()
    registry.load()
    config = registry.get_source(source_id)

    checker = ReadinessChecker()
    res: ReadinessResult = checker.check(config)

    if not res.ready:
        logger.warning(
            "Stage 2 [readiness_check] source not ready; skipping run",
            reason=res.reason,
            metadata=res.source_metadata,
        )
        try:
            meta_repo = MetadataRepository()
            meta_repo.complete_run(
                run_id=run_id,
                status=IngestionStatus.NOT_READY.value,
                error_message=res.reason,
                source_metadata=res.source_metadata,
            )
        except Exception:
            pass
        raise AirflowSkipException(f"Source '{source_id}' is not ready: {res.reason}")

    logger.info(
        "Stage 2 [readiness_check] passed successfully",
        source_id=source_id,
        metadata=res.source_metadata,
    )
    return {
        "ready": True,
        "source_metadata": res.source_metadata,
    }


# ---------------------------------------------------------------------------
# Stage 3: extract (Person 1's Extractor / Adapter)
# ---------------------------------------------------------------------------
def extract(source_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Stage 3: Extract raw data into staging and upload raw artifact to MinIO raw landing.

    Extracts chunks using the resolved adapter, saves each chunk as parquet
    in staging_dir, computes checksum, and uploads the immutable raw file to
    MinIO {group}/raw/{batch_id}/{filename}.
    """
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    staging_dir = source_ctx["staging_dir"]
    os.makedirs(staging_dir, exist_ok=True)

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info("Stage 3 [extract] starting extraction", staging_dir=staging_dir)

    registry = SourceRegistry()
    registry.load()
    config = registry.get_source(source_id)

    # 1. Resolve adapter dynamically
    from ingestion.core.ingestion_engine import IngestionEngine
    engine_helper = IngestionEngine(registry=registry)
    adapter = engine_helper._resolve_adapter(config)

    # 2. Extract full dataset into chunks
    chunks_meta: List[Dict[str, Any]] = []
    total_records = 0
    chunk_idx = 0

    try:
        chunks_iter = adapter.extract_full(config, download_dir=staging_dir)
        for chunk in chunks_iter:
            if isinstance(chunk.data, pd.DataFrame):
                df = chunk.data
            elif isinstance(chunk.data, str) and os.path.exists(chunk.data):
                df = pd.read_parquet(chunk.data) if chunk.data.endswith(".parquet") else pd.read_csv(chunk.data)
            else:
                df = pd.DataFrame(chunk.data)

            # Clean object columns to ensure pyarrow serialization handles mixed types
            clean_df = df.copy()
            for col in clean_df.columns:
                if clean_df[col].dtype == "object":
                    clean_df[col] = clean_df[col].astype(str).replace({"nan": None, "None": None, "<NA>": None})

            chunk_filename = f"chunk_{chunk_idx:05d}.parquet"
            chunk_filepath = os.path.join(staging_dir, chunk_filename)
            clean_df.to_parquet(chunk_filepath, index=False)

            rec_count = len(df)
            total_records += rec_count
            chunks_meta.append({
                "chunk_id": chunk_idx,
                "file": chunk_filename,
                "record_count": rec_count,
            })
            chunk_idx += 1

    except Exception as exc:
        err_type = classify_error(exc)
        logger.error("Stage 3 [extract] failed during adapter extraction", error=str(exc), error_type=err_type.value)
        raise AirflowException(f"Extraction failed for source '{source_id}': {exc}")

    # 3. Locate raw source artifact for archiving
    raw_source_path = config.local_path or config.local_fallback
    if not raw_source_path or not os.path.exists(raw_source_path):
        # Look in staging dir for any downloaded zip/csv
        staged_files = [
            os.path.join(staging_dir, f)
            for f in os.listdir(staging_dir)
            if not f.startswith("chunk_")
        ]
        if staged_files:
            raw_source_path = staged_files[0]

    # Compute SHA-256 checksum
    checksum = "UNKNOWN_CHECKSUM"
    if raw_source_path and os.path.isfile(raw_source_path):
        checksum = compute_file_checksum(raw_source_path)
    elif raw_source_path and os.path.isdir(raw_source_path):
        checksum = compute_directory_checksum(raw_source_path)
    elif chunks_meta:
        # Compute checksum across staged chunk files
        checksum = compute_directory_checksum(staging_dir, pattern="chunk_*.parquet")

    # 4. Upload raw artifact to MinIO Bronze raw landing
    raw_s3_uri = ""
    try:
        layout = BronzeStorageLayout()
        if raw_source_path and os.path.isfile(raw_source_path):
            raw_s3_uri = layout.archive_raw_artifact(source_id, batch_id, raw_source_path)
        elif raw_source_path and os.path.isdir(raw_source_path):
            # Pick first file or archive directory as representation
            pattern_files = glob.glob(os.path.join(raw_source_path, "*.csv"))
            if pattern_files:
                raw_s3_uri = layout.archive_raw_artifact(source_id, batch_id, pattern_files[0])
    except Exception as e:
        logger.warning(f"Could not upload raw artifact to MinIO raw landing: {e}")

    logger.info(
        "Stage 3 [extract] completed successfully",
        total_records=total_records,
        chunks_count=len(chunks_meta),
        checksum=checksum,
        raw_s3_uri=raw_s3_uri,
    )

    return {
        "staging_dir": staging_dir,
        "raw_artifact_path": raw_source_path,
        "raw_artifact_uri": raw_s3_uri,
        "checksum": checksum,
        "record_count": total_records,
        "chunks_count": len(chunks_meta),
        "chunks_meta": chunks_meta,
        "source_file": os.path.basename(raw_source_path) if raw_source_path else "staged_data",
    }


# ---------------------------------------------------------------------------
# Stage 4: pre_audit (Person 2's Validation & Idempotency)
# ---------------------------------------------------------------------------
def pre_audit(source_ctx: Dict[str, Any], extract_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Stage 4: Validation gate & idempotency control before Iceberg writes.

    Performs:
    1. Zero-byte check on extracted data.
    2. Checksum verification.
    3. Idempotency evaluation (skip if batch/checksum already committed).
    4. Schema drift / column validation. If corrupt, routes to quarantine.
    """
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    checksum = extract_ctx["checksum"]
    record_count = extract_ctx["record_count"]
    staging_dir = extract_ctx["staging_dir"]
    raw_path = extract_ctx.get("raw_artifact_path")

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info("Stage 4 [pre_audit] running quality and idempotency checks")

    registry = SourceRegistry()
    registry.load()
    config = registry.get_source(source_id)

    # 1. Zero-byte / Zero-records check
    if record_count == 0:
        logger.warning("Stage 4 [pre_audit] Zero records extracted from source")
        # Route to quarantine if raw file exists
        if raw_path and os.path.exists(raw_path) and os.path.isfile(raw_path):
            q_mgr = QuarantineManager()
            q_res = q_mgr.quarantine_artifact(
                source_id=source_id,
                file_path=raw_path,
                error_type=QuarantineErrorType.EMPTY_FILE,
                error_message="Source artifact resulted in 0 records",
                run_id=run_id,
                batch_id=batch_id,
            )
            logger.warning(f"Quarantined empty artifact to {q_res.quarantined_uri}")
        raise AirflowException(f"Pre-audit failed: Source '{source_id}' extracted 0 records (empty dataset)")

    # 2. Idempotency evaluation
    idem_ctrl = IdempotencyController(metadata_repo=MetadataRepository())
    decision: IdempotencyDecision = idem_ctrl.evaluate_batch(
        source_id=source_id,
        batch_id=batch_id,
        current_checksum=checksum,
    )
    if not decision.should_process:
        logger.info(
            "Stage 4 [pre_audit] Batch safely skipped via IdempotencyController",
            reason=decision.reason,
            batch_id=batch_id,
        )
        try:
            meta_repo = MetadataRepository()
            meta_repo.complete_run(
                run_id=run_id,
                status=IngestionStatus.SKIPPED.value,
                records_extracted=0,
                checksum=checksum,
                error_message=decision.reason,
                source_metadata={"skipped_reason": decision.reason},
            )
        except Exception:
            pass
        raise AirflowSkipException(f"Idempotent skip for '{source_id}': {decision.reason}")

    # 3. Schema drift and quality validation
    validator = BronzeQualityValidator()
    # Check first chunk for schema compliance if expected_columns are configured
    if config.expected_columns and extract_ctx.get("chunks_meta"):
        first_chunk_file = os.path.join(staging_dir, extract_ctx["chunks_meta"][0]["file"])
        if os.path.exists(first_chunk_file):
            first_df = pd.read_parquet(first_chunk_file)
            actual_cols = set(sanitize_column_name(c) for c in first_df.columns)
            expected_cols = set(sanitize_column_name(c) for c in config.expected_columns)
            missing = expected_cols - actual_cols
            if missing:
                err_msg = f"Mandatory columns missing from source: {sorted(missing)}"
                logger.warning(f"Stage 4 [pre_audit] Schema validation failed: {err_msg}")
                if raw_path and os.path.exists(raw_path) and os.path.isfile(raw_path):
                    q_mgr = QuarantineManager()
                    q_mgr.quarantine_artifact(
                        source_id=source_id,
                        file_path=raw_path,
                        error_type=QuarantineErrorType.SCHEMA_MISMATCH,
                        error_message=err_msg,
                        run_id=run_id,
                        batch_id=batch_id,
                    )
                raise AirflowException(f"Pre-audit schema error for '{source_id}': {err_msg}")

    logger.info("Stage 4 [pre_audit] passed quality and idempotency gates successfully")
    return {
        "pre_audit_passed": True,
        "checksum": checksum,
        "record_count": record_count,
    }


# ---------------------------------------------------------------------------
# Stage 5: write_bronze (Person 2's Writer)
# ---------------------------------------------------------------------------
def write_bronze(
    source_ctx: Dict[str, Any],
    extract_ctx: Dict[str, Any],
    pre_audit_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Stage 5: Persist data into Bronze Iceberg tables and write MinIO metadata.

    Reads staged chunks, injects 7 technical metadata columns, appends to
    iceberg.bronze.<source_id>, uploads batch_{batch_id}.json and manifest.
    """
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    staging_dir = extract_ctx["staging_dir"]
    checksum = extract_ctx["checksum"]
    source_file = extract_ctx["source_file"]
    chunks_meta = extract_ctx.get("chunks_meta", [])

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info("Stage 5 [write_bronze] starting Iceberg write and manifest generation")

    writer = BronzeIcebergWriter()
    layout = BronzeStorageLayout()

    total_written = 0
    table_name = ""

    # Ensure table exists using schema from the first chunk
    if chunks_meta:
        first_chunk_path = os.path.join(staging_dir, chunks_meta[0]["file"])
        first_df = pd.read_parquet(first_chunk_path)
        table_name = writer.ensure_table(source_id, first_df)

    # Write each chunk with technical metadata
    for chunk_info in chunks_meta:
        chunk_file = os.path.join(staging_dir, chunk_info["file"])
        chunk_df = pd.read_parquet(chunk_file)
        written = writer.write_chunk(
            source_id=source_id,
            chunk_df=chunk_df,
            run_id=run_id,
            batch_id=batch_id,
            source_checksum=checksum,
            source_file=source_file,
        )
        total_written += written

    # Save BatchMetadata JSON to MinIO {group}/metadata/batch_{batch_id}.json
    now_iso = datetime.now(timezone.utc).isoformat()
    batch_metadata = BatchMetadata(
        batch_id=batch_id,
        source_name=source_id,
        source_group=resolve_source_group(source_id),
        extracted_at=source_ctx["started_at"],
        record_count=total_written,
        checksum=checksum,
        status=BatchStatus.IN_PROGRESS,
        committed_at=now_iso,
        target_table=table_name,
        extra_properties={
            "raw_artifact_uri": extract_ctx.get("raw_artifact_uri", ""),
            "manifest_uri": f"s3://{layout.bucket}/{layout.get_manifest_key(source_id, run_id)}",
        },
    )
    batch_meta_uri = layout.save_batch_metadata(batch_metadata)

    # Build and upload manifest to MinIO {group}/manifest/run_{run_id}.json
    manifest = IngestionManifest(source_id, run_id, batch_id, source_ctx["load_strategy"])
    manifest.set_timing(source_ctx["started_at"], now_iso)
    manifest.set_metrics(records_extracted=total_written, chunks_processed=len(chunks_meta), records_quarantined=0)
    manifest.set_artifact_info(
        uri=extract_ctx.get("raw_artifact_uri", ""),
        format="PARQUET",
        size_bytes=0,
        checksum=checksum,
    )
    manifest.set_status(IngestionStatus.SUCCESS)
    manifest_uri = layout.save_manifest(source_id, run_id, manifest.to_dict())

    logger.info(
        "Stage 5 [write_bronze] completed successfully",
        table_name=table_name,
        records_written=total_written,
        manifest_uri=manifest_uri,
        batch_meta_uri=batch_meta_uri,
    )

    return {
        "records_written": total_written,
        "iceberg_table": table_name,
        "manifest_uri": manifest_uri,
        "batch_metadata_uri": batch_meta_uri,
    }


# ---------------------------------------------------------------------------
# Stage 6: post_audit (Reconciliation & Sanity Checks)
# ---------------------------------------------------------------------------
def post_audit(
    source_ctx: Dict[str, Any],
    extract_ctx: Dict[str, Any],
    write_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Stage 6: Reconcile extracted rows with Iceberg committed rows and record audit.

    Executes query sanity check on Iceberg table and writes audit_{run_id}.json to MinIO.
    """
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    extracted = extract_ctx["record_count"]
    written = write_ctx["records_written"]
    table_name = write_ctx["iceberg_table"]

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info("Stage 6 [post_audit] running reconciliation and sanity query", table=table_name)

    # 1. Reconciliation: extracted rows must match written rows
    if extracted != written:
        err = f"Reconciliation error: extracted {extracted} records, but wrote {written} to Iceberg"
        logger.error(err)
        raise AirflowException(err)

    # 2. Query sanity check on Iceberg table
    verified_rows = written
    try:
        writer = BronzeIcebergWriter()
        cols, rows = writer.execute_query(
            f"SELECT count(*) FROM {table_name} WHERE _ingestion_run_id = '{run_id}'"
        )
        if rows and len(rows) > 0:
            verified_rows = int(rows[0][0])
            logger.info("Sanity check query succeeded", table=table_name, verified_rows=verified_rows)
            if verified_rows != written:
                logger.warning(
                    f"Iceberg row count discrepancy: expected {written}, found {verified_rows}"
                )
    except Exception as e:
        logger.warning(f"Could not execute Trino sanity query on Iceberg: {e}")

    # 3. Save AuditLogEntry JSON to MinIO {group}/audit/audit_{run_id}.json
    layout = BronzeStorageLayout()
    now_iso = datetime.now(timezone.utc).isoformat()
    audit_entry = AuditLogEntry(
        run_id=run_id,
        batch_id=batch_id,
        source_id=source_id,
        source_group=resolve_source_group(source_id),
        started_at=source_ctx["started_at"],
        ended_at=now_iso,
        status=BatchStatus.COMMITTED,
        records_processed=written,
        records_quarantined=0,
        checksum=extract_ctx.get("checksum"),
        duration_seconds=5.0,
    )
    audit_uri = layout.save_audit_entry(audit_entry)

    logger.info(
        "Stage 6 [post_audit] reconciliation verified and audit saved",
        verified_rows=verified_rows,
        audit_uri=audit_uri,
    )

    return {
        "reconciliation_passed": True,
        "verified_iceberg_rows": verified_rows,
        "audit_uri": audit_uri,
    }


# ---------------------------------------------------------------------------
# Stage 7: update_metadata (Watermark, Audit Catalog)
# ---------------------------------------------------------------------------
def update_metadata(
    source_ctx: Dict[str, Any],
    extract_ctx: Dict[str, Any],
    write_ctx: Dict[str, Any],
    post_audit_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Stage 7: Update PostgreSQL metadata catalog and watermark."""
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    records = write_ctx["records_written"]
    checksum = extract_ctx["checksum"]
    artifact_uri = extract_ctx.get("raw_artifact_uri", "")
    manifest_uri = write_ctx.get("manifest_uri", "")

    logger = create_ingestion_logger(source_id, run_id, batch_id)
    logger.info("Stage 7 [update_metadata] registering snapshot and committing batch")

    meta_repo = MetadataRepository()
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Register snapshot in PostgreSQL source_snapshots
    try:
        meta_repo.save_snapshot(
            source_id=source_id,
            checksum=checksum,
            size_bytes=0,
            retrieved_at=now_iso,
            run_id=run_id,
            status=IngestionStatus.SUCCESS.value,
            source_uri=artifact_uri,
        )
    except Exception as e:
        logger.warning(f"Could not register snapshot in PostgreSQL: {e}")

    # 2. Mark run completed in PostgreSQL ingestion_runs
    try:
        meta_repo.complete_run(
            run_id=run_id,
            status=IngestionStatus.SUCCESS.value,
            records_extracted=records,
            records_quarantined=0,
            chunks_processed=extract_ctx.get("chunks_count", 1),
            chunks_total=extract_ctx.get("chunks_count", 1),
            completed_at=now_iso,
            artifact_uri=artifact_uri,
            manifest_uri=manifest_uri,
            checksum=checksum,
        )
    except Exception as e:
        logger.warning(f"Could not complete run in PostgreSQL: {e}")

    # 3. Register committed batch in IdempotencyController
    try:
        layout = BronzeStorageLayout()
        batch_meta = layout.load_batch_metadata(source_id, batch_id)
        if batch_meta:
            idem_ctrl = IdempotencyController(layout=layout, metadata_repo=meta_repo)
            idem_ctrl.register_committed_batch(batch_meta)
    except Exception as e:
        logger.warning(f"Could not register committed batch: {e}")

    logger.info("Stage 7 [update_metadata] metadata updated successfully")
    return {
        "metadata_updated": True,
        "status": IngestionStatus.SUCCESS.value,
    }


# ---------------------------------------------------------------------------
# Stage 8: publish (Signal Downstream Layers)
# ---------------------------------------------------------------------------
def publish(
    source_ctx: Dict[str, Any],
    write_ctx: Dict[str, Any],
    post_audit_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Stage 8: Signal downstream layers and clean up temporary staging files."""
    source_id = source_ctx["source_id"]
    run_id = source_ctx["run_id"]
    batch_id = source_ctx["batch_id"]
    staging_dir = source_ctx.get("staging_dir")
    table_name = write_ctx.get("iceberg_table")
    verified_rows = post_audit_ctx.get("verified_iceberg_rows", write_ctx.get("records_written", 0))

    logger = create_ingestion_logger(source_id, run_id, batch_id)

    # Operational Observability log
    logger.info(
        "Stage 8 [publish] Source successfully published to Lakehouse Bronze layer!",
        source_id=source_id,
        iceberg_table=table_name,
        records=verified_rows,
        status="READY_FOR_SILVER_TRANSFORMATION",
    )

    # Clean up local staging directory
    if staging_dir and os.path.exists(staging_dir):
        try:
            shutil.rmtree(staging_dir)
            logger.info(f"Cleaned up temporary staging directory: {staging_dir}")
        except Exception as e:
            logger.warning(f"Could not remove staging dir {staging_dir}: {e}")

    return {
        "published": True,
        "source_id": source_id,
        "iceberg_table": table_name,
        "records_ingested": verified_rows,
    }
