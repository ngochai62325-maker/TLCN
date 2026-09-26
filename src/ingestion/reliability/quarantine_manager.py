"""Quarantine management for Bronze layer.

Handles corrupted, empty, or schema-drifted files by:
1. Moving/uploading invalid files to {source_group}/quarantine/{timestamp}_{filename}
2. Generating a companion diagnostic file: {timestamp}_{filename}.error.json
3. Recording an audit entry with status QUARANTINED
4. Preventing pipeline crashes by returning a graceful QuarantineResult
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ingestion.reliability.bronze_quality_validator import ValidationResult
from ingestion.storage.bronze_storage_layout import BronzeStorageLayout, resolve_source_group
from ingestion.storage.metadata_schemas import (
    AuditLogEntry,
    BatchMetadata,
    BatchStatus,
    QuarantineDiagnostic,
    QuarantineErrorType,
)
from ingestion.utils.logging_config import create_ingestion_logger


@dataclass
class QuarantineResult:
    """Result returned when a file or batch is quarantined."""

    is_quarantined: bool
    quarantined_uri: str
    diagnostic_uri: str
    error_type: QuarantineErrorType
    error_message: str
    source_id: str
    batch_id: Optional[str] = None


class QuarantineManager:
    """Safely isolates corrupted or invalid data artifacts into the Bronze quarantine directory."""

    def __init__(self, layout: Optional[BronzeStorageLayout] = None) -> None:
        self.layout = layout or BronzeStorageLayout()
        self.logger = create_ingestion_logger("quarantine_manager")

    def quarantine_artifact(
        self,
        source_id: str,
        file_path: str,
        validation_result: Optional[ValidationResult] = None,
        error_type: Optional[QuarantineErrorType] = None,
        error_message: Optional[str] = None,
        run_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        traceback_str: Optional[str] = None,
        extra_details: Optional[Dict[str, Any]] = None,
    ) -> QuarantineResult:
        """Route invalid file to quarantine and write companion .error.json diagnostic."""
        source_group = resolve_source_group(source_id)
        now_utc = datetime.now(timezone.utc)
        ts_str = now_utc.strftime("%Y%m%d_%H%M%S")
        now_iso = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Resolve error details from validation_result or explicit args
        resolved_type = (
            (validation_result.error_type if validation_result else None)
            or error_type
            or QuarantineErrorType.CORRUPT_FILE
        )
        resolved_msg = (
            (validation_result.error_message if validation_result else None)
            or error_message
            or "Unknown file corruption error"
        )
        file_size = (
            (validation_result.file_size_bytes if validation_result else None)
            or (os.path.getsize(file_path) if os.path.exists(file_path) else 0)
        )
        checksum = (
            (validation_result.calculated_checksum if validation_result else None)
            or "UNKNOWN_CHECKSUM"
        )
        record_count = validation_result.record_count if validation_result else 0
        corrupted_snippet = validation_result.corrupted_snippet if validation_result else None
        schema_diff = validation_result.schema_diff if validation_result else None

        # Build diagnostic descriptor
        diagnostic = QuarantineDiagnostic(
            original_file=os.path.basename(file_path),
            quarantined_at=now_iso,
            error_type=resolved_type,
            error_message=resolved_msg,
            source_id=source_id,
            source_group=source_group,
            checksum=checksum,
            file_size_bytes=file_size,
            record_count=record_count,
            line_number=validation_result.line_number if validation_result else None,
            corrupted_snippet=corrupted_snippet,
            schema_diff=schema_diff,
            traceback=traceback_str,
        )

        # Upload quarantined file + .error.json
        upload_info = self.layout.quarantine_file(
            source_id=source_id,
            local_path=file_path,
            diagnostic=diagnostic,
            timestamp_str=ts_str,
        )

        # Persist audit record with status QUARANTINED
        if run_id:
            audit_entry = AuditLogEntry(
                run_id=run_id,
                batch_id=batch_id or f"quarantine_{ts_str}",
                source_id=source_id,
                source_group=source_group,
                started_at=now_iso,
                ended_at=now_iso,
                status=BatchStatus.QUARANTINED,
                records_processed=0,
                records_quarantined=record_count,
                error_message=resolved_msg,
                checksum=checksum,
            )
            try:
                self.layout.save_audit_entry(audit_entry)
            except Exception as e:
                self.logger.warning(f"Could not save audit entry for quarantine: {e}")

        # Update batch metadata to QUARANTINED if batch_id provided
        if batch_id:
            batch_meta = BatchMetadata(
                batch_id=batch_id,
                source_name=source_id,
                source_group=source_group,
                extracted_at=now_iso,
                record_count=record_count,
                checksum=checksum,
                file_size_bytes=file_size,
                status=BatchStatus.QUARANTINED,
            )
            try:
                self.layout.save_batch_metadata(batch_meta)
            except Exception as e:
                self.logger.warning(f"Could not update batch metadata for quarantine: {e}")

        self.logger.warning(
            f"Artifact quarantined safely without crashing: {upload_info['quarantined_uri']}",
            source_id=source_id,
            error_type=resolved_type.value,
            diagnostic=upload_info["diagnostic_uri"],
        )

        return QuarantineResult(
            is_quarantined=True,
            quarantined_uri=upload_info["quarantined_uri"],
            diagnostic_uri=upload_info["diagnostic_uri"],
            error_type=resolved_type,
            error_message=resolved_msg,
            source_id=source_id,
            batch_id=batch_id,
        )
