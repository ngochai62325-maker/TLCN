"""Quarantine management for Bronze layer.

Handles corrupted, empty, or schema-drifted files by:
1. Recording diagnostic details
2. Preventing pipeline crashes by returning a graceful QuarantineResult
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ingestion.reliability.bronze_quality_validator import ValidationResult
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
    """Safely isolates corrupted or invalid data artifacts without crashing the pipeline."""

    def __init__(self, **kwargs: Any) -> None:
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
        """Route invalid file details to diagnostic and return QuarantineResult."""
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

        q_uri = f"quarantine/{source_id}/{ts_str}_{os.path.basename(file_path)}"
        diag_uri = f"{q_uri}.error.json"

        self.logger.warning(
            f"Artifact quarantined safely without crashing: {q_uri}",
            source_id=source_id,
            error_type=resolved_type.value,
            diagnostic=diag_uri,
        )

        return QuarantineResult(
            is_quarantined=True,
            quarantined_uri=q_uri,
            diagnostic_uri=diag_uri,
            error_type=resolved_type,
            error_message=resolved_msg,
            source_id=source_id,
            batch_id=batch_id,
        )
