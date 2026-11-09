"""Persist rejected-artifact diagnostics; the original local Raw file stays put."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ingestion.reliability.bronze_quality_validator import ValidationResult
from ingestion.storage.metadata_schemas import QuarantineErrorType


class QuarantinePersistenceError(RuntimeError):
    """Input was rejected, but its diagnostic could not be durably recorded."""


@dataclass
class QuarantineResult:
    is_quarantined: bool
    quarantined_uri: str  # Existing Raw URI, NOT an archival/copy claim.
    diagnostic_uri: str
    error_type: QuarantineErrorType
    error_message: str
    source_id: str
    batch_id: Optional[str] = None


class QuarantineManager:
    def __init__(self, metadata_repo=None):
        self.metadata_repo = metadata_repo

    def quarantine_artifact(
        self, source_id: str, file_path: str,
        validation_result: Optional[ValidationResult] = None,
        error_type: Optional[QuarantineErrorType] = None, error_message: Optional[str] = None,
        run_id: Optional[str] = None, batch_id: Optional[str] = None,
        traceback_str: Optional[str] = None, extra_details: Optional[Dict[str, Any]] = None,
    ) -> QuarantineResult:
        for name, value in (("source_id", source_id), ("run_id", run_id), ("file_path", file_path)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if validation_result is not None and not isinstance(validation_result, ValidationResult):
            raise ValueError("validation_result must be a ValidationResult")
        if validation_result is not None and validation_result.is_valid:
            raise ValueError("Cannot quarantine an artifact that passed validation")
        kind = (validation_result.error_type if validation_result else None) or error_type
        message = (validation_result.error_message if validation_result else None) or error_message
        if not isinstance(kind, QuarantineErrorType) or not isinstance(message, str) or not message.strip():
            raise ValueError("Missing validation context: error_type and error_message are required")
        path = Path(file_path).resolve()
        checksum = validation_result.calculated_checksum if validation_result else None
        if not checksum and path.is_file():
            from ingestion.utils.hashing import compute_file_checksum
            checksum = compute_file_checksum(str(path))
        diagnostic = {
            "source_id": source_id, "run_id": run_id, "batch_id": batch_id,
            "original_file": str(path), "checksum": checksum,
            "file_size_bytes": path.stat().st_size if path.is_file() else None,
            "error_type": kind.value, "error_message": message,
            "record_count": validation_result.record_count if validation_result else None,
            "line_number": validation_result.line_number if validation_result else None,
            "schema_diff": validation_result.schema_diff if validation_result else None,
            "corrupted_snippet": validation_result.corrupted_snippet if validation_result else None,
            "validation_details": validation_result.details if validation_result else None,
            "traceback": traceback_str, "context": extra_details,
        }
        diagnostic_id = hashlib.sha256(json.dumps(diagnostic, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        diagnostic["diagnostic_id"] = diagnostic_id
        diagnostic["quarantined_at"] = datetime.now(timezone.utc).isoformat()
        try:
            if self.metadata_repo is None:
                raise RuntimeError("No diagnostic persistence repository configured")
            record_id = self.metadata_repo.persist_quarantine_diagnostic(diagnostic)
            if type(record_id) is not int or record_id <= 0:
                raise RuntimeError("Diagnostic persistence returned no durable record identity")
        except Exception as exc:
            raise QuarantinePersistenceError(f"Rejected input ({kind.value}: {message}); diagnostic persistence failed: {exc}") from exc
        return QuarantineResult(True, path.as_uri() if path.is_file() else "",
                                f"postgresql://ingestion.dead_letter_records/{record_id}",
                                kind, message, source_id, batch_id)
