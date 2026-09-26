"""Metadata schemas for Bronze Lakehouse storage layer.

Defines standardized data models for:
1. Batch Metadata (batch_id, source_name, extracted_at, record_count, checksum, etc.)
2. Audit Logs (run_id, batch_id, execution history, status)
3. Quarantine Diagnostics (error classification, line/row details, corrupted sample)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class BatchStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMMITTED = "COMMITTED"
    QUARANTINED = "QUARANTINED"
    FAILED = "FAILED"
    SKIPPED_IDEMPOTENT = "SKIPPED_IDEMPOTENT"


class QuarantineErrorType(str, Enum):
    EMPTY_FILE = "EMPTY_FILE"
    CORRUPT_FILE = "CORRUPT_FILE"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    RECORD_COUNT_MISMATCH = "RECORD_COUNT_MISMATCH"
    PARSE_FAILURE = "PARSE_FAILURE"


@dataclass
class BatchMetadata:
    """Standardized batch metadata model for Bronze layer."""

    batch_id: str
    source_name: str
    source_group: str
    extracted_at: str
    record_count: int
    checksum: str
    file_size_bytes: int = 0
    schema_version: str = "1.0"
    status: BatchStatus = BatchStatus.IN_PROGRESS
    committed_at: Optional[str] = None
    target_table: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    extra_properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if isinstance(self.status, BatchStatus):
            data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> BatchMetadata:
        d = dict(data)
        if "status" in d and isinstance(d["status"], str):
            d["status"] = BatchStatus(d["status"])
        return cls(**d)


@dataclass
class AuditLogEntry:
    """Standardized audit log entry for batch execution tracking."""

    run_id: str
    batch_id: str
    source_id: str
    source_group: str
    started_at: str
    ended_at: Optional[str] = None
    status: BatchStatus = BatchStatus.IN_PROGRESS
    records_processed: int = 0
    records_quarantined: int = 0
    error_message: Optional[str] = None
    checksum: Optional[str] = None
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if isinstance(self.status, BatchStatus):
            data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditLogEntry:
        d = dict(data)
        if "status" in d and isinstance(d["status"], str):
            d["status"] = BatchStatus(d["status"])
        return cls(**d)


@dataclass
class QuarantineDiagnostic:
    """Standardized companion diagnostic (.error.json) for quarantined files."""

    original_file: str
    quarantined_at: str
    error_type: QuarantineErrorType
    error_message: str
    source_id: str
    source_group: str
    checksum: str
    file_size_bytes: int
    record_count: int = 0
    line_number: Optional[int] = None
    corrupted_snippet: Optional[str] = None
    schema_diff: Optional[Dict[str, Any]] = None
    traceback: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if isinstance(self.error_type, QuarantineErrorType):
            data["error_type"] = self.error_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> QuarantineDiagnostic:
        d = dict(data)
        if "error_type" in d and isinstance(d["error_type"], str):
            d["error_type"] = QuarantineErrorType(d["error_type"])
        return cls(**d)
