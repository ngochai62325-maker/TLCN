"""Result and data-transfer contracts.

These dataclasses form the interface between Person 1 (Ingestion Engine)
and Person 2 (Bronze Writer).  Person 2 consumes :class:`IngestionResult`
without needing to know adapter internals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ingestion.core.enums import ArtifactFormat, IngestionStatus, LoadStrategy


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Chunked data transfer
# ---------------------------------------------------------------------------
@dataclass
class DataChunk:
    """A single chunk of extracted data within a batch.

    Used by adapters that yield data in chunks (e.g., 50 000-row
    pandas DataFrames from an 8 GB CSV).
    """

    chunk_id: int
    data: Any  # pandas DataFrame, bytes, or file path
    row_start: int
    row_end: int
    record_count: int
    checksum: Optional[str] = None


# ---------------------------------------------------------------------------
# Readiness probe result
# ---------------------------------------------------------------------------
@dataclass
class ReadinessResult:
    """Standardised result of a source readiness check."""

    ready: bool
    reason: str
    checked_at: datetime = field(default_factory=_utcnow)
    source_metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Primary contract: IngestionResult
# ---------------------------------------------------------------------------
@dataclass
class IngestionResult:
    """The artifact-based handoff contract between Person 1 and Person 2.

    After an ingestion run completes (successfully or not), this object is
    the single source of truth describing what happened.  Person 2 (Bronze
    Writer) uses ``artifact_uri`` and ``manifest`` to locate, validate, and
    load the extracted data into Bronze Iceberg tables.

    Person 3 (Airflow) inspects ``status`` to decide on retry / skip /
    alert.
    """

    # Identity
    source_id: str
    run_id: str
    batch_id: str

    # Classification
    load_type: LoadStrategy
    status: IngestionStatus

    # Artifact location (MinIO / local staging)
    artifact_uri: Optional[str] = None
    manifest_uri: Optional[str] = None
    data_format: Optional[ArtifactFormat] = None

    # Metrics
    records_extracted: int = 0
    records_quarantined: int = 0
    chunks_processed: int = 0
    chunks_total: int = 0

    # Integrity
    checksum: Optional[str] = None

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Watermark (only meaningful for INCREMENTAL sources)
    watermark_before: Optional[Any] = None
    watermark_after: Optional[Any] = None

    # Source provenance
    source_metadata: Dict[str, Any] = field(default_factory=dict)

    # Error detail
    error_message: Optional[str] = None
    error_type: Optional[str] = None

    # Full manifest dict (written to manifest.json alongside artifact)
    manifest: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status in (IngestionStatus.SUCCESS, IngestionStatus.NO_NEW_DATA)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dict (JSON-safe after datetime conversion)."""
        d: Dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif isinstance(v, (LoadStrategy, IngestionStatus, ArtifactFormat)):
                d[k] = v.value
            else:
                d[k] = v
        return d
