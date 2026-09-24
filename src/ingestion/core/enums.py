"""Enumerations for the ingestion framework.

Defines all status, strategy, and classification types used across
the ingestion engine, adapters, loaders, and storage layers.
"""

from enum import Enum


class LoadStrategy(str, Enum):
    """How data is extracted from a source."""

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class IngestionStatus(str, Enum):
    """Outcome status of an ingestion run or batch."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    NOT_READY = "NOT_READY"
    NO_NEW_DATA = "NO_NEW_DATA"
    SKIPPED = "SKIPPED"  # idempotent skip — same snapshot already ingested
    IN_PROGRESS = "IN_PROGRESS"
    QUARANTINED = "QUARANTINED"


class ErrorType(str, Enum):
    """Classification of errors for retry/alert decisions."""

    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    DATA_QUALITY = "DATA_QUALITY"
    SCHEMA = "SCHEMA"
    SYSTEM = "SYSTEM"


class SourceType(str, Enum):
    """How the ingestion engine accesses a source."""

    HTTP_BULK_ZIP = "HTTP_BULK_ZIP"
    HTTP_FILE = "HTTP_FILE"
    API = "API"
    LOCAL_FILE = "LOCAL_FILE"


class ArtifactFormat(str, Enum):
    """Format of the extracted data artifact."""

    CSV = "CSV"
    EXCEL = "EXCEL"
    JSON = "JSON"
    PARQUET = "PARQUET"
    ZIP = "ZIP"


class ChunkStatus(str, Enum):
    """Processing status of a single data chunk."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"
