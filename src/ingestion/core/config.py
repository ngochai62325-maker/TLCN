"""Configuration dataclasses for sources, retry policies, and readiness checks.

These are the declarative configuration objects loaded from source_registry.yaml
and used by the ingestion engine, adapters, and loaders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ingestion.core.enums import ArtifactFormat, LoadStrategy, SourceType


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RetryConfig:
    """Configurable retry policy with exponential backoff."""

    max_attempts: int = 5
    initial_delay_seconds: float = 2.0
    max_delay_seconds: float = 60.0
    exponential_backoff: bool = True
    jitter: bool = True
    respect_retry_after: bool = True


# ---------------------------------------------------------------------------
# Readiness check configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReadinessConfig:
    """What to check before starting ingestion for a source."""

    require_http_200: bool = True
    require_content_length: bool = False
    min_file_size_bytes: int = 0
    required_columns: List[str] = field(default_factory=list)
    expected_content_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Source configuration (loaded from registry YAML)
# ---------------------------------------------------------------------------
@dataclass
class SourceConfig:
    """Complete configuration for a single data source.

    Loaded from ``source_registry.yaml`` by :class:`SourceRegistry`.
    Passed to adapters, loaders, and the ingestion engine.
    """

    source_id: str
    provider: str
    dataset: str
    source_type: SourceType
    load_strategy: LoadStrategy
    format: ArtifactFormat

    # Access
    endpoint: Optional[str] = None
    local_path: Optional[str] = None

    # Processing
    chunk_size: int = 50_000
    encoding: str = "utf-8"

    # Policies
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Scheduling / metadata
    frequency: str = "annual"
    watermark_column: Optional[str] = None
    business_key: Optional[List[str]] = None
    partition_column: Optional[str] = None
    expected_columns: Optional[List[str]] = None

    # Adapter binding
    adapter_class: Optional[str] = None

    # Extension point — arbitrary provider-specific settings
    extra: Dict[str, Any] = field(default_factory=dict)
