"""Abstract base class for source-specific adapters.

Every data source (FAOSTAT, USDA, World Bank, NSO, …) must implement a
concrete subclass of :class:`BaseSourceAdapter`.  The adapter is responsible
*only* for reaching the external source and extracting raw data — no
business transformation, no Silver logic, no Gold aggregation.

Bronze ingestion is **lossless / pass-through replication**.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult


class BaseSourceAdapter(ABC):
    """Contract that every source adapter must satisfy.

    Lifecycle (called by the ingestion engine):

    1. ``check_readiness(config)``  →  can we reach the source?
    2. ``extract_full(config)``     →  full snapshot extraction
       **or**
       ``extract_incremental(config, watermark)``  →  delta extraction
    """

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------
    @abstractmethod
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Probe the source and return whether it is ready for extraction.

        Must **not** have side-effects (no data download).
        """

    # ------------------------------------------------------------------
    # Full extraction
    # ------------------------------------------------------------------
    @abstractmethod
    def extract_full(
        self,
        config: SourceConfig,
        *,
        download_dir: str,
    ) -> Iterator[DataChunk]:
        """Download/read the full dataset and yield it in chunks.

        Parameters
        ----------
        config:
            Source configuration from the registry.
        download_dir:
            Local directory where downloaded artifacts are staged.

        Yields
        ------
        DataChunk
            Successive chunks of extracted data (e.g. 50 000-row DataFrames).

        Notes
        -----
        * The adapter MUST stream / chunk large datasets — never load the
          entire file into memory.
        * The adapter MUST NOT perform business transformations.
        """

    # ------------------------------------------------------------------
    # Incremental extraction (opt-in)
    # ------------------------------------------------------------------
    def extract_incremental(
        self,
        config: SourceConfig,
        watermark: object,
        *,
        download_dir: str,
    ) -> Iterator[DataChunk]:
        """Extract only data newer than *watermark*.

        Override this method in adapters whose source genuinely supports
        incremental semantics (e.g. an API with ``updated_since``).

        The default implementation raises :class:`NotImplementedError` —
        which is correct for bulk-snapshot sources.
        """
        raise NotImplementedError(
            f"Source '{config.source_id}' does not support incremental "
            f"extraction.  Its load_strategy should be FULL."
        )

    # ------------------------------------------------------------------
    # Artifact checksum (after download)
    # ------------------------------------------------------------------
    def get_artifact_checksum(self, artifact_path: str) -> str | None:
        """Return the SHA-256 hex digest of the downloaded artifact.

        Adapters may override this for custom hashing strategies (e.g.
        hashing only the ZIP, not the extracted CSV).  The default
        implementation is handled by the engine using ``hashing.py``.
        """
        return None
