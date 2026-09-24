"""Abstract base loader and loader interface.

The loader sits between the ingestion engine and the adapter.  It
orchestrates the extraction lifecycle (checkpoints, watermark safety,
manifest assembly) while delegating actual data retrieval to the adapter.

Concrete implementations:
    * :class:`FullLoader`         – for ``load_strategy = FULL``
    * :class:`IncrementalLoader`  – for ``load_strategy = INCREMENTAL``
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ingestion.core.config import SourceConfig
from ingestion.core.result import IngestionResult


class BaseLoader(ABC):
    """Contract that every loader (Full / Incremental) must satisfy."""

    @abstractmethod
    def execute(
        self,
        config: SourceConfig,
        run_id: str,
        batch_id: str,
    ) -> IngestionResult:
        """Run the extraction lifecycle and return an :class:`IngestionResult`.

        The loader is responsible for:

        1. Calling the adapter to extract data (full or incremental).
        2. Computing checksums.
        3. Managing checkpoints (for chunked processing).
        4. Building the ingestion manifest.
        5. Uploading the artifact to staging storage.
        6. Returning a complete :class:`IngestionResult`.

        The loader MUST NOT:

        * Update the watermark — that is the engine's job *after* confirming
          success.
        * Write to Bronze Iceberg tables — that is Person 2's job.
        * Apply business transformations.
        """
