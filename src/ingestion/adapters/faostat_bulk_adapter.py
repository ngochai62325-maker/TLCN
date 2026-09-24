"""FAOSTAT Bulk ZIP Download Adapter.

Handles downloading ZIP archives from FAOSTAT's bulk download service,
extracting the CSV, and yielding data in memory-safe chunks.

Features:
- Streamed download to disk (avoids keeping multi-hundred MB ZIP in RAM).
- Local cache check: avoids re-downloading if ZIP already exists in staging.
- Memory-safe chunked CSV parsing (e.g. 50,000 rows per chunk).
- Fast C-level skiprows support for resuming after checkpoint failure.
- In-memory chunk checksum computation.
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import pandas as pd

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.config import SourceConfig
from ingestion.core.enums import ArtifactFormat, ChunkStatus, ErrorType, IngestionStatus, LoadStrategy, SourceType
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.utils.error_classifier import DataQualityError, PermanentError, TransientError
from ingestion.utils.hashing import compute_bytes_checksum, compute_file_checksum
from ingestion.utils.http_client import HttpClient
from ingestion.utils.logging_config import create_ingestion_logger


class FaostatBulkAdapter(BaseSourceAdapter):
    """Adapter for FAOSTAT bulk download datasets."""

    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Check availability of the FAOSTAT endpoint via HEAD request."""
        logger = create_ingestion_logger(config.source_id)
        if not config.endpoint:
            return ReadinessResult(ready=False, reason="Missing endpoint in configuration")

        try:
            client = HttpClient(config.retry)
            head_result = client.head(config.endpoint)

            if hasattr(head_result, "status_code") and head_result.status_code != 200:
                return ReadinessResult(
                    ready=False,
                    reason=f"HTTP endpoint returned status {head_result.status_code}",
                )

            metadata: Dict[str, Any] = {}
            if hasattr(head_result, "headers"):
                headers = head_result.headers
                content_length = headers.get("Content-Length")
                if config.readiness and config.readiness.require_content_length and not content_length:
                    return ReadinessResult(ready=False, reason="Missing Content-Length header")

                content_type = headers.get("Content-Type")
                if content_type:
                    metadata["content_type"] = content_type
                if content_length:
                    metadata["size"] = int(content_length)
                metadata["etag"] = headers.get("ETag")
                metadata["last_modified"] = headers.get("Last-Modified")

            return ReadinessResult(ready=True, reason="Endpoint is accessible", source_metadata=metadata)
        except Exception as e:
            logger.error(f"Readiness check failed: {str(e)}")
            return ReadinessResult(ready=False, reason=f"Connection error: {str(e)}")

    def extract_full(
        self,
        config: SourceConfig,
        download_dir: str,
        resume_chunk_id: Optional[int] = None,
        resume_row_start: Optional[int] = None,
    ) -> Generator[DataChunk, None, None]:
        """Download (or use cached/local) dataset and yield in chunks.

        If resume_row_start is provided, skips rows prior to the checkpoint
        directly during CSV parsing to eliminate deserialization overhead.
        """
        logger = create_ingestion_logger(config.source_id)
        os.makedirs(download_dir, exist_ok=True)

        csv_file_path: Optional[str] = None
        zip_path = os.path.join(download_dir, f"{config.source_id}_bulk.zip")

        try:
            # 1. Check if local_path is specified and valid
            if config.local_path and os.path.exists(config.local_path):
                if os.path.isdir(config.local_path):
                    # Search for CSV inside directory
                    csv_candidates = [
                        os.path.join(config.local_path, f)
                        for f in os.listdir(config.local_path)
                        if f.endswith(".csv")
                    ]
                    if csv_candidates:
                        csv_file_path = max(csv_candidates, key=os.path.getsize)
                elif config.local_path.endswith(".csv"):
                    csv_file_path = config.local_path
                elif config.local_path.endswith(".zip"):
                    zip_path = config.local_path

            # 2. Download ZIP if not already available
            if not csv_file_path:
                if not os.path.exists(zip_path) or os.path.getsize(zip_path) == 0:
                    logger.info(f"Downloading ZIP from {config.endpoint}")
                    client = HttpClient(config.retry)
                    client.download_streaming(config.endpoint, zip_path, chunk_size_bytes=1024 * 1024)
                else:
                    logger.info(f"Using existing cached ZIP at {zip_path}")

                # Extract CSV from ZIP
                logger.info("Extracting ZIP contents")
                with zipfile.ZipFile(zip_path, "r") as zf:
                    csv_files = [f for f in zf.infolist() if f.filename.endswith(".csv")]
                    if not csv_files:
                        raise DataQualityError("No CSV file found in downloaded ZIP")
                    largest_csv = max(csv_files, key=lambda x: x.file_size)
                    zf.extract(largest_csv, download_dir)
                    csv_file_path = os.path.join(download_dir, largest_csv.filename)

            logger.info(f"Reading CSV file in chunks: {csv_file_path}")
            encoding = config.encoding or "latin-1"
            chunk_size = config.chunk_size or 50000

            # 3. Determine resume skip configuration
            skip_count = 0
            start_chunk_idx = 0
            if resume_chunk_id is not None and resume_chunk_id >= 0:
                start_chunk_idx = resume_chunk_id + 1
                if resume_row_start is not None and resume_row_start > 0:
                    skip_count = resume_row_start
                    logger.info(f"Resuming: skipping {skip_count} already-checkpointed rows at C-level")

            chunk_id = start_chunk_idx
            row_start = skip_count

            # If skipping rows, pass skiprows range(1, skip_count + 1) to keep header at row 0
            skiprows_arg = range(1, skip_count + 1) if skip_count > 0 else None

            for chunk in pd.read_csv(
                csv_file_path,
                chunksize=chunk_size,
                encoding=encoding,
                skiprows=skiprows_arg,
                low_memory=False,
            ):
                record_count = len(chunk)
                row_end = row_start + record_count - 1

                # Compute checksum of chunk payload
                chunk_bytes = chunk.to_csv(index=False).encode("utf-8")
                chunk_checksum = compute_bytes_checksum(chunk_bytes, "sha256")

                data_chunk = DataChunk(
                    chunk_id=chunk_id,
                    data=chunk,
                    row_start=row_start,
                    row_end=row_end,
                    record_count=record_count,
                    checksum=chunk_checksum,
                )

                logger.info(
                    f"Yielding chunk {chunk_id}, records {record_count}, cumulative {row_end + 1}"
                )
                yield data_chunk

                chunk_id += 1
                row_start += record_count

        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise PermanentError(f"Failed to extract full dataset: {str(e)}")

    def extract_incremental(
        self,
        config: SourceConfig,
        download_dir: str,
        watermark: Any,
    ) -> Generator[DataChunk, None, None]:
        """Incremental extraction not supported for bulk snapshot datasets."""
        raise NotImplementedError("Incremental extract not implemented for FaostatBulkAdapter")

    def get_artifact_checksum(self, file_path: str) -> str:
        """Compute SHA-256 of downloaded file."""
        return compute_file_checksum(file_path)
