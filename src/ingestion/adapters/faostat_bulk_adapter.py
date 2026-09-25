"""FAOSTAT Bulk ZIP & CSV Download Adapter.

Handles downloading ZIP archives from FAOSTAT's bulk download service (or utilizing
local fixtures / fallbacks), extracting the CSV, validating schemas, and yielding
data in memory-safe chunks.

Features:
- Streamed download to disk (avoids keeping multi-hundred MB ZIP in RAM).
- Local fallback check: seamlessly uses local snapshot if live download fails or offline.
- Memory-safe chunked CSV parsing (e.g. 50,000 rows per chunk).
- BOM-safe decoding (UTF-8-SIG) ensuring clean column names.
- Schema verification against config.expected_columns.
- Fast C-level skiprows support for resuming after checkpoint failure.
- In-memory chunk checksum computation.
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Any, Dict, Generator, Optional, List

import pandas as pd

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.config import SourceConfig
from ingestion.core.enums import (
    ArtifactFormat,
    ChunkStatus,
    ErrorType,
    IngestionStatus,
    LoadStrategy,
    SourceType,
)
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.utils.error_classifier import (
    DataQualityError,
    PermanentError,
    SchemaError,
    TransientError,
)
from ingestion.utils.hashing import compute_bytes_checksum, compute_file_checksum
from ingestion.utils.http_client import HttpClient
from ingestion.utils.logging_config import create_ingestion_logger


class FaostatBulkAdapter(BaseSourceAdapter):
    """Adapter for FAOSTAT bulk download and snapshot datasets."""

    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Check availability of the FAOSTAT local file or remote endpoint."""
        logger = create_ingestion_logger(config.source_id)

        # 1. Primary: LOCAL_FILE checks local file/directory directly without network
        if config.source_type == SourceType.LOCAL_FILE:
            local_candidate = config.local_path or config.local_fallback
            if not local_candidate:
                return ReadinessResult(ready=False, reason="Missing local_path for LOCAL_FILE source")
            if not os.path.exists(local_candidate):
                return ReadinessResult(ready=False, reason=f"Local file not found: {local_candidate}")
            if not os.access(local_candidate, os.R_OK):
                return ReadinessResult(ready=False, reason=f"Local file not readable: {local_candidate}")

            if os.path.isdir(local_candidate):
                files = [f for f in os.listdir(local_candidate) if not f.startswith(".")]
                if not files:
                    return ReadinessResult(ready=False, reason=f"Directory is empty: {local_candidate}")
                size = sum(os.path.getsize(os.path.join(local_candidate, f)) for f in files if os.path.isfile(os.path.join(local_candidate, f)))
                file_count = len(files)
            else:
                size = os.path.getsize(local_candidate)
                file_count = 1

            if size == 0:
                return ReadinessResult(ready=False, reason=f"Local file is empty (0 bytes): {local_candidate}")

            min_size = getattr(config.readiness, "min_file_size_bytes", 0) if config.readiness else 0
            if min_size and size < min_size:
                return ReadinessResult(ready=False, reason=f"File size {size} < min_file_size_bytes {min_size}")

            return ReadinessResult(
                ready=True,
                reason="Local file readiness check passed",
                source_metadata={
                    "path": local_candidate,
                    "size": size,
                    "file_count": file_count,
                    "last_modified": str(os.path.getmtime(local_candidate)),
                },
            )

        # 2. Check local fallback preference if configured
        use_fallback_pref = (
            os.environ.get("INGESTION_USE_LOCAL_FALLBACK", "").lower() in ("1", "true", "yes")
            or config.extra.get("use_local_fallback", False)
        )
        if use_fallback_pref and config.local_fallback and os.path.exists(config.local_fallback):
            file_size = os.path.getsize(config.local_fallback)
            return ReadinessResult(
                ready=True,
                reason=f"Local fallback file is ready: {config.local_fallback}",
                source_metadata={"fallback": True, "local_path": config.local_fallback, "size": file_size},
            )

        # 3. Live endpoint probe only for remote sources
        if config.endpoint:
            try:
                client = HttpClient(config.retry)
                head_result = client.head(config.endpoint)

                if hasattr(head_result, "status_code") and head_result.status_code == 200:
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
                logger.warning(f"Endpoint HEAD check failed for {config.source_id}: {e}")

        # 4. Fallback to local_fallback if endpoint failed
        if config.local_fallback and os.path.exists(config.local_fallback):
            file_size = os.path.getsize(config.local_fallback)
            return ReadinessResult(
                ready=True,
                reason=f"Local fallback file is ready: {config.local_fallback}",
                source_metadata={"fallback": True, "local_path": config.local_fallback, "size": file_size},
            )

        if not config.endpoint:
            return ReadinessResult(ready=False, reason="Missing endpoint and no valid local path in configuration")

        return ReadinessResult(ready=False, reason=f"Endpoint '{config.endpoint}' is inaccessible and no valid local fallback found")

    def _resolve_source_file(self, config: SourceConfig, download_dir: str) -> str:
        """Resolve the source CSV path from local_path, live download, or local_fallback."""
        logger = create_ingestion_logger(config.source_id)

        # 1. Primary: If source_type == LOCAL_FILE or explicit local_path provided
        if config.source_type == SourceType.LOCAL_FILE or config.local_path:
            local_candidate = config.local_path or config.local_fallback
            if not local_candidate:
                raise FileNotFoundError(f"Missing local_path configuration for '{config.source_id}'.")
            if not os.path.exists(local_candidate):
                raise FileNotFoundError(f"Local source file not found for '{config.source_id}': {local_candidate}")

            if os.path.isdir(local_candidate):
                csv_candidates = [
                    os.path.join(local_candidate, f)
                    for f in os.listdir(local_candidate)
                    if f.endswith(".csv")
                ]
                if csv_candidates:
                    return max(csv_candidates, key=os.path.getsize)

                zip_candidates = [
                    os.path.join(local_candidate, f)
                    for f in os.listdir(local_candidate)
                    if f.endswith(".zip")
                ]
                if zip_candidates:
                    return self._extract_zip(max(zip_candidates, key=os.path.getsize), download_dir)

                raise DataQualityError(f"No CSV or ZIP files found in directory '{local_candidate}'.")

            if local_candidate.lower().endswith(".zip"):
                return self._extract_zip(local_candidate, download_dir)

            if local_candidate.lower().endswith(".csv"):
                return local_candidate

            raise PermanentError(
                f"Unsupported file format for '{config.source_id}': '{local_candidate}'. Expected .csv or .zip"
            )

        # 2. Preferred local fallback check for non-LOCAL_FILE sources
        use_fallback_pref = (
            os.environ.get("INGESTION_USE_LOCAL_FALLBACK", "").lower() in ("1", "true", "yes")
            or config.extra.get("use_local_fallback", False)
        )
        if use_fallback_pref and config.local_fallback and os.path.exists(config.local_fallback):
            logger.info(f"Using preferred local fallback for '{config.source_id}': {config.local_fallback}")
            if config.local_fallback.endswith(".zip"):
                return self._extract_zip(config.local_fallback, download_dir)
            return config.local_fallback

        # 3. Live download only for HTTP/remote sources
        download_err = None
        if config.endpoint:
            zip_path = os.path.join(download_dir, f"{config.source_id}_bulk.zip")
            if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
                logger.info(f"Using existing cached ZIP at {zip_path}")
                return self._extract_zip(zip_path, download_dir)
            else:
                try:
                    logger.info(f"Downloading ZIP from {config.endpoint}")
                    client = HttpClient(config.retry)
                    client.download_streaming(config.endpoint, zip_path, chunk_size_bytes=1024 * 1024)
                    return self._extract_zip(zip_path, download_dir)
                except Exception as e:
                    download_err = e
                    logger.warning(f"Download failed from {config.endpoint}: {e}")

        # 4. Fallback if download failed
        if config.local_fallback and os.path.exists(config.local_fallback):
            logger.info(f"Using local fallback for '{config.source_id}': {config.local_fallback}")
            if config.local_fallback.endswith(".zip"):
                return self._extract_zip(config.local_fallback, download_dir)
            return config.local_fallback

        if download_err:
            raise PermanentError(f"Failed to download from endpoint and no fallback available: {download_err}")

        target = config.local_path or config.local_fallback or "unknown"
        raise FileNotFoundError(f"Source file not found: {target}")

    def _extract_zip(self, zip_path: str, extract_to: str) -> str:
        """Extract the largest CSV file from a ZIP archive."""
        if not os.path.exists(zip_path) or os.path.getsize(zip_path) == 0:
            raise DataQualityError(f"ZIP file '{zip_path}' is empty or does not exist.")
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_files = [f for f in zf.infolist() if f.filename.endswith(".csv")]
                if not csv_files:
                    raise DataQualityError(f"No CSV file found inside ZIP archive '{zip_path}'.")
                largest_csv = max(csv_files, key=lambda x: x.file_size)
                zf.extract(largest_csv, extract_to)
                return os.path.join(extract_to, largest_csv.filename)
        except zipfile.BadZipFile as e:
            raise DataQualityError(f"Corrupted or invalid ZIP file '{zip_path}': {e}")

    def extract_full(
        self,
        config: SourceConfig,
        download_dir: str,
        resume_chunk_id: Optional[int] = None,
        resume_row_start: Optional[int] = None,
    ) -> Generator[DataChunk, None, None]:
        """Download or resolve dataset and yield data in chunks."""
        logger = create_ingestion_logger(config.source_id)
        os.makedirs(download_dir, exist_ok=True)

        try:
            csv_file_path = self._resolve_source_file(config, download_dir)
        except (FileNotFoundError, PermanentError, DataQualityError, SchemaError):
            raise

        # Check empty file
        if not os.path.exists(csv_file_path):
            raise PermanentError(f"Resolved CSV file does not exist: {csv_file_path}")

        if os.path.getsize(csv_file_path) == 0:
            logger.warning(f"Source file '{csv_file_path}' is empty (0 bytes).")
            return

        logger.info(f"Reading CSV file in chunks: {csv_file_path}")
        raw_encoding = config.encoding or "utf-8"
        # Use utf-8-sig to automatically strip UTF-8 BOM (e.g. \ufeff) if present
        encoding = "utf-8-sig" if raw_encoding.lower().replace("-", "") == "utf8" else raw_encoding
        chunk_size = config.chunk_size or 50000

        # Determine resume skip configuration
        skip_count = 0
        start_chunk_idx = 0
        if resume_chunk_id is not None and resume_chunk_id >= 0:
            start_chunk_idx = resume_chunk_id + 1
            if resume_row_start is not None and resume_row_start > 0:
                skip_count = resume_row_start
                logger.info(f"Resuming: skipping {skip_count} already-checkpointed rows at C-level")

        chunk_id = start_chunk_idx
        row_start = skip_count
        skiprows_arg = range(1, skip_count + 1) if skip_count > 0 else None

        first_chunk = True
        try:
            reader = pd.read_csv(
                csv_file_path,
                chunksize=chunk_size,
                encoding=encoding,
                skiprows=skiprows_arg,
                low_memory=False,
            )
        except pd.errors.EmptyDataError:
            logger.warning(f"Source CSV '{csv_file_path}' has no columns or data.")
            return
        except pd.errors.ParserError as pe:
            raise DataQualityError(f"Malformed CSV in '{config.source_id}': {pe}")
        except UnicodeDecodeError as ue:
            raise DataQualityError(f"Encoding error parsing '{config.source_id}': {ue}")
        except Exception as e:
            raise PermanentError(f"Failed to initialize CSV reader for '{config.source_id}': {e}")

        try:
            for chunk in reader:
                record_count = len(chunk)

                # Schema protection check on the first chunk
                if first_chunk and config.expected_columns:
                    missing_cols = [c for c in config.expected_columns if c not in chunk.columns]
                    if missing_cols:
                        raise SchemaError(
                            f"Schema mismatch for '{config.source_id}': missing required columns: {missing_cols}"
                        )
                    first_chunk = False

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
        except (SchemaError, DataQualityError, PermanentError):
            raise
        except pd.errors.ParserError as pe:
            raise DataQualityError(f"Malformed CSV during chunk iteration in '{config.source_id}': {pe}")
        except Exception as e:
            logger.error(f"Error during extraction loop: {e}")
            raise PermanentError(f"Failed to extract full dataset: {e}")

    def extract_incremental(
        self,
        config: SourceConfig,
        download_dir: str,
        watermark: Any,
    ) -> Generator[DataChunk, None, None]:
        """Incremental extraction not supported for bulk snapshot datasets."""
        raise NotImplementedError("Incremental extract not implemented for FaostatBulkAdapter")

    def get_artifact_checksum(self, file_path: str) -> str:
        """Compute SHA-256 of downloaded or staged file."""
        return compute_file_checksum(file_path)
