"""NSO Vietnam (General Statistics Office) Adapter.

Handles multi-file ingestion for Vietnam General Statistics Office (GSO/NSO)
V06 agricultural rice statistics (V06.12.csv to V06.24.csv and metadata).

Features:
- Pure LOCAL_FILE runtime operation (zero HTTP/network requests).
- Multi-file discovery using configurable pattern (e.g. 'V06.*.csv').
- Multi-encoding resilience (latin-1, cp1252, utf-8-sig, utf-8) handling Vietnamese text and BOMs.
- Per-file SHA-256 checksum computation for reliable file-arrival tracking.
- Per-file DataChunk emission with row boundaries and source traceability (_source_file).
- Strict Ingestion Exception Hierarchy classification (FileNotFoundError, PermanentError, DataQualityError, SchemaError).
"""

from __future__ import annotations

import glob
import os
from typing import Any, Dict, Generator, List, Optional

import pandas as pd

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.config import SourceConfig
from ingestion.core.enums import SourceType
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.utils.error_classifier import (
    DataQualityError,
    PermanentError,
    SchemaError,
)
from ingestion.utils.hashing import compute_file_checksum
from ingestion.utils.logging_config import create_ingestion_logger


class NsoVietnamAdapter(BaseSourceAdapter):
    """Adapter for General Statistics Office Vietnam V06 rice statistics."""

    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Check availability and readiness of the local NSO directory and files."""
        logger = create_ingestion_logger(config.source_id)

        target_dir = config.local_path or config.local_fallback
        if not target_dir:
            return ReadinessResult(ready=False, reason="Missing local_path for NSO adapter")

        if not os.path.exists(target_dir):
            return ReadinessResult(ready=False, reason=f"Local path does not exist: {target_dir}")

        if not os.path.isdir(target_dir):
            return ReadinessResult(ready=False, reason=f"Local path is not a directory: {target_dir}")

        if not os.access(target_dir, os.R_OK):
            return ReadinessResult(ready=False, reason=f"Local directory is not readable: {target_dir}")

        pattern = (config.extra.get("file_pattern") if config.extra else None) or "V06.*.csv"
        files = glob.glob(os.path.join(target_dir, pattern))

        if not files:
            return ReadinessResult(
                ready=False,
                reason=f"No files matching pattern '{pattern}' found in {target_dir}",
            )

        total_size = sum(os.path.getsize(f) for f in files if os.path.isfile(f))
        min_size = getattr(config.readiness, "min_file_size_bytes", 0) if config.readiness else 0

        if min_size and total_size < min_size:
            return ReadinessResult(
                ready=False,
                reason=f"Total file size {total_size} bytes is less than min_file_size_bytes {min_size}",
            )

        return ReadinessResult(
            ready=True,
            reason=f"Found {len(files)} files matching '{pattern}' ({total_size:,} bytes)",
            source_metadata={
                "path": target_dir,
                "file_count": len(files),
                "file_pattern": pattern,
                "files": [os.path.basename(f) for f in sorted(files)],
                "total_size_bytes": total_size,
            },
        )

    def extract_full(
        self,
        config: SourceConfig,
        download_dir: str,
        resume_chunk_id: Optional[int] = None,
        resume_row_start: Optional[int] = None,
    ) -> Generator[DataChunk, None, None]:
        """Discover and extract all matching CSV files from the local directory."""
        logger = create_ingestion_logger(config.source_id)

        target_dir = config.local_path or config.local_fallback
        if not target_dir or not os.path.exists(target_dir):
            raise FileNotFoundError(f"Local NSO directory not found: '{target_dir}'")

        if not os.path.isdir(target_dir):
            raise PermanentError(f"Local path is not a directory: '{target_dir}'")

        pattern = (config.extra.get("file_pattern") if config.extra else None) or "V06.*.csv"
        files = sorted(glob.glob(os.path.join(target_dir, pattern)))

        if not files:
            raise DataQualityError(f"No files matching pattern '{pattern}' found in directory '{target_dir}'")

        chunk_id = 0
        row_start = 0

        # Candidate encodings to handle Vietnamese exports (latin-1, cp1252) and standard UTF-8 / BOM
        configured_enc = config.encoding
        candidate_encodings: List[str] = []
        if configured_enc and configured_enc.lower() not in ("utf-8", "utf8"):
            candidate_encodings.append(configured_enc)
        candidate_encodings.extend(["utf-8-sig", "utf-8", "latin-1", "cp1252"])

        first_schema: Optional[List[str]] = None
        require_uniform = config.extra.get("require_uniform_schema", False) if config.extra else False

        for file_path in files:
            filename = os.path.basename(file_path)

            # 1. Handle empty file (0 bytes)
            if os.path.getsize(file_path) == 0:
                logger.warning(f"Skipping empty file (0 bytes): {filename}")
                continue

            df = None
            last_err = None

            # 2. Try candidate encodings
            for enc in candidate_encodings:
                try:
                    df = pd.read_csv(file_path, encoding=enc, low_memory=False)
                    break
                except UnicodeDecodeError as ue:
                    last_err = ue
                    continue
                except pd.errors.ParserError as pe:
                    raise DataQualityError(f"Malformed CSV in '{filename}': {pe}")
                except Exception as e:
                    last_err = e
                    continue

            if df is None:
                raise DataQualityError(
                    f"Failed to decode or parse '{filename}' with attempted encodings {candidate_encodings}: {last_err}"
                )

            # 3. Handle file with header but 0 records
            if len(df) == 0:
                logger.warning(f"File '{filename}' contains header but 0 records.")
                continue

            # 4. Technical Schema Contract verification
            current_cols = list(df.columns)
            if config.expected_columns:
                missing_cols = [c for c in config.expected_columns if c not in current_cols]
                if missing_cols:
                    raise SchemaError(
                        f"Schema mismatch in '{filename}': missing required columns: {missing_cols}"
                    )

            if require_uniform:
                if first_schema is None:
                    first_schema = current_cols
                elif current_cols != first_schema:
                    raise SchemaError(
                        f"Inconsistent schema in '{filename}'. Expected {first_schema}, got {current_cols}"
                    )

            # 5. Compute file-level SHA-256 checksum for tracking / idempotency
            file_checksum = compute_file_checksum(file_path)

            # 6. Attach source traceability column without altering data semantics
            df["_source_file"] = filename

            record_count = len(df)
            row_end = row_start + record_count - 1

            data_chunk = DataChunk(
                chunk_id=chunk_id,
                data=df,
                row_start=row_start,
                row_end=row_end,
                record_count=record_count,
                checksum=file_checksum,
            )

            logger.info(
                f"Yielded chunk {chunk_id} from '{filename}' with {record_count} records (rows {row_start}-{row_end})"
            )
            yield data_chunk

            chunk_id += 1
            row_start += record_count

    def extract_incremental(
        self,
        config: SourceConfig,
        download_dir: str,
        watermark: Any,
    ) -> Generator[DataChunk, None, None]:
        """Incremental extraction not implemented for snapshot NSO directory."""
        raise NotImplementedError("Incremental extract not implemented for NsoVietnamAdapter")

    def get_artifact_checksum(self, file_path: str) -> str:
        """Compute SHA-256 checksum of a file."""
        return compute_file_checksum(file_path)
