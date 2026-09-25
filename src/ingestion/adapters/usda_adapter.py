"""USDA Adapters for Bronze Ingestion Layer.

Handles ingestion for:
1. USDA ERS Rice Yearbook export price dataset
   (data/raw/usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv)
2. USDA FAS Production, Supply and Distribution (PSD) dataset
   (data/raw/usda/usda.xls)

Features:
- Pure LOCAL_FILE runtime operation (zero HTTP/network requests).
- Actual file format detection (detecting HTML table masquerading as .xls vs binary XLS/XLSX vs CSV).
- Memory-safe chunked reading for USDA Rice Yearbook CSV.
- Exact table extraction for USDA PSD HTML table preserving source structure.
- Lineage & traceability via `_source_file` and SHA-256 checksum on every DataChunk.
- Standard Ingestion Exception Hierarchy (PermanentError, DataQualityError, SchemaError).
"""

from __future__ import annotations

import os
from typing import Any, Generator, List, Optional

import pandas as pd

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.utils.error_classifier import (
    DataQualityError,
    PermanentError,
    SchemaError,
)
from ingestion.utils.hashing import compute_file_checksum
from ingestion.utils.logging_config import create_ingestion_logger


class UsdaRiceYearbookAdapter(BaseSourceAdapter):
    """Adapter for USDA ERS Rice Yearbook export price dataset."""

    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Check availability and readability of local USDA Rice Yearbook CSV file."""
        logger = create_ingestion_logger(config.source_id)

        target_file = config.local_path or config.local_fallback
        if not target_file:
            return ReadinessResult(ready=False, reason="Missing local_path for USDA Rice Yearbook adapter")

        if not os.path.exists(target_file):
            return ReadinessResult(ready=False, reason=f"Local file not found: {target_file}")

        if not os.path.isfile(target_file):
            return ReadinessResult(ready=False, reason=f"Local path is not a file: {target_file}")

        if not os.access(target_file, os.R_OK):
            return ReadinessResult(ready=False, reason=f"Local file not readable: {target_file}")

        file_size = os.path.getsize(target_file)
        if file_size == 0:
            return ReadinessResult(ready=False, reason=f"Local file is empty (0 bytes): {target_file}")

        min_size = getattr(config.readiness, "min_file_size_bytes", 0) if config.readiness else 0
        if min_size and file_size < min_size:
            return ReadinessResult(
                ready=False,
                reason=f"File size {file_size} bytes is less than min_file_size_bytes {min_size}",
            )

        # Inspect format (must be readable text/CSV)
        try:
            with open(target_file, "rb") as f:
                head = f.read(512)
            if b"\x00" in head:
                return ReadinessResult(
                    ready=False,
                    reason=f"File '{target_file}' appears to be binary, expected CSV text.",
                )
        except Exception as e:
            return ReadinessResult(
                ready=False,
                reason=f"Failed to inspect file '{target_file}': {e}",
            )

        return ReadinessResult(
            ready=True,
            reason=f"Local USDA Rice Yearbook file is ready ({file_size:,} bytes)",
            source_metadata={
                "path": target_file,
                "size_bytes": file_size,
                "format": "CSV",
            },
        )

    def extract_full(
        self,
        config: SourceConfig,
        download_dir: str,
        resume_chunk_id: Optional[int] = None,
        resume_row_start: Optional[int] = None,
    ) -> Generator[DataChunk, None, None]:
        """Extract dataset from USDA Rice Yearbook CSV in streaming chunks."""
        logger = create_ingestion_logger(config.source_id)

        target_file = config.local_path or config.local_fallback
        if not target_file or not os.path.exists(target_file):
            raise FileNotFoundError(f"Local USDA Rice Yearbook file not found: '{target_file}'")

        file_size = os.path.getsize(target_file)
        if file_size == 0:
            logger.warning(f"USDA Rice Yearbook file '{target_file}' is empty (0 bytes).")
            return

        # Check for binary file
        with open(target_file, "rb") as f:
            head = f.read(512)
        if b"\x00" in head:
            raise PermanentError(f"File '{target_file}' is binary, expected CSV format.")

        encoding = config.encoding or "utf-8"
        chunk_size = config.chunk_size or 50000

        try:
            file_checksum = compute_file_checksum(target_file)
        except Exception as e:
            raise PermanentError(f"Failed to compute checksum for '{target_file}': {e}")

        source_file = os.path.basename(target_file)
        chunk_id = 0
        row_start = 0

        try:
            reader = pd.read_csv(
                target_file,
                chunksize=chunk_size,
                encoding=encoding,
                low_memory=False,
            )

            for chunk in reader:
                record_count = len(chunk)

                # Skip chunks prior to resume checkpoint
                if resume_chunk_id is not None and chunk_id < resume_chunk_id:
                    chunk_id += 1
                    row_start += record_count
                    continue

                # Schema Contract Enforcement
                if config.expected_columns:
                    missing_cols = [c for c in config.expected_columns if c not in chunk.columns]
                    if missing_cols:
                        raise SchemaError(
                            f"Schema mismatch in '{target_file}': missing required columns: {missing_cols}"
                        )

                # Lineage & Traceability
                chunk["_source_file"] = source_file

                row_end = row_start + record_count - 1
                data_chunk = DataChunk(
                    chunk_id=chunk_id,
                    data=chunk,
                    row_start=row_start,
                    row_end=row_end,
                    record_count=record_count,
                    checksum=file_checksum,
                )

                logger.info(
                    f"Yielded DataChunk {chunk_id} from '{target_file}': "
                    f"rows [{row_start}..{row_end}] ({record_count} records)"
                )
                yield data_chunk

                chunk_id += 1
                row_start += record_count

        except SchemaError:
            raise
        except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as e:
            raise DataQualityError(f"Malformed CSV in '{target_file}': {e}")
        except Exception as e:
            if isinstance(e, (PermanentError, DataQualityError, SchemaError)):
                raise
            raise DataQualityError(f"Failed to read CSV '{target_file}': {e}")

    def extract_incremental(
        self,
        config: SourceConfig,
        download_dir: str,
        watermark: Any,
    ) -> Generator[DataChunk, None, None]:
        """Incremental extraction not implemented for USDA Rice Yearbook snapshot dataset."""
        raise NotImplementedError("Incremental extract not implemented for UsdaRiceYearbookAdapter")

    def get_artifact_checksum(self, file_path: str) -> str:
        """Compute SHA-256 checksum of file."""
        return compute_file_checksum(file_path)


# Backward-compatibility alias for registry and existing callers
UsdaLocalAdapter = UsdaRiceYearbookAdapter


class UsdaPsdAdapter(BaseSourceAdapter):
    """Adapter for USDA FAS Production, Supply and Distribution (PSD) dataset."""

    @staticmethod
    def detect_file_format(file_path: str) -> str:
        """Detect actual file format based on magic bytes / content signature.

        Returns one of: 'HTML', 'EXCEL_BIFF', 'EXCEL_OPENXML', 'CSV', 'EMPTY', 'UNSUPPORTED'.
        """
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return "EMPTY"

        with open(file_path, "rb") as f:
            head = f.read(4096)

        head_lower = head.lower()

        # Check for HTML signature (e.g. usda.xls masquerading as XLS)
        if (
            b"<html" in head_lower
            or b"<!doctype html" in head_lower
            or b"<table" in head_lower
            or b"<body" in head_lower
        ):
            return "HTML"

        # Check for OLE2 Compound Document (binary Excel .xls BIFF8)
        if head.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
            return "EXCEL_BIFF"

        # Check for Zip Archive (Excel .xlsx OpenXML)
        if head.startswith(b"PK\x03\x04"):
            return "EXCEL_OPENXML"

        # Check if printable text / CSV
        try:
            sample_text = head.decode("utf-8")
            if any(sep in sample_text for sep in [",", "\t", ";", "\n"]):
                return "CSV"
        except UnicodeDecodeError:
            pass

        return "UNSUPPORTED"

    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        """Check availability and readiness of local USDA PSD file."""
        logger = create_ingestion_logger(config.source_id)

        target_file = config.local_path or config.local_fallback
        if not target_file:
            return ReadinessResult(ready=False, reason="Missing local_path for USDA PSD adapter")

        if not os.path.exists(target_file):
            return ReadinessResult(ready=False, reason=f"Local file not found: {target_file}")

        if not os.path.isfile(target_file):
            return ReadinessResult(ready=False, reason=f"Local path is not a file: {target_file}")

        if not os.access(target_file, os.R_OK):
            return ReadinessResult(ready=False, reason=f"Local file not readable: {target_file}")

        file_size = os.path.getsize(target_file)
        if file_size == 0:
            return ReadinessResult(ready=False, reason=f"Local file is empty (0 bytes): {target_file}")

        min_size = getattr(config.readiness, "min_file_size_bytes", 0) if config.readiness else 0
        if min_size and file_size < min_size:
            return ReadinessResult(
                ready=False,
                reason=f"File size {file_size} bytes is less than min_file_size_bytes {min_size}",
            )

        actual_format = self.detect_file_format(target_file)
        if actual_format == "UNSUPPORTED":
            return ReadinessResult(
                ready=False,
                reason=f"Unsupported actual file format for USDA PSD: '{target_file}'",
            )

        # Probe format parsing
        tables_count = 0
        if actual_format == "HTML":
            try:
                tables = pd.read_html(target_file, encoding=config.encoding or "utf-8", flavor="lxml")
                if not tables:
                    return ReadinessResult(
                        ready=False,
                        reason=f"No HTML tables found in file: '{target_file}'",
                    )
                tables_count = len(tables)
            except Exception as e:
                return ReadinessResult(
                    ready=False,
                    reason=f"Failed to parse HTML in '{target_file}': {e}",
                )

        return ReadinessResult(
            ready=True,
            reason=f"Local USDA PSD file is ready (detected format: {actual_format}, {file_size:,} bytes)",
            source_metadata={
                "path": target_file,
                "size_bytes": file_size,
                "detected_format": actual_format,
                "tables_count": tables_count,
            },
        )

    def extract_full(
        self,
        config: SourceConfig,
        download_dir: str,
        resume_chunk_id: Optional[int] = None,
        resume_row_start: Optional[int] = None,
    ) -> Generator[DataChunk, None, None]:
        """Extract dataset from USDA PSD file preserving source structure."""
        logger = create_ingestion_logger(config.source_id)

        target_file = config.local_path or config.local_fallback
        if not target_file or not os.path.exists(target_file):
            raise FileNotFoundError(f"Local USDA PSD file not found: '{target_file}'")

        file_size = os.path.getsize(target_file)
        if file_size == 0:
            logger.warning(f"USDA PSD file '{target_file}' is empty (0 bytes).")
            return

        if resume_chunk_id is not None and resume_chunk_id > 0:
            logger.info("Single-chunk USDA PSD already extracted prior to resume checkpoint.")
            return

        actual_format = self.detect_file_format(target_file)

        if actual_format == "UNSUPPORTED":
            raise PermanentError(f"Unsupported actual file format for USDA PSD: '{target_file}'")

        df: pd.DataFrame
        if actual_format == "HTML":
            try:
                tables = pd.read_html(target_file, encoding=config.encoding or "utf-8", flavor="lxml")
            except Exception as e:
                raise DataQualityError(f"Malformed or unreadable HTML table in '{target_file}': {e}")
            if not tables:
                raise DataQualityError(f"No HTML tables found in '{target_file}'")
            df = tables[0]
        elif actual_format in ("EXCEL_BIFF", "EXCEL_OPENXML"):
            try:
                df = pd.read_excel(target_file)
            except Exception as e:
                raise DataQualityError(f"Failed to read Excel workbook '{target_file}': {e}")
        elif actual_format == "CSV":
            try:
                df = pd.read_csv(target_file, encoding=config.encoding or "utf-8")
            except Exception as e:
                raise DataQualityError(f"Failed to read CSV '{target_file}': {e}")
        else:
            raise PermanentError(f"Cannot process format '{actual_format}' for '{target_file}'")

        # Drop entirely empty rows if any
        df.dropna(how="all", inplace=True)
        record_count = len(df)
        if record_count == 0:
            logger.warning(f"USDA PSD file '{target_file}' contains no records.")
            return

        # Schema Contract Enforcement
        if config.expected_columns:
            missing_cols = [c for c in config.expected_columns if c not in df.columns]
            if missing_cols:
                raise SchemaError(
                    f"Schema mismatch in '{target_file}': missing required columns: {missing_cols}"
                )

        # Lineage & Traceability
        df["_source_file"] = os.path.basename(target_file)

        try:
            file_checksum = compute_file_checksum(target_file)
        except Exception as e:
            raise PermanentError(f"Failed to compute checksum for '{target_file}': {e}")

        data_chunk = DataChunk(
            chunk_id=0,
            data=df,
            row_start=0,
            row_end=record_count - 1,
            record_count=record_count,
            checksum=file_checksum,
        )

        logger.info(
            f"Yielded DataChunk from '{target_file}' (detected format: {actual_format}): "
            f"{record_count} records, {len(df.columns)} columns"
        )
        yield data_chunk

    def extract_incremental(
        self,
        config: SourceConfig,
        download_dir: str,
        watermark: Any,
    ) -> Generator[DataChunk, None, None]:
        """Incremental extraction not implemented for USDA PSD snapshot dataset."""
        raise NotImplementedError("Incremental extract not implemented for UsdaPsdAdapter")

    def get_artifact_checksum(self, file_path: str) -> str:
        """Compute SHA-256 checksum of file."""
        return compute_file_checksum(file_path)


__all__ = ["UsdaRiceYearbookAdapter", "UsdaLocalAdapter", "UsdaPsdAdapter"]
