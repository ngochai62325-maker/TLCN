"""Bronze layer data quality and schema protection validator.

Performs:
1. Zero-byte file verification (file_size > 0).
2. Checksum validation (SHA-256 match).
3. Record count integrity check.
4. Schema drift and mandatory column validation.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from ingestion.storage.metadata_schemas import QuarantineErrorType


@dataclass
class ValidationResult:
    """Result of quality and schema validation."""

    is_valid: bool
    error_type: Optional[QuarantineErrorType] = None
    error_message: Optional[str] = None
    file_size_bytes: int = 0
    calculated_checksum: Optional[str] = None
    record_count: int = 0
    line_number: Optional[int] = None
    corrupted_snippet: Optional[str] = None
    schema_diff: Optional[Dict[str, Any]] = None
    details: Dict[str, Any] = field(default_factory=dict)


class BronzeQualityValidator:
    """Validates raw source artifacts before ingestion into Bronze layer."""

    @staticmethod
    def calculate_checksum(file_path: str, block_size: int = 65536) -> str:
        """Compute SHA-256 checksum for a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(block_size), b""):
                sha256.update(block)
        return sha256.hexdigest()

    def validate_file(
        self,
        file_path: str,
        expected_checksum: Optional[str] = None,
        expected_columns: Optional[List[str]] = None,
        min_records: int = 1,
    ) -> ValidationResult:
        """Run full validation suite on a file artifact."""
        if not os.path.exists(file_path):
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.CORRUPT_FILE,
                error_message=f"File does not exist: {file_path}",
                file_size_bytes=0,
            )

        file_size = os.path.getsize(file_path)

        # 1. Zero-byte check
        if file_size == 0:
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.EMPTY_FILE,
                error_message="File is empty (0 bytes)",
                file_size_bytes=0,
            )

        # 2. Checksum validation
        calc_checksum = self.calculate_checksum(file_path)
        if expected_checksum and calc_checksum.lower() != expected_checksum.lower():
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.CHECKSUM_MISMATCH,
                error_message=(
                    f"Checksum mismatch: expected {expected_checksum}, calculated {calc_checksum}"
                ),
                file_size_bytes=file_size,
                calculated_checksum=calc_checksum,
            )

        # 3. Quick structural & parse validation
        ext = os.path.splitext(file_path)[1].lower()
        if ext in (".csv", ".tsv", ".txt"):
            return self._validate_delimited_file(
                file_path=file_path,
                file_size=file_size,
                checksum=calc_checksum,
                expected_columns=expected_columns,
                min_records=min_records,
                sep="," if ext == ".csv" else "\t",
            )
        elif ext in (".json", ".jsonl"):
            return self._validate_json_file(
                file_path=file_path,
                file_size=file_size,
                checksum=calc_checksum,
                expected_columns=expected_columns,
                min_records=min_records,
            )

        # Generic valid result for binary/other formats
        return ValidationResult(
            is_valid=True,
            file_size_bytes=file_size,
            calculated_checksum=calc_checksum,
        )

    def _validate_delimited_file(
        self,
        file_path: str,
        file_size: int,
        checksum: str,
        expected_columns: Optional[List[str]],
        min_records: int,
        sep: str = ",",
    ) -> ValidationResult:
        """Validate delimited file (CSV/TSV) for corrupt rows, encoding, and schema."""
        try:
            # Test readability of header and first 100 rows
            sample_df = pd.read_csv(file_path, sep=sep, nrows=100, on_bad_lines="error")
        except Exception as e:
            # Extract sample corrupted snippet
            snippet = None
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    snippet = "".join([f.readline() for _ in range(5)])
            except Exception:
                pass

            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.PARSE_FAILURE,
                error_message=f"Delimiter parse error: {e}",
                file_size_bytes=file_size,
                calculated_checksum=checksum,
                corrupted_snippet=snippet,
            )

        # Count total records approximately
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                record_count = sum(1 for _ in f) - 1
            if record_count < 0:
                record_count = 0
        except Exception:
            record_count = len(sample_df)

        if record_count < min_records:
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.RECORD_COUNT_MISMATCH,
                error_message=f"File contains {record_count} records; expected >= {min_records}",
                file_size_bytes=file_size,
                calculated_checksum=checksum,
                record_count=record_count,
            )

        # Schema drift check
        if expected_columns:
            actual_cols = set(c.strip().lower() for c in sample_df.columns)
            exp_cols = set(c.strip().lower() for c in expected_columns)
            missing_cols = exp_cols - actual_cols
            if missing_cols:
                return ValidationResult(
                    is_valid=False,
                    error_type=QuarantineErrorType.SCHEMA_DRIFT,
                    error_message=f"Schema drift detected. Missing expected columns: {sorted(list(missing_cols))}",
                    file_size_bytes=file_size,
                    calculated_checksum=checksum,
                    record_count=record_count,
                    schema_diff={
                        "missing_columns": sorted(list(missing_cols)),
                        "actual_columns": sorted(list(actual_cols)),
                        "expected_columns": sorted(list(exp_cols)),
                    },
                )

        return ValidationResult(
            is_valid=True,
            file_size_bytes=file_size,
            calculated_checksum=checksum,
            record_count=record_count,
        )

    def _validate_json_file(
        self,
        file_path: str,
        file_size: int,
        checksum: str,
        expected_columns: Optional[List[str]],
        min_records: int,
    ) -> ValidationResult:
        """Validate JSON/JSONL file."""
        import json

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                first_char = f.read(1)
                f.seek(0)
                if first_char == "[":
                    data = json.load(f)
                    records = data if isinstance(data, list) else [data]
                else:
                    records = [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.PARSE_FAILURE,
                error_message=f"JSON parse error: {e}",
                file_size_bytes=file_size,
                calculated_checksum=checksum,
            )

        record_count = len(records)
        if record_count < min_records:
            return ValidationResult(
                is_valid=False,
                error_type=QuarantineErrorType.RECORD_COUNT_MISMATCH,
                error_message=f"JSON contains {record_count} records; expected >= {min_records}",
                file_size_bytes=file_size,
                calculated_checksum=checksum,
                record_count=record_count,
            )

        return ValidationResult(
            is_valid=True,
            file_size_bytes=file_size,
            calculated_checksum=checksum,
            record_count=record_count,
        )
