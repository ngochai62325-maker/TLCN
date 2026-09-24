"""Tests for source adapters."""

import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from ingestion.core.config import SourceConfig, ReadinessConfig
from ingestion.core.enums import SourceType, LoadStrategy, ArtifactFormat
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.faostat_bulk_adapter import FaostatBulkAdapter
from ingestion.adapters.nso_adapter import NsoVietnamAdapter


class TestFaostatBulkAdapterReadiness:
    def test_no_endpoint(self):
        config = SourceConfig(
            source_id="test",
            provider="FAOSTAT",
            dataset="Test",
            source_type=SourceType.HTTP_BULK_ZIP,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            endpoint=None,
        )
        adapter = FaostatBulkAdapter()
        result = adapter.check_readiness(config)
        assert result.ready is False
        assert "endpoint" in result.reason.lower() or "Missing" in result.reason


class TestNsoVietnamAdapter:
    def test_readiness_with_files(self, tmp_path):
        """Create V06 CSV files and check readiness."""
        for i in range(12, 25):
            f = tmp_path / f"V06.{i}.csv"
            f.write_text("col1,col2\n1,2\n")

        config = SourceConfig(
            source_id="nso_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        adapter = NsoVietnamAdapter()
        result = adapter.check_readiness(config)
        assert result.ready is True

    def test_readiness_empty_dir(self, tmp_path):
        config = SourceConfig(
            source_id="nso_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        adapter = NsoVietnamAdapter()
        result = adapter.check_readiness(config)
        assert result.ready is False

    def test_extract_full(self, tmp_path):
        """Create CSV files and extract them."""
        for i in range(12, 14):
            f = tmp_path / f"V06.{i}.csv"
            f.write_text("col1,col2\n1,2\n3,4\n")

        config = SourceConfig(
            source_id="nso_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        adapter = NsoVietnamAdapter()
        chunks = list(adapter.extract_full(config, download_dir=str(tmp_path)))
        assert len(chunks) >= 1
        for chunk in chunks:
            assert isinstance(chunk, DataChunk)
            assert chunk.record_count > 0
