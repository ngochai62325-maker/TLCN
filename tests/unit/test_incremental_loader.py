"""Tests for ingestion.core.incremental_loader.IncrementalLoader."""

import pytest
import pandas as pd
from unittest.mock import MagicMock
from datetime import datetime, timezone

from ingestion.core.incremental_loader import IncrementalLoader
from ingestion.core.result import DataChunk
from ingestion.core.enums import IngestionStatus, LoadStrategy
from ingestion.core.config import SourceConfig
from ingestion.core.enums import SourceType, ArtifactFormat


def _make_config():
    return SourceConfig(
        source_id="test_incr",
        provider="Test",
        dataset="TestIncr",
        source_type=SourceType.API,
        load_strategy=LoadStrategy.INCREMENTAL,
        format=ArtifactFormat.CSV,
        endpoint="http://example.com/api",
        watermark_column="updated_at",
    )


def _make_chunks(n=2):
    df = pd.DataFrame({"id": [1], "updated_at": ["2024-06-01"]})
    return [
        DataChunk(chunk_id=i, data=df, row_start=i, row_end=i, record_count=1)
        for i in range(n)
    ]


class TestIncrementalLoaderSuccess:
    def test_new_data(self):
        adapter = MagicMock()
        adapter.extract_incremental.return_value = iter(_make_chunks(2))
        ws = MagicMock()
        ws.get.return_value = None  # no previous watermark
        cs = MagicMock()
        ms = MagicMock()
        mr = MagicMock()

        loader = IncrementalLoader(adapter, cs, ws, ms, mr)
        result = loader.execute(_make_config(), "run_1", "batch_1")

        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 2
        assert result.watermark_before is None
        # watermark_store.set should NOT be called by loader
        ws.set.assert_not_called()

    def test_no_new_data(self):
        adapter = MagicMock()
        adapter.extract_incremental.return_value = iter([])  # empty
        ws = MagicMock()
        ws.get.return_value = MagicMock(watermark_value="2024-01-01")
        cs = MagicMock()
        ms = MagicMock()
        mr = MagicMock()

        loader = IncrementalLoader(adapter, cs, ws, ms, mr)
        result = loader.execute(_make_config(), "run_1", "batch_1")

        assert result.status == IngestionStatus.NO_NEW_DATA
        assert result.watermark_before == "2024-01-01"


class TestIncrementalLoaderWatermarkSafety:
    def test_watermark_not_updated_on_failure(self):
        adapter = MagicMock()
        adapter.extract_incremental.side_effect = RuntimeError("API error")
        ws = MagicMock()
        ws.get.return_value = MagicMock(watermark_value="2024-01-01")
        cs = MagicMock()
        ms = MagicMock()
        mr = MagicMock()

        loader = IncrementalLoader(adapter, cs, ws, ms, mr)
        result = loader.execute(_make_config(), "run_1", "batch_1")

        assert result.status == IngestionStatus.FAILED
        assert result.watermark_before == "2024-01-01"
        assert result.watermark_after is None
        # Watermark store should NEVER be called on failure
        ws.set.assert_not_called()

    def test_watermark_only_in_result(self):
        """Loader sets watermark_after in result but does NOT commit it."""
        adapter = MagicMock()
        adapter.extract_incremental.return_value = iter(_make_chunks(1))
        ws = MagicMock()
        ws.get.return_value = MagicMock(watermark_value="2024-01-01")
        cs = MagicMock()
        ms = MagicMock()
        mr = MagicMock()

        loader = IncrementalLoader(adapter, cs, ws, ms, mr)
        result = loader.execute(_make_config(), "run_1", "batch_1")

        assert result.status == IngestionStatus.SUCCESS
        # Watermark is in the result but NOT committed by the loader
        ws.set.assert_not_called()
