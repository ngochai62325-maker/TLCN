"""Tests for ingestion.core.ingestion_engine.IngestionEngine."""

import pytest
from unittest.mock import MagicMock, patch
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.enums import IngestionStatus, LoadStrategy, SourceType, ArtifactFormat
from ingestion.core.config import SourceConfig
from ingestion.core.result import IngestionResult


def _make_config():
    return SourceConfig(
        source_id="test_src",
        provider="Test",
        dataset="TestData",
        source_type=SourceType.LOCAL_FILE,
        load_strategy=LoadStrategy.FULL,
        format=ArtifactFormat.CSV,
        adapter_class="ingestion.adapters.faostat_bulk_adapter.FaostatBulkAdapter",
    )


class TestIngestionEngine:
    @pytest.fixture
    def engine(self):
        registry = MagicMock()
        registry.load.return_value = None
        registry.get_source.return_value = _make_config()
        mr = MagicMock()
        cs = MagicMock()
        cs.get_last_successful.return_value = None
        ws = MagicMock()
        ms = MagicMock()
        ms.upload_file.return_value = "s3://bronze/raw/test/data.csv"
        ms.upload_json.return_value = "s3://bronze/raw/test/manifest.json"
        ms.build_raw_landing_key.return_value = "raw/test/data.csv"
        ms.build_manifest_key.return_value = "raw/test/manifest.json"
        mr.find_snapshot_by_checksum.return_value = None
        return IngestionEngine(
            registry=registry,
            metadata_repo=mr,
            checkpoint_store=cs,
            watermark_store=ws,
            minio_storage=ms,
        )

    @patch("ingestion.core.ingestion_engine.importlib.import_module")
    def test_run_full_load(self, mock_import, engine):
        """Engine resolves adapter and runs full load."""
        mock_adapter_cls = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.extract_full.return_value = iter([])
        mock_adapter_cls.return_value = mock_adapter
        mock_module = MagicMock()
        mock_module.FaostatBulkAdapter = mock_adapter_cls
        mock_import.return_value = mock_module

        result = engine.run("test_src")
        assert result.source_id == "test_src"
        assert result.status in (IngestionStatus.SUCCESS, IngestionStatus.SKIPPED)

    def test_run_with_readiness_not_ready(self, engine):
        """When readiness check fails, return NOT_READY."""
        engine.readiness_checker = MagicMock()
        engine.readiness_checker.check.return_value = MagicMock(
            ready=False, reason="Source unavailable", source_metadata={}
        )
        result = engine.run_with_readiness_check("test_src")
        assert result.status == IngestionStatus.NOT_READY

    @patch("ingestion.core.ingestion_engine.importlib.import_module")
    def test_run_with_readiness_ready(self, mock_import, engine):
        """When readiness passes, proceed to run."""
        engine.readiness_checker = MagicMock()
        engine.readiness_checker.check.return_value = MagicMock(
            ready=True, reason="OK", source_metadata={}
        )
        mock_adapter_cls = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.extract_full.return_value = iter([])
        mock_adapter_cls.return_value = mock_adapter
        mock_module = MagicMock()
        mock_module.FaostatBulkAdapter = mock_adapter_cls
        mock_import.return_value = mock_module

        result = engine.run_with_readiness_check("test_src")
        assert result.status in (IngestionStatus.SUCCESS, IngestionStatus.SKIPPED)

    def test_unknown_source(self, engine):
        """Unknown source_id should fail gracefully."""
        engine.registry.get_source.side_effect = KeyError("not found")
        result = engine.run("unknown_source")
        assert result.status == IngestionStatus.FAILED

    def test_bad_adapter_class(self, engine):
        """Invalid adapter_class should fail gracefully."""
        config = _make_config()
        config.adapter_class = "nonexistent.module.Adapter"
        engine.registry.get_source.return_value = config
        result = engine.run("test_src")
        assert result.status == IngestionStatus.FAILED
