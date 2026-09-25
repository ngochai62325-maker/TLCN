"""Tests for source adapters."""

import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from ingestion.config.registry import SourceRegistry
from ingestion.core.config import SourceConfig, ReadinessConfig
from ingestion.core.enums import SourceType, LoadStrategy, ArtifactFormat, IngestionStatus
from ingestion.core.full_loader import FullLoader
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.faostat_bulk_adapter import FaostatBulkAdapter
from ingestion.adapters.nso_adapter import NsoVietnamAdapter
from ingestion.utils.error_classifier import SchemaError, PermanentError, DataQualityError



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


class TestFaostatProductionAdapter:
    """Rigorous unit and integration test suite for FAOSTAT Production Adapter."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    @pytest.fixture
    def prod_config(self, registry, monkeypatch):
        monkeypatch.setenv("INGESTION_USE_LOCAL_FALLBACK", "1")
        return registry.get_source("faostat_production")

    @pytest.fixture
    def adapter(self):
        return FaostatBulkAdapter()

    def test_01_adapter_initialization(self, prod_config, adapter):
        """1. Adapter initialization & configuration resolution."""
        assert prod_config.source_id == "faostat_production"
        assert prod_config.load_strategy == LoadStrategy.FULL
        assert prod_config.source_type == SourceType.LOCAL_FILE
        assert prod_config.local_path == "data/raw/faostat/production_world.csv"
        assert prod_config.enabled is True
        assert len(prod_config.expected_columns) == 15
        assert isinstance(adapter, FaostatBulkAdapter)

    def test_02_local_extraction(self, prod_config, adapter, tmp_path):
        """2. Local extraction yields valid chunks."""
        chunks = list(adapter.extract_full(prod_config, download_dir=str(tmp_path)))
        assert len(chunks) >= 1
        assert all(isinstance(c, DataChunk) for c in chunks)

    def test_03_datachunk_contract(self, prod_config, adapter, tmp_path):
        """3. DataChunk contract: chunk_id, row boundaries, record_count, SHA-256 checksum."""
        chunks = list(adapter.extract_full(prod_config, download_dir=str(tmp_path)))
        chunk = chunks[0]
        assert chunk.chunk_id == 0
        assert chunk.row_start == 0
        assert chunk.row_end == chunk.record_count - 1
        assert chunk.record_count == 22738
        assert isinstance(chunk.data, pd.DataFrame)
        assert len(chunk.checksum) == 64  # valid SHA-256 hex string

    def test_04_record_count(self, prod_config, adapter, tmp_path):
        """4. Total record count equals exactly 22,738 rows from production_world.csv."""
        chunks = list(adapter.extract_full(prod_config, download_dir=str(tmp_path)))
        total_rows = sum(c.record_count for c in chunks)
        assert total_rows == 22738

    def test_05_required_source_columns(self, prod_config, adapter, tmp_path):
        """5. Extracted data contains all 15 required domain columns without BOM corruption."""
        chunks = list(adapter.extract_full(prod_config, download_dir=str(tmp_path)))
        chunk_cols = list(chunks[0].data.columns)
        for expected in prod_config.expected_columns:
            assert expected in chunk_cols, f"Missing column: {expected}"
        assert not any(c.startswith("\ufeff") for c in chunk_cols), "BOM corruption detected in column names"

    def test_06_empty_file_handling(self, prod_config, adapter, tmp_path):
        """6. Graceful handling of empty CSV file (0 bytes)."""
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("", encoding="utf-8")
        cfg = SourceConfig(
            source_id="faostat_empty_test",
            provider="FAOSTAT",
            dataset="Empty",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_fallback=str(empty_csv),
            local_path=str(empty_csv),
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path)))
        assert len(chunks) == 0

    def test_07_missing_file_handling(self, adapter, tmp_path):
        """7. Missing source file raises FileNotFoundError or SourceNotReadyError."""
        cfg = SourceConfig(
            source_id="faostat_missing_test",
            provider="FAOSTAT",
            dataset="Missing",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_fallback=str(tmp_path / "non_existent.csv"),
            local_path=str(tmp_path / "non_existent.csv"),
        )
        with pytest.raises((FileNotFoundError, PermanentError)):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_08_malformed_input_schema_protection(self, adapter, tmp_path):
        """8. Malformed CSV missing required schema columns triggers SchemaError."""
        bad_csv = tmp_path / "corrupted.csv"
        bad_csv.write_text("Col_A,Col_B\n1,2\n3,4\n", encoding="utf-8")
        cfg = SourceConfig(
            source_id="faostat_bad_schema",
            provider="FAOSTAT",
            dataset="BadSchema",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_fallback=str(bad_csv),
            local_path=str(bad_csv),
            expected_columns=["Domain Code", "Domain", "Value"],
        )
        with pytest.raises(SchemaError, match="missing required columns"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_09_local_fallback_preference(self, prod_config, adapter, tmp_path, monkeypatch):
        """9. When INGESTION_USE_LOCAL_FALLBACK=1 is set, uses local fallback without network."""
        monkeypatch.setenv("INGESTION_USE_LOCAL_FALLBACK", "1")
        cfg = SourceConfig(
            source_id=prod_config.source_id,
            provider=prod_config.provider,
            dataset=prod_config.dataset,
            source_type=prod_config.source_type,
            load_strategy=prod_config.load_strategy,
            format=prod_config.format,
            endpoint="http://0.0.0.0:1/unreachable.zip",
            local_fallback=prod_config.local_fallback,
            chunk_size=prod_config.chunk_size,
            expected_columns=prod_config.expected_columns,
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path)))
        assert len(chunks) >= 1
        assert chunks[0].record_count == 22738

    def test_10_full_loader_integration(self, prod_config, adapter):
        """10. FullLoader executes full ingestion lifecycle with mocked landing & checkpoints."""
        cs = MagicMock()
        cs.get_last_successful.return_value = None
        ms = MagicMock()
        ms.build_raw_landing_key.return_value = "raw/faostat_production/test.csv"
        ms.build_manifest_key.return_value = "raw/faostat_production/manifest.json"
        ms.upload_file.return_value = "s3://bronze/raw/faostat_production/test.csv"
        ms.upload_json.return_value = "s3://bronze/raw/faostat_production/manifest.json"
        mr = MagicMock()
        mr.get_latest_run.return_value = None
        mr.find_snapshot_by_checksum.return_value = None
        mr.save_snapshot.return_value = 1

        loader = FullLoader(
            adapter=adapter,
            checkpoint_store=cs,
            minio_storage=ms,
            metadata_repo=mr,
            bronze_writer=None,
        )
        res = loader.execute(prod_config, run_id="unit_test_run", batch_id="unit_test_batch")
        assert res.status == IngestionStatus.SUCCESS
        assert res.records_extracted == 22738
        assert res.checksum is not None
        assert cs.save.called

    def test_11_live_readiness_probe(self, prod_config, adapter):
        """11. Probe live FAOSTAT endpoint via HEAD request."""
        res = adapter.check_readiness(prod_config)
        assert isinstance(res, ReadinessResult)
        if res.ready:
            assert res.source_metadata is not None

    def test_12_bronze_writer_and_idempotency_contract(self, prod_config, adapter):
        """12. FullLoader detects existing snapshot and skips ingestion (idempotent)."""
        cs = MagicMock()
        ms = MagicMock()
        mr = MagicMock()
        mr.find_snapshot_by_checksum.return_value = {
            "snapshot_id": 42,
            "run_id": "previous_run_123",
            "status": "SUCCESS",
        }

        loader = FullLoader(
            adapter=adapter,
            checkpoint_store=cs,
            minio_storage=ms,
            metadata_repo=mr,
            bronze_writer=None,
        )
        res = loader.execute(prod_config, run_id="unit_test_rerun", batch_id="unit_test_rerun_batch")
        assert res.status == IngestionStatus.SKIPPED
        assert (res.source_metadata or {}).get("skipped_reason") == "unchanged_snapshot"
        assert res.records_extracted == 0

