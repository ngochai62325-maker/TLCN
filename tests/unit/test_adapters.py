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
from ingestion.adapters.worldbank_adapter import WorldBankAdapter
from ingestion.adapters.thitruongnongsan_adapter import ThitruongNongsanAdapter
from ingestion.adapters.usda_adapter import UsdaRiceYearbookAdapter, UsdaLocalAdapter, UsdaPsdAdapter
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


class TestFaostatAdapterPhase2A:
    """Rigorous Phase 2A test suite for FaostatBulkAdapter."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    @pytest.fixture
    def adapter(self):
        return FaostatBulkAdapter()

    def test_readiness_local_file_no_network(self, registry, adapter):
        """Readiness check executes locally with zero HTTP calls."""
        cfg = registry.get_source("faostat_production")
        with patch("requests.head", side_effect=RuntimeError("Network forbidden!")), \
             patch("requests.get", side_effect=RuntimeError("Network forbidden!")):
            res = adapter.check_readiness(cfg)
            assert res.ready is True
            assert "Local file readiness check passed" in res.reason
            assert res.source_metadata["size"] > 0

    def test_readiness_local_file_missing(self, adapter, tmp_path):
        """Readiness fails when local file is missing."""
        cfg = SourceConfig(
            source_id="test_missing",
            provider="FAOSTAT",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path / "does_not_exist.csv"),
        )
        res = adapter.check_readiness(cfg)
        assert res.ready is False
        assert "not found" in res.reason.lower()

    def test_readiness_local_file_empty(self, adapter, tmp_path):
        """Readiness fails when local file is 0 bytes."""
        empty_f = tmp_path / "empty.csv"
        empty_f.write_text("", encoding="utf-8")
        cfg = SourceConfig(
            source_id="test_empty",
            provider="FAOSTAT",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_f),
        )
        res = adapter.check_readiness(cfg)
        assert res.ready is False
        assert "empty" in res.reason.lower()

    def test_extract_faostat_trade_large_file_chunking(self, registry, adapter, tmp_path):
        """Test memory-safe chunked reading on the large FAOSTAT Trade dataset (168 MB)."""
        trade_cfg = registry.get_source("faostat_trade")
        trade_cfg.chunk_size = 10000
        gen = adapter.extract_full(trade_cfg, download_dir=str(tmp_path))

        chunk_0 = next(gen)
        assert chunk_0.chunk_id == 0
        assert chunk_0.row_start == 0
        assert chunk_0.row_end == 9999
        assert chunk_0.record_count == 10000
        assert isinstance(chunk_0.data, pd.DataFrame)
        assert len(chunk_0.checksum) == 64
        for col in trade_cfg.expected_columns:
            assert col in chunk_0.data.columns

        chunk_1 = next(gen)
        assert chunk_1.chunk_id == 1
        assert chunk_1.row_start == 10000
        assert chunk_1.row_end == 19999
        assert chunk_1.record_count == 10000

    def test_extract_zip_archive(self, adapter, tmp_path):
        """Test extraction from a local ZIP archive containing a CSV."""
        import zipfile
        zip_file = tmp_path / "test_bulk.zip"
        csv_content = (
            "Domain Code,Domain,Area Code,Area,Element Code,Element,Item Code,Item,Year Code,Year,Unit,Value,Flag\n"
            "QCL,Crops,237,Vietnam,5510,Production,27,Rice,2022,2022,t,42700000,A\n"
        )
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("test_data.csv", csv_content)

        cfg = SourceConfig(
            source_id="test_zip",
            provider="FAOSTAT",
            dataset="TestZip",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(zip_file),
            expected_columns=["Domain Code", "Domain", "Area", "Element", "Item", "Year", "Unit", "Value"],
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path / "extracted")))
        assert len(chunks) == 1
        assert chunks[0].record_count == 1
        assert chunks[0].data["Area"].iloc[0] == "Vietnam"

    def test_bom_handling_utf8_sig(self, adapter, tmp_path):
        """Test that UTF-8 BOM (\\xef\\xbb\\xbf) is stripped cleanly from headers."""
        bom_file = tmp_path / "with_bom.csv"
        bom_bytes = b"\xef\xbb\xbfDomain Code,Domain,Value\nQCL,Crops,100\n"
        bom_file.write_bytes(bom_bytes)

        cfg = SourceConfig(
            source_id="test_bom",
            provider="FAOSTAT",
            dataset="TestBOM",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bom_file),
            encoding="utf-8",
            expected_columns=["Domain Code", "Domain", "Value"],
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        cols = list(chunks[0].data.columns)
        assert cols[0] == "Domain Code"
        assert not cols[0].startswith("\ufeff")

    def test_unsupported_file_format_raises_permanent_error(self, adapter, tmp_path):
        """Unsupported file extensions raise PermanentError."""
        bad_format_file = tmp_path / "unsupported.parquet"
        bad_format_file.write_text("dummy", encoding="utf-8")
        cfg = SourceConfig(
            source_id="test_bad_fmt",
            provider="FAOSTAT",
            dataset="TestBadFmt",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_format_file),
        )
        with pytest.raises(PermanentError, match="Unsupported file format"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_corrupted_zip_archive_raises_data_quality_error(self, adapter, tmp_path):
        """Corrupted ZIP archive raises DataQualityError."""
        bad_zip = tmp_path / "corrupted.zip"
        bad_zip.write_bytes(b"NOT_A_VALID_ZIP_HEADER_DATA_1234567890")
        cfg = SourceConfig(
            source_id="test_corrupted_zip",
            provider="FAOSTAT",
            dataset="TestCorruptedZip",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_zip),
        )
        with pytest.raises(DataQualityError, match="Corrupted or invalid ZIP"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_zip_with_no_csv_raises_data_quality_error(self, adapter, tmp_path):
        """ZIP archive without CSV raises DataQualityError."""
        import zipfile
        empty_zip = tmp_path / "no_csv.zip"
        with zipfile.ZipFile(empty_zip, "w") as zf:
            zf.writestr("notes.txt", "No CSV here")
        cfg = SourceConfig(
            source_id="test_no_csv_zip",
            provider="FAOSTAT",
            dataset="TestNoCsvZip",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_zip),
        )
        with pytest.raises(DataQualityError, match="No CSV file found inside ZIP"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_directory_with_no_csv_or_zip_raises_data_quality_error(self, adapter, tmp_path):
        """Directory with no CSV or ZIP raises DataQualityError."""
        sub_dir = tmp_path / "empty_sub"
        sub_dir.mkdir()
        (sub_dir / "random.json").write_text("{}", encoding="utf-8")
        cfg = SourceConfig(
            source_id="test_no_csv_dir",
            provider="FAOSTAT",
            dataset="TestNoCsvDir",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(sub_dir),
        )
        with pytest.raises(DataQualityError, match="No CSV or ZIP files found in directory"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_all_canonical_faostat_sources_extract_first_chunk(self, registry, adapter, tmp_path):
        """All 4 canonical FAOSTAT sources in data/raw extract first chunk successfully."""
        faostat_sources = [
            "faostat_production",
            "faostat_monthly_price",
            "faostat_trade",
            "faostat_supply_utilization",
        ]
        for sid in faostat_sources:
            cfg = registry.get_source(sid)
            cfg.chunk_size = 100
            gen = adapter.extract_full(cfg, download_dir=str(tmp_path))
            chunk = next(gen)
            assert chunk.record_count > 0
            assert isinstance(chunk.data, pd.DataFrame)
            for exp_col in cfg.expected_columns:
                assert exp_col in chunk.data.columns, f"Source '{sid}' missing column '{exp_col}'"


class TestNsoAdapterPhase2B:
    """Rigorous Phase 2B test suite for NsoVietnamAdapter."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    @pytest.fixture
    def nso_config(self, registry):
        return registry.get_source("nso_vietnam")

    @pytest.fixture
    def adapter(self):
        return NsoVietnamAdapter()

    def test_01_initialization(self, nso_config, adapter):
        """1. Adapter initialization & configuration contract."""
        assert nso_config.source_id == "nso_vietnam"
        assert nso_config.source_type == SourceType.LOCAL_FILE
        assert nso_config.load_strategy == LoadStrategy.FULL
        assert nso_config.local_path == "data/raw/nso"
        assert nso_config.extra.get("file_pattern") == "V06.*.csv"
        assert isinstance(adapter, NsoVietnamAdapter)

    def test_02_local_directory_readiness(self, nso_config, adapter):
        """2. Local directory readiness check succeeds with correct file count."""
        res = adapter.check_readiness(nso_config)
        assert res.ready is True
        assert res.source_metadata["file_count"] == 13
        assert len(res.source_metadata["files"]) == 13
        assert res.source_metadata["total_size_bytes"] > 0

    def test_03_missing_directory(self, adapter, tmp_path):
        """3. Missing directory fails readiness and raises FileNotFoundError on extract."""
        cfg = SourceConfig(
            source_id="nso_missing",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path / "non_existent_nso_dir"),
        )
        res = adapter.check_readiness(cfg)
        assert res.ready is False
        assert "not exist" in res.reason.lower()

        with pytest.raises(FileNotFoundError, match="Local NSO directory not found"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_04_empty_directory(self, adapter, tmp_path):
        """4. Directory with no matching files fails readiness and raises DataQualityError on extract."""
        empty_dir = tmp_path / "empty_nso"
        empty_dir.mkdir()
        cfg = SourceConfig(
            source_id="nso_empty",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_dir),
            extra={"file_pattern": "V06.*.csv"},
        )
        res = adapter.check_readiness(cfg)
        assert res.ready is False
        assert "no files matching" in res.reason.lower()

        with pytest.raises(DataQualityError, match="No files matching pattern"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_05_multi_file_discovery(self, nso_config, adapter):
        """5. Multi-file discovery finds all 13 CSVs in sorted sequence (V06.12 to V06.24)."""
        pattern = nso_config.extra.get("file_pattern", "V06.*.csv")
        import glob
        discovered = [
            os.path.basename(f)
            for f in sorted(glob.glob(os.path.join(nso_config.local_path, pattern)))
        ]
        assert len(discovered) == 13
        expected_names = [f"V06.{i}.csv" for i in range(12, 25)]
        assert discovered == expected_names

    def test_06_multi_file_extraction(self, nso_config, adapter, tmp_path):
        """6. Multi-file extraction yields exactly 13 DataChunks for 13 files."""
        chunks = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        assert len(chunks) == 13
        extracted_files = [c.data["_source_file"].iloc[0] for c in chunks]
        expected_names = [f"V06.{i}.csv" for i in range(12, 25)]
        assert extracted_files == expected_names

    def test_07_datachunk_contract(self, nso_config, adapter, tmp_path):
        """7. DataChunk contract: int chunk_id, row boundaries, record_count, SHA-256 checksum."""
        chunks = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        cum_rows = 0
        for i, chunk in enumerate(chunks):
            assert chunk.chunk_id == i
            assert isinstance(chunk.chunk_id, int)
            assert chunk.row_start == cum_rows
            assert chunk.row_end == cum_rows + chunk.record_count - 1
            assert isinstance(chunk.data, pd.DataFrame)
            assert len(chunk.checksum) == 64  # valid SHA-256
            assert "_source_file" in chunk.data.columns
            cum_rows += chunk.record_count

    def test_08_record_count_real_dataset(self, nso_config, adapter, tmp_path):
        """8. Total record count equals exactly 833 rows across all 13 files."""
        chunks = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        total_rows = sum(c.record_count for c in chunks)
        assert total_rows == 833

    def test_09_bom_handling(self, adapter, tmp_path):
        """9. UTF-8 BOM is stripped cleanly and does not corrupt column names."""
        bom_file = tmp_path / "V06.12.csv"
        # Write bytes with UTF-8 BOM
        bom_bytes = b"\xef\xbb\xbfCol_A,Col_B,Col_C\n1,2,3\n4,5,6\n"
        bom_file.write_bytes(bom_bytes)

        cfg = SourceConfig(
            source_id="nso_bom_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        cols = list(chunks[0].data.columns)
        assert "Col_A" in cols
        assert not cols[0].startswith("\ufeff")

    def test_10_schema_contract_expected_columns(self, adapter, tmp_path):
        """10. Expected columns contract is enforced when specified."""
        f = tmp_path / "V06.12.csv"
        f.write_text("Province,1995,1996\nHanoi,10,20\n", encoding="utf-8")
        cfg = SourceConfig(
            source_id="nso_schema_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
            expected_columns=["Province", "1995", "Missing_Column"],
        )
        with pytest.raises(SchemaError, match="missing required columns"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_11_inconsistent_schema_between_files(self, adapter, tmp_path):
        """11. Inconsistent schema between files raises SchemaError when require_uniform_schema=True."""
        f1 = tmp_path / "V06.12.csv"
        f1.write_text("Col_A,Col_B\n1,2\n", encoding="utf-8")
        f2 = tmp_path / "V06.13.csv"
        f2.write_text("Col_X,Col_Y\n3,4\n", encoding="utf-8")

        cfg = SourceConfig(
            source_id="nso_uniform_test",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv", "require_uniform_schema": True},
        )
        with pytest.raises(SchemaError, match="Inconsistent schema"):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_12_malformed_csv_raises_data_quality_error(self, adapter, tmp_path):
        """12. Malformed CSV syntax raises DataQualityError."""
        bad_f = tmp_path / "V06.12.csv"
        bad_f.write_bytes(b'col1,col2\n"unclosed quote line 1\nline2,val,extra\n')

        cfg = SourceConfig(
            source_id="nso_bad_csv",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        with pytest.raises(DataQualityError):
            list(adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_13_empty_file_and_header_only_handling(self, adapter, tmp_path):
        """13. Empty (0-byte) and header-only files are skipped gracefully without emitting empty chunks."""
        (tmp_path / "V06.12.csv").write_bytes(b"")
        (tmp_path / "V06.13.csv").write_text("colA,colB\n", encoding="utf-8")
        (tmp_path / "V06.14.csv").write_text("colA,colB\n1,2\n", encoding="utf-8")

        cfg = SourceConfig(
            source_id="nso_empty_handling",
            provider="GSO",
            dataset="V06",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path),
            extra={"file_pattern": "V06.*.csv"},
        )
        chunks = list(adapter.extract_full(cfg, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        assert chunks[0].data["_source_file"].iloc[0] == "V06.14.csv"
        assert chunks[0].record_count == 1

    def test_14_checksum_stability_and_file_idempotency(self, nso_config, adapter, tmp_path):
        """14. Checksum stability: re-extracting produces identical SHA-256 for identical files."""
        chunks_run1 = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        chunks_run2 = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        assert len(chunks_run1) == len(chunks_run2)
        for c1, c2 in zip(chunks_run1, chunks_run2):
            assert c1.checksum == c2.checksum
            assert c1.record_count == c2.record_count

    def test_15_network_isolation(self, nso_config, adapter, tmp_path):
        """15. Pure local execution: zero network/HTTP calls during readiness and extraction."""
        with patch("requests.get", side_effect=RuntimeError("HTTP call forbidden!")), \
             patch("requests.head", side_effect=RuntimeError("HTTP call forbidden!")):
            res = adapter.check_readiness(nso_config)
            assert res.ready is True
            chunks = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
            assert len(chunks) == 13

    def test_16_real_data_profiling_and_extraction(self, nso_config, adapter, tmp_path):
        """16. Profile real dataset: V06.12 is 10 cols, V06.13..24 are 31 cols, all parsed cleanly."""
        chunks = list(adapter.extract_full(nso_config, download_dir=str(tmp_path)))
        assert len(chunks[0].data.columns) == 11
        assert chunks[0].record_count == 71
        assert "Nam" in chunks[0].data.columns

        for c in chunks[1:]:
            assert len(c.data.columns) == 32
            assert "1995" in c.data.columns
            assert "2023" in c.data.columns
            assert c.record_count in (38, 72)


class TestExcelAdaptersPhase2C:
    """Rigorous Phase 2C test suite for Excel-based adapters (World Bank & ThiTruongNongSan)."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    @pytest.fixture
    def wb_config(self, registry):
        return registry.get_source("worldbank_pinksheet")

    @pytest.fixture
    def ttns_config(self, registry):
        return registry.get_source("thitruongnongsan")

    @pytest.fixture
    def wb_adapter(self):
        return WorldBankAdapter()

    @pytest.fixture
    def ttns_adapter(self):
        return ThitruongNongsanAdapter()

    def test_01_initialization(self, wb_config, ttns_config, wb_adapter, ttns_adapter):
        """1. Adapter initialization & configuration contract."""
        assert wb_config.source_id == "worldbank_pinksheet"
        assert wb_config.source_type == SourceType.LOCAL_FILE
        assert wb_config.format == ArtifactFormat.EXCEL
        assert wb_config.local_path == "data/raw/world_bank/CMO-Historical-Data-Monthly.xlsx"
        assert wb_config.extra.get("sheet_name") == "Monthly Prices"
        assert isinstance(wb_adapter, WorldBankAdapter)

        assert ttns_config.source_id == "thitruongnongsan"
        assert ttns_config.source_type == SourceType.LOCAL_FILE
        assert ttns_config.format == ArtifactFormat.EXCEL
        assert ttns_config.local_path == "data/raw/thitruongnongsan/price_luagao.xlsx"
        assert ttns_config.extra.get("sheet_name") == "price_lua_gao"
        assert len(ttns_config.expected_columns) == 8
        assert isinstance(ttns_adapter, ThitruongNongsanAdapter)

    def test_02_local_file_readiness(self, wb_config, ttns_config, wb_adapter, ttns_adapter):
        """2. Local Excel file readiness check succeeds on real files."""
        res_wb = wb_adapter.check_readiness(wb_config)
        assert res_wb.ready is True
        assert res_wb.source_metadata["size_bytes"] == 778415
        assert res_wb.source_metadata["sheet_name"] == "Monthly Prices"

        res_ttns = ttns_adapter.check_readiness(ttns_config)
        assert res_ttns.ready is True
        assert res_ttns.source_metadata["size_bytes"] == 756440
        assert res_ttns.source_metadata["sheet_name"] == "price_lua_gao"

    def test_03_missing_file_handling(self, wb_adapter, ttns_adapter, tmp_path):
        """3. Missing Excel file fails readiness and raises FileNotFoundError on extract."""
        cfg = SourceConfig(
            source_id="test_missing_excel",
            provider="Test",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(tmp_path / "non_existent.xlsx"),
        )
        assert wb_adapter.check_readiness(cfg).ready is False
        assert ttns_adapter.check_readiness(cfg).ready is False

        with pytest.raises(FileNotFoundError):
            list(wb_adapter.extract_full(cfg, download_dir=str(tmp_path)))
        with pytest.raises(FileNotFoundError):
            list(ttns_adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_04_empty_file_handling(self, wb_adapter, ttns_adapter, tmp_path):
        """4. Empty (0-byte) Excel file fails readiness and yields 0 chunks on extract."""
        empty_f = tmp_path / "empty.xlsx"
        empty_f.write_bytes(b"")
        cfg = SourceConfig(
            source_id="test_empty_excel",
            provider="Test",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(empty_f),
        )
        assert wb_adapter.check_readiness(cfg).ready is False
        assert ttns_adapter.check_readiness(cfg).ready is False

        assert len(list(wb_adapter.extract_full(cfg, download_dir=str(tmp_path)))) == 0
        assert len(list(ttns_adapter.extract_full(cfg, download_dir=str(tmp_path)))) == 0

    def test_05_unsupported_file_format(self, wb_adapter, ttns_adapter, tmp_path):
        """5. Unsupported file extensions (.csv, .txt) raise PermanentError."""
        bad_f = tmp_path / "sample.csv"
        bad_f.write_text("col1,col2\n1,2\n", encoding="utf-8")
        cfg = SourceConfig(
            source_id="test_unsupported_fmt",
            provider="Test",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(bad_f),
        )
        with pytest.raises(PermanentError, match="Unsupported file format"):
            list(wb_adapter.extract_full(cfg, download_dir=str(tmp_path)))
        with pytest.raises(PermanentError, match="Unsupported file format"):
            list(ttns_adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_06_sheet_discovery_available_sheets(self, wb_config, wb_adapter):
        """6. Sheet discovery finds all sheets in workbook and records them in metadata."""
        res = wb_adapter.check_readiness(wb_config)
        sheets = res.source_metadata.get("available_sheets", [])
        assert "Monthly Prices" in sheets
        assert "Monthly Indices" in sheets
        assert "Description" in sheets

    def test_07_expected_sheet_extraction(self, ttns_config, ttns_adapter, tmp_path):
        """7. Extraction targets only configured sheet and yields valid DataChunk."""
        chunks = list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        assert chunks[0].record_count == 20394

    def test_08_missing_sheet_raises_data_quality_error(self, tmp_path, wb_adapter, ttns_adapter):
        """8. Workbook missing the expected sheet raises DataQualityError."""
        dummy_excel = tmp_path / "dummy.xlsx"
        df = pd.DataFrame({"A": [1, 2]})
        df.to_excel(dummy_excel, sheet_name="Sheet1", index=False)

        cfg_wb = SourceConfig(
            source_id="wb_bad_sheet",
            provider="WorldBank",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(dummy_excel),
            extra={"sheet_name": "Monthly Prices"},
        )
        with pytest.raises(DataQualityError, match="Expected sheet 'Monthly Prices' not found"):
            list(wb_adapter.extract_full(cfg_wb, download_dir=str(tmp_path)))

        cfg_ttns = SourceConfig(
            source_id="ttns_bad_sheet",
            provider="IPSARD",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(dummy_excel),
            extra={"sheet_name": "price_lua_gao"},
        )
        with pytest.raises(DataQualityError, match="Expected sheet 'price_lua_gao' not found"):
            list(ttns_adapter.extract_full(cfg_ttns, download_dir=str(tmp_path)))

    def test_09_wb_header_detection_multi_row(self, wb_config, wb_adapter, tmp_path):
        """9. World Bank multi-row header (row 4 commodity + row 5 unit) parses correctly."""
        chunks = list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        cols = list(chunks[0].data.columns)
        assert cols[0] == "Period"
        assert "Crude oil, average ($/bbl)" in cols
        assert "Rice, Viet Namese 5% ($/mt)" in cols
        assert "Rice, Thai 5% ($/mt)" in cols

    def test_10_schema_contract_expected_columns(self, ttns_adapter, tmp_path):
        """10. Expected columns contract is enforced; missing columns trigger SchemaError."""
        dummy_excel = tmp_path / "dummy_ttns.xlsx"
        df = pd.DataFrame({"Col1": [1, 2], "Col2": [3, 4]})
        df.to_excel(dummy_excel, sheet_name="price_lua_gao", index=False)

        cfg = SourceConfig(
            source_id="ttns_schema_test",
            provider="IPSARD",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(dummy_excel),
            expected_columns=["Tên_mặt_hàng", "Giá"],
            extra={"sheet_name": "price_lua_gao"},
        )
        with pytest.raises(SchemaError, match="missing required columns"):
            list(ttns_adapter.extract_full(cfg, download_dir=str(tmp_path)))

    def test_11_datachunk_contract(self, wb_config, ttns_config, wb_adapter, ttns_adapter, tmp_path):
        """11. DataChunk contract: int chunk_id=0, row boundaries, record_count, SHA-256."""
        chunk_wb = list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))[0]
        assert chunk_wb.chunk_id == 0
        assert chunk_wb.row_start == 0
        assert chunk_wb.row_end == 791
        assert chunk_wb.record_count == 792
        assert len(chunk_wb.checksum) == 64
        assert "_source_file" in chunk_wb.data.columns

        chunk_ttns = list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))[0]
        assert chunk_ttns.chunk_id == 0
        assert chunk_ttns.row_start == 0
        assert chunk_ttns.row_end == 20393
        assert chunk_ttns.record_count == 20394
        assert len(chunk_ttns.checksum) == 64
        assert "_source_file" in chunk_ttns.data.columns

    def test_12_checksum_stability(self, wb_config, ttns_config, wb_adapter, ttns_adapter, tmp_path):
        """12. Checksum stability: multiple extractions yield identical SHA-256."""
        c1 = list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))[0]
        c2 = list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))[0]
        assert c1.checksum == c2.checksum

        t1 = list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))[0]
        t2 = list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))[0]
        assert t1.checksum == t2.checksum

    def test_13_network_isolation(self, wb_config, ttns_config, wb_adapter, ttns_adapter, tmp_path):
        """13. Pure local execution: zero network calls during readiness and extraction."""
        with patch("requests.get", side_effect=RuntimeError("Network forbidden!")), \
             patch("requests.head", side_effect=RuntimeError("Network forbidden!")):
            assert wb_adapter.check_readiness(wb_config).ready is True
            assert ttns_adapter.check_readiness(ttns_config).ready is True
            assert len(list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))) == 1
            assert len(list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))) == 1

    def test_14_real_worldbank_workbook_extraction(self, wb_config, wb_adapter, tmp_path):
        """14. Live extraction on CMO-Historical-Data-Monthly.xlsx yields exactly 792 rows, 90 cols."""
        chunks = list(wb_adapter.extract_full(wb_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        df = chunks[0].data
        assert df.shape == (792, 90)
        assert df["Period"].iloc[0] == "1960M01"
        assert df["Period"].iloc[-1] == "2025M12"
        assert df["_source_file"].iloc[0] == "CMO-Historical-Data-Monthly.xlsx"

    def test_15_real_thitruongnongsan_workbook_extraction(self, ttns_config, ttns_adapter, tmp_path):
        """15. Live extraction on price_luagao.xlsx yields exactly 20,394 rows, 9 cols."""
        chunks = list(ttns_adapter.extract_full(ttns_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        df = chunks[0].data
        assert df.shape == (20394, 9)
        for exp_col in ttns_config.expected_columns:
            assert exp_col in df.columns
        assert df["_source_file"].iloc[0] == "price_luagao.xlsx"

    def test_16_corrupted_workbook_raises_data_quality_error(self, wb_adapter, ttns_adapter, tmp_path):
        """16. Corrupted/invalid Excel workbook raises DataQualityError."""
        corrupt_f = tmp_path / "corrupt.xlsx"
        corrupt_f.write_bytes(b"NOT_A_VALID_ZIP_OR_EXCEL_HEADER_XYZ")

        cfg = SourceConfig(
            source_id="test_corrupt_wb",
            provider="Test",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.EXCEL,
            local_path=str(corrupt_f),
        )
        with pytest.raises(DataQualityError, match="Corrupted or invalid Excel"):
            list(wb_adapter.extract_full(cfg, download_dir=str(tmp_path)))
        with pytest.raises(DataQualityError, match="Corrupted or invalid Excel"):
            list(ttns_adapter.extract_full(cfg, download_dir=str(tmp_path)))


class TestUsdaAdaptersPhase2D:
    """Rigorous unit and regression test suite for Phase 2D USDA Adapters."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    @pytest.fixture
    def yb_config(self, registry):
        return registry.get_source("usda_rice_yearbook")

    @pytest.fixture
    def psd_config(self, registry):
        return registry.get_source("usda_psd")

    @pytest.fixture
    def yb_adapter(self):
        return UsdaRiceYearbookAdapter()

    @pytest.fixture
    def psd_adapter(self):
        return UsdaPsdAdapter()

    def test_01_adapter_initialization(self, yb_config, psd_config, yb_adapter, psd_adapter):
        """1. Adapter initialization & configuration resolution for both USDA sources."""
        assert yb_config.source_id == "usda_rice_yearbook"
        assert yb_config.source_type == SourceType.LOCAL_FILE
        assert yb_config.load_strategy == LoadStrategy.FULL
        assert yb_config.local_path == "data/raw/usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv"
        assert isinstance(yb_adapter, UsdaRiceYearbookAdapter)
        assert UsdaLocalAdapter is UsdaRiceYearbookAdapter

        assert psd_config.source_id == "usda_psd"
        assert psd_config.source_type == SourceType.LOCAL_FILE
        assert psd_config.load_strategy == LoadStrategy.FULL
        assert psd_config.local_path == "data/raw/usda/usda.xls"
        assert isinstance(psd_adapter, UsdaPsdAdapter)

    def test_02_local_readiness(self, yb_config, psd_config, yb_adapter, psd_adapter):
        """2. Local readiness probe verifies local file existence and valid format without HTTP."""
        res_yb = yb_adapter.check_readiness(yb_config)
        assert res_yb.ready is True
        assert res_yb.source_metadata["format"] == "CSV"
        assert res_yb.source_metadata["size_bytes"] == 2122534

        res_psd = psd_adapter.check_readiness(psd_config)
        assert res_psd.ready is True
        assert res_psd.source_metadata["detected_format"] == "HTML"
        assert res_psd.source_metadata["tables_count"] == 1
        assert res_psd.source_metadata["size_bytes"] == 45542

    def test_03_missing_file_handling(self, yb_adapter, psd_adapter, tmp_path):
        """3. Missing source file returns ready=False in readiness and raises FileNotFoundError in extract."""
        cfg_missing_yb = SourceConfig(
            source_id="test_missing_yb",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path / "non_existent.csv"),
        )
        assert yb_adapter.check_readiness(cfg_missing_yb).ready is False
        with pytest.raises(FileNotFoundError):
            list(yb_adapter.extract_full(cfg_missing_yb, download_dir=str(tmp_path)))

        cfg_missing_psd = SourceConfig(
            source_id="test_missing_psd",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(tmp_path / "non_existent.xls"),
        )
        assert psd_adapter.check_readiness(cfg_missing_psd).ready is False
        with pytest.raises(FileNotFoundError):
            list(psd_adapter.extract_full(cfg_missing_psd, download_dir=str(tmp_path)))

    def test_04_empty_file_handling(self, yb_adapter, psd_adapter, tmp_path):
        """4. Empty file (0 bytes) returns ready=False in readiness and yields 0 chunks in extract."""
        empty_f = tmp_path / "empty.csv"
        empty_f.write_text("", encoding="utf-8")

        cfg_yb = SourceConfig(
            source_id="test_empty_yb",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_f),
        )
        assert yb_adapter.check_readiness(cfg_yb).ready is False
        assert list(yb_adapter.extract_full(cfg_yb, download_dir=str(tmp_path))) == []

        empty_xls = tmp_path / "empty.xls"
        empty_xls.write_bytes(b"")

        cfg_psd = SourceConfig(
            source_id="test_empty_psd",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_xls),
        )
        assert psd_adapter.check_readiness(cfg_psd).ready is False
        assert list(psd_adapter.extract_full(cfg_psd, download_dir=str(tmp_path))) == []

    def test_05_actual_format_detection_html_vs_biff_vs_openxml(self, tmp_path):
        """5. Format detector distinguishes HTML table masquerading as XLS vs BIFF vs OpenXML vs CSV."""
        html_f = tmp_path / "test_masquerade.xls"
        html_f.write_text("<html><body><table><tr><td>1</td></tr></table></body></html>", encoding="utf-8")
        assert UsdaPsdAdapter.detect_file_format(str(html_f)) == "HTML"

        # Binary BIFF (.xls)
        biff_f = tmp_path / "test_binary.xls"
        biff_f.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00\x00\x00\x00")
        assert UsdaPsdAdapter.detect_file_format(str(biff_f)) == "EXCEL_BIFF"

        # OpenXML (.xlsx)
        openxml_f = tmp_path / "test_openxml.xlsx"
        openxml_f.write_bytes(b"PK\x03\x04\x14\x00\x06\x00")
        assert UsdaPsdAdapter.detect_file_format(str(openxml_f)) == "EXCEL_OPENXML"

        # CSV
        csv_f = tmp_path / "test.csv"
        csv_f.write_text("colA,colB\n1,2\n", encoding="utf-8")
        assert UsdaPsdAdapter.detect_file_format(str(csv_f)) == "CSV"

        # Unsupported format (ELF / random binary)
        bin_f = tmp_path / "test.bin"
        bin_f.write_bytes(b"\x7fELF\x02\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00")
        assert UsdaPsdAdapter.detect_file_format(str(bin_f)) == "UNSUPPORTED"

    def test_06_csv_chunked_extraction(self, yb_config, yb_adapter, tmp_path):
        """6. CSV extraction supports chunked reading and resume capability."""
        # Using a small chunk size of 5000 to verify multi-chunk behavior
        test_cfg = SourceConfig(
            source_id=yb_config.source_id,
            provider=yb_config.provider,
            dataset=yb_config.dataset,
            source_type=yb_config.source_type,
            load_strategy=yb_config.load_strategy,
            format=yb_config.format,
            local_path=yb_config.local_path,
            chunk_size=5000,
        )
        chunks = list(yb_adapter.extract_full(test_cfg, download_dir=str(tmp_path)))
        assert len(chunks) == 3
        assert chunks[0].record_count == 5000
        assert chunks[0].chunk_id == 0
        assert chunks[0].row_start == 0
        assert chunks[0].row_end == 4999

        assert chunks[1].record_count == 5000
        assert chunks[1].chunk_id == 1
        assert chunks[1].row_start == 5000
        assert chunks[1].row_end == 9999

        assert chunks[2].record_count == 3131
        assert chunks[2].chunk_id == 2
        assert chunks[2].row_start == 10000
        assert chunks[2].row_end == 13130

        total_records = sum(c.record_count for c in chunks)
        assert total_records == 13131

        # Test resume functionality: skip chunk 0
        resumed_chunks = list(
            yb_adapter.extract_full(test_cfg, download_dir=str(tmp_path), resume_chunk_id=1)
        )
        assert len(resumed_chunks) == 2
        assert resumed_chunks[0].chunk_id == 1

    def test_07_usda_psd_extraction_html(self, psd_config, psd_adapter, tmp_path):
        """7. USDA PSD extracts single chunk from usda.xls HTML table preserving exact shape (15, 71)."""
        chunks = list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk.chunk_id == 0
        assert chunk.row_start == 0
        assert chunk.row_end == 14
        assert chunk.record_count == 15
        assert chunk.data.shape == (15, 71)

    def test_08_malformed_input_handling(self, yb_adapter, psd_adapter, tmp_path):
        """8. Malformed HTML (no table) or corrupt CSV raises DataQualityError."""
        bad_html = tmp_path / "bad.xls"
        bad_html.write_text("<html><body><p>No table content here!</p></body></html>", encoding="utf-8")

        cfg_psd = SourceConfig(
            source_id="test_bad_psd",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_html),
        )
        with pytest.raises(DataQualityError, match="Malformed or unreadable HTML table|No HTML tables found"):
            list(psd_adapter.extract_full(cfg_psd, download_dir=str(tmp_path)))

        bad_csv = tmp_path / "bad.csv"
        # CSV with bad lines causing ParserError
        bad_csv.write_text("TABLE_NAME,TABLE_NUMBER\nval1\nval2,val3,val4\n", encoding="utf-8")
        cfg_yb = SourceConfig(
            source_id="test_bad_yb",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_csv),
        )
        with pytest.raises(DataQualityError):
            list(yb_adapter.extract_full(cfg_yb, download_dir=str(tmp_path)))

    def test_09_unsupported_format_raises_permanent_error(self, yb_adapter, psd_adapter, tmp_path):
        """9. Unsupported binary files raise PermanentError."""
        bad_bin = tmp_path / "mystery.bin"
        bad_bin.write_bytes(b"\x00\x01\x02\x03\x04\x05\x06\x07")

        cfg_yb = SourceConfig(
            source_id="test_bin_yb",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_bin),
        )
        with pytest.raises(PermanentError, match="binary"):
            list(yb_adapter.extract_full(cfg_yb, download_dir=str(tmp_path)))

        cfg_psd = SourceConfig(
            source_id="test_bin_psd",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(bad_bin),
        )
        with pytest.raises(PermanentError, match="Unsupported actual file format"):
            list(psd_adapter.extract_full(cfg_psd, download_dir=str(tmp_path)))

    def test_10_schema_contract_enforcement(self, yb_adapter, psd_adapter, tmp_path):
        """10. Expected columns contract is enforced; missing columns trigger SchemaError."""
        dummy_csv = tmp_path / "dummy_yb.csv"
        dummy_csv.write_text("Col1,Col2\n1,2\n", encoding="utf-8")

        cfg_yb = SourceConfig(
            source_id="test_schema_yb",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(dummy_csv),
            expected_columns=["TABLE_NAME", "TABLE_NUMBER"],
        )
        with pytest.raises(SchemaError, match="missing required columns"):
            list(yb_adapter.extract_full(cfg_yb, download_dir=str(tmp_path)))

        dummy_html = tmp_path / "dummy_psd.xls"
        dummy_html.write_text(
            "<html><body><table><tr><th>ColA</th><th>ColB</th></tr><tr><td>1</td><td>2</td></tr></table></body></html>",
            encoding="utf-8",
        )
        cfg_psd = SourceConfig(
            source_id="test_schema_psd",
            provider="USDA",
            dataset="Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(dummy_html),
            expected_columns=["Commodity", "Country"],
        )
        with pytest.raises(SchemaError, match="missing required columns"):
            list(psd_adapter.extract_full(cfg_psd, download_dir=str(tmp_path)))

    def test_11_datachunk_contract(self, yb_config, psd_config, yb_adapter, psd_adapter, tmp_path):
        """11. DataChunk contract: int chunk_id, row boundaries, record_count, SHA-256."""
        chunk_yb = list(yb_adapter.extract_full(yb_config, download_dir=str(tmp_path)))[0]
        assert isinstance(chunk_yb.chunk_id, int)
        assert chunk_yb.chunk_id == 0
        assert chunk_yb.row_start == 0
        assert chunk_yb.row_end == 13130
        assert chunk_yb.record_count == 13131
        assert len(chunk_yb.checksum) == 64
        assert "_source_file" in chunk_yb.data.columns

        chunk_psd = list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))[0]
        assert isinstance(chunk_psd.chunk_id, int)
        assert chunk_psd.chunk_id == 0
        assert chunk_psd.row_start == 0
        assert chunk_psd.row_end == 14
        assert chunk_psd.record_count == 15
        assert len(chunk_psd.checksum) == 64
        assert "_source_file" in chunk_psd.data.columns

    def test_12_checksum_stability(self, yb_config, psd_config, yb_adapter, psd_adapter, tmp_path):
        """12. Checksum stability: repeated extraction yields identical SHA-256."""
        c1 = list(yb_adapter.extract_full(yb_config, download_dir=str(tmp_path)))[0]
        c2 = list(yb_adapter.extract_full(yb_config, download_dir=str(tmp_path)))[0]
        assert c1.checksum == c2.checksum

        p1 = list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))[0]
        p2 = list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))[0]
        assert p1.checksum == p2.checksum

    def test_13_network_isolation(self, yb_config, psd_config, yb_adapter, psd_adapter, tmp_path):
        """13. Pure local execution: zero network calls during readiness and extraction."""
        with patch("requests.get", side_effect=RuntimeError("Network forbidden!")), \
             patch("requests.head", side_effect=RuntimeError("Network forbidden!")), \
             patch("urllib.request.urlopen", side_effect=RuntimeError("Network forbidden!")):
            assert yb_adapter.check_readiness(yb_config).ready is True
            assert psd_adapter.check_readiness(psd_config).ready is True
            assert len(list(yb_adapter.extract_full(yb_config, download_dir=str(tmp_path)))) == 1
            assert len(list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))) == 1

    def test_14_real_usda_rice_yearbook_data_extraction(self, yb_config, yb_adapter, tmp_path):
        """14. Live extraction on Export-prices-Thailand-Vietnam-India-and-Pakistan.csv."""
        chunks = list(yb_adapter.extract_full(yb_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        df = chunks[0].data
        assert df.shape == (13131, 11)  # 10 raw columns + 1 _source_file
        for exp_col in yb_config.expected_columns:
            assert exp_col in df.columns
        assert df["_source_file"].iloc[0] == "Export-prices-Thailand-Vietnam-India-and-Pakistan.csv"
        assert set(df["LOCATION_DESCRIPTION"].unique()) == {"THAILAND", "VIETNAM", "INDIA", "PAKISTAN"}
        assert df["YEAR"].min() == 1985
        assert df["YEAR"].max() == 2025

    def test_15_real_usda_psd_data_extraction(self, psd_config, psd_adapter, tmp_path):
        """15. Live extraction on usda.xls (HTML format) yields exactly 15 rows, 71 columns."""
        chunks = list(psd_adapter.extract_full(psd_config, download_dir=str(tmp_path)))
        assert len(chunks) == 1
        df = chunks[0].data
        assert df.shape == (15, 71)  # 70 raw columns + 1 _source_file
        assert df["Country"].iloc[0] == "Vietnam"
        assert df["Attribute"].nunique() == 15
        assert df["_source_file"].iloc[0] == "usda.xls"
        assert "1960/1961" in df.columns
        assert "2025/2026" in df.columns
        assert "Unit Description" in df.columns

    def test_16_usda_xls_actual_format_verification(self, psd_config, psd_adapter):
        """16. Rigorous test that usda.xls is parsed as HTML and NOT assumed to be binary BIFF Excel."""
        target_file = psd_config.local_path
        detected = psd_adapter.detect_file_format(target_file)
        assert detected == "HTML", f"Expected HTML detection, got: {detected}"

        # Verify magic bytes proof
        with open(target_file, "rb") as f:
            magic_bytes = f.read(16)
        assert magic_bytes.lower().startswith(b"<html><head>")





