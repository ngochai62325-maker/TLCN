"""Tests for ingestion.config.registry.SourceRegistry."""

import os
import pytest
from ingestion.config.registry import SourceRegistry, RegistryValidationError
from ingestion.core.enums import SourceType, LoadStrategy, ArtifactFormat
from ingestion.utils.error_classifier import PermanentError


class TestSourceRegistry:
    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load()
        return r

    def test_load_registry(self, registry):
        sources = registry.list_sources()
        assert len(sources) >= 8  # at least 8 sources in YAML

    def test_get_source(self, registry):
        config = registry.get_source("faostat_trade")
        assert config is not None
        assert config.source_id == "faostat_trade"
        assert config.provider == "FAOSTAT"
        assert config.source_type == SourceType.LOCAL_FILE

    def test_get_source_not_found(self, registry):
        with pytest.raises(PermanentError, match="not found"):
            registry.get_source("does_not_exist")

    def test_list_sources(self, registry):
        sources = registry.list_sources()
        assert "faostat_trade" in sources
        assert "faostat_production" in sources
        assert "usda_psd" in sources

    def test_retry_config_parsed(self, registry):
        config = registry.get_source("faostat_trade")
        assert config.retry is not None
        assert isinstance(config.retry.max_attempts, int)
        assert config.retry.max_attempts >= 1

    def test_readiness_config_parsed(self, registry):
        config = registry.get_source("faostat_trade")
        assert config.readiness is not None
        assert config.readiness.min_file_size_bytes is not None

    def test_source_type_enum(self, registry):
        config = registry.get_source("faostat_trade")
        assert config.source_type == SourceType.LOCAL_FILE
        local_config = registry.get_source("nso_vietnam")
        assert local_config.source_type == SourceType.LOCAL_FILE

    def test_load_strategy_enum(self, registry):
        config = registry.get_source("faostat_trade")
        assert config.load_strategy == LoadStrategy.FULL

    def test_env_var_substitution(self, monkeypatch):
        """Verify ${ENV_VAR} substitution works in endpoint."""
        monkeypatch.setenv("TEST_HOST", "my-server.com")
        registry = SourceRegistry()
        # Test the internal method
        result = registry._substitute_env_vars("https://${TEST_HOST}/data")
        assert result == "https://my-server.com/data"

    def test_get_all_sources(self, registry):
        all_sources = registry.get_all_sources()
        assert isinstance(all_sources, dict)
        assert len(all_sources) >= 8


class TestSourceRegistryHardening:
    """Rigorous verification of the 8 production sources and registry contracts."""

    @pytest.fixture
    def registry(self):
        r = SourceRegistry()
        r.load(validate=True)
        return r

    def test_01_all_8_canonical_sources_exist(self, registry):
        """Test 1: All 8 required sources exist and are retrievable."""
        canonical_8 = [
            "faostat_production",
            "faostat_monthly_price",
            "faostat_trade",
            "faostat_supply_utilization",
            "nso_vietnam",
            "usda_rice_yearbook",
            "worldbank_pinksheet",
            "thitruongnongsan",
        ]
        for src in canonical_8:
            cfg = registry.get_source(src)
            assert cfg is not None
            assert cfg.source_id == src
            assert cfg.enabled is True

        # Backward compatibility aliases
        assert registry.get_source("faostat_price").source_id == "faostat_monthly_price"
        assert registry.get_source("faostat_sua").source_id == "faostat_supply_utilization"
        assert registry.get_source("usda_export_prices").source_id == "usda_rice_yearbook"

    def test_02_all_source_ids_unique(self, registry):
        """Test 2: All registered source_ids are strictly unique."""
        sources = registry.list_sources()
        assert len(sources) == len(set(sources))

    def test_03_all_enabled_sources_have_valid_configuration(self, registry):
        """Test 3: All enabled sources have valid provider, dataset, format, and locations."""
        for source_id, cfg in registry.get_all_sources(enabled_only=True).items():
            assert cfg.provider, f"{source_id} missing provider"
            assert cfg.dataset, f"{source_id} missing dataset"
            assert cfg.format in ArtifactFormat
            assert cfg.source_type in SourceType
            assert cfg.load_strategy in LoadStrategy
            # Check location
            has_location = bool(cfg.endpoint or cfg.local_path or cfg.local_fallback)
            assert has_location is True, f"{source_id} has no valid location or fallback"

    def test_04_all_snapshot_only_sources_use_full(self, registry):
        """Test 4: All 8 snapshot/bulk sources strictly use FULL load strategy."""
        canonical_8 = [
            "faostat_production",
            "faostat_monthly_price",
            "faostat_trade",
            "faostat_supply_utilization",
            "nso_vietnam",
            "usda_rice_yearbook",
            "worldbank_pinksheet",
            "thitruongnongsan",
        ]
        for src in canonical_8:
            cfg = registry.get_source(src)
            assert cfg.load_strategy == LoadStrategy.FULL, f"{src} must use FULL load_strategy"

    def test_05_no_source_is_incorrectly_configured_as_incremental(self, registry):
        """Test 5: No snapshot sources are fake-configured as INCREMENTAL."""
        for source_id, cfg in registry.get_all_sources().items():
            assert cfg.load_strategy != LoadStrategy.INCREMENTAL, (
                f"Source '{source_id}' is configured as INCREMENTAL. "
                "Fake incremental is strictly disallowed for snapshot sources."
            )

    def test_06_invalid_configuration_is_rejected(self, tmp_path):
        """Test 6: Registry validator rejects invalid configurations."""
        # Missing required field
        invalid_yaml_1 = tmp_path / "invalid_1.yaml"
        invalid_yaml_1.write_text("sources:\n  bad_src:\n    dataset: 'Test'\n", encoding="utf-8")
        r1 = SourceRegistry(str(invalid_yaml_1))
        with pytest.raises(RegistryValidationError, match="provider"):
            r1.load()

        # Unknown load strategy
        invalid_yaml_2 = tmp_path / "invalid_2.yaml"
        invalid_yaml_2.write_text(
            "sources:\n  bad_src:\n    provider: 'P'\n    dataset: 'D'\n"
            "    source_type: 'LOCAL_FILE'\n    load_strategy: 'MAGIC_STREAM'\n    format: 'CSV'\n",
            encoding="utf-8"
        )
        r2 = SourceRegistry(str(invalid_yaml_2))
        with pytest.raises(RegistryValidationError, match="load_strategy"):
            r2.load()

        # Incremental without watermark_column (fake incremental)
        invalid_yaml_3 = tmp_path / "invalid_3.yaml"
        invalid_yaml_3.write_text(
            "sources:\n  bad_src:\n    provider: 'P'\n    dataset: 'D'\n"
            "    source_type: 'LOCAL_FILE'\n    load_strategy: 'INCREMENTAL'\n    format: 'CSV'\n",
            encoding="utf-8"
        )
        r3 = SourceRegistry(str(invalid_yaml_3))
        with pytest.raises(RegistryValidationError, match="watermark_column"):
            r3.load()

        # Duplicate source_id
        invalid_yaml_4 = tmp_path / "invalid_4.yaml"
        invalid_yaml_4.write_text(
            "sources:\n"
            "  dup_src:\n    provider: 'P1'\n    dataset: 'D1'\n    source_type: 'LOCAL_FILE'\n    load_strategy: 'FULL'\n    format: 'CSV'\n"
            "  dup_src:\n    provider: 'P2'\n    dataset: 'D2'\n    source_type: 'LOCAL_FILE'\n    load_strategy: 'FULL'\n    format: 'CSV'\n",
            encoding="utf-8"
        )
        r4 = SourceRegistry(str(invalid_yaml_4))
        with pytest.raises(RegistryValidationError, match="Duplicate source_id"):
            r4.load()

    def test_07_registry_loaded_by_ingestion_engine(self):
        """Test 7: IngestionEngine can initialize and query all sources from registry."""
        from ingestion.core.ingestion_engine import IngestionEngine
        engine = IngestionEngine.create_default()
        assert engine.registry is not None
        assert engine.registry.has_source("faostat_trade") is True
        assert engine.registry.has_source("faostat_monthly_price") is True
        assert engine.registry.has_source("faostat_price") is True  # alias
