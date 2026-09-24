"""Tests for ingestion.config.registry.SourceRegistry."""

import os
import pytest
from ingestion.config.registry import SourceRegistry
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
        assert config.source_type == SourceType.HTTP_BULK_ZIP

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
        assert config.readiness.require_content_length is True

    def test_source_type_enum(self, registry):
        config = registry.get_source("faostat_trade")
        assert config.source_type == SourceType.HTTP_BULK_ZIP
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
