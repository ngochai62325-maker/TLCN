"""Tests for ingestion.readiness.source_readiness.ReadinessChecker."""

import os
import pytest
import responses
from unittest.mock import MagicMock, patch

from ingestion.core.config import SourceConfig, ReadinessConfig, RetryConfig
from ingestion.core.enums import SourceType, LoadStrategy, ArtifactFormat
from ingestion.readiness.source_readiness import ReadinessChecker


@pytest.fixture
def checker():
    return ReadinessChecker()


def _make_config(source_type, endpoint=None, local_path=None, readiness=None):
    return SourceConfig(
        source_id="test_src",
        provider="Test",
        dataset="TestData",
        source_type=source_type,
        load_strategy=LoadStrategy.FULL,
        format=ArtifactFormat.CSV,
        endpoint=endpoint,
        local_path=local_path,
        readiness=readiness or ReadinessConfig(),
    )


class TestLocalFileReadiness:
    def test_file_ready(self, checker, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("col1,col2\n1,2\n")
        config = _make_config(SourceType.LOCAL_FILE, local_path=str(f))
        result = checker.check(config)
        assert result.ready is True

    def test_file_not_exists(self, checker, tmp_path):
        config = _make_config(SourceType.LOCAL_FILE, local_path=str(tmp_path / "missing.csv"))
        result = checker.check(config)
        assert result.ready is False
        assert "not found" in result.reason.lower() or "not readable" in result.reason.lower() or "File not found" in result.reason

    def test_file_empty_with_min_size(self, checker, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        config = _make_config(
            SourceType.LOCAL_FILE,
            local_path=str(f),
            readiness=ReadinessConfig(min_file_size_bytes=100),
        )
        result = checker.check(config)
        assert result.ready is False

    def test_missing_local_path(self, checker):
        config = _make_config(SourceType.LOCAL_FILE, local_path=None)
        result = checker.check(config)
        assert result.ready is False
        assert "local_path" in result.reason.lower() or "Missing" in result.reason

    def test_local_directory_ready(self, checker, tmp_path):
        sub_file = tmp_path / "part_1.csv"
        sub_file.write_text("a,b\n1,2\n")
        config = _make_config(SourceType.LOCAL_FILE, local_path=str(tmp_path))
        result = checker.check(config)
        assert result.ready is True
        assert result.source_metadata["file_count"] >= 1

    def test_local_file_no_http_call_when_local_file(self, checker, tmp_path):
        """Verify that when SourceType is LOCAL_FILE, zero HTTP calls are made even if endpoint is configured."""
        f = tmp_path / "valid.csv"
        f.write_text("x,y\n3,4\n")
        # Provide an unroutable endpoint to prove it is never contacted
        config = _make_config(SourceType.LOCAL_FILE, endpoint="http://0.0.0.0:1/unreachable", local_path=str(f))
        result = checker.check(config)
        assert result.ready is True
        assert "local" in result.reason.lower()


class TestHttpReadiness:
    @responses.activate
    def test_http_200_ready(self, checker):
        url = "http://example.com/data.zip"
        responses.add(responses.HEAD, url, status=200, headers={"Content-Length": "1024"})
        config = _make_config(SourceType.HTTP_BULK_ZIP, endpoint=url)
        result = checker.check(config)
        assert result.ready is True

    @responses.activate
    def test_http_500_not_ready(self, checker):
        url = "http://example.com/data.zip"
        responses.add(responses.HEAD, url, status=500)
        config = _make_config(SourceType.HTTP_BULK_ZIP, endpoint=url)
        result = checker.check(config)
        assert result.ready is False

    def test_missing_endpoint(self, checker):
        config = _make_config(SourceType.HTTP_BULK_ZIP, endpoint=None)
        result = checker.check(config)
        assert result.ready is False
        assert "endpoint" in result.reason.lower() or "Missing" in result.reason
