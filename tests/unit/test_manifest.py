"""Tests for ingestion.manifest.manifest.IngestionManifest."""

import json
import pytest
from ingestion.manifest.manifest import IngestionManifest
from ingestion.core.enums import IngestionStatus


class TestIngestionManifest:
    def test_creation(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        assert m.source_id == "src1"
        assert m.run_id == "run1"
        assert m.batch_id == "batch1"
        assert m.load_type == "FULL"
        assert m.pipeline_version == "1.0.0"
        assert m.created_at is not None

    def test_to_dict(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        m.set_status(IngestionStatus.SUCCESS)
        d = m.to_dict()
        assert d["source_id"] == "src1"
        assert d["status"] == "SUCCESS"
        assert "created_at" in d
        assert "artifact_info" in d
        assert "source_metadata" in d
        assert "metrics" in d
        assert "timing" in d

    def test_to_json(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        j = m.to_json()
        d = json.loads(j)
        assert d["source_id"] == "src1"

    def test_save(self, tmp_path):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        path = tmp_path / "manifest.json"
        m.save(str(path))
        assert path.exists()
        with open(path, "r") as f:
            d = json.load(f)
        assert d["run_id"] == "run1"

    def test_set_artifact_info(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        m.set_artifact_info("s3://bucket/key", "CSV", 1024, "abc123")
        d = m.to_dict()
        assert d["artifact_info"]["uri"] == "s3://bucket/key"
        assert d["artifact_info"]["format"] == "CSV"
        assert d["artifact_info"]["size_bytes"] == 1024
        assert d["artifact_info"]["checksum"] == "abc123"

    def test_set_metrics(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        m.set_metrics(100, 5, 2)
        assert m.records_extracted == 100
        assert m.chunks_processed == 5
        assert m.records_quarantined == 2

    def test_set_error(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        m.set_error("Download failed", "TRANSIENT")
        assert m.error_message == "Download failed"
        assert m.error_type == "TRANSIENT"

    def test_set_source_metadata(self):
        m = IngestionManifest("src1", "run1", "batch1", "FULL")
        m.set_source_metadata("FAOSTAT", "Trade_Matrix", "https://example.com")
        d = m.to_dict()
        assert d["source_metadata"]["provider"] == "FAOSTAT"
        assert d["source_metadata"]["dataset"] == "Trade_Matrix"

    def test_set_watermark(self):
        m = IngestionManifest("src1", "run1", "batch1", "INCREMENTAL")
        m.set_watermark("2024-01-01", "2024-06-01")
        d = m.to_dict()
        assert d["watermark"]["before"] == "2024-01-01"
        assert d["watermark"]["after"] == "2024-06-01"
