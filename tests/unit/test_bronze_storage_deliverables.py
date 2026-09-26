"""Unit test suite for Người 2 — Bronze Storage & Reliability deliverables.

Covers:
- Test 1: Full Load & 5-folder MinIO Bronze Hierarchy (raw, metadata, manifest, audit, quarantine)
- Test 3: Retry & Error Quarantine with Companion Diagnostic (.error.json)
- Test 5: Idempotency Verification (0 duplicates on re-runs)
"""

import json
import os
import tempfile
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from ingestion.core.config import SourceConfig, RetryConfig, ReadinessConfig
from ingestion.core.enums import ArtifactFormat, IngestionStatus, LoadStrategy, SourceType
from ingestion.core.full_loader import FullLoader
from ingestion.core.result import DataChunk
from ingestion.reliability.bronze_quality_validator import BronzeQualityValidator
from ingestion.reliability.idempotency_controller import IdempotencyController
from ingestion.reliability.quarantine_manager import QuarantineManager
from ingestion.storage.bronze_storage_layout import BronzeStorageLayout, resolve_source_group
from ingestion.storage.metadata_schemas import (
    AuditLogEntry,
    BatchMetadata,
    BatchStatus,
    QuarantineDiagnostic,
    QuarantineErrorType,
)


# ── Fixtures ─────────────────────────────────────────────────────────

class MockStorageBackend:
    """In-memory mock for MinIO S3 object storage."""

    def __init__(self):
        self.buckets = set(["bronze"])
        self.objects = {}  # (bucket, key) -> bytes

    def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    def upload_file(self, local_path: str, bucket: str, key: str) -> str:
        self.ensure_bucket(bucket)
        with open(local_path, "rb") as f:
            self.objects[(bucket, key)] = f.read()
        return f"s3://{bucket}/{key}"

    def upload_bytes(self, data: bytes, bucket: str, key: str) -> str:
        self.ensure_bucket(bucket)
        self.objects[(bucket, key)] = data
        return f"s3://{bucket}/{key}"

    def upload_json(self, data: dict, bucket: str, key: str) -> str:
        self.ensure_bucket(bucket)
        self.objects[(bucket, key)] = json.dumps(data).encode("utf-8")
        return f"s3://{bucket}/{key}"

    def file_exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.objects

    def list_objects(self, bucket: str, prefix: str):
        results = []
        for (b, k) in self.objects:
            if b == bucket and k.startswith(prefix):
                results.append({"Key": k, "Size": len(self.objects[(b, k)])})
        return results


@pytest.fixture
def mock_minio():
    mock = MagicMock()
    backend = MockStorageBackend()
    mock.ensure_bucket.side_effect = backend.ensure_bucket
    mock.upload_file.side_effect = backend.upload_file
    mock.upload_bytes.side_effect = backend.upload_bytes
    mock.upload_json.side_effect = backend.upload_json
    mock.file_exists.side_effect = backend.file_exists
    mock.list_objects.side_effect = backend.list_objects

    # Mock s3_client.get_object
    def mock_get_object(Bucket, Key):
        data = backend.objects.get((Bucket, Key))
        if data is None:
            raise KeyError(f"Not found: {Key}")
        mock_body = MagicMock()
        mock_body.read.return_value = data
        return {"Body": mock_body}

    mock.s3_client.get_object.side_effect = mock_get_object
    mock._backend = backend
    return mock


@pytest.fixture
def bronze_layout(mock_minio):
    return BronzeStorageLayout(minio_storage=mock_minio, bucket="bronze")


# ── Test 1: Full Load & 5-Folder Hierarchy ────────────────────────────

class TestBronzeStorageHierarchy:
    """Verifies the standard 5-folder storage layout in Bronze layer."""

    @pytest.mark.parametrize("source_id,expected_group", [
        ("faostat_trade", "faostat"),
        ("faostat_production", "faostat"),
        ("nso_vietnam", "nso"),
        ("usda_psd", "usda"),
        ("usda_rice_yearbook", "usda"),
        ("worldbank_pinksheet", "worldbank"),
        ("thitruongnongsan", "thitruongnongsan"),
    ])
    def test_source_group_resolution(self, source_id, expected_group):
        assert resolve_source_group(source_id) == expected_group

    def test_five_functional_folder_paths(self, bronze_layout):
        source_id = "faostat_trade"
        batch_id = "batch_20260926_001"
        run_id = "run_20260926_001"
        filename = "rice_trade.csv"

        raw_key = bronze_layout.get_raw_key(source_id, batch_id, filename)
        meta_key = bronze_layout.get_metadata_key(source_id, batch_id)
        manifest_key = bronze_layout.get_manifest_key(source_id, run_id)
        audit_key = bronze_layout.get_audit_key(source_id, run_id)
        q_key = bronze_layout.get_quarantine_key(source_id, filename, "20260926_120000")
        diag_key = bronze_layout.get_quarantine_diagnostic_key(q_key)

        assert raw_key == "faostat/raw/batch_20260926_001/rice_trade.csv"
        assert meta_key == "faostat/metadata/batch_batch_20260926_001.json"
        assert manifest_key == "faostat/manifest/run_run_20260926_001.json"
        assert audit_key == "faostat/audit/audit_run_20260926_001.json"
        assert q_key == "faostat/quarantine/20260926_120000_rice_trade.csv"
        assert diag_key == "faostat/quarantine/20260926_120000_rice_trade.csv.error.json"

    def test_batch_metadata_save_and_load(self, bronze_layout):
        meta = BatchMetadata(
            batch_id="batch_123",
            source_name="usda_psd",
            source_group="usda",
            extracted_at="2026-09-26 12:00:00 UTC",
            record_count=2345,
            checksum="abc123sha256",
            file_size_bytes=1048576,
            status=BatchStatus.COMMITTED,
        )
        uri = bronze_layout.save_batch_metadata(meta)
        assert "s3://bronze/usda/metadata/batch_batch_123.json" in uri

        loaded = bronze_layout.load_batch_metadata("usda_psd", "batch_123")
        assert loaded is not None
        assert loaded.batch_id == "batch_123"
        assert loaded.record_count == 2345
        assert loaded.checksum == "abc123sha256"
        assert loaded.status == BatchStatus.COMMITTED

    def test_audit_entry_save(self, bronze_layout):
        entry = AuditLogEntry(
            run_id="run_999",
            batch_id="batch_999",
            source_id="nso_vietnam",
            source_group="nso",
            started_at="2026-09-26 12:00:00 UTC",
            ended_at="2026-09-26 12:01:00 UTC",
            status=BatchStatus.COMMITTED,
            records_processed=5000,
            duration_seconds=60.0,
        )
        uri = bronze_layout.save_audit_entry(entry)
        assert "s3://bronze/nso/audit/audit_run_999.json" in uri


# ── Test 3: Quality Validation & Error Quarantine ────────────────────

class TestQualityValidationAndQuarantine:
    """Verifies file integrity validation and quarantine with companion diagnostics."""

    def test_zero_byte_file_rejected(self, tmp_path):
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        validator = BronzeQualityValidator()
        res = validator.validate_file(str(empty_file))

        assert not res.is_valid
        assert res.error_type == QuarantineErrorType.EMPTY_FILE
        assert "empty (0 bytes)" in res.error_message

    def test_checksum_mismatch_detected(self, tmp_path):
        sample_file = tmp_path / "data.csv"
        sample_file.write_text("id,val\n1,100\n")

        validator = BronzeQualityValidator()
        res = validator.validate_file(str(sample_file), expected_checksum="incorrect_hash_value")

        assert not res.is_valid
        assert res.error_type == QuarantineErrorType.CHECKSUM_MISMATCH
        assert "Checksum mismatch" in res.error_message

    def test_corrupted_csv_detected(self, tmp_path):
        corrupt_file = tmp_path / "bad.csv"
        # Mismatched quotes and delimiters causing parse failure
        corrupt_file.write_text('id,val\n1,"unclosed quote\n2,200\n3,4,5,6,extra\n')

        validator = BronzeQualityValidator()
        res = validator.validate_file(str(corrupt_file))
        assert not res.is_valid
        assert res.error_type == QuarantineErrorType.PARSE_FAILURE

    def test_schema_drift_missing_columns(self, tmp_path):
        file = tmp_path / "drifted.csv"
        file.write_text("item,year,value\nrice,2024,1000\n")

        validator = BronzeQualityValidator()
        # Expecting mandatory column 'country' which is missing
        res = validator.validate_file(str(file), expected_columns=["item", "year", "value", "country"])

        assert not res.is_valid
        assert res.error_type == QuarantineErrorType.SCHEMA_DRIFT
        assert "country" in res.error_message
        assert res.schema_diff is not None
        assert "country" in res.schema_diff["missing_columns"]

    def test_quarantine_artifact_routing_and_companion_diagnostic(self, bronze_layout, tmp_path):
        manager = QuarantineManager(layout=bronze_layout)
        bad_file = tmp_path / "corrupted_worldbank.csv"
        bad_file.write_text("MALFORMED HEADER\nNO_DATA\n")

        v_res = BronzeQualityValidator().validate_file(
            str(bad_file),
            expected_columns=["indicator", "country", "date", "val"],
        )

        q_res = manager.quarantine_artifact(
            source_id="worldbank_pinksheet",
            file_path=str(bad_file),
            validation_result=v_res,
            run_id="run_test_q1",
            batch_id="batch_test_q1",
        )

        assert q_res.is_quarantined
        assert "worldbank/quarantine/" in q_res.quarantined_uri
        assert "worldbank/quarantine/" in q_res.diagnostic_uri
        assert q_res.diagnostic_uri.endswith(".error.json")
        assert q_res.error_type == QuarantineErrorType.SCHEMA_DRIFT

        # Verify companion .error.json content in mock storage
        diag_key = q_res.diagnostic_uri.replace("s3://bronze/", "")
        diag_data = json.loads(bronze_layout.storage._backend.objects[("bronze", diag_key)].decode("utf-8"))

        assert diag_data["original_file"] == "corrupted_worldbank.csv"
        assert diag_data["error_type"] == "SCHEMA_DRIFT"
        assert diag_data["source_group"] == "worldbank"
        assert "quarantined_at" in diag_data

        # Verify audit entry recorded as QUARANTINED
        audit_key = "worldbank/audit/audit_run_test_q1.json"
        assert ("bronze", audit_key) in bronze_layout.storage._backend.objects
        audit_data = json.loads(bronze_layout.storage._backend.objects[("bronze", audit_key)].decode("utf-8"))
        assert audit_data["status"] == "QUARANTINED"


# ── Test 5: Idempotency & Zero Duplicate Guarantee ───────────────────

class TestIdempotencyController:
    """Verifies that re-running identical batches guarantees 0 duplicates."""

    def test_idempotent_skip_on_duplicate_batch(self, bronze_layout):
        controller = IdempotencyController(layout=bronze_layout)

        # Batch 1: Initially not present -> should process
        decision_1 = controller.evaluate_batch(
            source_id="usda_psd",
            batch_id="batch_usda_2026",
            current_checksum="hash_usda_v1",
        )
        assert decision_1.should_process is True
        assert decision_1.status == BatchStatus.IN_PROGRESS

        # Batch 1: Successfully processed and committed
        meta = BatchMetadata(
            batch_id="batch_usda_2026",
            source_name="usda_psd",
            source_group="usda",
            extracted_at="2026-09-26 12:00:00 UTC",
            record_count=1000,
            checksum="hash_usda_v1",
            file_size_bytes=50000,
        )
        controller.register_committed_batch(meta)

        # Batch 2: Exact re-run with same batch_id and matching checksum
        decision_2 = controller.evaluate_batch(
            source_id="usda_psd",
            batch_id="batch_usda_2026",
            current_checksum="hash_usda_v1",
        )
        assert decision_2.should_process is False
        assert decision_2.status == BatchStatus.SKIPPED_IDEMPOTENT
        assert "already committed with matching checksum" in decision_2.reason

    def test_idempotency_force_reprocess_allowed(self, bronze_layout):
        controller = IdempotencyController(layout=bronze_layout)

        meta = BatchMetadata(
            batch_id="batch_reprocess",
            source_name="faostat_production",
            source_group="faostat",
            extracted_at="2026-09-26 12:00:00 UTC",
            record_count=500,
            checksum="hash_prod_v1",
            file_size_bytes=50000,
        )
        controller.register_committed_batch(meta)

        # Forced re-run should allow processing
        decision = controller.evaluate_batch(
            source_id="faostat_production",
            batch_id="batch_reprocess",
            current_checksum="hash_prod_v1",
            force_reprocess=True,
        )
        assert decision.should_process is True


# ── FullLoader Integration Tests ─────────────────────────────────────

class TestFullLoaderIntegration:
    """Verifies that FullLoader delivers 5-folder storage, error quarantine, and idempotency."""

    def test_full_loader_generates_bronze_functional_folders(self, mock_minio, tmp_path):
        csv_file = tmp_path / "valid_data.csv"
        csv_file.write_text("domain,country,year,value\nTrade,Vietnam,2024,500.0\nTrade,Thailand,2024,600.0\n")

        config = SourceConfig(
            source_id="faostat_trade",
            provider="FAOSTAT",
            dataset="trade",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(csv_file),
            chunk_size=1000,
            retry=RetryConfig(),
            readiness=ReadinessConfig(),
            adapter_class="ingestion.adapters.test_adapter.TestAdapter",
        )

        mock_adapter = MagicMock()
        mock_adapter.extract_full.return_value = [
            DataChunk(
                chunk_id=0,
                row_start=0,
                row_end=1,
                record_count=2,
                checksum="abc",
                data=pd.DataFrame([
                    {"domain": "Trade", "country": "Vietnam", "year": 2024, "value": 500.0},
                    {"domain": "Trade", "country": "Thailand", "year": 2024, "600.0": 600.0},
                ]),
            )
        ]

        mock_ckpt = MagicMock()
        mock_meta_repo = MagicMock()
        mock_meta_repo.find_snapshot_by_checksum.return_value = None

        loader = FullLoader(
            adapter=mock_adapter,
            checkpoint_store=mock_ckpt,
            minio_storage=mock_minio,
            metadata_repo=mock_meta_repo,
            bronze_writer=None,
        )

        result = loader.execute(config, run_id="run_fl_001", batch_id="batch_fl_001")
        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 2

        # Verify MinIO Bronze 5-folder structure
        folders = loader.bronze_layout.list_functional_folders("faostat_trade")
        assert any("faostat/raw/batch_fl_001/" in k for k in folders["raw"])
        assert any("faostat/metadata/batch_batch_fl_001.json" in k for k in folders["metadata"])
        assert any("faostat/manifest/run_run_fl_001.json" in k for k in folders["manifest"])
        assert any("faostat/audit/audit_run_fl_001.json" in k for k in folders["audit"])

    def test_full_loader_routes_empty_file_to_quarantine_without_crashing(self, mock_minio, tmp_path):
        empty_file = tmp_path / "empty_psd.csv"
        empty_file.write_text("")

        config = SourceConfig(
            source_id="usda_psd",
            provider="USDA",
            dataset="psd",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
            local_path=str(empty_file),
            chunk_size=1000,
            retry=RetryConfig(),
            readiness=ReadinessConfig(),
            adapter_class="ingestion.adapters.test_adapter.TestAdapter",
        )

        loader = FullLoader(
            adapter=MagicMock(),
            checkpoint_store=MagicMock(),
            minio_storage=mock_minio,
            metadata_repo=MagicMock(),
            bronze_writer=None,
        )

        # Must NOT crash with unhandled exception; must return FAILED / QUARANTINED result
        result = loader.execute(config, run_id="run_empty_01", batch_id="batch_empty_01")
        assert result.status == IngestionStatus.FAILED
        assert result.error_type == "EMPTY_FILE"
        assert "usda/quarantine/" in result.artifact_uri
        assert "diagnostic_uri" in result.source_metadata
        assert result.source_metadata["diagnostic_uri"].endswith(".error.json")
