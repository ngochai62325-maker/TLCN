"""Phase 2 Validation and Integration Test Suite.

Executes real end-to-end vertical slice on the live Docker infrastructure:
- PostgreSQL metadata repository (5 tables in schema ingestion)
- MinIO object storage (Raw Landing in s3://bronze/raw/)
- Apache Iceberg REST Catalog + Trino query engine (queryable Bronze table)
- Checkpoint recovery on multi-chunk extraction
- Idempotency verification (unchanged snapshot skip)
"""

from __future__ import annotations

import os
import time
from typing import Any, List
import pandas as pd
import pytest

from ingestion.config.registry import SourceRegistry
from ingestion.core.enums import IngestionStatus, LoadStrategy
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.result import DataChunk
from ingestion.readiness.source_readiness import ReadinessChecker
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.minio_storage import MinioStorage


@pytest.fixture(scope="module", autouse=True)
def setup_infrastructure():
    """Ensure PostgreSQL metadata schema and MinIO buckets are initialized and clean."""
    repo = MetadataRepository()
    repo.initialize()

    storage = MinioStorage()
    for bucket in ["bronze", "silver", "gold", "warehouse"]:
        storage.ensure_bucket(bucket)

    writer = BronzeIcebergWriter()
    writer.ensure_schema()

    # Clean up any leftover test tables
    for test_src in ["faostat_trade_validation", "faostat_trade_recovery_test"]:
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{test_src}")
        except Exception:
            pass

    # Clean up test metadata records in PostgreSQL
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id LIKE 'faostat_trade_%'")
            cur.execute("DELETE FROM ingestion.ingestion_checkpoints WHERE source_id LIKE 'faostat_trade_%'")
            cur.execute("DELETE FROM ingestion.dead_letter_records WHERE source_id LIKE 'faostat_trade_%'")
            cur.execute("DELETE FROM ingestion.ingestion_runs WHERE source_id LIKE 'faostat_trade_%'")
            conn.commit()

    return {"repo": repo, "storage": storage, "writer": writer}


class TestPhase2LiveValidation:
    """Live verification of the FAOSTAT Trade Matrix vertical slice."""

    def test_01_fao_trade_live_readiness_check(self, setup_infrastructure):
        """Step 3: Verify local file readiness on primary raw artifact."""
        registry = SourceRegistry()
        registry.load()
        config = registry.get_source("faostat_trade")

        checker = ReadinessChecker()
        result = checker.check(config)

        assert result.ready is True, f"Readiness failed: {result.reason}"
        assert result.source_metadata is not None
        assert "size" in result.source_metadata
        assert int(result.source_metadata["size"]) > 100_000_000  # ~168 MB raw CSV
        assert "path" in result.source_metadata
        assert result.source_metadata["last_modified"] is not None

    def test_02_e2e_vertical_slice_success(self, setup_infrastructure):
        """Steps 1-13: Full vertical slice end-to-end execution on live stack."""
        engine = IngestionEngine.create_default()
        source_id = "faostat_trade_validation"

        # Clean up any existing table for clean test
        writer = setup_infrastructure["writer"]
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass

        # Execute Ingestion
        result = engine.run(source_id)

        # 1. Assert result status
        assert result.status == IngestionStatus.SUCCESS, f"Ingestion failed: {result.error_message}"
        assert result.records_extracted == 2000
        assert result.chunks_processed == 2
        assert result.artifact_uri is not None
        assert result.artifact_uri.startswith("s3://bronze/raw/faostat_trade_validation/")
        assert result.manifest_uri is not None
        assert result.checksum is not None

        # 2. Verify MinIO Raw Landing immutability
        storage = setup_infrastructure["storage"]
        # Parse s3 uri: s3://bronze/raw/...
        artifact_key = result.artifact_uri.replace("s3://bronze/", "")
        manifest_key = result.manifest_uri.replace("s3://bronze/", "")
        assert storage.file_exists("bronze", artifact_key) is True
        assert storage.file_exists("bronze", manifest_key) is True

        # 3. Verify PostgreSQL metadata tables
        repo = setup_infrastructure["repo"]
        run_record = repo.get_run(result.run_id)
        assert run_record is not None
        assert run_record["status"] == "SUCCESS"
        assert run_record["records_extracted"] == 2000
        assert run_record["chunks_processed"] == 2
        assert run_record["checksum"] == result.checksum

        snapshot_record = repo.find_snapshot_by_checksum(source_id, result.checksum)
        assert snapshot_record is not None
        assert snapshot_record["status"] == "SUCCESS"
        assert snapshot_record["run_id"] == result.run_id

        # 4. Verify Bronze Iceberg table and technical metadata columns
        table_count = writer.get_row_count(source_id)
        assert table_count == 2000, f"Expected 2000 rows in Iceberg Bronze table, got {table_count}"

        # Sample query via Trino
        cols, sample_rows = writer.execute_query(
            f"SELECT _ingestion_run_id, _ingestion_batch_id, _source_id, _source_checksum, \"value\" "
            f"FROM iceberg.bronze.{source_id} LIMIT 5"
        )
        assert len(sample_rows) == 5
        for row in sample_rows:
            assert row[0] == result.run_id
            assert row[1] == result.batch_id
            assert row[2] == source_id
            assert row[3] == result.checksum

    def test_03_idempotency_validation(self, setup_infrastructure):
        """Item 5: Run same snapshot a second time; assert SKIPPED and no duplicates."""
        engine = IngestionEngine.create_default()
        source_id = "faostat_trade_validation"
        writer = setup_infrastructure["writer"]

        count_before = writer.get_row_count(source_id)
        assert count_before == 2000

        # Run 2: Exact same snapshot
        result_run2 = engine.run(source_id)

        assert result_run2.status == IngestionStatus.SKIPPED
        assert result_run2.source_metadata.get("skipped_reason") == "unchanged_snapshot"

        # Verify no duplicate records were inserted into Bronze Iceberg
        count_after = writer.get_row_count(source_id)
        assert count_after == count_before, "Idempotency violated: duplicate rows inserted into Bronze Iceberg!"

    def test_04_checkpoint_recovery_audit_and_validation(self, setup_infrastructure):
        """Item 2 & 6: Simulate failure after Chunk 0, then restart and verify resume."""
        repo = setup_infrastructure["repo"]
        writer = setup_infrastructure["writer"]
        source_id = "faostat_trade_recovery_test"

        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass

        # Prepare 2 data chunks
        df_chunk0 = pd.DataFrame({"item": ["Rice_A", "Rice_B"], "val": [10.0, 20.0]})
        df_chunk1 = pd.DataFrame({"item": ["Rice_C", "Rice_D"], "val": [30.0, 40.0]})

        # Adapter that fails on chunk 1 on the first run
        class FaultyAdapter:
            def __init__(self, should_fail: bool):
                self.should_fail = should_fail

            def extract_full(self, config, download_dir, resume_chunk_id=None, resume_row_start=None):
                # Chunk 0
                if resume_chunk_id is None or resume_chunk_id < 0:
                    yield DataChunk(chunk_id=0, data=df_chunk0, row_start=0, row_end=1, record_count=2, checksum="c0")
                if self.should_fail:
                    raise RuntimeError("Simulated network/disk crash during chunk 1")
                # Chunk 1
                yield DataChunk(chunk_id=1, data=df_chunk1, row_start=2, row_end=3, record_count=2, checksum="c1")

            def get_artifact_checksum(self, path):
                return "dummy_recovery_checksum"

        from ingestion.core.config import SourceConfig
        from ingestion.core.checkpoint import PostgresCheckpointStore
        from ingestion.core.enums import SourceType, ArtifactFormat
        from ingestion.core.full_loader import FullLoader

        cfg = SourceConfig(
            source_id=source_id,
            provider="FAOSTAT",
            dataset="Recovery_Test",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
        )

        ckpt_store = PostgresCheckpointStore(repo)
        storage = setup_infrastructure["storage"]

        # Run 1: Failure during chunk 1
        loader_fail = FullLoader(
            adapter=FaultyAdapter(should_fail=True),
            checkpoint_store=ckpt_store,
            minio_storage=storage,
            metadata_repo=repo,
            bronze_writer=writer,
        )

        ts = int(time.time() * 1000)
        run_id_1 = f"{source_id}_run_{ts}_1"
        batch_id = f"{source_id}_batch_{ts}"
        repo.create_run(run_id_1, source_id, "FULL", "2026-09-24T00:00:00Z")

        result1 = loader_fail.execute(cfg, run_id_1, batch_id)
        assert result1.status == IngestionStatus.FAILED

        # Verify: Chunk 0 was written to Bronze, but not Chunk 1
        count_run1 = writer.get_row_count(source_id)
        assert count_run1 == 2  # Only Chunk 0

        # Verify Checkpoint table has Chunk 0 as SUCCESS
        last_ckpt = ckpt_store.get_last_successful(source_id, run_id_1)
        assert last_ckpt is not None
        assert last_ckpt.chunk_id == 0

        # Run 2: Restart with resume from run_id_1
        loader_resume = FullLoader(
            adapter=FaultyAdapter(should_fail=False),
            checkpoint_store=ckpt_store,
            minio_storage=storage,
            metadata_repo=repo,
            bronze_writer=writer,
        )

        run_id_2 = f"{source_id}_run_{ts}_2"
        repo.create_run(run_id_2, source_id, "FULL", "2026-09-24T00:01:00Z")

        result2 = loader_resume.execute(cfg, run_id_2, batch_id, resume_from_run_id=run_id_1)
        assert result2.status == IngestionStatus.SUCCESS
        assert result2.records_extracted == 4  # 2 prior + 2 new

        # Verify: Bronze Iceberg table has exactly 4 rows (no duplicate Chunk 0)
        final_count = writer.get_row_count(source_id)
        assert final_count == 4, f"Expected exactly 4 rows after recovery, got {final_count} (duplicate detected!)"

        # Clean up test table
        writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
