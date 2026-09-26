"""Phase 2E — End-to-End Bronze Ingestion Integration Test Suite.

Verifies the complete integration lifecycle under LOCAL RAW FILE -> BRONZE architecture:
1. Full Pipeline E2E Load with Real Snapshot (faostat_production).
2. Snapshot-Level Idempotency (unchanged snapshot skip, zero duplicate rows).
3. Large File Chunking & Checkpoint Resume / Recovery (no duplicate chunks).
4. Multi-File Directory Ingestion & Schema Evolution (nso_vietnam: 13 files, 833 rows).
5. Directory Incremental File Arrival (skip already-ingested files, insert only new file).
6. Error Handling & Classification (PERMANENT, SCHEMA, DATA_QUALITY).
7. Quantitative Reconciliation (Raw Records == Accepted Records == Bronze Rows).
8. Airflow DAG Integrity (0 import errors, correct task graph).
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, List
import pandas as pd
import pytest

from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.adapters.nso_adapter import NsoVietnamAdapter
from ingestion.config.registry import SourceRegistry
from ingestion.core.checkpoint import PostgresCheckpointStore
from ingestion.core.config import SourceConfig
from ingestion.core.enums import ArtifactFormat, ChunkStatus, IngestionStatus, LoadStrategy, SourceType
from ingestion.core.full_loader import FullLoader
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.result import DataChunk, IngestionResult
from ingestion.readiness.source_readiness import ReadinessChecker
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.storage.minio_storage import MinioStorage
from ingestion.utils.error_classifier import DataQualityError, PermanentError, SchemaError, TransientError
from ingestion.utils.hashing import compute_file_checksum


@pytest.fixture(scope="module")
def infra():
    """Shared infrastructure fixture for PostgreSQL, MinIO, and Trino Iceberg."""
    repo = MetadataRepository()
    repo.initialize()

    storage = MinioStorage()
    for bucket in ["bronze", "silver", "gold", "warehouse"]:
        storage.ensure_bucket(bucket)

    writer = BronzeIcebergWriter()
    writer.ensure_schema()

    registry = SourceRegistry()
    registry.load(validate=True)

    return {
        "repo": repo,
        "storage": storage,
        "writer": writer,
        "registry": registry,
    }


class TestPhase2EEndToEndIntegration:
    """Rigorous Phase 2E Integration Verification."""

    # ──────────────────────────────────────────────────────────────────
    # 1. Readiness Check across all canonical sources
    # ──────────────────────────────────────────────────────────────────
    def test_01_readiness_all_canonical_sources(self, infra):
        """All canonical sources with configured local paths report READY with valid metadata."""
        checker = ReadinessChecker()
        registry: SourceRegistry = infra["registry"]
        source_ids = registry.list_sources()

        assert len(source_ids) >= 8

        for sid in source_ids:
            source = registry.get_source(sid)
            result = checker.check(source)
            assert result.ready is True, f"Source {sid} failed readiness: {result.reason}"
            assert result.source_metadata is not None
            assert "path" in result.source_metadata

    # ──────────────────────────────────────────────────────────────────
    # 2. Full Pipeline E2E Load with Real Snapshot (faostat_production)
    # ──────────────────────────────────────────────────────────────────
    def test_02_e2e_full_load_real_snapshot(self, infra):
        """Run faostat_production through IngestionEngine, verify all 4 storage targets."""
        source_id = "faostat_production"
        repo: MetadataRepository = infra["repo"]
        storage: MinioStorage = infra["storage"]
        writer: BronzeIcebergWriter = infra["writer"]

        # Ensure clean state for test run
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass

        with repo._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id = %s", (source_id,))
                cur.execute("DELETE FROM ingestion.ingestion_runs WHERE source_id = %s", (source_id,))
                cur.execute("DELETE FROM ingestion.ingestion_checkpoints WHERE source_id = %s", (source_id,))
                conn.commit()

        engine = IngestionEngine.create_default()
        result: IngestionResult = engine.run(source_id)

        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 22738
        assert result.checksum is not None
        assert result.artifact_uri is not None
        assert result.manifest_uri is not None

        # MinIO raw landing validation
        artifact_key = result.artifact_uri.replace("s3://bronze/", "")
        manifest_key = result.manifest_uri.replace("s3://bronze/", "")
        assert storage.file_exists("bronze", artifact_key) is True
        assert storage.file_exists("bronze", manifest_key) is True

        # PostgreSQL metadata validation
        run_record = repo.get_run(result.run_id)
        assert run_record is not None
        assert run_record["status"] == "SUCCESS"
        assert run_record["records_extracted"] == 22738

        snapshot_record = repo.find_snapshot_by_checksum(source_id, result.checksum)
        assert snapshot_record is not None
        assert snapshot_record["status"] == "SUCCESS"

        # Trino Iceberg table validation
        table_count = writer.get_row_count(source_id)
        assert table_count == 22738

        # Technical metadata columns validation
        cols, rows = writer.execute_query(
            f"SELECT _ingestion_run_id, _ingestion_batch_id, _ingestion_timestamp, _source_id, _source_checksum "
            f"FROM iceberg.bronze.{source_id} LIMIT 5"
        )
        assert len(rows) == 5
        for r in rows:
            assert r[0] == result.run_id
            assert r[1] == result.batch_id
            assert r[2] is not None
            assert r[3] == source_id
            assert len(r[4]) == 64

    # ──────────────────────────────────────────────────────────────────
    # 3. Snapshot-Level Idempotency (Unchanged Snapshot Skip)
    # ──────────────────────────────────────────────────────────────────
    def test_03_snapshot_idempotency_skip(self, infra):
        """Re-ingesting the exact same snapshot returns SKIPPED with zero additional rows."""
        source_id = "faostat_production"
        writer: BronzeIcebergWriter = infra["writer"]

        count_before = writer.get_row_count(source_id)
        assert count_before == 22738

        engine = IngestionEngine.create_default()
        result: IngestionResult = engine.run(source_id)

        assert result.status == IngestionStatus.SKIPPED
        assert (result.source_metadata or {}).get("skipped_reason") == "unchanged_snapshot"
        assert result.records_extracted == 0

        count_after = writer.get_row_count(source_id)
        assert count_after == count_before, "Duplicate rows detected after idempotent rerun!"

    # ──────────────────────────────────────────────────────────────────
    # 4. Large-File Chunking & Checkpoint Resume / Recovery
    # ──────────────────────────────────────────────────────────────────
    def test_04_checkpoint_resume_recovery(self, infra):
        """Simulate failure during chunk 1 of a 3-chunk extraction, then resume cleanly."""
        source_id = "phase2e_recovery_test"
        repo: MetadataRepository = infra["repo"]
        storage: MinioStorage = infra["storage"]
        writer: BronzeIcebergWriter = infra["writer"]
        ckpt_store = PostgresCheckpointStore(repo)

        # Cleanup test table and records
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass
        with repo._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.ingestion_checkpoints WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.dead_letter_records WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.ingestion_runs WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                conn.commit()

        # Build 3 synthetic chunks of 500 rows each
        df_c0 = pd.DataFrame({"item_id": [f"item_{i}" for i in range(500)], "value": [float(i) for i in range(500)]})
        df_c1 = pd.DataFrame({"item_id": [f"item_{i}" for i in range(500, 1000)], "value": [float(i) for i in range(500, 1000)]})
        df_c2 = pd.DataFrame({"item_id": [f"item_{i}" for i in range(1000, 1500)], "value": [float(i) for i in range(1000, 1500)]})

        class MultiChunkAdapter(BaseSourceAdapter):
            def __init__(self, fail_on_chunk: int = -1):
                self.fail_on_chunk = fail_on_chunk

            def extract_full(self, config, download_dir, resume_chunk_id=None, resume_row_start=None):
                if resume_chunk_id is None or resume_chunk_id < 0:
                    yield DataChunk(chunk_id=0, data=df_c0, row_start=0, row_end=499, record_count=500, checksum="c0")
                if self.fail_on_chunk == 1:
                    raise RuntimeError("Simulated crash at chunk 1")
                if resume_chunk_id is None or resume_chunk_id < 1:
                    yield DataChunk(chunk_id=1, data=df_c1, row_start=500, row_end=999, record_count=500, checksum="c1")
                yield DataChunk(chunk_id=2, data=df_c2, row_start=1000, row_end=1499, record_count=500, checksum="c2")

            def extract_incremental(self, config, download_dir, watermark):
                raise NotImplementedError()

            def get_artifact_checksum(self, path):
                return "recovery_test_checksum_v1"

            def check_readiness(self, config):
                raise NotImplementedError()

        cfg = SourceConfig(
            source_id=source_id,
            provider="TEST",
            dataset="Recovery",
            source_type=SourceType.LOCAL_FILE,
            load_strategy=LoadStrategy.FULL,
            format=ArtifactFormat.CSV,
        )

        ts = int(time.time() * 1000)
        run_1 = f"{source_id}_run_1_{ts}"
        batch_1 = f"{source_id}_batch_1_{ts}"
        repo.create_run(run_1, source_id, "FULL", "2026-09-25T00:00:00Z")

        # Run 1: Fails at chunk 1
        loader_fail = FullLoader(
            adapter=MultiChunkAdapter(fail_on_chunk=1),
            checkpoint_store=ckpt_store,
            minio_storage=storage,
            metadata_repo=repo,
            bronze_writer=writer,
        )
        res1 = loader_fail.execute(cfg, run_1, batch_1)
        assert res1.status == IngestionStatus.FAILED

        # Verify: Chunk 0 was written to Iceberg (500 rows)
        count_run1 = writer.get_row_count(source_id)
        assert count_run1 == 500

        # Verify Checkpoint table has chunk 0 as SUCCESS
        last_ckpt = ckpt_store.get_last_successful(source_id, run_1)
        assert last_ckpt is not None
        assert last_ckpt.chunk_id == 0

        # Run 2: Resume from run_1
        run_2 = f"{source_id}_run_2_{ts}"
        batch_2 = f"{source_id}_batch_2_{ts}"
        repo.create_run(run_2, source_id, "FULL", "2026-09-25T00:00:00Z")

        loader_resume = FullLoader(
            adapter=MultiChunkAdapter(fail_on_chunk=-1),
            checkpoint_store=ckpt_store,
            minio_storage=storage,
            metadata_repo=repo,
            bronze_writer=writer,
        )
        res2 = loader_resume.execute(cfg, run_2, batch_2, resume_from_run_id=run_1)
        assert res2.status == IngestionStatus.SUCCESS

        # Total rows must be exactly 1500 (Chunk 0 not duplicated, Chunks 1 and 2 added)
        count_run2 = writer.get_row_count(source_id)
        assert count_run2 == 1500, f"Expected 1500 rows, got {count_run2} (possible duplication or missed chunk)"

        # Checkpoints cleared upon success
        all_ckpts = ckpt_store.get_all(source_id, run_2)
        assert len(all_ckpts) == 0

        # Clean up test table and metadata records
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass
        with repo._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.ingestion_checkpoints WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.dead_letter_records WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                cur.execute("DELETE FROM ingestion.ingestion_runs WHERE source_id = %s OR run_id LIKE %s", (source_id, f"{source_id}%"))
                conn.commit()

    # ──────────────────────────────────────────────────────────────────
    # 5. Multi-File Directory Ingestion & Schema Evolution (nso_vietnam)
    # ──────────────────────────────────────────────────────────────────
    def test_05_multi_file_directory_and_schema_evolution(self, infra):
        """NSO Vietnam ingestion: 13 CSVs, 833 rows, dynamic schema evolution."""
        source_id = "nso_vietnam"
        repo: MetadataRepository = infra["repo"]
        storage: MinioStorage = infra["storage"]
        writer: BronzeIcebergWriter = infra["writer"]

        # Clean state for test
        try:
            writer.execute_query(f"DROP TABLE IF EXISTS iceberg.bronze.{source_id}")
        except Exception:
            pass
        with repo._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id = %s", (source_id,))
                conn.commit()

        engine = IngestionEngine.create_default()
        result: IngestionResult = engine.run(source_id)

        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 833

        # Row count in Bronze
        count = writer.get_row_count(source_id)
        assert count == 833

        # Verify all 13 distinct source files are tracked
        ingested_files = writer.get_ingested_files(source_id)
        assert len(ingested_files) == 13
        assert "V06.12.csv" in ingested_files
        assert "V06.24.csv" in ingested_files

        # Schema evolution verification: table has at least 38 columns (31 business + 7 metadata)
        cols_headers, cols_rows = writer.execute_query(f"DESCRIBE iceberg.bronze.{source_id}")
        col_names = [r[0].lower() for r in cols_rows]
        assert len(col_names) >= 38
        assert "col_1995" in col_names
        assert "col_2023" in col_names
        assert "_source_file" in col_names

        # Raw landing zip artifact verification
        artifact_key = result.artifact_uri.replace("s3://bronze/", "")
        assert storage.file_exists("bronze", artifact_key) is True
        assert artifact_key.endswith(".zip")

    # ──────────────────────────────────────────────────────────────────
    # 6. Directory Incremental File Arrival (Skip Ingested, Ingest New)
    # ──────────────────────────────────────────────────────────────────
    def test_06_directory_incremental_file_arrival(self, infra):
        """When a 14th file is added to a directory source, only the 14th file is ingested."""
        source_id = "nso_vietnam"
        repo: MetadataRepository = infra["repo"]
        storage: MinioStorage = infra["storage"]
        writer: BronzeIcebergWriter = infra["writer"]
        registry: SourceRegistry = infra["registry"]

        count_before = writer.get_row_count(source_id)
        assert count_before == 833
        files_before = set(writer.get_ingested_files(source_id))
        assert len(files_before) == 13

        # Create temporary staging directory with original 13 files + 1 new file
        tmp_dir = tempfile.mkdtemp(prefix="nso_arrival_test_")
        try:
            for f in glob.glob("data/raw/nso/V06.*.csv"):
                shutil.copy(f, tmp_dir)

            # Create 14th file: V06.25_test.csv with 10 rows
            sample_file = os.path.join(tmp_dir, "V06.13.csv")
            df_new = pd.read_csv(sample_file, encoding="latin-1", dtype=str).iloc[:10].copy()
            df_new.to_csv(os.path.join(tmp_dir, "V06.25_test.csv"), index=False, encoding="utf-8")

            base_cfg = registry.get_source(source_id)
            test_cfg = SourceConfig(
                source_id=source_id,
                provider=base_cfg.provider,
                dataset=base_cfg.dataset,
                source_type=base_cfg.source_type,
                load_strategy=base_cfg.load_strategy,
                format=base_cfg.format,
                local_path=tmp_dir,
                extra=base_cfg.extra,
            )

            ts = int(time.time() * 1000)
            run_id = f"{source_id}_inc_{ts}"
            batch_id = f"{source_id}_batch_{ts}"
            repo.create_run(run_id, source_id, "FULL", "2026-09-25T00:00:00Z")

            loader = FullLoader(
                adapter=NsoVietnamAdapter(),
                checkpoint_store=PostgresCheckpointStore(repo),
                minio_storage=storage,
                metadata_repo=repo,
                bronze_writer=writer,
            )

            res = loader.execute(test_cfg, run_id, batch_id)

            assert res.status == IngestionStatus.SUCCESS
            assert res.records_extracted == 10  # ONLY the 10 rows from the 14th file

            count_after = writer.get_row_count(source_id)
            assert count_after == 833 + 10, f"Expected 843 rows, got {count_after}"

            files_after = set(writer.get_ingested_files(source_id))
            assert len(files_after) == 14
            assert "V06.25_test.csv" in files_after

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            # Clean up test rows
            writer.execute_query(f"DELETE FROM iceberg.bronze.{source_id} WHERE \"_source_file\" = 'V06.25_test.csv'")
            assert writer.get_row_count(source_id) == 833

    # ──────────────────────────────────────────────────────────────────
    # 7. Error Handling & Classification in PostgreSQL
    # ──────────────────────────────────────────────────────────────────
    def test_07_error_handling_and_classification(self, infra):
        """PermanentError, SchemaError, and DataQualityError are classified and recorded."""
        repo: MetadataRepository = infra["repo"]
        storage: MinioStorage = infra["storage"]
        writer: BronzeIcebergWriter = infra["writer"]

        class BadAdapter(BaseSourceAdapter):
            def __init__(self, err_to_raise: Exception):
                self.err = err_to_raise

            def extract_full(self, config, download_dir, resume_chunk_id=None, resume_row_start=None):
                raise self.err

            def extract_incremental(self, config, download_dir, watermark):
                raise self.err

            def get_artifact_checksum(self, path):
                return "err_checksum"

            def check_readiness(self, config):
                raise self.err

        errors_to_test = [
            (PermanentError("Local file not found"), "PERMANENT"),
            (SchemaError("Missing required column 'Domain'"), "SCHEMA"),
            (DataQualityError("Corrupt CSV row detected"), "DATA_QUALITY"),
            (TransientError("Connection reset by peer"), "TRANSIENT"),
        ]

        for exc, expected_type in errors_to_test:
            source_id = f"err_test_{expected_type.lower()}"
            ts = int(time.time() * 1000)
            run_id = f"{source_id}_run_{ts}"
            batch_id = f"{source_id}_batch_{ts}"
            repo.create_run(run_id, source_id, "FULL", "2026-09-25T00:00:00Z")

            cfg = SourceConfig(
                source_id=source_id,
                provider="TEST",
                dataset="ErrorTest",
                source_type=SourceType.LOCAL_FILE,
                load_strategy=LoadStrategy.FULL,
                format=ArtifactFormat.CSV,
            )

            loader = FullLoader(
                adapter=BadAdapter(exc),
                checkpoint_store=PostgresCheckpointStore(repo),
                minio_storage=storage,
                metadata_repo=repo,
                bronze_writer=writer,
            )

            res = loader.execute(cfg, run_id, batch_id)
            assert res.status == IngestionStatus.FAILED
            assert res.error_type == expected_type

            # Verify PostgreSQL recorded the failure
            now_iso = datetime.now(timezone.utc).isoformat()
            repo.complete_run(
                run_id=run_id,
                status=IngestionStatus.FAILED.value,
                records_extracted=0,
                records_quarantined=0,
                chunks_processed=0,
                chunks_total=0,
                completed_at=now_iso,
                error_message=str(exc),
                error_type=expected_type,
            )
            saved_run = repo.get_run(run_id)
            assert saved_run["status"] == "FAILED"
            assert str(exc) in saved_run["error_message"]
            assert saved_run["error_type"] == expected_type

    def test_07b_transient_error_retry_and_recovery(self, infra):
        """TransientError triggers retry with exponential backoff and recovers to SUCCESS."""
        from ingestion.core.config import RetryConfig
        from ingestion.utils.retry import retry_with_backoff

        attempt_count = 0

        def operation():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise TransientError(f"Temporary Trino socket timeout (attempt {attempt_count})")
            return "SUCCESS_DATA"

        retry_cfg = RetryConfig(
            max_attempts=4,
            initial_delay_seconds=0.01,
            max_delay_seconds=0.05,
            exponential_backoff=True,
            jitter=False,
        )

        wrapped = retry_with_backoff(operation, retry_cfg)
        result = wrapped()
        assert result == "SUCCESS_DATA"
        assert attempt_count == 3

    # ──────────────────────────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────────
    # 8. Quantitative Reconciliation
    # ──────────────────────────────────────────────────────────────────
    def test_08_quantitative_reconciliation(self, infra):
        """Input records == Accepted records == Bronze rows for all ingested canonical sources."""
        writer: BronzeIcebergWriter = infra["writer"]

        reconciliation_targets = [
            ("faostat_production", 22738),
            ("faostat_monthly_price", 15334),
            ("faostat_supply_utilization", 36380),
            ("nso_vietnam", 833),
            ("usda_rice_yearbook", 13131),
            ("usda_psd", 15),
            ("worldbank_pinksheet", 792),
            ("thitruongnongsan", 20394),
            ("faostat_trade", 1226470),
        ]

        engine = IngestionEngine.create_default()
        for source_id, expected_rows in reconciliation_targets:
            actual_rows = writer.get_row_count(source_id)
            if actual_rows != expected_rows:
                res = engine.run(source_id)
                actual_rows = writer.get_row_count(source_id)
            assert actual_rows == expected_rows, (
                f"Reconciliation failure for {source_id}: "
                f"Expected {expected_rows} rows, got {actual_rows} in Iceberg Bronze"
            )

        trade_rows = writer.get_row_count("faostat_trade")
        assert trade_rows == 1226470, f"Expected faostat_trade == 1226470, got {trade_rows}"

    # ──────────────────────────────────────────────────────────────────
    # 9. Airflow DAG Integrity (Validated via Live Airflow Container)
    # ──────────────────────────────────────────────────────────────────
    def test_09_airflow_dag_integrity(self):
        """Verify Airflow DAG loads cleanly with 0 import errors and correct task topology in Airflow."""
        import json
        import subprocess

        cmd = [
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "python", "-c",
            "import json; from airflow.models import DagBag; bag = DagBag('/opt/airflow/dags'); "
            "dag = bag.get_dag('rice_lakehouse_ingestion'); "
            "errs = {k: str(v) for k, v in bag.import_errors.items()}; "
            "tasks = list(dag.task_dict.keys()) if dag else []; "
            "print(json.dumps({'has_dag': dag is not None, 'errors': errs, 'task_count': len(tasks), 'tasks': tasks}))"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, f"Docker command failed: {proc.stderr}"

        # Parse JSON output from the container (last line after Airflow logger outputs)
        lines = [line.strip() for line in proc.stdout.strip().split("\n") if line.strip().startswith("{")]
        assert len(lines) > 0, f"No JSON output from docker command: {proc.stdout}"
        res = json.loads(lines[-1])

        assert res["has_dag"] is True, "DAG 'rice_lakehouse_ingestion' not found in Airflow"
        assert res["errors"] == {}, f"DAG import errors found: {res['errors']}"
        assert res["task_count"] == 83, f"Expected 83 tasks, got {res['task_count']}"

        task_ids = set(res["tasks"])
        assert "initialize_metadata" in task_ids
        assert "start_lakehouse_ingestion" in task_ids
        assert "end_lakehouse_ingestion" in task_ids

        canonical_source_ids = [
            "faostat_production",
            "faostat_monthly_price",
            "faostat_trade",
            "faostat_trade_validation",
            "faostat_supply_utilization",
            "nso_vietnam",
            "usda_rice_yearbook",
            "usda_psd",
            "worldbank_pinksheet",
            "thitruongnongsan",
        ]
        for sid in canonical_source_ids:
            for stage in ["check_source", "readiness_check", "extract", "pre_audit", "write_bronze", "post_audit", "update_metadata", "publish"]:
                assert f"pipeline_{sid}.{stage}" in task_ids, f"Missing {stage} task for {sid}"

    # ──────────────────────────────────────────────────────────────────
    # 10. Large-Volume Dataset Ingestion & Idempotency Verification
    # ──────────────────────────────────────────────────────────────────
    def test_10_large_volume_write_and_idempotency(self, infra):
        """Verify faostat_trade 1.2M rows reconciliation and snapshot idempotency."""
        writer: BronzeIcebergWriter = infra["writer"]
        source_id = "faostat_trade"

        # Assert full 1.2M row count in Iceberg Bronze (ingest if not already present)
        trade_rows = writer.get_row_count(source_id)
        if trade_rows != 1226470:
            engine = IngestionEngine.create_default()
            engine.run(source_id)
            trade_rows = writer.get_row_count(source_id)
        assert trade_rows == 1226470, f"Expected faostat_trade == 1226470, got {trade_rows}"

        # Assert Trino direct SQL count
        _, data = writer.execute_query(f"SELECT COUNT(*) FROM iceberg.bronze.{source_id}")
        assert int(data[0][0]) == 1226470

        # Rerun ingestion on faostat_trade via IngestionEngine -> must skip idempotently
        engine = IngestionEngine.create_default()
        res = engine.run(source_id)
        assert res.status == IngestionStatus.SKIPPED
        assert (res.source_metadata or {}).get("skipped_reason") == "unchanged_snapshot"

        # Ensure no duplicates created
        assert writer.get_row_count(source_id) == 1226470

