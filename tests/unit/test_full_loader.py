"""Tests for ingestion.core.full_loader.FullLoader."""

import os
import pandas as pd
from unittest.mock import MagicMock

from ingestion.core.full_loader import FullLoader
from ingestion.core.result import DataChunk
from ingestion.core.enums import IngestionStatus, LoadStrategy, SourceType, ArtifactFormat
from ingestion.core.config import SourceConfig


def _make_config():
    return SourceConfig(
        source_id="test_source",
        provider="Test",
        dataset="TestData",
        source_type=SourceType.LOCAL_FILE,
        load_strategy=LoadStrategy.FULL,
        format=ArtifactFormat.CSV,
        local_path="/tmp/test.csv",
    )


def _make_chunks(n=3):
    df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
    return [
        DataChunk(
            chunk_id=i,
            data=df,
            row_start=i * 2,
            row_end=i * 2 + 1,
            record_count=2,
        )
        for i in range(n)
    ]


def _make_adapter(chunks):
    adapter = MagicMock()
    adapter.extract_full.return_value = iter(chunks)
    return adapter


def _make_loader(adapter, checkpoint_store=None, minio_storage=None, metadata_repo=None):
    cs = checkpoint_store or MagicMock()
    cs.get_last_successful.return_value = None
    ms = minio_storage or MagicMock()
    mr = metadata_repo or MagicMock()
    mr.find_snapshot_by_checksum.return_value = None
    return FullLoader(adapter=adapter, checkpoint_store=cs, minio_storage=ms, metadata_repo=mr)


class TestFullLoaderSuccess:
    def test_happy_path(self):
        chunks = _make_chunks(3)
        adapter = _make_adapter(chunks)
        loader = _make_loader(adapter)
        config = _make_config()

        result = loader.execute(config, "run_1", "batch_1")

        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 6  # 3 chunks * 2 records
        assert result.chunks_processed == 3
        assert result.source_id == "test_source"

    def test_single_chunk(self):
        chunks = _make_chunks(1)
        adapter = _make_adapter(chunks)
        loader = _make_loader(adapter)
        config = _make_config()

        result = loader.execute(config, "run_1", "batch_1")
        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 2


class TestFullLoaderIdempotency:
    def test_force_reprocess_reads_all_files_without_failed_run_checkpoint(self):
        chunks = _make_chunks(2)
        for chunk in chunks:
            chunk.data = chunk.data.assign(_source_file=f"file_{chunk.chunk_id}.csv")
        adapter = _make_adapter(chunks)
        loader = _make_loader(adapter)
        loader.metadata_repo.get_latest_run.return_value = {"run_id": "failed", "status": "FAILED"}
        loader.checkpoint_store.get_last_successful.return_value = MagicMock(chunk_id=0, row_end=1)
        loader.bronze_writer = MagicMock()
        loader.bronze_writer.get_ingested_files.return_value = ["file_0.csv", "file_1.csv"]
        loader.metadata_repo.find_snapshot_by_checksum.return_value = {"run_id": "old", "status": "SUCCESS"}

        result = loader.execute(
            _make_config(), "reprocess", "batch", resume_from_run_id="failed", force_reprocess=True
        )

        assert result.status == IngestionStatus.SUCCESS
        assert result.records_extracted == 4
        assert result.chunks_processed == 2
        assert loader.bronze_writer.write_chunk.call_count == 2
        loader.bronze_writer.get_ingested_files.assert_not_called()
        loader.checkpoint_store.get_last_successful.assert_not_called()
        loader.metadata_repo.get_latest_run.assert_not_called()
        assert "resume_chunk_id" not in adapter.extract_full.call_args.kwargs

    def test_skip_when_same_snapshot(self):
        chunks = _make_chunks(1)
        adapter = _make_adapter(chunks)
        mr = MagicMock()
        # The loader computes checksum AFTER extraction. When no real file
        # exists on disk, the checksum is "no-artifact". Return a match
        # for that checksum to trigger idempotent skip.
        mr.find_snapshot_by_checksum.return_value = {"run_id": "old_run", "status": "SUCCESS"}

        cs = MagicMock()
        cs.get_last_successful.return_value = None
        ms = MagicMock()

        loader = FullLoader(adapter=adapter, checkpoint_store=cs, minio_storage=ms, metadata_repo=mr)
        config = _make_config()

        result = loader.execute(config, "run_2", "batch_2")
        assert result.status == IngestionStatus.SKIPPED
        assert result.source_metadata.get("skipped_reason") == "unchanged_snapshot"


class TestFullLoaderCheckpoint:
    def test_checkpoint_save_called(self):
        chunks = _make_chunks(2)
        adapter = _make_adapter(chunks)
        cs = MagicMock()
        cs.get_last_successful.return_value = None
        loader = _make_loader(adapter, checkpoint_store=cs)
        config = _make_config()

        loader.execute(config, "run_1", "batch_1")
        assert cs.save.call_count == 2

    def test_checkpoint_cleared_on_success(self):
        chunks = _make_chunks(1)
        adapter = _make_adapter(chunks)
        cs = MagicMock()
        cs.get_last_successful.return_value = None
        loader = _make_loader(adapter, checkpoint_store=cs)
        config = _make_config()

        result = loader.execute(config, "run_1", "batch_1")
        assert result.status == IngestionStatus.SUCCESS
        cs.clear.assert_called_once_with("test_source", "run_1")


class TestFullLoaderFailure:
    def test_adapter_raises(self):
        adapter = MagicMock()
        adapter.extract_full.side_effect = ValueError("Connection lost")
        loader = _make_loader(adapter)
        config = _make_config()

        result = loader.execute(config, "run_1", "batch_1")
        assert result.status == IngestionStatus.FAILED
        assert "Connection lost" in result.error_message
