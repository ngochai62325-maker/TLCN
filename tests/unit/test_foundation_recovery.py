from unittest.mock import MagicMock
from datetime import datetime, timezone

import pandas as pd
import pytest

from ingestion.core.full_loader import FullLoader
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.enums import IngestionStatus
from ingestion.core.result import DataChunk, IngestionResult


@pytest.fixture
def physical_loader(tmp_path, sample_source_config):
    raw = tmp_path / "raw.csv"
    raw.write_text("id,date,value\n1,2024-01-01,0\n2,2024-01-02,-3\n", encoding="utf-8")
    sample_source_config.local_path = str(raw)
    repo, checkpoints, writer, adapter = [MagicMock() for _ in range(4)]
    repo.get_latest_run.return_value = None
    repo.find_snapshot_by_checksum.return_value = None
    checkpoints.get_last_successful.return_value = None
    writer.verify_run.return_value = dict(uuid="uuid", snapshot_id="123", metadata_location="metadata", run_id="r", rows=2)
    chunk = DataChunk(chunk_id=0, data=pd.DataFrame({"id": [1, 2], "date": ["2024-01-01", "2024-01-02"], "value": [0, -3]}), row_start=0, row_end=1, record_count=2)
    adapter.extract_full.return_value = iter([chunk])
    return FullLoader(adapter, checkpoints, MagicMock(), repo, writer), sample_source_config, raw, chunk


@pytest.mark.parametrize("phase", ["write", "readback", "batch", "snapshot", "checkpoint"])
def test_failure_never_returns_success_or_clears_evidence(physical_loader, phase):
    loader, config, raw, _ = physical_loader
    failures = {"write": loader.bronze_writer.write_chunk, "readback": loader.bronze_writer.verify_run,
                "batch": loader.metadata_repo.save_batch_commit, "snapshot": loader.metadata_repo.save_snapshot,
                "checkpoint": loader.checkpoint_store.save}
    failures[phase].side_effect = RuntimeError("injected " + phase)
    result = loader.execute(config, "r", "b")
    assert result.status == IngestionStatus.FAILED
    loader.checkpoint_store.clear.assert_not_called()
    assert raw.exists()


def test_proof_and_honest_source_snapshot_identity(physical_loader):
    loader, config, _, _ = physical_loader
    result = loader.execute(config, "r", "b")
    assert result.status == IngestionStatus.SUCCESS
    assert loader.bronze_writer.write_chunk.call_args.kwargs["source_snapshot_id"] is None
    assert result.source_metadata["batch_commit"]["extra_properties"]["bronze_proof"]["snapshot_id"] == "123"
    loader.metadata_repo.save_batch_commit.assert_called_once()


def test_source_changes_during_extract_fails_closed(physical_loader):
    loader, config, raw, chunk = physical_loader
    def extraction(*args, **kwargs):
        yield chunk
        raw.write_text("changed", encoding="utf-8")
    loader.adapter.extract_full.side_effect = extraction
    result = loader.execute(config, "r", "b")
    assert result.status == IngestionStatus.FAILED and "Source content changed" in result.error_message
    loader.metadata_repo.save_snapshot.assert_not_called()


@pytest.mark.parametrize("mode", ["missing_row", "inconsistent_parent", "metadata_unavailable"])
def test_idempotent_skip_requires_physical_and_metadata_proof(physical_loader, mode):
    loader, config, _, _ = physical_loader
    checksum = loader._compute_source_checksum(config, "unused")
    loader.metadata_repo.find_snapshot_by_checksum.return_value = dict(run_id="old", status="SUCCESS")
    loader.metadata_repo.get_run.return_value = dict(run_id="old", source_id=config.source_id, status="SUCCESS", checksum=checksum, records_extracted=2)
    if mode == "missing_row":
        loader.bronze_writer.verify_run.side_effect = RuntimeError("missing committed row")
    elif mode == "inconsistent_parent":
        loader.metadata_repo.get_run.return_value["status"] = "FAILED"
    else:
        loader.metadata_repo.find_snapshot_by_checksum.side_effect = OSError("metadata unavailable")
    assert loader.execute(config, "r", "b").status == IngestionStatus.FAILED
    loader.bronze_writer.write_chunk.assert_not_called()


def test_checkpoint_cannot_authorize_different_run_identity(physical_loader):
    loader, config, _, _ = physical_loader
    result = loader.execute(config, "new", "b", resume_from_run_id="interrupted")
    assert result.status == IngestionStatus.FAILED and "RECOVERY_REQUIRED" in result.error_message
    loader.bronze_writer.write_chunk.assert_not_called()


def test_unresolved_unknown_commit_cannot_be_replayed_automatically(physical_loader):
    loader, config, _, _ = physical_loader
    loader.metadata_repo.get_run.return_value = {"status": "FAILED", "error_message": "UNKNOWN_COMMIT: catalog unavailable"}
    result = loader.execute(config, "new", "b", resume_from_run_id="old")
    assert result.status == IngestionStatus.FAILED and "automatic replay forbidden" in result.error_message
    loader.bronze_writer.write_chunk.assert_not_called()


@pytest.mark.parametrize("changed", [None, "checksum", "layout"])
def test_recovery_reuses_only_compatible_durable_physical_identity(physical_loader, changed):
    loader, config, _, _ = physical_loader
    context = dict(checksum=loader._compute_source_checksum(config, "unused"), layout=loader._layout_fingerprint(config),
                   physical_run_id="original", physical_batch_id="original_batch")
    if changed:
        context[changed] = "changed"
    loader.metadata_repo.get_run.return_value = dict(source_id=config.source_id, status="FAILED", source_metadata={"input_context": context})
    result = loader.execute(config, "recovery", "new_batch", resume_from_run_id="original")
    if changed:
        assert result.status == IngestionStatus.FAILED and "RECOVERY_REQUIRED" in result.error_message
        loader.bronze_writer.write_chunk.assert_not_called()
    else:
        assert result.status == IngestionStatus.SUCCESS
        assert loader.bronze_writer.write_chunk.call_args.kwargs["run_id"] == "original"
        assert loader.bronze_writer.write_chunk.call_args.kwargs["batch_id"] == "original_batch"
        assert result.source_metadata["physical_run_id"] == "original"


@pytest.mark.parametrize("phase", ["start", "finish"])
def test_engine_does_not_swallow_metadata_failure(phase, sample_source_config):
    repo, registry = MagicMock(), MagicMock()
    registry.get_source.return_value = sample_source_config
    engine = IngestionEngine(metadata_repo=repo, registry=registry, bronze_writer=MagicMock(), minio_storage=MagicMock())
    loader = MagicMock()
    now = datetime.now(timezone.utc)
    loader.execute.return_value = IngestionResult(source_id=sample_source_config.source_id, run_id="r", batch_id="b",
        load_type=sample_source_config.load_strategy, status=IngestionStatus.SUCCESS, started_at=now, completed_at=now)
    engine._select_loader = MagicMock(return_value=loader)
    if phase == "start":
        repo.create_run.side_effect = RuntimeError("start unavailable")
    else:
        repo.complete_run.side_effect = RuntimeError("finish unavailable")
    assert engine.run(sample_source_config.source_id).status == IngestionStatus.FAILED
    if phase == "start":
        loader.execute.assert_not_called()
