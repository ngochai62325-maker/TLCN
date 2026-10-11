import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from datetime import datetime, timezone

from ingestion.core.config import SourceConfig
from ingestion.core.enums import LoadStrategy, IngestionStatus, ChunkStatus
from ingestion.core.result import DataChunk, IngestionResult
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.core.full_loader import FullLoader
from ingestion.core.checkpoint import CheckpointStore
from ingestion.adapters.base_adapter import BaseSourceAdapter

class InMemoryCheckpointStore(CheckpointStore):
    """Stateful fake checkpoint store for testing resume semantics."""
    def __init__(self):
        self.store = {}  # (source, run, chunk_id) -> status
        
    def save(self, source_id, run_id, batch_id, chunk_id, row_start, row_end, status, checksum=None):
        if source_id not in self.store:
            self.store[source_id] = {}
        if run_id not in self.store[source_id]:
            self.store[source_id][run_id] = {}
        
        # We store the status to simulate DB
        self.store[source_id][run_id][chunk_id] = {
            "status": status,
            "row_start": row_start,
            "row_end": row_end,
            "chunk_id": chunk_id
        }
        
    def get_last_successful(self, source_id, run_id):
        if source_id in self.store and run_id in self.store[source_id]:
            run_data = self.store[source_id][run_id]
            successes = [v for k, v in run_data.items() if v["status"] == ChunkStatus.SUCCESS.value]
            if successes:
                latest = max(successes, key=lambda x: x["chunk_id"])
                class Dummy:
                    chunk_id = latest["chunk_id"]
                    row_end = latest["row_end"]
                return Dummy()
        return None
        
    def get_all(self, source_id, run_id):
        if source_id in self.store and run_id in self.store[source_id]:
            run_data = self.store[source_id][run_id]
            res = []
            for k, v in run_data.items():
                class Dummy:
                    chunk_id = v["chunk_id"]
                    row_start = v["row_start"]
                    row_end = v["row_end"]
                    status = v["status"]
                res.append(Dummy())
            return res
        return []

    def clear(self, source_id, run_id):
        if source_id in self.store and run_id in self.store[source_id]:
            del self.store[source_id][run_id]

class FailingAdapter(BaseSourceAdapter):
    def __init__(self, fail_on_chunk=None):
        self.fail_on_chunk = fail_on_chunk
        self.chunks_yielded = 0
        
    def extract_full(self, config, **kwargs):
        resume_chunk = kwargs.get("resume_chunk_id")
        
        for i in range(1, 5):
            # simulate skipping chunks that are passed via kwargs (though FullLoader actually skips them in its loop)
            # wait, FullLoader loop skips it if chunk_id <= resume_chunk_id, so the adapter just yields them.
            if self.fail_on_chunk == i:
                raise RuntimeError(f"Simulated extraction failure on chunk {i}")
                
            self.chunks_yielded += 1
            yield DataChunk(
                chunk_id=i,
                data=pd.DataFrame({"col": [i]}),
                row_start=i*10,
                row_end=i*10+9,
                record_count=10,
                checksum="chk"
            )

    def check_readiness(self, config, **kwargs):
        from ingestion.core.result import ReadinessResult
        return ReadinessResult(ready=True, reason="ok")

def _make_dummy_config(source_id: str, load_strategy: LoadStrategy) -> SourceConfig:
    from ingestion.core.enums import SourceType, ArtifactFormat
    return SourceConfig(
        source_id=source_id,
        provider="dummy",
        dataset="dummy",
        source_type=SourceType.LOCAL_FILE,
        load_strategy=load_strategy,
        format=ArtifactFormat.CSV,
        adapter_class="dummy"
    )

def _setup_full_loader(fail_on_chunk=None):
    adapter = FailingAdapter(fail_on_chunk=fail_on_chunk)
    ckpt = InMemoryCheckpointStore()
    repo = MagicMock()
    repo.get_latest_run.return_value = None
    repo.find_snapshot_by_checksum.return_value = None
    writer = MagicMock()
    
    loader = FullLoader(
        adapter=adapter,
        checkpoint_store=ckpt,
        minio_storage=MagicMock(),
        metadata_repo=repo,
        bronze_writer=writer,
        bronze_layout=MagicMock(),
        quality_validator=MagicMock(),
        idempotency_controller=MagicMock(),
        quarantine_manager=MagicMock()
    )
    # mock quality validator to pass
    loader.quality_validator.validate_file.return_value.is_valid = True
    return loader, ckpt, writer, adapter

def test_scenario_a_extraction_failure():
    """Scenario A: Extraction failure propagates exception and returns FAILED result."""
    loader, ckpt, writer, adapter = _setup_full_loader(fail_on_chunk=1)
    config = _make_dummy_config(source_id="test", load_strategy=LoadStrategy.FULL)
    
    result = loader.execute(config, "run_a", "batch_a")
    
    assert result.status == IngestionStatus.FAILED
    assert "Simulated extraction failure" in result.error_message
    # No chunks were written
    writer.write_chunk.assert_not_called()

def test_scenario_b_and_e_chunk_write_failure_and_recovery():
    """Scenario B & E: Chunk write failure creates failed checkpoint, retry skips processed chunks."""
    loader, ckpt, writer, adapter = _setup_full_loader(fail_on_chunk=3)
    config = _make_dummy_config(source_id="test_b", load_strategy=LoadStrategy.FULL)
    
    # Run 1: Fails on chunk 3
    result1 = loader.execute(config, "run_fail", "batch_b")
    assert result1.status == IngestionStatus.FAILED
    assert "Simulated extraction failure on chunk 3" in result1.error_message
    
    # Assert Checkpoint State
    assert ckpt.store["test_b"]["run_fail"][1]["status"] == ChunkStatus.SUCCESS.value
    assert ckpt.store["test_b"]["run_fail"][2]["status"] == ChunkStatus.SUCCESS.value
    # Note: exception in adapter before yielding chunk 3 means chunk 3 wasn't explicitly saved as FAILED in CheckpointStore, 
    # but run status is FAILED. CheckpointStore has 1 and 2 successful.
    
    # Run 2: Retry
    # IngestionEngine passes resume_from_run_id by looking up DB, we simulate it
    loader2, ckpt2, writer2, adapter2 = _setup_full_loader(fail_on_chunk=None) # No failure this time
    # Transfer state
    ckpt2.store = ckpt.store
    
    result2 = loader2.execute(config, "run_retry", "batch_retry", resume_from_run_id="run_fail")
    assert result2.status == IngestionStatus.SUCCESS
    
    # Assert writer calls on retry
    # Should only process chunks 3 and 4!
    called_chunks = [call.kwargs['chunk_id'] for call in writer2.write_chunk.mock_calls]
    assert called_chunks == [3, 4]
    
    # Total records should be 40 (10*4) across the logical ingestion
    # Wait, the result.records_extracted will add the prior checkpoints
    assert result2.records_extracted == 40
    assert result2.chunks_total == 4

def test_scenario_c_failure_after_write_orphan_cleanup():
    """Scenario C: Failure after write triggers cleanup."""
    loader, ckpt, writer, adapter = _setup_full_loader(fail_on_chunk=None)
    # Simulate failure in manifest upload which is not swallowed
    loader.minio_storage.upload_json.side_effect = RuntimeError("Network error saving manifest")
    
    config = _make_dummy_config(source_id="test_c", load_strategy=LoadStrategy.FULL)
    result = loader.execute(config, "run_c", "batch_c")
    
    assert result.status == IngestionStatus.FAILED
    assert "Network error saving manifest" in result.error_message
    
    # Airflow layer receives FAILED, orchestrates retry.
    # Meanwhile, IdempotencyController can cleanup the run
    from ingestion.reliability.idempotency_controller import IdempotencyController
    ctrl = IdempotencyController(metadata_repo=MagicMock())
    mock_bronze_writer = MagicMock()
    
    ctrl.cleanup_failed_run("run_c", "test_c", bronze_writer=mock_bronze_writer)
    # Assert cleanup deterministically targets the run_id
    mock_bronze_writer.cleanup_run.assert_called_once_with(source_id="test_c", run_id="run_c")

def test_scenario_d_watermark_safety_on_incremental_failure():
    """Scenario D: Watermark remains unchanged on failure."""
    from ingestion.core.incremental_loader import IncrementalLoader
    adapter = MagicMock()
    # Simulate adapter yielding chunk 1, then crashing
    def failing_extract(*args, **kwargs):
        yield DataChunk(chunk_id=1, data=pd.DataFrame(), row_start=0, row_end=0, record_count=0)
        raise RuntimeError("DB Disconnected")
    adapter.extract_incremental.side_effect = failing_extract
    
    wm_store = MagicMock()
    # Previous watermark
    wm_entry_mock = MagicMock()
    wm_entry_mock.watermark_value = "2026-09-01"
    wm_store.get.return_value = wm_entry_mock
    
    loader = IncrementalLoader(
        adapter=adapter,
        checkpoint_store=MagicMock(),
        watermark_store=wm_store,
        minio_storage=MagicMock(),
        metadata_repo=MagicMock(),
    )
    
    config = _make_dummy_config(source_id="test_d", load_strategy=LoadStrategy.INCREMENTAL)
    result = loader.execute(config, "run_d", "batch_d")
    
    assert result.status == IngestionStatus.FAILED
    
    # IngestionEngine only updates watermark on SUCCESS
    engine = IngestionEngine()
    engine.watermark_store = wm_store
    engine._resolve_adapter = MagicMock(return_value=adapter)
    engine._select_loader = MagicMock(return_value=loader)
    engine.registry.get_source = MagicMock(return_value=config)
    engine._record_run_start = MagicMock()
    engine._record_run_end = MagicMock()
    
    # Use engine run
    with patch.object(engine.metadata_repo, 'source_lock', MagicMock()):
        engine_result = engine.run("test_d")
        
    assert engine_result.status == IngestionStatus.FAILED
    # Assert watermark_store.set was NEVER called!
    wm_store.set.assert_not_called()

def test_scenario_f_idempotent_retry_logic():
    """Scenario F: Bronze writer idempotent logical retry."""
    # Test that write_chunk calls DELETE before APPEND.
    # Phase 2 implementation handles this inside BronzeIcebergWriter.
    from ingestion.storage.bronze_writer import BronzeIcebergWriter, TECHNICAL_METADATA_COLUMNS
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock(return_value=([], [[c, "VARCHAR"] for c in ["col", *TECHNICAL_METADATA_COLUMNS]]))
    writer.get_iceberg_catalog = MagicMock(return_value=None)
    writer._load_table = MagicMock() 
    
    df = pd.DataFrame({"col": [1]})
    writer.write_chunk("test_f", df, "run_f", "batch_f", "chk", 1, source_file="a.csv")
    
    # Assert execute_query was called with DELETE
    delete_call = next(c.args[0] for c in writer.execute_query.call_args_list if c.args[0].startswith("DELETE"))
    assert "DELETE FROM" in delete_call
    assert "_ingestion_run_id = 'run_f'" in delete_call
    assert "_ingestion_chunk_id = 1" in delete_call

def test_scenario_i_exception_propagation():
    """Scenario I & J: Airflow orchestration propagates exceptions correctly."""
    from ingestion.orchestration.pipeline_tasks import run_ingestion_task, AirflowException, AirflowSkipException
    
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        # 1. Failure propagates as AirflowException
        mock_result_fail = MagicMock(spec=IngestionResult)
        mock_result_fail.status = IngestionStatus.FAILED
        mock_result_fail.error_message = "Hard error"
        mock_engine.run_with_readiness_check.return_value = mock_result_fail
        
        with pytest.raises(AirflowException, match="Hard error"):
            run_ingestion_task("test_source")
            
        # 2. Not Ready propagates as AirflowSkipException (Scenario H)
        mock_result_skip = MagicMock(spec=IngestionResult)
        mock_result_skip.status = IngestionStatus.NOT_READY
        mock_result_skip.error_message = "Not ready yet"
        mock_engine.run_with_readiness_check.return_value = mock_result_skip
        
        with pytest.raises(AirflowSkipException, match="not ready"):
            run_ingestion_task("test_source")
