import pytest
from unittest.mock import MagicMock
from pathlib import Path

from ingestion.core.config import SourceConfig, RetryConfig, ReadinessConfig
from ingestion.core.enums import LoadStrategy, SourceType, ArtifactFormat, IngestionStatus, ChunkStatus
from ingestion.core.result import DataChunk, ReadinessResult, IngestionResult
from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.core.checkpoint import CheckpointStore, CheckpointEntry
from ingestion.core.watermark import WatermarkStore, WatermarkEntry

@pytest.fixture
def sample_retry_config():
    return RetryConfig(
        max_attempts=3,
        initial_delay_seconds=0.1,
        max_delay_seconds=1.0,
        exponential_backoff=True,
        jitter=False,
        respect_retry_after=False
    )

@pytest.fixture
def sample_readiness_config():
    return ReadinessConfig(
        require_http_200=True,
        require_content_length=False,
        min_file_size_bytes=0,
        required_columns=[],
        expected_content_type=None
    )

@pytest.fixture
def sample_source_config(sample_retry_config, sample_readiness_config, tmp_path):
    return SourceConfig(
        source_id="test_source",
        provider="test_provider",
        dataset="test_dataset",
        source_type=SourceType.LOCAL_FILE,
        load_strategy=LoadStrategy.FULL,
        format=ArtifactFormat.CSV,
        endpoint=None,
        local_path=str(tmp_path / "test.csv"),
        chunk_size=1000,
        encoding="utf-8",
        readiness=sample_readiness_config,
        retry=sample_retry_config,
        frequency="daily",
        watermark_column="date",
        business_key=["id"],
        partition_column="year",
        expected_columns=["id", "date", "value"],
        adapter_class="ingestion.adapters.test_adapter.TestAdapter",
        extra={}
    )

@pytest.fixture
def mock_metadata_repo():
    repo = MagicMock()
    return repo

@pytest.fixture
def mock_checkpoint_store():
    store = MagicMock()
    return store

@pytest.fixture
def mock_watermark_store():
    store = MagicMock()
    return store

@pytest.fixture
def mock_minio_storage():
    storage = MagicMock()
    return storage

@pytest.fixture
def tmp_data_dir(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def sample_csv_file(tmp_path):
    file_path = tmp_path / "test.csv"
    file_path.write_text("id,date,value\n1,2023-01-01,100\n2,2023-01-02,200\n")
    return file_path

class MockAdapter(BaseSourceAdapter):
    def __init__(self, config, data_chunks):
        super().__init__(config)
        self.data_chunks = data_chunks

    def check_readiness(self):
        return ReadinessResult(ready=True, reason="OK")

    def extract_full(self, run_id, batch_id, checkpoint_store=None):
        for chunk in self.data_chunks:
            yield chunk
            
    def extract_incremental(self, run_id, batch_id, start_watermark=None, end_watermark=None, checkpoint_store=None):
        for chunk in self.data_chunks:
            yield chunk
            
    def get_artifact_checksum(self):
        return "fake-checksum"

@pytest.fixture
def create_mock_adapter():
    def _create(config, chunks):
        return MockAdapter(config, chunks)
    return _create
