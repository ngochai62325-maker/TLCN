import pytest
from unittest.mock import patch, MagicMock
from ingestion.orchestration.pipeline_tasks import run_ingestion_task, AirflowException, AirflowSkipException
from ingestion.core.enums import IngestionStatus
from ingestion.core.result import IngestionResult

def test_run_ingestion_task_success():
    """Test 1: Verify Airflow task invokes IngestionEngine (Unit Test)."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        # Setup mock result
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.SUCCESS
        mock_result.to_dict.return_value = {"status": "SUCCESS"}
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        result = run_ingestion_task("test_source")
        
        # Assert engine was instantiated and called
        MockEngine.create_default.assert_called_once()
        mock_engine.run_with_readiness_check.assert_called_once_with("test_source")
        assert result == {"status": "SUCCESS"}

def test_run_ingestion_task_failure():
    """Test 11: Airflow failure propagation. Engine exception must fail task."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.FAILED
        mock_result.error_message = "Data corrupt"
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowException, match="Ingestion failed for source test_source: Data corrupt"):
            run_ingestion_task("test_source")

def test_run_ingestion_task_not_ready():
    """Test: Source not ready skips Airflow task cleanly."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.NOT_READY
        mock_result.error_message = "API unavailable"
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowSkipException, match="not ready"):
            run_ingestion_task("test_source")

def test_run_ingestion_task_skipped_idempotency():
    """Test: Source skipped due to idempotency skips Airflow task cleanly."""
    with patch("ingestion.orchestration.pipeline_tasks.IngestionEngine") as MockEngine:
        mock_engine = MockEngine.create_default.return_value
        
        mock_result = MagicMock(spec=IngestionResult)
        mock_result.status = IngestionStatus.SKIPPED
        mock_result.source_metadata = {"idempotency_reason": "No changes"}
        mock_engine.run_with_readiness_check.return_value = mock_result
        
        with pytest.raises(AirflowSkipException, match="safely skipped"):
            run_ingestion_task("test_source")
