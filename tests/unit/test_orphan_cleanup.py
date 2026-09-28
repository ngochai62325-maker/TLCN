import pytest
from unittest.mock import MagicMock
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from ingestion.reliability.idempotency_controller import IdempotencyController

def test_cleanup_failed_run():
    """Test 1 & 9: Cleanup failed run executes the correct SQL predicate"""
    writer = BronzeIcebergWriter()
    writer.execute_query = MagicMock(return_value=([], []))
    writer.ensure_schema = MagicMock()
    
    writer.cleanup_run("test_src", "run_A")
    
    writer.execute_query.assert_called_once_with(
        "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_A'"
    )

def test_multiple_chunks_removed():
    """Test 2 & 4: The predicate naturally removes all chunks for the run"""
    # Since the query is `DELETE FROM table WHERE _ingestion_run_id = 'run_A'`,
    # it doesn't specify chunk_id, therefore it removes ALL chunks belonging to run_A.
    # This test verifies that chunk_id is NOT in the predicate.
    writer = BronzeIcebergWriter()
    writer.execute_query = MagicMock(return_value=([], []))
    writer.ensure_schema = MagicMock()
    
    writer.cleanup_run("test_src", "run_A")
    
    call_args = writer.execute_query.call_args[0][0]
    assert "_ingestion_chunk_id" not in call_args
    assert "business_key" not in call_args
    assert "checksum" not in call_args
    assert "_ingestion_run_id = 'run_A'" in call_args

def test_different_run_isolation():
    """Test 3: Different run isolation is respected by predicate"""
    writer = BronzeIcebergWriter()
    writer.execute_query = MagicMock(return_value=([], []))
    writer.ensure_schema = MagicMock()
    
    writer.cleanup_run("test_src", "run_A")
    
    call_args = writer.execute_query.call_args[0][0]
    assert "run_A" in call_args
    assert "run_B" not in call_args

def test_cleanup_idempotency():
    """Test 5: Cleanup can be called multiple times without issues"""
    writer = BronzeIcebergWriter()
    writer.execute_query = MagicMock(return_value=([], []))
    writer.ensure_schema = MagicMock()
    
    writer.cleanup_run("test_src", "run_A")
    writer.cleanup_run("test_src", "run_A")
    
    assert writer.execute_query.call_count == 2
    # Both calls exactly the same
    assert writer.execute_query.call_args_list[0][0][0] == writer.execute_query.call_args_list[1][0][0]

def test_empty_cleanup():
    """Test 6: Empty cleanup (e.g. table does not exist) does not fail"""
    writer = BronzeIcebergWriter()
    # Mocking execute_query to raise a "does not exist" error
    writer.execute_query = MagicMock(side_effect=RuntimeError("Table 'iceberg.bronze.test_src' does not exist"))
    writer.ensure_schema = MagicMock()
    
    # Should not raise an exception
    writer.cleanup_run("test_src", "run_A")

def test_invalid_run_id():
    """Test 7: Invalid run_id raises ValueError"""
    writer = BronzeIcebergWriter()
    controller = IdempotencyController()
    
    for invalid_id in [None, "", "   "]:
        with pytest.raises(ValueError, match="cannot be null"):
            writer.cleanup_run("test_src", invalid_id)
            
        with pytest.raises(ValueError, match="cannot be null"):
            controller.cleanup_failed_run(invalid_id, "test_src", bronze_writer=writer)

def test_delete_failure_propagated():
    """Test 8: DELETE failure (other than table not existing) is propagated"""
    writer = BronzeIcebergWriter()
    writer.execute_query = MagicMock(side_effect=RuntimeError("Trino network timeout"))
    writer.ensure_schema = MagicMock()
    
    with pytest.raises(RuntimeError, match="Orphan cleanup failure: could not delete data"):
        writer.cleanup_run("test_src", "run_A")

def test_controller_integration():
    """Test that IdempotencyController calls writer correctly"""
    controller = IdempotencyController()
    mock_writer = MagicMock()
    
    controller.cleanup_failed_run("run_Z", "test_src", bronze_writer=mock_writer)
    
    mock_writer.cleanup_run.assert_called_once_with(source_id="test_src", run_id="run_Z")
