import pytest
from unittest.mock import MagicMock, call
from ingestion.storage.metadata_repository import MetadataRepository
from ingestion.reliability.idempotency_controller import IdempotencyController

def test_deterministic_key():
    """Test 1 & 11: source_A gives same key always, no randomized hash."""
    key1 = MetadataRepository._derive_lock_key("faostat_production")
    key2 = MetadataRepository._derive_lock_key("faostat_production")
    assert key1 == key2
    assert isinstance(key1, int)
    # Check bounds for BIGINT
    assert -9223372036854775808 <= key1 <= 9223372036854775807

def test_different_source_keys():
    """Test 2: Different sources give different keys."""
    key_a = MetadataRepository._derive_lock_key("source_a")
    key_b = MetadataRepository._derive_lock_key("source_b")
    assert key_a != key_b

def test_invalid_source_id():
    """Test 3: Invalid source_id raises ValueError."""
    for invalid in [None, "", "   "]:
        with pytest.raises(ValueError, match="Concurrency control failure"):
            MetadataRepository._derive_lock_key(invalid)
        
        repo = MetadataRepository()
        with pytest.raises(ValueError, match="Concurrency control failure"):
            with repo.source_lock(invalid):
                pass

def test_successful_acquire_release():
    """Test 4: Successful acquire and release."""
    repo = MetadataRepository()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    repo._get_connection = MagicMock(return_value=mock_conn)
    
    # Simulate pg_try_advisory_lock returning True (acquired)
    mock_cursor.fetchone.return_value = (True,)
    
    with repo.source_lock("test_source"):
        pass
    
    # Assert try_advisory_lock called
    assert mock_cursor.execute.call_args_list[0] == call("SELECT pg_try_advisory_lock(%s)", (repo._derive_lock_key("test_source"),))
    # Assert unlock called
    assert mock_cursor.execute.call_args_list[1] == call("SELECT pg_advisory_unlock(%s)", (repo._derive_lock_key("test_source"),))
    # Assert connection closed
    mock_conn.close.assert_called_once()

def test_exception_releases_lock():
    """Test 5: Lock is released even if exception occurs inside the block."""
    repo = MetadataRepository()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    repo._get_connection = MagicMock(return_value=mock_conn)
    
    mock_cursor.fetchone.return_value = (True,)
    
    with pytest.raises(RuntimeError, match="Application error"):
        with repo.source_lock("test_source"):
            raise RuntimeError("Application error")
            
    assert mock_cursor.execute.call_args_list[1] == call("SELECT pg_advisory_unlock(%s)", (repo._derive_lock_key("test_source"),))
    mock_conn.close.assert_called_once()

def test_lock_contention():
    """Test 6: Lock contention results in explicit exception (fail-fast)."""
    repo = MetadataRepository()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    repo._get_connection = MagicMock(return_value=mock_conn)
    
    # Simulate lock already held
    mock_cursor.fetchone.return_value = (False,)
    
    with pytest.raises(RuntimeError, match="LockAcquisitionError"):
        with repo.source_lock("test_source"):
            pass
            
    # Should NOT try to unlock if it didn't acquire it
    assert len(mock_cursor.execute.call_args_list) == 1 
    # Connection should still close
    mock_conn.close.assert_called_once()

def test_db_connection_failure():
    """Test 9: Database connection failure propagates exception."""
    repo = MetadataRepository()
    repo._get_connection = MagicMock(side_effect=Exception("Database down"))
    
    with pytest.raises(Exception, match="Database down"):
        with repo.source_lock("test_source"):
            pass

def test_cleanup_and_ingestion_same_lock():
    """Test 8 & 10: IdempotencyController uses the metadata_repo lock."""
    repo = MetadataRepository()
    repo.source_lock = MagicMock()
    # Need to simulate the context manager yielding
    repo.source_lock.return_value.__enter__.return_value = None
    
    controller = IdempotencyController(metadata_repo=repo)
    
    mock_writer = MagicMock()
    controller.cleanup_failed_run("run_A", "test_source", mock_writer)
    
    # Assert the controller requested the lock for "test_source"
    repo.source_lock.assert_called_once_with("test_source")
    # Assert writer cleanup was called
    mock_writer.cleanup_run.assert_called_once_with(source_id="test_source", run_id="run_A")

def test_cleanup_lock_exception_handling():
    """Test 10: Release after cleanup failure.
    Since we use contextmanager, the release is guaranteed. Let's just verify propagation.
    """
    repo = MetadataRepository()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    repo._get_connection = MagicMock(return_value=mock_conn)
    
    mock_cursor.fetchone.return_value = (True,)
    
    controller = IdempotencyController(metadata_repo=repo)
    mock_writer = MagicMock()
    mock_writer.cleanup_run.side_effect = RuntimeError("Cleanup failed")
    
    with pytest.raises(RuntimeError, match="Cleanup failed"):
        controller.cleanup_failed_run("run_A", "test_source", mock_writer)
        
    assert mock_cursor.execute.call_args_list[1] == call("SELECT pg_advisory_unlock(%s)", (repo._derive_lock_key("test_source"),))
    mock_conn.close.assert_called_once()

