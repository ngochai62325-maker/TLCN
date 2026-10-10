import pytest
from unittest.mock import MagicMock, patch
from ingestion.storage.metadata_repository import MetadataRepository

def test_get_pending_silver_run_logic():
    """Test the get_pending_silver_run query behavior for missing, false, and true silver_status values."""
    repo = MetadataRepository()
    
    # We will mock _get_connection and cursor
    with patch.object(repo, '_get_connection') as mock_conn:
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test 1: Standard call
        repo.get_pending_silver_run("test_source")
        
        # Verify query structure
        called_query = mock_cursor.execute.call_args[0][0]
        called_args = mock_cursor.execute.call_args[0][1]
        
        assert "WHERE source_id = %s" in called_query
        assert "status = 'SUCCESS'" in called_query
        assert "source_metadata->>'silver_status' IS NULL" in called_query
        assert "NOT IN ('SUCCESS', 'true')" in called_query
        assert called_args == ("test_source",)

def test_mark_silver_status_logic():
    """Test that mark_silver_status uses correct jsonb_build_object injection."""
    repo = MetadataRepository()
    
    with patch.object(repo, '_get_connection') as mock_conn:
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        repo.mark_silver_status("run_123", "SUCCESS")
        
        called_query = mock_cursor.execute.call_args[0][0]
        called_args = mock_cursor.execute.call_args[0][1]
        
        assert "jsonb_build_object('silver_status', %s)" in called_query
        assert called_args == ("SUCCESS", "run_123")
