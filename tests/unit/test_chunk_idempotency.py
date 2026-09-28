import pytest
import pandas as pd
from unittest.mock import MagicMock
from ingestion.storage.bronze_writer import BronzeIcebergWriter

@pytest.fixture
def chunk_df():
    return pd.DataFrame({
        "business_key": [1, 2, 3],
        "data_value": ["A", "B", "C"]
    })

@pytest.fixture
def chunk_df_alt():
    return pd.DataFrame({
        "business_key": [1, 2, 3],
        "data_value": ["X", "Y", "Z"]
    })

def test_first_write_empty_delete(chunk_df):
    """Test 1 & 6: First write executes an empty delete."""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.bronze.test_src")
    writer.execute_query = MagicMock(return_value=([], []))
    writer._dataframe_to_arrow = MagicMock()
    
    catalog_mock = MagicMock()
    table_mock = MagicMock()
    catalog_mock.load_table.return_value = table_mock
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    rows = writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_1",
        batch_id="batch_1",
        source_checksum="chk",
        chunk_id=0
    )
    
    assert rows == 3
    # Check that DELETE was executed
    writer.execute_query.assert_any_call(
        "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_1' AND _ingestion_chunk_id = 0"
    )
    # Check that append was executed
    table_mock.append.assert_called_once()

def test_retry_same_chunk(chunk_df):
    """Test 2 & 5: Retry same chunk (same run, same chunk_id, different data)."""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.bronze.test_src")
    writer.execute_query = MagicMock(return_value=([], []))
    writer._dataframe_to_arrow = MagicMock()
    
    catalog_mock = MagicMock()
    table_mock = MagicMock()
    catalog_mock.load_table.return_value = table_mock
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    # Write A
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_1",
        batch_id="batch_1",
        source_checksum="chk",
        chunk_id=3
    )
    
    # Write B
    chunk_df_alt = pd.DataFrame({"business_key": [4], "data_value": ["D"]})
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df_alt,
        run_id="run_1",
        batch_id="batch_1",
        source_checksum="chk",
        chunk_id=3
    )
    
    # DELETE should be called twice with the same predicate
    delete_sql = "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_1' AND _ingestion_chunk_id = 3"
    assert writer.execute_query.call_args_list[0][0][0] == delete_sql
    assert writer.execute_query.call_args_list[1][0][0] == delete_sql
    
    # Append should be called twice
    assert table_mock.append.call_count == 2
    
def test_different_chunks(chunk_df):
    """Test 3: Different chunks have distinct predicates."""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.bronze.test_src")
    writer.execute_query = MagicMock(return_value=([], []))
    writer._dataframe_to_arrow = MagicMock()
    
    catalog_mock = MagicMock()
    table_mock = MagicMock()
    catalog_mock.load_table.return_value = table_mock
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="run_1", batch_id="batch_1", source_checksum="chk", chunk_id=1)
    writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="run_1", batch_id="batch_1", source_checksum="chk", chunk_id=2)
    
    delete_1 = "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_1' AND _ingestion_chunk_id = 1"
    delete_2 = "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_1' AND _ingestion_chunk_id = 2"
    
    writer.execute_query.assert_any_call(delete_1)
    writer.execute_query.assert_any_call(delete_2)

def test_different_runs(chunk_df):
    """Test 4: Different runs have distinct predicates."""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.bronze.test_src")
    writer.execute_query = MagicMock(return_value=([], []))
    writer._dataframe_to_arrow = MagicMock()
    
    catalog_mock = MagicMock()
    table_mock = MagicMock()
    catalog_mock.load_table.return_value = table_mock
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="run_A", batch_id="batch_1", source_checksum="chk", chunk_id=0)
    writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="run_B", batch_id="batch_1", source_checksum="chk", chunk_id=0)
    
    delete_A = "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_A' AND _ingestion_chunk_id = 0"
    delete_B = "DELETE FROM iceberg.bronze.test_src WHERE _ingestion_run_id = 'run_B' AND _ingestion_chunk_id = 0"
    
    writer.execute_query.assert_any_call(delete_A)
    writer.execute_query.assert_any_call(delete_B)

def test_invalid_identity(chunk_df):
    """Test 7: Fail fast on invalid identities."""
    writer = BronzeIcebergWriter()
    
    with pytest.raises(ValueError, match="is missing or empty"):
        writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="", batch_id="batch_1", source_checksum="chk", chunk_id=0)
        
    with pytest.raises(TypeError, match="must be an integer"):
        writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="r1", batch_id="b1", source_checksum="chk", chunk_id="abc")
        
    with pytest.raises(ValueError, match="cannot be negative"):
        writer.write_chunk(source_id="test_src", chunk_df=chunk_df, run_id="r1", batch_id="b1", source_checksum="chk", chunk_id=-1)

def test_writer_failure_delete_rollback(chunk_df):
    """Test 8: Writer failure injects a RuntimeError if DELETE fails."""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.bronze.test_src")
    writer.execute_query = MagicMock(side_effect=Exception("Trino network error"))
    
    with pytest.raises(RuntimeError, match="Idempotency failure: could not delete existing chunk"):
        writer.write_chunk(
            source_id="test_src",
            chunk_df=chunk_df,
            run_id="run_1",
            batch_id="batch_1",
            source_checksum="chk",
            chunk_id=0
        )
