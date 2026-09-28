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

def test_chunk_metadata_0(chunk_df):
    """Test 1: chunk_id = 0 -> _ingestion_chunk_id == 0"""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock()
    
    catalog_mock = MagicMock()
    table_mock = MagicMock()
    catalog_mock.load_table.return_value = table_mock
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    def mock_to_arrow(df, schema):
        assert "_ingestion_chunk_id" in df.columns
        assert df["_ingestion_chunk_id"].iloc[0] == 0
        return MagicMock()
        
    writer._dataframe_to_arrow = mock_to_arrow
    
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_1",
        batch_id="batch_1",
        source_checksum="chk",
        chunk_id=0
    )

def test_chunk_metadata_5(chunk_df):
    """Test 2: chunk_id = 5 -> _ingestion_chunk_id == 5"""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock(return_value=([], []))
    catalog_mock = MagicMock()
    catalog_mock.load_table.return_value = MagicMock()
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    def mock_to_arrow(df, schema):
        assert "_ingestion_chunk_id" in df.columns
        assert df["_ingestion_chunk_id"].iloc[0] == 5
        return MagicMock()
        
    writer._dataframe_to_arrow = mock_to_arrow
    
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_1",
        batch_id="batch_1",
        source_checksum="chk",
        chunk_id=5
    )

def test_different_chunks(chunk_df):
    """Test 3: Different chunks"""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock(return_value=([], []))
    catalog_mock = MagicMock()
    catalog_mock.load_table.return_value = MagicMock()
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    captured_chunks = []
    
    def mock_to_arrow(df, schema):
        captured_chunks.append(df["_ingestion_chunk_id"].iloc[0])
        return MagicMock()
        
    writer._dataframe_to_arrow = mock_to_arrow
    
    for i in range(3):
        writer.write_chunk(
            source_id="test_src",
            chunk_df=chunk_df,
            run_id="run_1",
            batch_id="batch_1",
            source_checksum="chk",
            chunk_id=i
        )
        
    assert captured_chunks == [0, 1, 2]

def test_same_retry(chunk_df):
    """Test 4: Same retry"""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock(return_value=([], []))
    catalog_mock = MagicMock()
    catalog_mock.load_table.return_value = MagicMock()
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    captured_chunks = []
    
    def mock_to_arrow(df, schema):
        captured_chunks.append(df["_ingestion_chunk_id"].iloc[0])
        return MagicMock()
        
    writer._dataframe_to_arrow = mock_to_arrow
    
    # Run 1
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_retry",
        batch_id="batch_retry",
        source_checksum="chk",
        chunk_id=2
    )
    
    # Run 2 (retry)
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df,
        run_id="run_retry",
        batch_id="batch_retry",
        source_checksum="chk",
        chunk_id=2
    )
    
    assert captured_chunks == [2, 2]
    
def test_data_contract_test(chunk_df):
    """Test 16: Ensure data contract test"""
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock()
    writer.execute_query = MagicMock(return_value=([], []))
    catalog_mock = MagicMock()
    catalog_mock.load_table.return_value = MagicMock()
    writer.get_iceberg_catalog = MagicMock(return_value=catalog_mock)
    
    # We will simulate existing ingestion metadata to make sure it's not lost
    chunk_df_with_meta = chunk_df.copy()
    chunk_df_with_meta["_source_file"] = "custom_file.csv"
    
    def mock_to_arrow(df, schema):
        # Must have all required columns
        expected_meta = [
            "_ingestion_run_id",
            "_ingestion_batch_id",
            "_ingestion_chunk_id",
            "_ingestion_timestamp",
            "_source_id",
            "_source_file",
            "_source_checksum"
        ]
        for col in expected_meta:
            assert col in df.columns
            
        assert df["_source_file"].iloc[0] == "custom_file.csv"
        assert df["_ingestion_chunk_id"].iloc[0] == 99
        return MagicMock()
        
    writer._dataframe_to_arrow = mock_to_arrow
    
    writer.write_chunk(
        source_id="test_src",
        chunk_df=chunk_df_with_meta,
        run_id="run_contract",
        batch_id="batch_contract",
        source_checksum="chk_contract",
        chunk_id=99
    )
