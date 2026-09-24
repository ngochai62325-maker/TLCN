import os
import zipfile
import pandas as pd
from typing import Generator, Optional, Any
from pathlib import Path

from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.utils.logging_config import create_ingestion_logger
from ingestion.utils.http_client import HttpClient
from ingestion.utils.error_classifier import PermanentError, DataQualityError
from ingestion.utils.hashing import compute_file_checksum

class UsdaPsdAdapter(BaseSourceAdapter):
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        logger = create_ingestion_logger(config.source_id)
        if not config.endpoint:
            return ReadinessResult(ready=False, reason="Missing endpoint in configuration")
            
        try:
            client = HttpClient(config.retry)
            client.head(config.endpoint)
            return ReadinessResult(ready=True, reason="USDA endpoint is accessible")
        except Exception as e:
            logger.error(f"Readiness check failed: {str(e)}")
            return ReadinessResult(ready=False, reason=f"Connection error: {str(e)}")

    def extract_full(self, config: SourceConfig, download_dir: str) -> Generator[DataChunk, None, None]:
        logger = create_ingestion_logger(config.source_id)
        client = HttpClient(config.retry)
        
        zip_path = os.path.join(download_dir, f"{config.source_id}_bulk.zip")
        
        try:
            logger.info(f"Downloading ZIP from {config.endpoint}")
            client.download_streaming(config.endpoint, zip_path, chunk_size_bytes=1024*1024)
            
            logger.info("Extracting ZIP contents")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_files = [f for f in zf.infolist() if f.filename.endswith('.csv')]
                if not csv_files:
                    raise DataQualityError("No CSV file found in downloaded ZIP")
                
                largest_csv = max(csv_files, key=lambda x: x.file_size)
                zf.extract(largest_csv, download_dir)
                csv_file_path = os.path.join(download_dir, largest_csv.filename)
                
            logger.info(f"Reading CSV file in chunks: {csv_file_path}")
            encoding = config.encoding or 'utf-8'
            chunk_size = config.chunk_size or 100000
            
            chunk_id = 0
            row_start = 0
            
            for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, encoding=encoding, low_memory=False):
                record_count = len(chunk)
                
                if 'Commodity_Description' in chunk.columns:
                    commodities = chunk['Commodity_Description'].unique()
                    logger.info(f"Chunk {chunk_id} commodities: {', '.join([str(c) for c in commodities[:5]])}...")
                
                row_end = row_start + record_count - 1
                
                yield DataChunk(
                    chunk_id=str(chunk_id),
                    data=chunk,
                    row_start=row_start,
                    row_end=row_end,
                    record_count=record_count,
                    checksum=None
                )
                
                chunk_id += 1
                row_start += record_count
                
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise PermanentError(f"Failed to extract full dataset: {str(e)}")

    def extract_incremental(self, config: SourceConfig, download_dir: str, watermark: Any) -> Generator[DataChunk, None, None]:
        raise NotImplementedError()
        
    def get_artifact_checksum(self, file_path: str) -> str:
        return compute_file_checksum(file_path)


class UsdaLocalAdapter(BaseSourceAdapter):
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        if not config.local_path or not os.path.exists(config.local_path):
            return ReadinessResult(ready=False, reason=f"Local path {config.local_path} does not exist")
        if os.path.getsize(config.local_path) == 0:
            return ReadinessResult(ready=False, reason="File is empty")
        return ReadinessResult(ready=True, reason="Local file is ready", source_metadata={'size': os.path.getsize(config.local_path)})

    def extract_full(self, config: SourceConfig, download_dir: str) -> Generator[DataChunk, None, None]:
        logger = create_ingestion_logger(config.source_id)
        
        try:
            encoding = config.encoding or 'utf-8'
            chunk_size = config.chunk_size or 50000
            
            chunk_id = 0
            row_start = 0
            
            for chunk in pd.read_csv(config.local_path, chunksize=chunk_size, encoding=encoding, low_memory=False):
                record_count = len(chunk)
                row_end = row_start + record_count - 1
                
                yield DataChunk(
                    chunk_id=str(chunk_id),
                    data=chunk,
                    row_start=row_start,
                    row_end=row_end,
                    record_count=record_count,
                    checksum=None
                )
                
                chunk_id += 1
                row_start += record_count
        except Exception as e:
            logger.error(f"Error reading local file: {str(e)}")
            raise PermanentError(f"Failed to read local file: {str(e)}")

    def extract_incremental(self, config: SourceConfig, download_dir: str, watermark: Any) -> Generator[DataChunk, None, None]:
        raise NotImplementedError()
        
    def get_artifact_checksum(self, file_path: str) -> str:
        return compute_file_checksum(file_path)
