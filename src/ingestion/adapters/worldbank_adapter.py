import os
import pandas as pd
from typing import Generator, Optional, Any

from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.utils.logging_config import create_ingestion_logger
from ingestion.utils.http_client import HttpClient
from ingestion.utils.error_classifier import PermanentError
from ingestion.utils.hashing import compute_file_checksum

class WorldBankAdapter(BaseSourceAdapter):
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        logger = create_ingestion_logger(config.source_id)
        if not config.endpoint:
            return ReadinessResult(ready=False, reason="Missing endpoint in configuration")
            
        try:
            client = HttpClient(config.retry)
            client.head(config.endpoint)
            return ReadinessResult(ready=True, reason="World Bank endpoint is accessible")
        except Exception as e:
            logger.error(f"Readiness check failed: {str(e)}")
            return ReadinessResult(ready=False, reason=f"Connection error: {str(e)}")

    def extract_full(self, config: SourceConfig, download_dir: str) -> Generator[DataChunk, None, None]:
        logger = create_ingestion_logger(config.source_id)
        client = HttpClient(config.retry)
        
        file_path = os.path.join(download_dir, f"{config.source_id}.xlsx")
        
        try:
            logger.info(f"Downloading Excel from {config.endpoint}")
            client.download_streaming(config.endpoint, file_path, chunk_size_bytes=1024*1024)
            
            logger.info(f"Reading Excel file: {file_path}")
            df = pd.read_excel(file_path, sheet_name='Monthly Prices', engine='openpyxl')
            
            record_count = len(df)
            
            yield DataChunk(
                chunk_id="0",
                data=df,
                row_start=0,
                row_end=record_count - 1 if record_count > 0 else 0,
                record_count=record_count,
                checksum=None
            )
            
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise PermanentError(f"Failed to extract World Bank dataset: {str(e)}")

    def extract_incremental(self, config: SourceConfig, download_dir: str, watermark: Any) -> Generator[DataChunk, None, None]:
        raise NotImplementedError()
        
    def get_artifact_checksum(self, file_path: str) -> str:
        return compute_file_checksum(file_path)
