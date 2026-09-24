import os
import pandas as pd
from typing import Generator, Optional, Any

from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.utils.logging_config import create_ingestion_logger
from ingestion.utils.error_classifier import PermanentError
from ingestion.utils.hashing import compute_file_checksum

class ThitruongNongsanAdapter(BaseSourceAdapter):
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        if not config.local_path or not os.path.exists(config.local_path):
            return ReadinessResult(ready=False, reason=f"Local path {config.local_path} does not exist")
        if os.path.getsize(config.local_path) == 0:
            return ReadinessResult(ready=False, reason="File is empty")
            
        return ReadinessResult(
            ready=True, 
            reason="Local Excel file is ready", 
            source_metadata={'size': os.path.getsize(config.local_path)}
        )

    def extract_full(self, config: SourceConfig, download_dir: str) -> Generator[DataChunk, None, None]:
        logger = create_ingestion_logger(config.source_id)
        
        try:
            logger.info(f"Reading Excel file: {config.local_path}")
            
            sheets_dict = pd.read_excel(config.local_path, sheet_name=None, engine='openpyxl')
            
            all_dfs = []
            for sheet_name, df in sheets_dict.items():
                df['_sheet_name'] = sheet_name
                all_dfs.append(df)
                
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            record_count = len(combined_df)
            
            yield DataChunk(
                chunk_id="0",
                data=combined_df,
                row_start=0,
                row_end=record_count - 1 if record_count > 0 else 0,
                record_count=record_count,
                checksum=None
            )
            
        except Exception as e:
            logger.error(f"Error reading local file: {str(e)}")
            raise PermanentError(f"Failed to read ThitruongNongsan dataset: {str(e)}")

    def extract_incremental(self, config: SourceConfig, download_dir: str, watermark: Any) -> Generator[DataChunk, None, None]:
        raise NotImplementedError()
        
    def get_artifact_checksum(self, file_path: str) -> str:
        return compute_file_checksum(file_path)
