import os
import glob
import pandas as pd
from typing import Generator, Optional, Any

from ingestion.core.config import SourceConfig
from ingestion.core.result import DataChunk, ReadinessResult
from ingestion.adapters.base_adapter import BaseSourceAdapter
from ingestion.utils.logging_config import create_ingestion_logger
from ingestion.utils.error_classifier import PermanentError
from ingestion.utils.hashing import compute_file_checksum

class NsoVietnamAdapter(BaseSourceAdapter):
    def check_readiness(self, config: SourceConfig) -> ReadinessResult:
        logger = create_ingestion_logger(config.source_id)
        if not config.local_path or not os.path.exists(config.local_path):
            return ReadinessResult(ready=False, reason=f"Local path {config.local_path} does not exist")
            
        files = glob.glob(os.path.join(config.local_path, "V06.*.csv"))
        if not files:
            return ReadinessResult(ready=False, reason="No V06.*.csv files found in directory")
            
        return ReadinessResult(
            ready=True, 
            reason=f"Found {len(files)} files", 
            source_metadata={'file_count': len(files)}
        )

    def extract_full(self, config: SourceConfig, download_dir: str) -> Generator[DataChunk, None, None]:
        logger = create_ingestion_logger(config.source_id)
        
        try:
            files = sorted(glob.glob(os.path.join(config.local_path, "V06.*.csv")))
            
            chunk_id = 0
            row_start = 0
            
            encodings_to_try = ['utf-8', 'utf-16', 'cp1252']
            
            for file_path in files:
                filename = os.path.basename(file_path)
                df = None
                last_err = None
                
                for enc in encodings_to_try:
                    try:
                        df = pd.read_csv(file_path, encoding=enc)
                        break
                    except Exception as e:
                        last_err = e
                        continue
                
                if df is None:
                    logger.error(f"Failed to read {filename} with all encodings.")
                    raise PermanentError(f"Encoding error reading {filename}: {str(last_err)}")
                
                df['_source_file'] = filename
                
                record_count = len(df)
                row_end = row_start + record_count - 1
                
                yield DataChunk(
                    chunk_id=str(chunk_id),
                    data=df,
                    row_start=row_start,
                    row_end=row_end,
                    record_count=record_count,
                    checksum=None
                )
                
                chunk_id += 1
                row_start += record_count
                
        except Exception as e:
            logger.error(f"Error reading local files: {str(e)}")
            raise PermanentError(f"Failed to read NSO dataset: {str(e)}")

    def extract_incremental(self, config: SourceConfig, download_dir: str, watermark: Any) -> Generator[DataChunk, None, None]:
        raise NotImplementedError()
        
    def get_artifact_checksum(self, file_path: str) -> str:
        return compute_file_checksum(file_path)
