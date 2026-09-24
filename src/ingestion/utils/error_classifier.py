from typing import Any, Dict, Optional
from ingestion.core.enums import ErrorType

class IngestionError(Exception):
    """Base exception for all ingestion errors."""
    pass

class TransientError(IngestionError):
    """Errors that can be retried."""
    pass

class PermanentError(IngestionError):
    """Errors that should not be retried."""
    pass

class DataQualityError(IngestionError):
    """Errors related to data quality."""
    pass

class SchemaError(IngestionError):
    """Errors related to schema mismatch."""
    pass

def classify_http_status(status_code: int) -> ErrorType:
    transient_codes = {429, 500, 502, 503, 504}
    permanent_codes = {400, 401, 403, 404}
    
    if status_code in transient_codes:
        return ErrorType.TRANSIENT
    if status_code in permanent_codes:
        return ErrorType.PERMANENT
    return ErrorType.SYSTEM

def classify_error(exception: Exception) -> ErrorType:
    if isinstance(exception, TransientError):
        return ErrorType.TRANSIENT
    if isinstance(exception, PermanentError):
        return ErrorType.PERMANENT
    if isinstance(exception, DataQualityError):
        return ErrorType.DATA_QUALITY
    if isinstance(exception, SchemaError):
        return ErrorType.SCHEMA
        
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return ErrorType.TRANSIENT
    if isinstance(exception, (FileNotFoundError, ValueError)):
        return ErrorType.PERMANENT
        
    return ErrorType.SYSTEM

def create_error_record(
    run_id: str, 
    batch_id: Optional[str], 
    source_id: str, 
    exception: Exception, 
    stage: str
) -> Dict[str, Any]:
    error_type = classify_error(exception)
    
    error_code = exception.__class__.__name__
    retryable = error_type == ErrorType.TRANSIENT
    
    return {
        "run_id": run_id,
        "batch_id": batch_id,
        "source_id": source_id,
        "error_type": error_type.name if hasattr(error_type, 'name') else str(error_type),
        "error_code": error_code,
        "error_message": str(exception),
        "stage": stage,
        "retryable": retryable
    }
