import time
import random
from functools import wraps
from typing import Callable, Any, Optional

from ingestion.core.config import RetryConfig
from ingestion.utils.error_classifier import classify_error, TransientError, PermanentError
from ingestion.core.enums import ErrorType

def retry_with_backoff(func: Callable, retry_config: RetryConfig, logger: Optional[Any] = None) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        attempts = 0
        while attempts <= retry_config.max_attempts:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempts += 1
                
                # Check if we should respect Retry-After header
                retry_after = getattr(e, 'retry_after', None)
                
                error_type = classify_error(e)
                is_transient = error_type == ErrorType.TRANSIENT or isinstance(e, TransientError)
                is_permanent = error_type == ErrorType.PERMANENT or isinstance(e, PermanentError)
                
                if is_permanent or not is_transient or attempts > retry_config.max_attempts:
                    if logger:
                        logger.error(f"Permanent error or max retries reached. Raising. attempt={attempts} max={retry_config.max_attempts}")
                    raise e
                
                if retry_after is not None and retry_config.respect_retry_after:
                    delay = float(retry_after)
                else:
                    if retry_config.exponential_backoff:
                        delay = min(retry_config.initial_delay_seconds * (2 ** (attempts - 1)), retry_config.max_delay_seconds)
                    else:
                        delay = min(retry_config.initial_delay_seconds, retry_config.max_delay_seconds)
                        
                    if retry_config.jitter:
                        delay = random.uniform(0, delay)
                
                if logger:
                    logger.warning(f"Retry attempt {attempts}/{retry_config.max_attempts} in {delay:.2f}s after error: {str(e)}")
                
                time.sleep(delay)
                
        raise RuntimeError("Should not be reached")
    
    return wrapper

def retryable(retry_config: RetryConfig):
    def decorator(func: Callable) -> Callable:
        return retry_with_backoff(func, retry_config)
    return decorator
