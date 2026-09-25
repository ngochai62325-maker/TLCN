import os
from typing import Any, Dict

from ingestion.core.enums import SourceType
from ingestion.core.config import SourceConfig
from ingestion.core.result import ReadinessResult
from ingestion.utils.http_client import HttpClient

class ReadinessChecker:
    def __init__(self, http_client: HttpClient = None):
        self.http_client = http_client or HttpClient()

    def check(self, config: SourceConfig) -> ReadinessResult:
        try:
            if config.source_type in [SourceType.HTTP_BULK_ZIP, SourceType.HTTP_FILE]:
                return self._check_http(config)
            elif config.source_type == SourceType.LOCAL_FILE:
                return self._check_local_file(config)
            elif config.source_type == SourceType.API:
                return self._check_api(config)
            else:
                return ReadinessResult(ready=False, reason=f"Unsupported source type: {config.source_type}")
        except Exception as e:
            return ReadinessResult(ready=False, reason=f"Check failed: {str(e)}")

    def _check_http(self, config: SourceConfig) -> ReadinessResult:
        if not config.endpoint:
            return ReadinessResult(ready=False, reason="Missing endpoint")
            
        try:
            response = self.http_client.head(config.endpoint)
            readiness_cfg = config.readiness
            
            if readiness_cfg:
                if readiness_cfg.require_http_200 and response.status_code != 200:
                    return ReadinessResult(ready=False, reason=f"Expected HTTP 200, got {response.status_code}")
                
                content_length = response.headers.get("Content-Length")
                if readiness_cfg.require_content_length and content_length is None:
                    return ReadinessResult(ready=False, reason="Missing Content-Length header")
                    
                if content_length is not None and readiness_cfg.min_file_size_bytes is not None:
                    if int(content_length) < readiness_cfg.min_file_size_bytes:
                         return ReadinessResult(ready=False, reason=f"Content-Length {content_length} < {readiness_cfg.min_file_size_bytes}")
                         
                if readiness_cfg.expected_content_type:
                    content_type = response.headers.get("Content-Type", "")
                    if readiness_cfg.expected_content_type not in content_type:
                        return ReadinessResult(ready=False, reason=f"Expected Content-Type {readiness_cfg.expected_content_type}, got {content_type}")
            
            metadata = {
                "size": response.headers.get("Content-Length"),
                "content_type": response.headers.get("Content-Type"),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified")
            }
            return ReadinessResult(ready=True, reason="HTTP readiness check passed", source_metadata=metadata)
        except Exception as e:
             return ReadinessResult(ready=False, reason=f"HTTP request failed: {str(e)}")

    def _check_local_file(self, config: SourceConfig) -> ReadinessResult:
        path = config.local_path or config.local_fallback
        if not path:
            return ReadinessResult(ready=False, reason="Missing local_path")

        if not os.path.exists(path):
            return ReadinessResult(ready=False, reason=f"File not found: {path}")

        if not os.access(path, os.R_OK):
            return ReadinessResult(ready=False, reason=f"File not readable: {path}")

        if os.path.isdir(path):
            files = [f for f in os.listdir(path) if not f.startswith(".")]
            if not files:
                return ReadinessResult(ready=False, reason=f"Directory is empty: {path}")
            size = sum(os.path.getsize(os.path.join(path, f)) for f in files if os.path.isfile(os.path.join(path, f)))
            file_count = len(files)
        else:
            size = os.path.getsize(path)
            file_count = 1

        if size == 0:
            return ReadinessResult(ready=False, reason=f"File is empty (0 bytes): {path}")

        if config.readiness and config.readiness.min_file_size_bytes is not None:
            if size < config.readiness.min_file_size_bytes:
                return ReadinessResult(ready=False, reason=f"File size {size} < {config.readiness.min_file_size_bytes}")

        metadata = {
            "size": size,
            "path": path,
            "file_count": file_count,
            "last_modified": str(os.path.getmtime(path)),
        }
        return ReadinessResult(ready=True, reason="Local file readiness check passed", source_metadata=metadata)

    def _check_api(self, config: SourceConfig) -> ReadinessResult:
        if not config.endpoint:
             return ReadinessResult(ready=False, reason="Missing endpoint")
        try:
            response = self.http_client.get(config.endpoint, params={"limit": 1})
            if config.readiness and config.readiness.require_http_200 and response.status_code != 200:
                 return ReadinessResult(ready=False, reason=f"Expected HTTP 200, got {response.status_code}")
            return ReadinessResult(ready=True, reason="API readiness check passed", source_metadata={})
        except Exception as e:
            return ReadinessResult(ready=False, reason=f"API request failed: {str(e)}")
