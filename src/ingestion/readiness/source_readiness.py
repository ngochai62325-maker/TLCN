import os

from ingestion.core.enums import SourceType
from ingestion.core.config import SourceConfig
from ingestion.core.result import ReadinessResult

class ReadinessChecker:
    def __init__(self):
        pass

    def check(self, config: SourceConfig) -> ReadinessResult:
        try:
            if config.source_type == SourceType.LOCAL_FILE:
                return self._check_local_file(config)
            else:
                return ReadinessResult(ready=False, reason=f"Unsupported source type: {config.source_type}")
        except Exception as e:
            return ReadinessResult(ready=False, reason=f"Check failed: {str(e)}")

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
