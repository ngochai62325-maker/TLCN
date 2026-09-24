import json
from datetime import datetime, timezone
import os

from ingestion.core.enums import IngestionStatus

class IngestionManifest:
    def __init__(self, source_id: str, run_id: str, batch_id: str, load_type: str):
        self.pipeline_version = "1.0.0"
        self.created_at = datetime.now(timezone.utc).isoformat()
        
        self.source_id = source_id
        self.run_id = run_id
        self.batch_id = batch_id
        self.load_type = load_type
        
        self.artifact_uri = None
        self.artifact_format = None
        self.artifact_size_bytes = None
        self.artifact_checksum = None
        
        self.source_provider = None
        self.source_dataset = None
        self.source_uri = None
        self.source_last_modified = None
        self.source_etag = None
        self.source_content_length = None
        
        self.records_extracted = 0
        self.chunks_processed = 0
        self.records_quarantined = 0
        
        self.started_at = None
        self.completed_at = None
        
        self.watermark_before = None
        self.watermark_after = None
        
        self.status = IngestionStatus.NOT_READY.name
        self.error_message = None
        self.error_type = None

    def set_artifact_info(self, uri: str, format: str, size_bytes: int, checksum: str) -> None:
        self.artifact_uri = uri
        self.artifact_format = format
        self.artifact_size_bytes = size_bytes
        self.artifact_checksum = checksum

    def set_source_metadata(self, provider: str, dataset: str, source_uri: str, last_modified: str = None, etag: str = None, content_length: int = None) -> None:
        self.source_provider = provider
        self.source_dataset = dataset
        self.source_uri = source_uri
        self.source_last_modified = last_modified
        self.source_etag = etag
        self.source_content_length = content_length

    def set_metrics(self, records_extracted: int, chunks_processed: int, records_quarantined: int = 0) -> None:
        self.records_extracted = records_extracted
        self.chunks_processed = chunks_processed
        self.records_quarantined = records_quarantined

    def set_timing(self, started_at: str, completed_at: str) -> None:
        self.started_at = started_at
        self.completed_at = completed_at

    def set_watermark(self, before: str, after: str) -> None:
        self.watermark_before = before
        self.watermark_after = after

    def set_status(self, status: IngestionStatus) -> None:
        self.status = status.name if isinstance(status, IngestionStatus) else status

    def set_error(self, error_message: str, error_type: str) -> None:
        self.error_message = error_message
        self.error_type = error_type

    def to_dict(self) -> dict:
        return {
            "pipeline_version": self.pipeline_version,
            "created_at": self.created_at,
            "source_id": self.source_id,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "load_type": self.load_type,
            "artifact_info": {
                "uri": self.artifact_uri,
                "format": self.artifact_format,
                "size_bytes": self.artifact_size_bytes,
                "checksum": self.artifact_checksum
            },
            "source_metadata": {
                "provider": self.source_provider,
                "dataset": self.source_dataset,
                "uri": self.source_uri,
                "last_modified": self.source_last_modified,
                "etag": self.source_etag,
                "content_length": self.source_content_length
            },
            "metrics": {
                "records_extracted": self.records_extracted,
                "chunks_processed": self.chunks_processed,
                "records_quarantined": self.records_quarantined
            },
            "timing": {
                "started_at": self.started_at,
                "completed_at": self.completed_at
            },
            "watermark": {
                "before": self.watermark_before,
                "after": self.watermark_after
            },
            "status": self.status,
            "error": {
                "message": self.error_message,
                "type": self.error_type
            }
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json())

    def save_to_storage(self, storage, bucket: str, key: str) -> str:
        data = self.to_dict()
        return storage.upload_json(data, bucket, key)
