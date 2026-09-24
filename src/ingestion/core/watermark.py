from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime

from ingestion.storage.metadata_repository import MetadataRepository

@dataclass
class WatermarkEntry:
    source_id: str
    watermark_value: str
    last_run_id: str
    last_batch_id: str
    status: str
    updated_at: Optional[datetime] = None

class WatermarkStore(ABC):
    @abstractmethod
    def get(self, source_id: str) -> Optional[WatermarkEntry]:
        pass

    @abstractmethod
    def set(self, source_id: str, watermark_value: str, run_id: str, batch_id: str, status: str) -> None:
        pass

class PostgresWatermarkStore(WatermarkStore):
    def __init__(self, repository: MetadataRepository):
        self.repository = repository

    def get(self, source_id: str) -> Optional[WatermarkEntry]:
        data = self.repository.get_watermark(source_id)
        if data:
            return WatermarkEntry(
                source_id=data['source_id'],
                watermark_value=data['watermark_value'],
                last_run_id=data['last_run_id'],
                last_batch_id=data['last_batch_id'],
                status=data['status'],
                updated_at=data['updated_at']
            )
        return None

    def set(self, source_id: str, watermark_value: str, run_id: str, batch_id: str, status: str) -> None:
        self.repository.set_watermark(source_id, watermark_value, run_id, batch_id, status)
