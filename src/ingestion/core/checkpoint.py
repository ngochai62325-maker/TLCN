from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

from ingestion.storage.metadata_repository import MetadataRepository

@dataclass
class CheckpointEntry:
    source_id: str
    run_id: str
    batch_id: str
    chunk_id: int
    chunk_key: Optional[str]
    row_start: Optional[int]
    row_end: Optional[int]
    status: str
    checksum: Optional[str]
    updated_at: Optional[datetime] = None

class CheckpointStore(ABC):
    @abstractmethod
    def save(self, source_id: str, run_id: str, batch_id: str, chunk_id: int, 
             row_start: Optional[int], row_end: Optional[int], status: str, 
             checksum: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def get_all(self, source_id: str, run_id: str) -> List[CheckpointEntry]:
        pass

    @abstractmethod
    def get_last_successful(self, source_id: str, run_id: str) -> Optional[CheckpointEntry]:
        pass

    @abstractmethod
    def clear(self, source_id: str, run_id: str) -> None:
        pass

class PostgresCheckpointStore(CheckpointStore):
    def __init__(self, repository: MetadataRepository):
        self.repository = repository

    def save(self, source_id: str, run_id: str, batch_id: str, chunk_id: int, 
             row_start: Optional[int], row_end: Optional[int], status: str, 
             checksum: Optional[str] = None) -> None:
        self.repository.save_checkpoint(
            source_id=source_id,
            run_id=run_id,
            batch_id=batch_id,
            chunk_id=chunk_id,
            row_start=row_start,
            row_end=row_end,
            status=status,
            checksum=checksum
        )

    def get_all(self, source_id: str, run_id: str) -> List[CheckpointEntry]:
        rows = self.repository.get_checkpoints(source_id, run_id)
        return [
            CheckpointEntry(
                source_id=row['source_id'],
                run_id=row['run_id'],
                batch_id=row['batch_id'],
                chunk_id=row['chunk_id'],
                chunk_key=row['chunk_key'],
                row_start=row['row_start'],
                row_end=row['row_end'],
                status=row['status'],
                checksum=row['checksum'],
                updated_at=row['updated_at']
            ) for row in rows
        ]

    def get_last_successful(self, source_id: str, run_id: str) -> Optional[CheckpointEntry]:
        row = self.repository.get_last_successful_checkpoint(source_id, run_id)
        if row:
            return CheckpointEntry(
                source_id=row['source_id'],
                run_id=row['run_id'],
                batch_id=row['batch_id'],
                chunk_id=row['chunk_id'],
                chunk_key=row['chunk_key'],
                row_start=row['row_start'],
                row_end=row['row_end'],
                status=row['status'],
                checksum=row['checksum'],
                updated_at=row['updated_at']
            )
        return None

    def clear(self, source_id: str, run_id: str) -> None:
        self.repository.clear_checkpoints(source_id, run_id)
