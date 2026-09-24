import os
import json
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2.extras import DictCursor

class MetadataRepository:
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.environ.get(
            'INGESTION_DB_URL', 'postgresql://airflow:airflow@localhost:5432/airflow'
        )

    def _get_connection(self):
        return psycopg2.connect(self.connection_string)

    def initialize(self) -> None:
        sql_path = os.path.join(os.path.dirname(__file__), 'init_db.sql')
        with open(sql_path, 'r') as f:
            sql = f.read()
            
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    def create_run(self, run_id: str, source_id: str, load_type: str, started_at: Any) -> None:
        query = """
            INSERT INTO ingestion.ingestion_runs (run_id, source_id, load_type, started_at)
            VALUES (%s, %s, %s, %s)
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id, source_id, load_type, started_at))
            conn.commit()

    def complete_run(self, run_id: str, status: str, records_extracted: int, 
                    records_quarantined: int, chunks_processed: int, chunks_total: int, 
                    completed_at: Any, artifact_uri: Optional[str] = None, 
                    manifest_uri: Optional[str] = None, checksum: Optional[str] = None, 
                    error_message: Optional[str] = None, error_type: Optional[str] = None, 
                    source_metadata: Optional[Dict[str, Any]] = None) -> None:
        query = """
            UPDATE ingestion.ingestion_runs
            SET status = %s, records_extracted = %s, records_quarantined = %s,
                chunks_processed = %s, chunks_total = %s, completed_at = %s,
                artifact_uri = %s, manifest_uri = %s, checksum = %s,
                error_message = %s, error_type = %s, source_metadata = %s
            WHERE run_id = %s
        """
        meta_json = json.dumps(source_metadata) if source_metadata else None
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (status, records_extracted, records_quarantined,
                                  chunks_processed, chunks_total, completed_at,
                                  artifact_uri, manifest_uri, checksum,
                                  error_message, error_type, meta_json, run_id))
            conn.commit()

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.ingestion_runs WHERE run_id = %s"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (run_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def get_latest_run(self, source_id: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.ingestion_runs WHERE source_id = %s ORDER BY started_at DESC LIMIT 1"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def save_snapshot(self, source_id: str, checksum: str, size_bytes: int, 
                     retrieved_at: Any, run_id: str, status: str, 
                     source_uri: Optional[str] = None, etag: Optional[str] = None, 
                     last_modified: Optional[str] = None) -> int:
        query = """
            INSERT INTO ingestion.source_snapshots 
            (source_id, checksum, size_bytes, retrieved_at, run_id, status, source_uri, etag, last_modified)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (source_id, checksum, size_bytes, retrieved_at, run_id, status, source_uri, etag, last_modified))
                snap_id = cur.fetchone()[0]
            conn.commit()
            return snap_id

    def get_latest_snapshot(self, source_id: str, status: str = 'SUCCESS') -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.source_snapshots WHERE source_id = %s AND status = %s ORDER BY retrieved_at DESC LIMIT 1"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id, status))
                row = cur.fetchone()
                return dict(row) if row else None
                
    def find_snapshot_by_checksum(self, source_id: str, checksum: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.source_snapshots WHERE source_id = %s AND checksum = %s LIMIT 1"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id, checksum))
                row = cur.fetchone()
                return dict(row) if row else None

    def save_checkpoint(self, source_id: str, run_id: str, batch_id: str, chunk_id: int, 
                       chunk_key: Optional[str] = None, row_start: Optional[int] = None, 
                       row_end: Optional[int] = None, status: str = 'PENDING', 
                       checksum: Optional[str] = None, error_message: Optional[str] = None) -> None:
        query = """
            INSERT INTO ingestion.ingestion_checkpoints 
            (source_id, run_id, batch_id, chunk_id, chunk_key, row_start, row_end, status, checksum, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id, run_id, chunk_id) 
            DO UPDATE SET status = EXCLUDED.status, checksum = EXCLUDED.checksum, 
                          error_message = EXCLUDED.error_message, updated_at = NOW()
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (source_id, run_id, batch_id, chunk_id, chunk_key, row_start, row_end, status, checksum, error_message))
            conn.commit()

    def get_checkpoints(self, source_id: str, run_id: str) -> List[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.ingestion_checkpoints WHERE source_id = %s AND run_id = %s ORDER BY chunk_id ASC"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id, run_id))
                return [dict(row) for row in cur.fetchall()]

    def get_last_successful_checkpoint(self, source_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.ingestion_checkpoints WHERE source_id = %s AND run_id = %s AND status = 'SUCCESS' ORDER BY chunk_id DESC LIMIT 1"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id, run_id))
                row = cur.fetchone()
                return dict(row) if row else None

    def clear_checkpoints(self, source_id: str, run_id: str) -> None:
        query = "DELETE FROM ingestion.ingestion_checkpoints WHERE source_id = %s AND run_id = %s"
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (source_id, run_id))
            conn.commit()

    def get_watermark(self, source_id: str) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM ingestion.source_watermarks WHERE source_id = %s"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (source_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def set_watermark(self, source_id: str, watermark_value: str, run_id: str, batch_id: str, status: str) -> None:
        query = """
            INSERT INTO ingestion.source_watermarks (source_id, watermark_value, last_run_id, last_batch_id, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_id) 
            DO UPDATE SET watermark_value = EXCLUDED.watermark_value, last_run_id = EXCLUDED.last_run_id,
                          last_batch_id = EXCLUDED.last_batch_id, status = EXCLUDED.status, updated_at = NOW()
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (source_id, watermark_value, run_id, batch_id, status))
            conn.commit()

    def save_dead_letter(self, run_id: str, batch_id: Optional[str], source_id: str, 
                        error_type: str, error_code: str, error_message: str, stage: str, 
                        record_data: Optional[Dict[str, Any]] = None, 
                        quarantine_uri: Optional[str] = None, retryable: bool = False, 
                        chunk_id: Optional[int] = None) -> None:
        query = """
            INSERT INTO ingestion.dead_letter_records 
            (run_id, batch_id, source_id, chunk_id, error_type, error_code, error_message, stage, record_data, quarantine_uri, retryable)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        r_data = json.dumps(record_data) if record_data else None
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (run_id, batch_id, source_id, chunk_id, error_type, error_code, error_message, stage, r_data, quarantine_uri, retryable))
            conn.commit()

    def get_dead_letters(self, source_id: Optional[str] = None, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        conditions = []
        params = []
        if source_id:
            conditions.append("source_id = %s")
            params.append(source_id)
        if run_id:
            conditions.append("run_id = %s")
            params.append(run_id)
            
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT * FROM ingestion.dead_letter_records {where_clause}"
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, tuple(params))
                return [dict(row) for row in cur.fetchall()]
