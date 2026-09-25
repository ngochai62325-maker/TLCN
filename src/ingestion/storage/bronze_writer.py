"""Bronze Iceberg table writer.

Responsible for writing raw extracted data chunks into queryable Apache Iceberg
tables in the Bronze layer via the Trino / Iceberg REST catalog interface.

Enforces:
1. Lossless pass-through of raw columns.
2. Injection of mandatory technical metadata audit columns:
   - _ingestion_run_id: Unique run ID of the ingestion execution
   - _ingestion_batch_id: Batch identifier for idempotency
   - _ingestion_timestamp: UTC timestamp of ingestion
   - _source_id: Source identifier (e.g. 'faostat_trade')
   - _source_file: Original filename or endpoint
   - _source_checksum: SHA-256 checksum of source artifact
   - _source_snapshot_id: Foreign key / ID to source_snapshots table
3. Append-only immutable batch semantics into Iceberg tables.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from ingestion.utils.logging_config import create_ingestion_logger


# Standard technical metadata column definitions for Bronze Iceberg tables
TECHNICAL_METADATA_COLUMNS: Dict[str, str] = {
    "_ingestion_run_id": "VARCHAR",
    "_ingestion_batch_id": "VARCHAR",
    "_ingestion_timestamp": "TIMESTAMP(6) WITH TIME ZONE",
    "_source_id": "VARCHAR",
    "_source_file": "VARCHAR",
    "_source_checksum": "VARCHAR",
    "_source_snapshot_id": "BIGINT",
}


def sanitize_column_name(col: str) -> str:
    """Sanitize column name for SQL/Iceberg compatibility.

    Replaces spaces, parentheses, slashes, and special characters with underscores.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", str(col).strip())
    cleaned = re.sub(r"_+", "_", cleaned)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"
    return cleaned.lower()


class BronzeIcebergWriter:
    """Writes and manages queryable Apache Iceberg tables in the Bronze layer."""

    def __init__(
        self,
        trino_host: Optional[str] = None,
        trino_port: Optional[int] = None,
        trino_user: str = "admin",
        catalog: str = "iceberg",
        schema: str = "bronze",
    ) -> None:
        self.host = trino_host or os.environ.get("TRINO_HOST", "localhost")
        self.port = trino_port or int(os.environ.get("TRINO_PORT", "8088"))
        self.user = trino_user
        self.catalog = catalog
        self.schema = schema
        self.base_url = f"http://{self.host}:{self.port}/v1/statement"

    def execute_query(self, sql: str) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query via Trino REST API and return (columns, rows)."""
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
        }
        resp = requests.post(self.base_url, headers=headers, data=sql.encode("utf-8"), timeout=60)
        if resp.status_code != 200 or not resp.text:
            raise RuntimeError(f"Trino query failed with HTTP {resp.status_code}: {resp.text}\nSQL: {sql[:200]}")
        res = resp.json()

        all_data: List[List[Any]] = []
        columns: List[str] = []

        while True:
            if "columns" in res and not columns:
                columns = [c["name"] for c in res["columns"]]
            if "data" in res:
                all_data.extend(res["data"])
            if "error" in res:
                error_msg = res["error"].get("message", "Unknown Trino error")
                error_code = res["error"].get("errorCode", "Unknown code")
                raise RuntimeError(f"Trino query failed ({error_code}): {error_msg}\nSQL: {sql[:300]}")
            if "nextUri" not in res:
                break
            next_resp = requests.get(res["nextUri"], headers=headers, timeout=60)
            if not next_resp.text:
                time.sleep(0.1)
                continue
            res = next_resp.json()

        return columns, all_data

    def ensure_schema(self) -> None:
        """Ensure the target catalog schema exists (e.g. iceberg.bronze)."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
        self.execute_query(sql)

    def _map_dtype_to_trino(self, dtype: Any) -> str:
        """Map pandas/numpy dtype to Trino Iceberg data type."""
        dtype_str = str(dtype).lower()
        if "int64" in dtype_str or "int32" in dtype_str:
            return "BIGINT"
        elif "int" in dtype_str:
            return "INTEGER"
        elif "float" in dtype_str or "double" in dtype_str:
            return "DOUBLE"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "datetime" in dtype_str:
            return "TIMESTAMP(6) WITH TIME ZONE"
        return "VARCHAR"

    def ensure_table(self, source_id: str, df: pd.DataFrame) -> str:
        """Ensure the Iceberg Bronze table exists with all source and audit columns.

        Returns the full table name (e.g. iceberg.bronze.faostat_trade).
        """
        self.ensure_schema()
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"

        # Build column definitions from DataFrame
        col_defs: List[str] = []
        for col in df.columns:
            clean_col = sanitize_column_name(col)
            sql_type = self._map_dtype_to_trino(df[col].dtype)
            col_defs.append(f'"{clean_col}" {sql_type}')

        # Add mandatory technical metadata columns
        for col_name, sql_type in TECHNICAL_METADATA_COLUMNS.items():
            col_defs.append(f'"{col_name}" {sql_type}')

        cols_str = ",\n    ".join(col_defs)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {cols_str}
        )
        """
        self.execute_query(create_sql)
        return full_table

    def write_chunk(
        self,
        source_id: str,
        chunk_df: pd.DataFrame,
        run_id: str,
        batch_id: str,
        source_checksum: str,
        source_snapshot_id: int = 0,
        source_file: str = "",
        batch_size: int = 1500,
    ) -> int:
        """Append a data chunk into the Bronze Iceberg table.

        Adds all 7 technical metadata columns before writing.
        Returns the number of rows inserted.
        """
        if chunk_df.empty:
            return 0

        # Copy to avoid mutating original
        df = chunk_df.copy()

        # Sanitize column names
        rename_map = {col: sanitize_column_name(col) for col in df.columns}
        df.rename(columns=rename_map, inplace=True)

        # Inject technical metadata
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        df["_ingestion_run_id"] = run_id
        df["_ingestion_batch_id"] = batch_id
        df["_ingestion_timestamp"] = now_utc
        df["_source_id"] = source_id
        df["_source_file"] = source_file
        df["_source_checksum"] = source_checksum
        df["_source_snapshot_id"] = source_snapshot_id

        full_table = self.ensure_table(source_id, chunk_df)
        cols = list(df.columns)
        quoted_cols = ", ".join(f'"{c}"' for c in cols)

        # Insert in sub-batches for Trino query size safety
        total_inserted = 0
        for start_idx in range(0, len(df), batch_size):
            sub_df = df.iloc[start_idx : start_idx + batch_size]
            val_rows: List[str] = []

            for row in sub_df.itertuples(index=False):
                vals: List[str] = []
                for val, col in zip(row, cols):
                    if pd.isna(val) or val is None:
                        vals.append("NULL")
                    elif isinstance(val, bool):
                        vals.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        vals.append(str(val))
                    elif col == "_ingestion_timestamp":
                        vals.append(f"TIMESTAMP '{val}'")
                    else:
                        escaped = str(val).replace("'", "''")
                        vals.append(f"'{escaped}'")
                val_rows.append(f"({', '.join(vals)})")

            insert_sql = f"INSERT INTO {full_table} ({quoted_cols}) VALUES\n" + ",\n".join(val_rows)
            self.execute_query(insert_sql)
            total_inserted += len(sub_df)

        return total_inserted

    def get_row_count(self, source_id: str) -> int:
        """Return total row count in the Bronze Iceberg table."""
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            _, data = self.execute_query(f"SELECT COUNT(*) FROM {full_table}")
            return int(data[0][0]) if data else 0
        except Exception:
            return 0
