"""Bronze Iceberg table writer.

Responsible for writing raw extracted data chunks into queryable Apache Iceberg
tables in the Bronze layer via native Iceberg Parquet append commit (PyIceberg)
with exact catalog readback and no ambiguous-commit SQL fallback.

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
4. High-performance bulk write: 1 Parquet data file + 1 atomic snapshot commit per chunk.
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
from ingestion.utils.error_classifier import SchemaError

# Backward compatibility patch: pyiceberg 0.12+ passes 'store_decimal_as_integer'
# to pyarrow.parquet.ParquetWriter, which is unsupported in pyarrow < 18.0.
try:
    import inspect
    import pyarrow.parquet as _pq
    import pyarrow.types as _pat
    if "store_decimal_as_integer" not in inspect.signature(_pq.ParquetWriter.__init__).parameters:
        _orig_pq_init = _pq.ParquetWriter.__init__
        def _compat_pq_init(self, *args, **kwargs):
            kwargs.pop("store_decimal_as_integer", None)
            return _orig_pq_init(self, *args, **kwargs)
        _pq.ParquetWriter.__init__ = _compat_pq_init
    if not hasattr(_pat, "is_string_view"):
        _pat.is_string_view = lambda t: False
    if not hasattr(_pat, "is_binary_view"):
        _pat.is_binary_view = lambda t: False
except Exception:
    pass


# Standard technical metadata column definitions for Bronze Iceberg tables
TECHNICAL_METADATA_COLUMNS: Dict[str, str] = {
    "_ingestion_run_id": "VARCHAR",
    "_ingestion_batch_id": "VARCHAR",
    "_ingestion_chunk_id": "BIGINT",
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
        iceberg_rest_uri: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_region: str = "us-east-1",
        schema_policy: str = "fail_fast",
        approved_schema_additions: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        if schema_policy not in ("fail_fast", "approved_evolution"):
            raise ValueError("schema_policy must be fail_fast or approved_evolution")
        self.schema_policy = schema_policy
        self.approved_schema_additions = approved_schema_additions or {}
        self.host = trino_host or os.environ.get("TRINO_HOST", "localhost")
        self.port = trino_port or int(os.environ.get("TRINO_PORT", "8088"))
        self.user = trino_user
        self.catalog = catalog
        self.schema = schema
        self.base_url = f"http://{self.host}:{self.port}/v1/statement"

        # Resolve Iceberg REST URI and MinIO S3 parameters
        rest_host = os.environ.get("ICEBERG_REST_HOST")
        if not rest_host:
            rest_host = "iceberg-rest" if self.host == "trino" else "localhost"
        rest_port = os.environ.get("ICEBERG_REST_PORT", "8181")
        self.rest_uri = iceberg_rest_uri or os.environ.get("ICEBERG_REST_URI", f"http://{rest_host}:{rest_port}")

        raw_endpoint = minio_endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        if not raw_endpoint.startswith("http://") and not raw_endpoint.startswith("https://"):
            raw_endpoint = f"http://{raw_endpoint}"
        self.minio_endpoint = raw_endpoint
        self.s3_access_key = minio_access_key or os.environ.get("MINIO_ROOT_USER", "admin")
        self.s3_secret_key = minio_secret_key or os.environ.get("MINIO_ROOT_PASSWORD", "password123")
        self.s3_region = minio_region
        self._iceberg_catalog = None

    def get_iceberg_catalog(self) -> Any:
        """Get or lazily initialize the PyIceberg REST catalog."""
        if self._iceberg_catalog is None:
            try:
                from pyiceberg.catalog import load_catalog
                self._iceberg_catalog = load_catalog(
                    self.catalog,
                    **{
                        "type": "rest",
                        "uri": self.rest_uri,
                        "s3.endpoint": self.minio_endpoint,
                        "s3.access-key-id": self.s3_access_key,
                        "s3.secret-access-key": self.s3_secret_key,
                        "s3.path-style-access": "true",
                        "s3.region": self.s3_region,
                    }
                )
            except Exception as e:
                logger = create_ingestion_logger(f"{self.catalog}.{self.schema}")
                logger.warning(f"Could not initialize PyIceberg REST catalog: {e}")
                return None
        return self._iceberg_catalog

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
        catalog = self.get_iceberg_catalog()
        if catalog is not None:
            try:
                catalog.create_namespace_if_not_exists(self.schema)
            except Exception:
                pass
        try:
            sql = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
            self.execute_query(sql)
        except Exception:
            pass

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
        incoming = [sanitize_column_name(c) for c in df.columns]
        self._validate_schema_columns(incoming, incoming)
        if any(not c for c in incoming):
            raise SchemaError("Empty sanitized source column; refusing write")
        self.ensure_schema()
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"

        # Fast path: If table already exists in Iceberg catalog, skip redundant DDL
        catalog = self.get_iceberg_catalog()
        if catalog is not None:
            try:
                if catalog.table_exists(f"{self.schema}.{table_name}"):
                    table = catalog.load_table(f"{self.schema}.{table_name}")
                    additions = self.plan_schema_additions(source_id, incoming, table.schema())
                    if additions:
                        from pyiceberg.types import StringType
                        # One metadata commit for all approved nullable additions.
                        # Never changes existing field IDs/types or drops fields.
                        with table.update_schema() as update:
                            for name in additions:
                                update.add_column(name, StringType(), required=False)
                    return full_table
            except SchemaError:
                raise
            except Exception:
                if self.schema_policy == "approved_evolution":
                    raise  # No partial ALTER/fallback after an evolution failure.
                pass

        # Build column definitions from DataFrame
        col_defs: List[str] = []
        df_cols_map: Dict[str, str] = {}
        for col in df.columns:
            clean_col = sanitize_column_name(col)
            if clean_col in TECHNICAL_METADATA_COLUMNS:
                continue
            sql_type = self._map_dtype_to_trino(df[col].dtype)
            df_cols_map[clean_col] = sql_type
            col_defs.append(f'"{clean_col}" {sql_type}')

        # Add mandatory technical metadata columns
        for col_name, sql_type in TECHNICAL_METADATA_COLUMNS.items():
            col_defs.append(f'"{col_name}" {sql_type}')

        cols_str = ",\n    ".join(col_defs)
        # The Trino fallback must obey the same lossless gate as PyIceberg.
        # Existing schema changes require an approved migration, not a best-
        # effort ALTER hidden inside a source write.
        try:
            _, existing_schema = self.execute_query(f"DESCRIBE {full_table}")
        except RuntimeError as exc:
            if "does not exist" not in str(exc).lower() and "not found" not in str(exc).lower():
                raise
        else:
            missing = set([*df_cols_map, *TECHNICAL_METADATA_COLUMNS]) - {str(row[0]) for row in existing_schema}
            if missing and self.schema_policy == "approved_evolution":
                raise SchemaError("Approved evolution requires the native Iceberg transaction path; no Trino ALTER fallback")
            self._validate_schema_columns(
                [*df_cols_map, *TECHNICAL_METADATA_COLUMNS],
                [str(row[0]) for row in existing_schema], full_table)
            return full_table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {cols_str}
        )
        """
        # Execute CREATE TABLE with retry and backoff to handle SQLite busy lock in Iceberg REST catalog
        max_retries = 6
        for attempt in range(max_retries):
            try:
                self.execute_query(create_sql)
                break
            except Exception as create_err:
                if attempt == max_retries - 1:
                    raise
                import random
                time.sleep(0.5 * (2 ** attempt) + random.uniform(0.1, 0.4))

        # Recheck after CREATE to detect races; never silently ALTER or ignore drift.
        _, existing_cols_data = self.execute_query(f"DESCRIBE {full_table}")
        self._validate_schema_columns([*df_cols_map, *TECHNICAL_METADATA_COLUMNS],
                                      [str(row[0]) for row in existing_cols_data], full_table)
        return full_table

    def plan_schema_additions(self, source_id: str, incoming, iceberg_schema: Any) -> List[str]:
        """Pure fail-fast/allowlist gate, reusable inside a recovery transaction."""
        self._validate_schema_columns(incoming, incoming)
        existing = {field.name: field for field in iceberg_schema.fields}
        missing = sorted(set(incoming) - set(existing))
        missing_metadata = set(TECHNICAL_METADATA_COLUMNS) - set(existing)
        if missing_metadata:
            raise SchemaError(f"Missing technical metadata: {sorted(missing_metadata)}")
        approved = self.approved_schema_additions.get(source_id, {})
        if self.schema_policy == "approved_evolution":
            for name, dtype in approved.items():
                if dtype != "string" or name in TECHNICAL_METADATA_COLUMNS or sanitize_column_name(name) != name:
                    raise SchemaError("Evolution permits only explicitly approved nullable source strings")
                if name in existing and str(existing[name].field_type) != "string":
                    raise SchemaError(f"Unsafe type change requested for {name}")
        if missing and (self.schema_policy != "approved_evolution" or set(missing) - set(approved)):
            raise SchemaError(f"{source_id}: schema lacks incoming columns {missing}; approved evolution/replay required")
        return missing

    @staticmethod
    def _validate_numeric_values(df: pd.DataFrame, schema_types: Dict[str, str]) -> None:
        """Reject unrepresentable raw values before commit, never coerce to NULL."""
        for name, dtype in schema_types.items():
            if name not in df:
                continue
            kind = dtype.upper()
            integer = any(t in kind for t in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT"))
            numeric = integer or any(t in kind for t in ("DOUBLE", "REAL", "FLOAT", "DECIMAL"))
            if not numeric:
                continue
            try:
                values = pd.to_numeric(df[name].dropna(), errors="raise")
                if integer:
                    bits = 64 if "BIGINT" in kind else 16 if "SMALLINT" in kind else 8 if "TINYINT" in kind else 32
                    if ((values % 1 != 0) | (values < -(2 ** (bits - 1))) | (values >= 2 ** (bits - 1))).any():
                        raise ValueError("fractional or out-of-range integer")
            except (ValueError, TypeError, OverflowError) as exc:
                raise SchemaError(f"{name}: raw values do not fit {dtype}; refusing lossy coercion") from exc

    @staticmethod
    def _validate_schema_columns(incoming, existing, table_name="Bronze"):
        if len(incoming) != len(set(incoming)):
            raise SchemaError(f"{table_name}: column sanitization collision; refusing lossy write")
        missing = sorted(set(incoming) - set(existing))
        if missing:
            raise SchemaError(f"{table_name}: schema lacks incoming columns {missing}; approved evolution/replay required")

    def _dataframe_to_arrow(self, df: pd.DataFrame, iceberg_schema: Any) -> Any:
        """Align pandas DataFrame to Iceberg table schema and convert to PyArrow Table."""
        self._validate_schema_columns(list(df.columns), [field.name for field in iceberg_schema.fields])
        import pyarrow as pa
        from pyiceberg.types import (
            BooleanType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            StringType,
            TimestampType,
            TimestamptzType,
            DateType,
        )

        def iceberg_type_to_arrow(field_type):
            if isinstance(field_type, BooleanType):
                return pa.bool_()
            elif isinstance(field_type, IntegerType):
                return pa.int32()
            elif isinstance(field_type, LongType):
                return pa.int64()
            elif isinstance(field_type, FloatType):
                return pa.float32()
            elif isinstance(field_type, DoubleType):
                return pa.float64()
            elif isinstance(field_type, (TimestampType, TimestamptzType)):
                return pa.timestamp("us", tz="UTC")
            elif isinstance(field_type, DateType):
                return pa.date32()
            else:
                return pa.string()

        clean_df = df.copy()
        arrow_fields = []

        for f in iceberg_schema.fields:
            col = f.name
            target_arrow_type = iceberg_type_to_arrow(f.field_type)
            arrow_fields.append((col, target_arrow_type))

            if col not in clean_df.columns:
                clean_df[col] = None

            if isinstance(f.field_type, LongType):
                clean_df[col] = pd.to_numeric(clean_df[col], errors="raise").astype("Int64")
            elif isinstance(f.field_type, IntegerType):
                clean_df[col] = pd.to_numeric(clean_df[col], errors="raise").astype("Int32")
            elif isinstance(f.field_type, DoubleType):
                clean_df[col] = pd.to_numeric(clean_df[col], errors="raise").astype("float64")
            elif isinstance(f.field_type, FloatType):
                clean_df[col] = pd.to_numeric(clean_df[col], errors="raise").astype("float32")
            elif isinstance(f.field_type, (TimestampType, TimestamptzType)):
                clean_df[col] = pd.to_datetime(clean_df[col], utc=True, format="mixed")
            elif isinstance(f.field_type, BooleanType):
                clean_df[col] = clean_df[col].astype("boolean")
            elif isinstance(f.field_type, DateType):
                clean_df[col] = pd.to_datetime(clean_df[col]).dt.date
            elif isinstance(f.field_type, StringType):
                clean_df[col] = clean_df[col].where(clean_df[col].notna(), None).astype("string")

        pa_schema = pa.schema(arrow_fields)
        cols_ordered = [f.name for f in iceberg_schema.fields]
        return pa.Table.from_pandas(clean_df[cols_ordered], schema=pa_schema, preserve_index=False)

    def write_chunk(
        self,
        source_id: str,
        chunk_df: pd.DataFrame,
        run_id: str,
        batch_id: str,
        source_checksum: str,
        chunk_id: int,
        source_snapshot_id: Optional[int] = None,
        source_file: str = "",
        batch_size: int = 500,
    ) -> int:
        """Append a data chunk into the Bronze Iceberg table.

        Adds all technical metadata columns before writing.
        Uses high-performance PyIceberg bulk write (1 Parquet file + 1 atomic commit per chunk)
        with exact readback; unavailable native writers fail closed.
        Returns the number of rows inserted.
        """
        if chunk_df.empty:
            return 0

        # Validate Identity
        if not run_id:
            raise ValueError("Idempotency failure: _ingestion_run_id is missing or empty.")
        if chunk_id is None:
            raise ValueError("Idempotency failure: _ingestion_chunk_id is missing.")
        if not isinstance(chunk_id, int):
            try:
                chunk_id = int(chunk_id)
            except (ValueError, TypeError):
                raise TypeError(f"Idempotency failure: _ingestion_chunk_id must be an integer, got {type(chunk_id).__name__}")
        if chunk_id < 0:
            raise ValueError(f"Idempotency failure: _ingestion_chunk_id cannot be negative, got {chunk_id}")

        logger = create_ingestion_logger(source_id, run_id, batch_id)

        # Copy to avoid mutating original
        df = chunk_df.copy()

        # Preserve per-file _source_file or _source_checksum if already set by adapter
        existing_source_file = None
        if "_source_file" in df.columns:
            existing_source_file = df["_source_file"].copy()
            df.drop(columns=["_source_file"], inplace=True)

        existing_source_checksum = None
        if "_source_checksum" in df.columns:
            existing_source_checksum = df["_source_checksum"].copy()
            df.drop(columns=["_source_checksum"], inplace=True)

        # Sanitize column names
        rename_map = {col: sanitize_column_name(col) for col in df.columns}
        df.rename(columns=rename_map, inplace=True)
        if len(df.columns) != len(set(df.columns)):
            raise SchemaError("Bronze column sanitization collision; refusing lossy write")

        # Inject technical metadata
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        df["_ingestion_run_id"] = run_id
        df["_ingestion_batch_id"] = batch_id
        df["_ingestion_chunk_id"] = chunk_id
        df["_ingestion_timestamp"] = now_utc
        df["_source_id"] = source_id

        if existing_source_file is not None:
            df["_source_file"] = existing_source_file
        else:
            df["_source_file"] = source_file

        if existing_source_checksum is not None:
            df["_source_checksum"] = existing_source_checksum
        else:
            df["_source_checksum"] = source_checksum

        df["_source_snapshot_id"] = source_snapshot_id

        full_table = self.ensure_table(source_id, chunk_df)
        table_name = sanitize_column_name(source_id)
        # Validate before any commit. No implicit schema repair here.
        _, schema_rows = self.execute_query(f"DESCRIBE {full_table}")
        self._validate_schema_columns(list(df.columns), [str(row[0]) for row in schema_rows], full_table)
        self._validate_numeric_values(df, {str(row[0]): str(row[1]) for row in schema_rows})
        catalog = self.get_iceberg_catalog()
        arrow_table = None
        if catalog is not None:
            # Conversion must succeed before the commit protocol starts.
            table = catalog.load_table(f"{self.schema}.{table_name}")
            try:
                arrow_table = self._dataframe_to_arrow(df, table.schema())
            except (ValueError, TypeError, OverflowError) as exc:
                raise SchemaError("Arrow conversion failed before write; raw values preserved") from exc

        if catalog is None:
            raise RuntimeError("Native Iceberg writer unavailable; refusing unsafe multi-commit SQL fallback")
        return self._commit_chunk(catalog, f"{self.schema}.{table_name}", arrow_table, run_id, chunk_id)

    @staticmethod
    def _same_chunk(actual, expected):
        # Compare complete values and multiplicity, not a digest or aggregate.
        from collections import Counter
        import json
        columns = [c for c in expected.column_names if c != "_ingestion_timestamp"]
        if not set(columns).issubset(actual.column_names):
            return False
        if not actual.select(columns).schema.equals(expected.select(columns).schema, check_metadata=False):
            return False
        def cells(table):
            return Counter(json.dumps(row, sort_keys=True, default=str, allow_nan=False)
                           for row in table.select(columns).to_pylist())
        return cells(actual) == cells(expected)

    def _commit_chunk(self, catalog, name, proposed, run_id, chunk_id):
        """One append, immutable retry identity, fresh exact readback.

        Caller MUST hold the source advisory lock (engine protocol). No external
        writer may bypass it. This is atomic per chunk, not per FULL source.
        """
        condition = "_ingestion_run_id = '" + run_id.replace("'", "''") + f"' AND _ingestion_chunk_id = {chunk_id}"
        def read():
            fresh = catalog.load_table(name)
            snapshot = fresh.current_snapshot()
            rows = fresh.scan(snapshot_id=snapshot.snapshot_id if snapshot else None,
                              row_filter=condition).to_arrow()
            return fresh, rows
        table, existing = read()
        if existing.num_rows:
            if not self._same_chunk(existing, proposed):
                raise RuntimeError("RECOVERY_REQUIRED: existing immutable chunk differs (partial write, checksum or content changed)")
            return proposed.num_rows
        try:
            table.append(proposed)
        except Exception as exc:
            # A lost response can follow a successful catalog commit. Never INSERT
            # or DELETE after this exception; verify using a newly loaded table.
            try:
                _, actual = read()
                if self._same_chunk(actual, proposed):
                    return proposed.num_rows
            except Exception as read_error:
                raise RuntimeError("UNKNOWN_COMMIT: catalog readback unavailable; no automatic retry") from read_error
            raise RuntimeError("UNKNOWN_COMMIT: append not exactly verified; no fallback write") from exc
        try:
            _, actual = read()
        except Exception as exc:
            raise RuntimeError("UNKNOWN_COMMIT: catalog readback unavailable; checkpoint forbidden") from exc
        if not self._same_chunk(actual, proposed):
            raise RuntimeError("UNKNOWN_COMMIT: append readback mismatch; checkpoint forbidden")
        return proposed.num_rows

    def verify_run(self, source_id: str, run_id: str, expected_records: int) -> Dict[str, Any]:
        """Read physical run membership; caller holds the source lock."""
        import pyarrow.compute as pc
        catalog = self.get_iceberg_catalog()
        if catalog is None:
            raise RuntimeError("Physical Bronze commit evidence unavailable")
        table = catalog.load_table(f"{self.schema}.{sanitize_column_name(source_id)}")
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise RuntimeError("No physical Bronze snapshot")
        rows = table.scan(snapshot_id=snapshot.snapshot_id, row_filter="_ingestion_run_id = '" + run_id.replace("'", "''") + "'").select("_source_id").to_arrow()
        if rows.num_rows != expected_records or set(pc.unique(rows["_source_id"]).to_pylist()) != {source_id}:
            raise RuntimeError("Physical Bronze run row/source reconciliation failed")
        return {"uuid": str(table.metadata.table_uuid), "snapshot_id": str(snapshot.snapshot_id),
                "metadata_location": table.metadata_location, "run_id": run_id, "rows": rows.num_rows}

    def get_row_count(self, source_id: str) -> int:
        """Return total row count in the Bronze Iceberg table."""
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            _, data = self.execute_query(f"SELECT COUNT(*) FROM {full_table}")
            return int(data[0][0]) if data else 0
        except Exception:
            return 0

    def get_ingested_files(self, source_id: str) -> List[str]:
        """Return list of distinct _source_file names already in the Bronze table."""
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            _, data = self.execute_query(f'SELECT DISTINCT "_source_file" FROM {full_table}')
            return [str(row[0]) for row in data if row and row[0] is not None]
        except Exception:
            return []

    def get_ingested_checksums(self, source_id: str) -> List[str]:
        """Return list of distinct _source_checksum strings already in the Bronze table."""
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            _, data = self.execute_query(f'SELECT DISTINCT "_source_checksum" FROM {full_table}')
            return [str(row[0]) for row in data if row and row[0] is not None]
        except Exception:
            return []

    def cleanup_run(self, source_id: str, run_id: str) -> None:
        """Clean up all chunks associated with a specific ingestion run.

        Used for orphan cleanup of failed runs to ensure they leave no garbage behind.
        Executes an idempotent DELETE WHERE _ingestion_run_id = run_id.
        Raises an exception if the run_id is invalid or if the database deletion fails.
        """
        if not run_id or not str(run_id).strip():
            raise ValueError("Orphan cleanup failure: _ingestion_run_id cannot be null, empty, or whitespace.")

        # Ensure the schema exists so we can safely form the table name
        self.ensure_schema()
        table_name = sanitize_column_name(source_id)
        full_table = f"{self.catalog}.{self.schema}.{table_name}"

        # Note: We do not call self.ensure_table here because if the table doesn't exist,
        # there is no data to clean up. We can just execute the DELETE and let Trino raise
        # an error if the table is missing, which we can catch and ignore.

        delete_sql = (
            f"DELETE FROM {full_table} "
            f"WHERE _ingestion_run_id = '{run_id}'"
        )
        
        logger = create_ingestion_logger(source_id, run_id)
        logger.info(f"Starting orphan cleanup for run {run_id} in {full_table}")

        try:
            self.execute_query(delete_sql)
            logger.info("Successfully executed orphan cleanup DELETE", run_id=run_id, table=full_table)
        except Exception as e:
            # If the table simply doesn't exist, it means there are no orphans.
            # Trino returns an error like "Table 'iceberg.bronze.table_name' does not exist"
            error_str = str(e).lower()
            if "does not exist" in error_str or "not found" in error_str:
                logger.info("Table does not exist; no orphan cleanup needed.", run_id=run_id, table=full_table)
                return
            
            logger.error("Failed to execute orphan cleanup DELETE", error=str(e))
            raise RuntimeError(f"Orphan cleanup failure: could not delete data for run {run_id}: {e}") from e
