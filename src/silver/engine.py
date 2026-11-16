"""Silver Layer Engine — Contracts, Spark Session, Base Transformer, Quality, Quarantine & Merge.

Consolidates what used to live in `silver/contract/`, `silver/core/`, and `silver/engine/`:
1. Contract Loader (`SilverContract`, `SilverContractLoader`): YAML contract parsing & validation.
2. Spark Session Factory (`get_spark_session`): Iceberg + MinIO S3A SparkSession builder.
3. Base Transformer (`BaseSilverTransformer`): Abstract interface for source-specific transforms.
4. Quality Engine (`SilverQualityEngine`, `ERROR_TYPE`, `empty_errors`): Vectorized SQL DQ rules.
5. Quarantine Manager (`SilverQuarantineManager`): Idempotent routing to `dead_letters`.
6. Merge Engine (`IcebergMergeEngine`, `quote`, `table_identifier`): xxhash64 dedup & Iceberg MERGE INTO.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import reduce
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional
import uuid
import yaml

logger = logging.getLogger("silver.engine")
ERROR_TYPE = "array<struct<rule_id:string,error_message:string,failed_column:string>>"


# ---------------------------------------------------------------------------
# 1. Silver Contract Loader (`contract/loader.py`)
# ---------------------------------------------------------------------------

class SilverContract:
    def __init__(self, data: Dict[str, Any]):
        self._data = data
        self._validate()

    def _validate(self):
        if not isinstance(self._data, dict):
            raise ValueError("Contract data must be a valid dictionary. Empty or malformed YAML is not allowed.")

        dataset = self._data.get("dataset")
        if not isinstance(dataset, dict):
            raise ValueError("Contract is missing required field: dataset (must be a dictionary)")

        if not dataset.get("name"):
            raise ValueError("Contract is missing required field: dataset.name")
        if not dataset.get("source_system"):
            raise ValueError("Contract is missing required field: dataset.source_system")
        if not dataset.get("bronze_input"):
            raise ValueError("Contract is missing required field: dataset.bronze_input")
        if not dataset.get("silver_output"):
            raise ValueError("Contract is missing required field: dataset.silver_output")

        grain = self._data.get("grain")
        if not isinstance(grain, dict):
            raise ValueError("Contract is missing required field: grain (must be a dictionary)")

        business_key = grain.get("business_key")
        if not isinstance(business_key, list) or len(business_key) == 0:
            raise ValueError("Contract is missing required field: grain.business_key (must be a non-empty list)")

        schema = self._data.get("schema")
        if not isinstance(schema, dict):
            raise ValueError("Contract is missing required field: schema (must be a dictionary)")

        columns = schema.get("columns")
        if not isinstance(columns, list) or len(columns) == 0:
            raise ValueError("Contract is missing required field: schema.columns (must be a non-empty list)")

        for col in columns:
            if not isinstance(col, dict):
                raise ValueError(f"Contract column definition must be a dictionary: {col}")
            if not col.get("name") or not col.get("data_type"):
                raise ValueError(f"Contract column definition missing name or data_type: {col}")

    @property
    def dataset_name(self) -> str:
        return self._data.get("dataset", {}).get("name", "")

    @property
    def source_system(self) -> str:
        return self._data.get("dataset", {}).get("source_system", "")

    @property
    def source_id(self) -> str:
        return self._data.get("dataset", {}).get("source_id", "")

    @property
    def bronze_input(self) -> str:
        return self._data.get("dataset", {}).get("bronze_input", "")

    @property
    def silver_output(self) -> str:
        return self._data.get("dataset", {}).get("silver_output", "")

    @property
    def business_key(self) -> List[str]:
        return self._data.get("grain", {}).get("business_key", [])

    @property
    def columns(self) -> List[Dict[str, Any]]:
        return self._data.get("schema", {}).get("columns", [])

    @property
    def data_quality_rules(self) -> List[Dict[str, Any]]:
        return self._data.get("data_quality", [])

    @property
    def deduplication_key(self) -> List[str]:
        return self._data.get("deduplication", {}).get("key", [])

    @property
    def deduplication_strategy(self) -> str:
        return self._data.get("deduplication", {}).get("strategy", "")

    @property
    def incremental_strategy(self) -> str:
        return self._data.get("incremental", {}).get("strategy", "")

    @property
    def quarantine_required_fields(self) -> List[str]:
        return self._data.get("quarantine", {}).get("required_fields", [])

    @property
    def status(self):
        return self._data.get("contract_status", "DRAFT")

    @property
    def partition_columns(self):
        return self._data.get("storage", {}).get("partition_columns", [])

    def validate_executable(self):
        """Reject unprofiled contracts and invalid SQL identifiers before reading."""
        if self.status != "READY":
            raise ValueError(f"{self.dataset_name}: contract is {self.status}; profiling/complete Bronze schema required")
        identifier = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        for table in (self.bronze_input, self.silver_output):
            if table == "UNKNOWN" or not all(identifier.fullmatch(p) for p in table.split(".")):
                raise ValueError(f"Invalid table identifier: {table}")
        names = [c["name"] for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate contract column names")
        for column in self.columns:
            if not identifier.fullmatch(column["name"]) or column["name"].startswith("_"):
                raise ValueError(f"Invalid or reserved column name: {column['name']}")
            data_type = column["data_type"].lower()
            if data_type not in {"string", "integer", "int", "long", "bigint", "double", "float", "boolean", "date", "timestamp"}:
                match = re.fullmatch(r"decimal\((\d+),(\d+)\)", data_type)
                if not match or not (0 <= int(match[2]) <= int(match[1]) <= 38 and int(match[1]) > 0):
                    raise ValueError(f"Invalid contract data_type: {data_type}")
        for key in self.business_key + self.partition_columns:
            if key not in names:
                raise ValueError(f"Key/partition column absent from schema: {key}")
        if self.deduplication_key and self.deduplication_key != self.business_key:
            raise ValueError("Deduplication key must match the MERGE business key")
        for rule in self.data_quality_rules:
            if not rule.get("sql_expr"):
                raise ValueError(f"Missing sql_expr: {rule.get('rule_id')}")
            if rule.get("action", "QUARANTINE") not in {"QUARANTINE", "LOG", "KEEP"}:
                raise ValueError(f"Unsupported DQ action: {rule.get('action')}")


class SilverContractLoader:
    def __init__(self, contract_dir: str = None):
        self.contract_dir = str(contract_dir or Path(__file__).resolve().parents[2] / "contracts/silver")

    def load_contract(self, dataset_id: str) -> SilverContract:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", dataset_id):
            raise ValueError(f"Invalid dataset_id: {dataset_id}")
        file_path = os.path.join(self.contract_dir, f"{dataset_id}.yaml")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Silver contract not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return SilverContract(data)


# ---------------------------------------------------------------------------
# 2. Spark Session Factory (`core/spark_session.py`)
# ---------------------------------------------------------------------------

def get_spark_session(app_name: str = "RiceLakehouseSilver") -> SparkSession:
    """Create or retrieve active SparkSession configured for Iceberg REST Catalog and MinIO."""
    from pyspark.sql import SparkSession
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    minio_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
    iceberg_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")

    if not minio_endpoint.startswith("http://") and not minio_endpoint.startswith("https://"):
        s3_endpoint = f"http://{minio_endpoint}"
    else:
        s3_endpoint = minio_endpoint

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SILVER_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", iceberg_uri)
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# 3. Base Transformer (`core/base_transformer.py`)
# ---------------------------------------------------------------------------

class BaseSilverTransformer(ABC):
    """Abstract base class for all Silver transformations."""

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark_session(self.__class__.__name__)

    @property
    @abstractmethod
    def source_table(self) -> str:
        pass

    @property
    @abstractmethod
    def target_table(self) -> str:
        pass

    @property
    @abstractmethod
    def business_keys(self) -> List[str]:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    def read_bronze(
        self,
        mode: str = "full",
        watermark: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> DataFrame:
        """Read data from Bronze Iceberg table with optional incremental filters."""
        from pyspark.sql import functions as F
        df = self.spark.table(self.source_table)

        if mode == "incremental":
            if watermark:
                logger.info(f"Filtering {self.source_table} with watermark > {watermark}")
                df = df.filter(F.col("_ingestion_timestamp") > F.lit(watermark))
            elif run_id:
                logger.info(f"Filtering {self.source_table} with _ingestion_run_id = {run_id}")
                df = df.filter(F.col("_ingestion_run_id") == F.lit(run_id))

        return df

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Deduplicate records based on business keys, keeping most recent _ingestion_timestamp."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        if "_ingestion_timestamp" in df.columns:
            order_expr = F.col("_ingestion_timestamp").desc_nulls_last()
        elif "_source_snapshot_id" in df.columns:
            order_expr = F.col("_source_snapshot_id").desc_nulls_last()
        elif "_ingestion_run_id" in df.columns:
            order_expr = F.col("_ingestion_run_id").desc_nulls_last()
        else:
            order_expr = F.lit(1)

        window_spec = Window.partitionBy([F.col(k) for k in self.business_keys]).orderBy(order_expr)

        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def write_silver(
        self,
        df: DataFrame,
        mode: str = "merge",
    ) -> Dict[str, Any]:
        """Write transformed DataFrame to Silver Iceberg table with idempotency guarantee."""
        from pyspark.sql import functions as F
        now_utc = datetime.now(timezone.utc)
        df_to_write = df.withColumn("_silver_processed_at", F.lit(now_utc))

        record_count = df_to_write.count()
        logger.info(f"Writing {record_count} records to {self.target_table} in mode '{mode}'")

        if record_count == 0:
            return {"status": "SUCCESS", "records_written": 0, "target_table": self.target_table}

        temp_view = f"temp_silver_{abs(hash(self.target_table))}"
        df_to_write.createOrReplaceTempView(temp_view)

        join_conditions = " AND ".join([f"target.{k} = source.{k}" for k in self.business_keys])

        merge_sql = f"""
        MERGE INTO {self.target_table} AS target
        USING {temp_view} AS source
        ON {join_conditions}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """

        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(temp_view)

        logger.info(f"Successfully merged {record_count} records into {self.target_table}")
        return {
            "status": "SUCCESS",
            "records_written": record_count,
            "target_table": self.target_table,
            "processed_at": now_utc.isoformat(),
        }

    def execute(
        self,
        mode: str = "full",
        watermark: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run the complete Silver pipeline via SilverTransformationFramework."""
        from silver.framework import SilverTransformationFramework
        contract_dir = Path(__file__).resolve().parents[2] / "contracts/silver"
        dataset_id = self.source_table.rsplit(".", 1)[-1]
        return SilverTransformationFramework(self.spark, str(contract_dir)).run(
            dataset_id, str(uuid.uuid4()), mode=mode, run_id=run_id, watermark=watermark)


# ---------------------------------------------------------------------------
# 4. Quality Engine (`engine/quality.py`)
# ---------------------------------------------------------------------------

def empty_errors():
    from pyspark.sql import functions as F
    return F.array().cast(ERROR_TYPE)


class SilverQualityEngine:
    def __init__(self, dq_rules):
        self.rules = dq_rules

    def evaluate(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        errors, warnings = [], []
        for rule in self.rules:
            if not rule.get("sql_expr"):
                raise ValueError(f"Unsupported DQ rule: {rule.get('rule_id')}. Missing explicit sql_expr translation.")
            action = rule.get("action", "QUARANTINE").upper()
            if action not in {"QUARANTINE", "LOG", "KEEP"}:
                raise ValueError(f"Unsupported DQ action: {action}")
            passed = F.coalesce(F.expr(rule["sql_expr"]), F.lit(False))
            error = F.when(~passed, F.struct(
                F.lit(rule.get("rule_id", "UNKNOWN")).alias("rule_id"),
                F.lit(rule.get("rule", "Rule failed")).alias("error_message"),
                F.lit(rule.get("failed_column", "unknown")).alias("failed_column")))
            (errors if action == "QUARANTINE" else warnings).append(error)

        def compact(items):
            return F.filter(F.array(*items).cast(ERROR_TYPE), lambda x: x.isNotNull()) if items else empty_errors()

        system = F.coalesce(F.col("_sys_cast_errors"), empty_errors()) if "_sys_cast_errors" in df.columns else empty_errors()
        return (df.withColumn("dq_errors", F.concat(compact(errors), system))
                .withColumn("_dq_warnings", compact(warnings)).drop("_sys_cast_errors"))

    def apply_rules(self, df: DataFrame):
        checked = self.evaluate(df)
        return (checked.filter(F.size("dq_errors") == 0).drop("dq_errors"),
                checked.filter(F.size("dq_errors") > 0))


# ---------------------------------------------------------------------------
# 5. Merge & Quarantine Engines (`engine/merge.py`, `engine/quarantine.py`)
# ---------------------------------------------------------------------------

def quote(name):
    return "`" + name.replace("`", "``") + "`"


def table_identifier(name):
    return ".".join(quote(part) for part in name.split("."))


class IcebergMergeEngine:
    def __init__(self, spark):
        self.spark = spark

    def deduplicate(self, df, business_keys, order_cols=None):
        if not business_keys:
            raise ValueError("Deduplication requires business keys")
        from pyspark.sql import DataFrame, functions as F
        from pyspark.sql.window import Window
        if type(df) is DataFrame:
            bk_h = F.xxhash64(*[F.col(quote(k)) for k in business_keys])
            if df.select(bk_h.alias("_h")).groupBy("_h").count().where("count > 1").limit(1).isEmpty():
                return df
        order_cols = order_cols if order_cols is not None else ["_ingestion_timestamp", "_source_file"]
        order = [F.col(c).desc_nulls_last() for c in order_cols if c in df.columns]
        skip = set(business_keys) | set(order_cols) | {"_silver_processed_at"}
        for field in df.schema:
            if field.name not in skip:
                value = F.col(field.name)
                if field.dataType.typeName() in {"array", "map", "struct"}:
                    value = F.to_json(value)
                order.append(value.asc_nulls_first())
        window = Window.partitionBy(*business_keys).orderBy(*order)
        return df.withColumn("_row_num", F.row_number().over(window)).filter(F.col("_row_num") == 1).drop("_row_num")

    def ensure_table(self, df, target_table, partition_columns=None):
        if self._table_exists(target_table):
            existing = self.spark.table(target_table).schema
            incoming = {f.name: f.dataType for f in df.schema}
            stored = {f.name: f.dataType for f in existing}
            for name, dtype in incoming.items():
                if name in stored and stored[name] != dtype:
                    raise ValueError(f"Silver schema mismatch {target_table}.{name}: {stored[name]} != {dtype}; explicit migration required")
                if name not in stored:
                    raise ValueError(f"Silver schema mismatch {target_table}: missing column {name}; explicit migration required")
            return
        from pyspark.sql import functions as F
        parts = target_table.split(".")
        if len(parts) > 2 or (len(parts) == 2 and not self.spark.conf.get(f"spark.sql.catalog.{parts[0]}", None)):
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table_identifier('.'.join(parts[:-1]))}")
        writer = (df.limit(0).writeTo(target_table).using("iceberg")
                  .tableProperty("format-version", "2")
                  .tableProperty("write.parquet.compression-codec", "zstd")
                  .tableProperty("write.distribution-mode", "hash")
                  .tableProperty("write.merge.distribution-mode", "hash")
                  .tableProperty("write.spark.fanout.enabled", "false"))
        if partition_columns:
            writer = writer.partitionedBy(*[F.col(c) for c in partition_columns])
        writer.create()

    def merge(self, df, target_table, business_keys, partition_columns=None):
        from pyspark.sql import DataFrame, functions as F
        if not business_keys or not set(business_keys).issubset(df.columns):
            raise ValueError("MERGE requires existing business keys")
        self.ensure_table(df, target_table, partition_columns)
        if type(df) is DataFrame:
            cmp_cols = [F.col(quote(c)) for c in df.columns if c != "_silver_processed_at"]
            h_expr = F.xxhash64(*cmp_cols)
            l_h = df.select(h_expr.alias("_h")).groupBy("_h").count().withColumnRenamed("count", "l_cnt")
            r_h = self.spark.table(target_table).select(*df.columns).select(h_expr.alias("_h")).groupBy("_h").count().withColumnRenamed("count", "r_cnt")
            if l_h.join(r_h, "_h", "full_outer").where("coalesce(l_cnt, 0) <> coalesce(r_cnt, 0)").limit(1).isEmpty():
                return
        if df.isEmpty():
            return
        incoming = df.alias("s")
        stored = self.spark.table(target_table).select(*df.columns).alias("t")
        key_match = reduce(lambda a, b: a & b, [F.col(f"s.{quote(k)}") == F.col(f"t.{quote(k)}") for k in business_keys])
        differences = reduce(lambda a, b: a | b, [~F.col(f"s.{quote(c)}").eqNullSafe(F.col(f"t.{quote(c)}"))
                           for c in df.columns if c != "_silver_processed_at"])
        fresh = F.lit(True)
        if "_ingestion_timestamp" in df.columns:
            fresh = F.col("t._ingestion_timestamp").isNull() | (F.col("s._ingestion_timestamp") >= F.col("t._ingestion_timestamp"))
        changed = incoming.join(stored, key_match, "left").filter(
            F.col(f"t.{quote(business_keys[0])}").isNull() | (fresh & differences))
        if changed.limit(1).isEmpty():
            return
        view = "incoming_updates_" + uuid.uuid4().hex
        df.createOrReplaceTempView(view)
        keys = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in business_keys)
        freshness = "true"
        if "_ingestion_timestamp" in df.columns:
            stamp = quote("_ingestion_timestamp")
            freshness = f"(t.{stamp} IS NULL OR s.{stamp} >= t.{stamp})"
        changed_cols = [c for c in df.columns if c != "_silver_processed_at"]
        differences_sql = " OR ".join(f"NOT (t.{quote(c)} <=> s.{quote(c)})" for c in changed_cols)
        target_columns = self.spark.table(target_table).columns
        updates = ", ".join(f"t.{quote(c)} = {'s' if c in df.columns else 't'}.{quote(c)}" for c in target_columns)
        columns = ", ".join(quote(c) for c in df.columns)
        values = ", ".join(f"s.{quote(c)}" for c in df.columns)
        try:
            self.spark.sql(f"""
                MERGE INTO {table_identifier(target_table)} t USING {quote(view)} s ON {keys}
                WHEN MATCHED AND {freshness} AND ({differences_sql}) THEN UPDATE SET {updates}
                WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
            """)
        finally:
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:
                logging.getLogger("silver.merge").warning("merge_view_cleanup_failed target=%s; preserve original outcome", target_table)

    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)


class SilverQuarantineManager:
    def __init__(self, spark, target_table="iceberg.silver.dead_letters"):
        self.spark = spark
        self.target_table = target_table

    def ensure_table(self):
        parts = self.target_table.split(".")
        if len(parts) > 2 or (len(parts) == 2 and not self.spark.conf.get(f"spark.sql.catalog.{parts[0]}", None)):
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table_identifier('.'.join(parts[:-1]))}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_identifier(self.target_table)} (
                quarantine_record_id STRING, source_dataset STRING, pipeline_run_id STRING,
                detected_at TIMESTAMP, business_key_hash STRING,
                errors ARRAY<STRUCT<rule_id:STRING,error_message:STRING,failed_column:STRING>>,
                payload STRING
            ) USING iceberg PARTITIONED BY (source_dataset, days(detected_at))
            TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
                'write.distribution-mode'='hash', 'write.merge.distribution-mode'='hash',
                'write.spark.fanout.enabled'='false')
        """)

    def route_quarantine(self, quarantine_df, dataset, run_id, business_keys):
        if quarantine_df.isEmpty():
            return
        from pyspark.sql import functions as F
        self.ensure_table()
        payload = F.to_json(F.struct(*[F.col(c) for c in sorted(quarantine_df.columns) if c != "dq_errors"]),
                            options={"ignoreNullFields": "false"})
        key = F.to_json(F.struct(*[F.col(c) for c in business_keys]), options={"ignoreNullFields": "false"}) if business_keys else F.lit(None).cast("string")
        letters = quarantine_df.select(
            F.sha2(F.to_json(F.struct(F.lit(dataset).alias("dataset"), payload.alias("payload"))), 256).alias("quarantine_record_id"),
            F.lit(dataset).alias("source_dataset"), F.lit(run_id).alias("pipeline_run_id"),
            F.current_timestamp().alias("detected_at"), F.sha2(key, 256).alias("business_key_hash"),
            F.col("dq_errors").cast(ERROR_TYPE).alias("errors"), payload.alias("payload"),
        ).dropDuplicates(["quarantine_record_id"])

        incoming = letters.alias("s")
        stored = self.spark.table(self.target_table).select(
            "quarantine_record_id", "payload", "errors").alias("t")
        matched = incoming.join(stored,
                                F.col("s.quarantine_record_id") == F.col("t.quarantine_record_id"), "left")
        exists = F.col("t.quarantine_record_id").isNotNull()
        if not matched.filter(exists & ~F.col("s.payload").eqNullSafe(F.col("t.payload"))).isEmpty():
            raise ValueError("Quarantine identity matches a different persisted payload")
        letters = matched.filter(
            ~exists | ~F.col("s.errors").eqNullSafe(F.col("t.errors"))).select("s.*")
        if letters.isEmpty():
            return
        view = "incoming_quarantine_" + uuid.uuid4().hex
        letters.createOrReplaceTempView(view)
        try:
            self.spark.sql(f"""
                MERGE INTO {table_identifier(self.target_table)} t USING {quote(view)} s
                ON t.quarantine_record_id = s.quarantine_record_id
                WHEN MATCHED THEN UPDATE SET t.pipeline_run_id = s.pipeline_run_id,
                    t.detected_at = s.detected_at, t.errors = s.errors
                WHEN NOT MATCHED THEN INSERT *
            """)
        finally:
            self.spark.catalog.dropTempView(view)

    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)
