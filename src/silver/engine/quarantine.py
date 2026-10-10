from typing import List, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import uuid
from datetime import datetime, timezone

class SilverQuarantineManager:
    def __init__(self, spark: Any, target_table: str = "iceberg.silver.dead_letters"):
        self.spark = spark
        # According to architecture: silver.system.dead_letters, but iceberg.silver_system might be the catalog path
        # Let's use iceberg.silver.dead_letters as a safe catalog.schema.table
        self.target_table = target_table

    def ensure_table(self):
        """Ensure the quarantine table exists."""
        # Trino/Iceberg creation
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.target_table} (
            quarantine_record_id STRING,
            source_dataset STRING,
            pipeline_run_id STRING,
            detected_at TIMESTAMP,
            business_key_hash STRING,
            errors ARRAY<STRUCT<rule_id:STRING,error_message:STRING,failed_column:STRING>>,
            payload STRING
        )
        USING iceberg PARTITIONED BY (source_dataset)
        """
        self.spark.sql(create_sql)

    def route_quarantine(self, quarantine_df: DataFrame, dataset: str, run_id: str, business_keys: List[str]):
        """
        Transforms and writes quarantine_df to the dead letter table.
        """
        if quarantine_df.isEmpty():
            return {"records_changed": 0, "status": "NOOP"}

        # Build payload JSON
        all_cols = sorted(quarantine_df.columns)
        struct_col = F.struct(*[F.col(c) for c in all_cols if c != "dq_errors"])
        
        # Build business key hash
        if business_keys:
            bk_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_keys]
            bk_hash = F.sha2(F.concat_ws("|", *bk_cols), 256)
        else:
            bk_hash = F.lit(None).cast("string")

        payload_col = F.to_json(struct_col, {"ignoreNullFields": "false"})
        # Deterministic quarantine ID based on dataset + payload to ensure idempotency across runs
        identity_payload = F.to_json(F.struct(*[F.col(c) for c in all_cols
            if c not in ("dq_errors", "dq_warnings", "_bronze_iceberg_snapshot_id")]), {"ignoreNullFields": "false"})
        source_identity = F.col("_source_payload") if "_source_payload" in all_cols else identity_payload
        quarantine_id_col = F.sha2(F.concat_ws("|", F.lit(dataset), F.coalesce(bk_hash, F.lit("")), source_identity), 256)
        
        dead_letters = quarantine_df.select(
            quarantine_id_col.alias("quarantine_record_id"),
            F.lit(dataset).alias("source_dataset"),
            F.lit(run_id).alias("pipeline_run_id"),
            F.current_timestamp().alias("detected_at"),
            bk_hash.alias("business_key_hash"),
            F.col("dq_errors").alias("errors"),
            payload_col.alias("payload")
        ).dropDuplicates(["quarantine_record_id"])

        if not self._table_exists(self.target_table):
            # Create the table if it doesn't exist
            dead_letters.limit(0).write.format("iceberg").partitionBy("source_dataset").saveAsTable(self.target_table)

        # Use MERGE INTO for idempotency
        temp_view = f"incoming_quarantine_{dataset}"
        dead_letters.createOrReplaceTempView(temp_view)
        candidates = self.spark.sql(f"SELECT s.* FROM {temp_view} s LEFT JOIN {self.target_table} t "
            "ON t.quarantine_record_id = s.quarantine_record_id "
            "WHERE t.quarantine_record_id IS NULL OR NOT (t.errors <=> s.errors)").cache()
        if candidates.isEmpty():
            candidates.unpersist()
            self.spark.catalog.dropTempView(temp_view)
            return {"records_changed": 0, "status": "NOOP"}
        changed_count = candidates.count()
        candidates.createOrReplaceTempView(temp_view)

        merge_sql = f"""
        MERGE INTO {self.target_table} t
        USING {temp_view} s
        ON t.quarantine_record_id = s.quarantine_record_id
        WHEN MATCHED AND NOT (t.errors <=> s.errors) THEN UPDATE SET
            t.pipeline_run_id = s.pipeline_run_id,
            t.detected_at = s.detected_at,
            t.errors = s.errors
        WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self.spark.sql(merge_sql)
        finally:
            candidates.unpersist()
            self.spark.catalog.dropTempView(temp_view)
        return {"records_changed": changed_count, "status": "MERGED"}

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE {table_name}")
            return True
        except Exception:
            return False

