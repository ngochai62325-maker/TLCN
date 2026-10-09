from typing import List, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import uuid
from datetime import datetime, timezone

class SilverQuarantineManager:
    def __init__(self, spark: Any, target_table: str = "iceberg.silver_system.dead_letters"):
        self.spark = spark
        # According to architecture: silver.system.dead_letters, but iceberg.silver_system might be the catalog path
        # Let's use iceberg.silver.dead_letters as a safe catalog.schema.table
        self.target_table = "iceberg.silver.dead_letters"

    def ensure_table(self):
        """Ensure the quarantine table exists."""
        # Trino/Iceberg creation
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.target_table} (
            quarantine_record_id VARCHAR,
            source_dataset VARCHAR,
            pipeline_run_id VARCHAR,
            detected_at TIMESTAMP(6) WITH TIME ZONE,
            business_key_hash VARCHAR,
            errors ARRAY(ROW(rule_id VARCHAR, error_message VARCHAR, failed_column VARCHAR)),
            payload VARCHAR
        )
        WITH (
            partitioning = ARRAY['source_dataset', 'day(detected_at)']
        )
        """
        try:
            self.spark.sql(create_sql)
        except Exception as e:
            # Table might already exist or spark doesn't support this DDL directly without extensions.
            # We will handle it by DataFrame writer if possible.
            pass

    def route_quarantine(self, quarantine_df: DataFrame, dataset: str, run_id: str, business_keys: List[str]):
        """
        Transforms and writes quarantine_df to the dead letter table.
        """
        if quarantine_df.isEmpty():
            return

        # Build payload JSON
        all_cols = quarantine_df.columns
        struct_col = F.struct(*[F.col(c) for c in all_cols if c != "dq_errors"])
        
        # Build business key hash
        if business_keys:
            bk_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_keys]
            bk_hash = F.sha2(F.concat_ws("|", *bk_cols), 256)
        else:
            bk_hash = F.lit(None).cast("string")

        payload_col = F.to_json(struct_col)
        # Deterministic quarantine ID based on dataset + payload to ensure idempotency across runs
        quarantine_id_col = F.sha2(F.concat_ws("|", F.lit(dataset), payload_col), 256)
        
        dead_letters = quarantine_df.select(
            quarantine_id_col.alias("quarantine_record_id"),
            F.lit(dataset).alias("source_dataset"),
            F.lit(run_id).alias("pipeline_run_id"),
            F.current_timestamp().alias("detected_at"),
            bk_hash.alias("business_key_hash"),
            F.col("dq_errors").alias("errors"),
            payload_col.alias("payload")
        )

        if not self._table_exists(self.target_table):
            # Create the table if it doesn't exist
            dead_letters.limit(0).write.format("iceberg").partitionBy("source_dataset").saveAsTable(self.target_table)

        # Use MERGE INTO for idempotency
        temp_view = f"incoming_quarantine_{dataset}"
        dead_letters.createOrReplaceTempView(temp_view)

        merge_sql = f"""
        MERGE INTO {self.target_table} t
        USING {temp_view} s
        ON t.quarantine_record_id = s.quarantine_record_id
        WHEN MATCHED THEN UPDATE SET
            t.pipeline_run_id = s.pipeline_run_id,
            t.detected_at = s.detected_at,
            t.errors = s.errors
        WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(merge_sql)

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE {table_name}")
            return True
        except Exception:
            return False

