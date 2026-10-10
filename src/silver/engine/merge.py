from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import List, Any
from uuid import uuid4
import re

class IcebergMergeEngine:
    def __init__(self, spark: Any):
        self.spark = spark

    def deduplicate(self, df: DataFrame, business_keys: List[str], order_cols: List[str] = None) -> DataFrame:
        """
        Deduplicates a DataFrame based on business keys using a Window function.
        """
        if not business_keys:
            return df
            
        if order_cols is None:
            order_cols = ["_ingestion_timestamp", "_source_file"]
            
        # Ensure the order cols actually exist in df
        actual_order_cols = [F.col(c).desc() for c in order_cols if c in df.columns]
        if not actual_order_cols:
            # Fallback if metadata cols don't exist
            actual_order_cols = [F.lit(1).desc()]
            
        # Stable tie-breaker independent of Spark partition/arrival order.
        payload = F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)
            if c not in ("_silver_processed_at", "_bronze_iceberg_snapshot_id")]), {"ignoreNullFields": "false"})
        actual_order_cols.append(F.sha2(payload, 256).desc())
        window_spec = Window.partitionBy(*business_keys).orderBy(*actual_order_cols)
        
        dedup_df = df.withColumn("_row_num", F.row_number().over(window_spec)) \
                     .filter(F.col("_row_num") == 1) \
                     .drop("_row_num")
                     
        return dedup_df

    def merge(self, df: DataFrame, target_table: str, business_keys: List[str]):
        """
        Merges df into target_table using business_keys.
        """
        if df.isEmpty():
            return {"records_changed": 0, "status": "NOOP"}
        if not business_keys or any(k not in df.columns for k in business_keys):
            raise ValueError("MERGE requires existing business key columns")
        invalid_key = F.lit(False)
        for key in business_keys:
            invalid_key = invalid_key | F.col(key).isNull()
        if df.filter(invalid_key).limit(1).count():
            raise ValueError("MERGE refuses null business keys; route through DQ first")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){1,2}", target_table):
            raise ValueError("Invalid qualified target table")
        df = self.deduplicate(df, business_keys)

        # Ensure table exists first. We can write an empty DF if it doesn't exist
        if not self._table_exists(target_table):
            df.limit(0).write.format("iceberg").saveAsTable(target_table)

        # Create a temp view for the incoming data
        temp_view = "incoming_updates_" + uuid4().hex
        df.createOrReplaceTempView(temp_view)
        
        match_condition = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in business_keys])
        
        # Build update set (all columns except keys, though updating keys with same value is harmless)
        cols = df.columns
        update_set = ", ".join([f"t.`{c}` = s.`{c}`" for c in cols])
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join([f"s.`{c}`" for c in cols])
        target = self.spark.table(target_table)
        if {(f.name, f.dataType.simpleString()) for f in target.schema} != {(f.name, f.dataType.simpleString()) for f in df.schema}:
            self.spark.catalog.dropTempView(temp_view)
            raise ValueError(f"Schema change requires approval: {target_table}")
        compared = [c for c in cols if c not in ("_silver_processed_at", "_bronze_iceberg_snapshot_id")]
        changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in compared)
        freshness = "true"
        if "_ingestion_timestamp" in cols:
            freshness = "s._ingestion_timestamp IS NOT NULL AND (t._ingestion_timestamp IS NULL OR s._ingestion_timestamp >= t._ingestion_timestamp)"
        eligible = f"({freshness}) AND ({changed})"
        # Filter unchanged/stale rows before MERGE so Iceberg creates no snapshot
        # for an exact rerun. Equal timestamps use the same payload tie-breaker.
        if "_ingestion_timestamp" in cols:
            def fingerprint(alias):
                fields = ", ".join(f"'{c}', {alias}.`{c}`" for c in sorted(compared))
                return f"sha2(to_json(named_struct({fields}), map('ignoreNullFields','false')), 256)"
            tie_break = f"{fingerprint('s')} > {fingerprint('t')}"
            if "_source_file" in cols:
                tie_break = f"coalesce(s._source_file,'') > coalesce(t._source_file,'') OR " \
                            f"(s._source_file <=> t._source_file) AND ({tie_break})"
            eligible += f" AND (NOT (s._ingestion_timestamp <=> t._ingestion_timestamp) OR ({tie_break}))"
        candidates = self.spark.sql(f"SELECT s.* FROM {temp_view} s LEFT JOIN {target_table} t ON {match_condition} "
                                   f"WHERE t.`{business_keys[0]}` IS NULL OR ({eligible})").cache()
        if candidates.isEmpty():
            candidates.unpersist()
            self.spark.catalog.dropTempView(temp_view)
            return {"records_changed": 0, "status": "NOOP"}
        changed_count = candidates.count()
        candidates.createOrReplaceTempView(temp_view)

        merge_sql = f"""
        MERGE INTO {target_table} t
        USING {temp_view} s
        ON {match_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
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
