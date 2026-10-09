from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import List, Any

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
            return

        # Ensure table exists first. We can write an empty DF if it doesn't exist
        if not self._table_exists(target_table):
            df.limit(0).write.format("iceberg").saveAsTable(target_table)

        # Create a temp view for the incoming data
        temp_view = "incoming_updates"
        df.createOrReplaceTempView(temp_view)
        
        match_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
        
        # Build update set (all columns except keys, though updating keys with same value is harmless)
        cols = df.columns
        update_set = ", ".join([f"t.{c} = s.{c}" for c in cols])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"s.{c}" for c in cols])

        merge_sql = f"""
        MERGE INTO {target_table} t
        USING {temp_view} s
        ON {match_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        self.spark.sql(merge_sql)

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE {table_name}")
            return True
        except Exception:
            return False
