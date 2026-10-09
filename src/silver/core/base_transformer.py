"""Base Transformer module for Silver Layer transformations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.core.spark_session import get_spark_session

logger = logging.getLogger("silver.transformer")


class BaseSilverTransformer(ABC):
    """Abstract base class for all Silver transformations.
    
    Provides standardized data reading from Bronze, deduplication based on
    business keys and timestamp survivorship, and idempotent MERGE INTO write.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark_session(self.__class__.__name__)

    @property
    @abstractmethod
    def source_table(self) -> str:
        """Fully-qualified source table in Bronze (e.g. 'iceberg.bronze.faostat_trade')."""
        pass

    @property
    @abstractmethod
    def target_table(self) -> str:
        """Fully-qualified target table in Silver (e.g. 'iceberg.silver.faostat_trade')."""
        pass

    @property
    @abstractmethod
    def business_keys(self) -> List[str]:
        """List of column names constituting the business grain / primary key."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Execute domain-specific cleaning, normalization, and typing."""
        pass

    def read_bronze(
        self,
        mode: str = "full",
        watermark: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> DataFrame:
        """Read data from Bronze Iceberg table with optional incremental filters."""
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
        # Find available timestamp/order column
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
        # Add technical processing timestamp
        now_utc = datetime.now(timezone.utc)
        df_to_write = df.withColumn("_silver_processed_at", F.lit(now_utc))

        record_count = df_to_write.count()
        logger.info(f"Writing {record_count} records to {self.target_table} in mode '{mode}'")

        if record_count == 0:
            return {"status": "SUCCESS", "records_written": 0, "target_table": self.target_table}

        # Idempotent MERGE INTO via Spark SQL
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
        """Run the complete Silver pipeline: Read -> Transform -> Deduplicate -> Write."""
        logger.info(f"Starting {self.__class__.__name__} execution (mode: {mode})")
        bronze_df = self.read_bronze(mode=mode, watermark=watermark, run_id=run_id)
        transformed_df = self.transform(bronze_df)
        deduped_df = self.deduplicate(transformed_df)
        return self.write_silver(deduped_df, mode="merge")
