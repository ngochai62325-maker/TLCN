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
        from silver.framework import SilverTransformationFramework
        return SilverTransformationFramework.filter_input(self.spark.table(self.source_table), mode, watermark, run_id)

    @staticmethod
    def prepare_source(df):
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns) if c != "_bronze_iceberg_snapshot_id"]),
                                                        {"ignoreNullFields": "false"}))
        return df

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Deduplicate records based on business keys, keeping most recent _ingestion_timestamp."""
        from silver.engine.merge import IcebergMergeEngine
        return IcebergMergeEngine(self.spark).deduplicate(df, self.business_keys)

    def write_silver(
        self,
        df: DataFrame,
        mode: str = "merge",
    ) -> Dict[str, Any]:
        """Write transformed DataFrame to Silver Iceberg table with idempotency guarantee."""
        from silver.engine.merge import IcebergMergeEngine
        if mode != "merge":
            raise ValueError("Only merge writes are supported")
        count = df.count()
        result = IcebergMergeEngine(self.spark).merge(df, self.target_table, self.business_keys)
        return {"status": "SUCCESS", "records_processed": count, "records_written": result["records_changed"],
                "target_table": self.target_table, "write_result": result}

    def execute(
        self,
        mode: str = "full",
        watermark: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run the complete Silver pipeline: Read -> Transform -> Deduplicate -> Write."""
        from uuid import uuid4
        from silver.framework import SilverTransformationFramework
        result = SilverTransformationFramework(self.spark).run(
            self.source_table.rsplit(".", 1)[-1], run_id or uuid4().hex,
            mode=mode, watermark=watermark, run_id=run_id, transformer=self)
        result["records_written"] = (result.get("write_result") or {}).get("records_changed", 0)
        return result
