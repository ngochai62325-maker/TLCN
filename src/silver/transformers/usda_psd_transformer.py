"""Silver Transformer for USDA Production, Supply and Distribution (PSD)."""

from __future__ import annotations

import re
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.core.base_transformer import BaseSilverTransformer


class UsdaPsdTransformer(BaseSilverTransformer):
    """Transforms raw USDA Rice PSD from Bronze wide format to Silver normalized format."""

    @property
    def source_table(self) -> str:
        return "iceberg.bronze.usda_psd"

    @property
    def target_table(self) -> str:
        return "iceberg.silver.usda_rice_psd"

    @property
    def business_keys(self) -> List[str]:
        return ["country", "commodity", "attribute", "market_year"]

    def transform(self, df: DataFrame) -> DataFrame:
        """Unpivot wide crop year columns and clean USDA PSD."""
        # 1. Fill default commodity for Rice PSD if empty or null
        clean_df = (
            df.filter(F.trim(F.coalesce(F.col("attribute"), F.lit(""))) != "")
            .withColumn(
                "commodity",
                F.when(
                    (F.col("commodity").isNull()) | (F.trim(F.col("commodity")) == ""),
                    F.lit("Rice, Milled"),
                ).otherwise(F.trim(F.col("commodity"))),
            )
            .withColumn(
                "country",
                F.when(
                    (F.col("country").isNull()) | (F.trim(F.col("country")) == ""),
                    F.lit("Vietnam"),
                ).otherwise(F.trim(F.col("country"))),
            )
            .withColumn("attribute", F.trim(F.col("attribute")))
            .withColumn(
                "unit",
                F.regexp_replace(F.trim(F.coalesce(F.col("unit_description"), F.lit(""))), "[\\(\\)]", ""),
            )
        )

        # 2. Identify all year columns matching 'col_YYYY_YYYY'
        year_cols = [c for c in clean_df.columns if re.match(r"^col_\d{4}_\d{4}$", c)]
        year_cols.sort()

        if not year_cols:
            raise ValueError(f"No wide year columns (col_YYYY_YYYY) found in {self.source_table}")

        # 3. Construct Spark SQL stack expression for dynamic unpivoting
        # stack(N, '1960/1961', 1960, col_1960_1961, ...)
        stack_parts = []
        for c in year_cols:
            m = re.match(r"^col_(\d{4})_(\d{4})$", c)
            start_yr, end_yr = m.group(1), m.group(2)
            crop_yr_str = f"{start_yr}/{end_yr}"
            mkt_yr_int = int(start_yr)
            stack_parts.append(f"'{crop_yr_str}', {mkt_yr_int}, `{c}`")

        stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (crop_year, market_year, value)"

        unpivoted_df = clean_df.select(
            "country",
            "commodity",
            "attribute",
            "unit",
            "_source_file",
            "_ingestion_run_id",
            "_ingestion_timestamp",
            F.expr(stack_expr),
        )

        return unpivoted_df.select(
            F.col("country"),
            F.col("commodity"),
            F.col("attribute"),
            F.col("market_year").cast("int").alias("market_year"),
            F.col("crop_year"),
            F.col("unit"),
            F.col("value").cast("double").alias("value"),
            F.col("_source_file"),
            F.col("_ingestion_run_id"),
            F.col("_ingestion_timestamp"),
        ).filter(
            F.col("country").isNotNull()
            & F.col("commodity").isNotNull()
            & F.col("attribute").isNotNull()
            & F.col("market_year").isNotNull()
        )
