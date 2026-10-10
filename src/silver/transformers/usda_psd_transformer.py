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
        df = self.prepare_source(df)
        # 1. Fill default commodity for Rice PSD if empty or null
        clean_df = (
            df
            .withColumn(
                "commodity",
                F.when(
                    (F.col("commodity").isNull()) | (F.trim(F.col("commodity")) == ""),
                    F.lit("Rice, Milled"),
                ).otherwise(F.trim(F.col("commodity"))),
            )
            .withColumn("country", F.trim(F.col("country")))
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
            stack_parts.append(f"'{crop_yr_str}', {mkt_yr_int}, cast(`{c}` as string)")

        stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (crop_year, market_year, raw_value)"

        unpivoted_df = clean_df.select(
            "country",
            "commodity",
            "attribute",
            "unit",
            *[c for c in df.columns if c.startswith("_")],
            F.expr(stack_expr),
        )

        return unpivoted_df.select(
            F.col("country"),
            F.col("commodity"),
            F.col("attribute"),
            F.col("market_year").cast("int").alias("market_year"),
            F.col("crop_year"),
            F.col("unit"),
            F.expr("try_cast(raw_value as double)").alias("value"),
            F.col("raw_value").alias("_raw_value"),
            (F.col("raw_value").isNotNull() & F.expr("try_cast(raw_value as double)").isNull()).alias("_numeric_parse_error"),
            *[F.col(c) for c in df.columns if c.startswith("_")],
        )

    def quality_rules(self):
        from silver.transformers.faostat_source import sql_rule
        return [sql_rule("PSD_PARSE", "NOT _numeric_parse_error", "value", "Malformed observation"),
                sql_rule("PSD_CROP_YEAR", "cast(substring(crop_year,1,4) as int) = market_year AND "
                         "cast(substring(crop_year,6,4) as int) = market_year + 1", "crop_year", "Marketing-year interval must remain intact"),
                sql_rule("PSD_UNIT", "unit IN ('1000 MT','1000 HA','MT/HA')", "unit", "Source unit requires review"),
                sql_rule("PSD_RATE_DEFINITION", "attribute NOT LIKE 'Milling Rate%'", "unit",
                         "Raw source labels Milling Rate (.9999) as 1000 MT; scaled ratio definition requires review")]
