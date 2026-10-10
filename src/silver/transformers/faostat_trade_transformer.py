"""Silver Transformer for FAOSTAT Detailed Trade Matrix."""

from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.core.base_transformer import BaseSilverTransformer


class FaostatTradeTransformer(BaseSilverTransformer):
    """Transforms raw FAOSTAT Rice Trade Matrix from Bronze to Silver."""

    @property
    def source_table(self) -> str:
        return "iceberg.bronze.faostat_trade"

    @property
    def target_table(self) -> str:
        return "iceberg.silver.faostat_trade"

    @property
    def business_keys(self) -> List[str]:
        return [
            "reporter_country_code",
            "partner_country_code",
            "commodity_code",
            "year",
            "element_code",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and normalize FAOSTAT Trade Matrix."""
        # 1. Clean M49 codes by removing leading apostrophes (e.g., "'764" -> "764")
        clean_reporter_code = F.regexp_replace(
            F.coalesce(F.col("reporter_country_code_m49_"), F.col("reporter_country_code").cast("string")),
            "^'",
            "",
        )
        clean_partner_code = F.regexp_replace(
            F.coalesce(F.col("partner_country_code_m49_"), F.col("partner_country_code").cast("string")),
            "^'",
            "",
        )
        clean_cpc_code = F.regexp_replace(
            F.coalesce(F.col("item_code_cpc_"), F.col("item_code").cast("string")),
            "^'",
            "",
        )

        # 2. Identify aggregates (World, regional aggregates, unspecified areas)
        aggregate_codes = ["1", "001", "5000", "5100", "5200", "5300", "5400", "5500", "5800"]
        is_rep_agg = (
            clean_reporter_code.isin(aggregate_codes)
            | F.lower(F.col("reporter_countries")).contains("world")
            | F.lower(F.col("reporter_countries")).contains("total")
            | F.lower(F.col("reporter_countries")).contains("all countries")
        )
        is_part_agg = (
            clean_partner_code.isin(aggregate_codes)
            | F.lower(F.col("partner_countries")).contains("world")
            | F.lower(F.col("partner_countries")).contains("total")
            | F.lower(F.col("partner_countries")).contains("all countries")
            | F.lower(F.col("partner_countries")).contains("unspecified")
        )

        # 3. Clean value and handle invalid negatives
        clean_value = F.when(F.col("value") < 0, None).otherwise(F.col("value").cast("double"))

        return df.select(
            clean_reporter_code.alias("reporter_country_code"),
            F.trim(F.col("reporter_countries")).alias("reporter_country_name"),
            is_rep_agg.alias("is_reporter_aggregate"),
            clean_partner_code.alias("partner_country_code"),
            F.trim(F.col("partner_countries")).alias("partner_country_name"),
            is_part_agg.alias("is_partner_aggregate"),
            clean_cpc_code.alias("commodity_code"),
            F.trim(F.col("item")).alias("commodity_name"),
            F.col("element_code").cast("string").alias("element_code"),
            F.trim(F.col("element")).alias("element_name"),
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("unit")).alias("unit"),
            clean_value.alias("value"),
            F.trim(F.col("flag")).alias("flag"),
            F.col("_source_file"),
            F.col("_ingestion_run_id"),
            F.col("_ingestion_timestamp"),
        ).filter(
            # Completeness check: key columns cannot be null
            F.col("reporter_country_code").isNotNull()
            & F.col("partner_country_code").isNotNull()
            & F.col("commodity_code").isNotNull()
            & F.col("year").isNotNull()
            & F.col("element_code").isNotNull()
        )
