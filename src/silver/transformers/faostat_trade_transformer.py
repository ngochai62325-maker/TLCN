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
        df = self.prepare_source(df)
        # 1. Clean M49 codes by removing leading apostrophes (e.g., "'764" -> "764")
        clean_reporter_code = F.regexp_replace(
            F.col("reporter_country_code_m49_").cast("string"),
            "^'",
            "",
        )
        clean_partner_code = F.regexp_replace(
            F.col("partner_country_code_m49_").cast("string"),
            "^'",
            "",
        )
        clean_cpc_code = F.regexp_replace(
            F.col("item_code_cpc_").cast("string"),
            "^'",
            "",
        )

        clean_reporter_code = F.regexp_replace(clean_reporter_code, r"\.0+$", "")
        clean_partner_code = F.regexp_replace(clean_partner_code, r"\.0+$", "")
        clean_reporter_code = F.when(clean_reporter_code.rlike(r"^[0-9]{1,3}$"), F.lpad(clean_reporter_code, 3, "0")).otherwise(clean_reporter_code)
        clean_partner_code = F.when(clean_partner_code.rlike(r"^[0-9]{1,3}$"), F.lpad(clean_partner_code, 3, "0")).otherwise(clean_partner_code)
        clean_cpc_code = F.regexp_replace(clean_cpc_code, r"\.0+$", "")
        clean_cpc_code = F.when(clean_cpc_code == "113", "0113").otherwise(clean_cpc_code)

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
        clean_value = F.expr("try_cast(value as double)")

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
            F.col("value").cast("string").alias("_raw_value"),
            (F.col("value").isNotNull() & clean_value.isNull()).alias("_numeric_parse_error"),
            F.trim(F.col("flag")).alias("flag"),
            *[F.col(c) for c in df.columns if c.startswith("_")],
        )

    def quality_rules(self):
        from silver.transformers.faostat_source import sql_rule
        return [
            sql_rule("TRADE_NUMBER", "NOT _numeric_parse_error", "value", "Malformed source number"),
            sql_rule("TRADE_M49", "reporter_country_code RLIKE '^[0-9]{3}$' AND partner_country_code RLIKE '^[0-9]{3}$'",
                     "country_code", "Retain source M49 codes; aggregates remain marked separately"),
            sql_rule("TRADE_UNIT", "(element_code IN ('5610','5910') AND unit = 't') OR "
                     "(element_code IN ('5622','5922') AND unit = '1000 USD')", "unit", "Quantity and monetary trade measures must stay distinct"),
        ]
