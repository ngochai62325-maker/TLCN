"""Silver Transformer for USDA Rice Export Prices (Yearbook Tables 25-28)."""

from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.core.base_transformer import BaseSilverTransformer


class UsdaExportPriceTransformer(BaseSilverTransformer):
    """Extracts and standardizes FOB Export Prices from USDA Rice Yearbook."""

    @property
    def source_table(self) -> str:
        return "iceberg.bronze.usda_rice_yearbook"

    @property
    def target_table(self) -> str:
        return "iceberg.silver.usda_export_price"

    @property
    def business_keys(self) -> List[str]:
        return [
            "exporter_country",
            "rice_class",
            "year",
            "reference_period",
            "statistic_description",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        """Filter export price tables and standardize price observations."""
        # 1. Focus exclusively on export price tables:
        # Table 25: Thailand
        # Table 26: Vietnam
        # Table 27: India
        # Table 28: Pakistan
        filtered_df = df.filter(F.col("table_number").isin([25, 26, 27, 28]))

        # Map exporter country canonically
        exporter_expr = (
            F.when(F.col("table_number") == 25, F.lit("THAILAND"))
            .when(F.col("table_number") == 26, F.lit("VIETNAM"))
            .when(F.col("table_number") == 27, F.lit("INDIA"))
            .when(F.col("table_number") == 28, F.lit("PAKISTAN"))
            .otherwise(F.upper(F.trim(F.coalesce(F.col("location_description"), F.lit("UNKNOWN")))))
        )

        clean_price = F.when(
            (F.col("value").isNull()) | (F.trim(F.col("value")) == "") | (F.trim(F.col("value")) == "NA"),
            None,
        ).otherwise(F.col("value").cast("double"))

        return filtered_df.select(
            exporter_expr.alias("exporter_country"),
            F.col("table_number").cast("bigint").alias("table_number"),
            F.trim(F.col("table_name")).alias("table_name"),
            F.trim(F.col("commodity_description")).alias("commodity"),
            F.trim(F.col("class_description")).alias("rice_class"),
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("reference_period_description")).alias("reference_period"),
            F.trim(F.col("statistic_description")).alias("statistic_description"),
            clean_price.alias("price_fob"),
            F.trim(F.col("unit_description")).alias("unit"),
            F.col("_source_file"),
            F.col("_ingestion_run_id"),
            F.col("_ingestion_timestamp"),
        ).filter(
            F.col("exporter_country").isNotNull()
            & F.col("rice_class").isNotNull()
            & F.col("year").isNotNull()
            & F.col("reference_period").isNotNull()
            & F.col("statistic_description").isNotNull()
        )
