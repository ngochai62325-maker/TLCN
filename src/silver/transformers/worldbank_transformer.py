"""Silver Transformer for World Bank Commodity Markets Pink Sheet."""

from __future__ import annotations

import re
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.core.base_transformer import BaseSilverTransformer


class WorldBankTransformer(BaseSilverTransformer):
    """Transforms raw World Bank Pink Sheet from Bronze wide format to Silver normalized format."""

    @property
    def source_table(self) -> str:
        return "iceberg.bronze.worldbank_pinksheet"

    @property
    def target_table(self) -> str:
        return "iceberg.silver.worldbank_commodity_monthly"

    @property
    def business_keys(self) -> List[str]:
        return ["commodity_code", "period_date"]

    @staticmethod
    def _derive_unit(col_name: str) -> str:
        col_lower = col_name.lower()
        if "_mt_" in col_lower or col_lower.endswith("_mt"):
            return "USD/mt"
        elif "_kg_" in col_lower or col_lower.endswith("_kg"):
            return "USD/kg"
        elif "_bbl_" in col_lower or col_lower.endswith("_bbl"):
            return "USD/bbl"
        elif "_mmbtu_" in col_lower:
            return "USD/mmbtu"
        elif "_troy_oz_" in col_lower:
            return "USD/troy_oz"
        elif "_sheet_" in col_lower:
            return "cents/sheet"
        return "USD/unit"

    @staticmethod
    def _derive_commodity_name(col_name: str) -> str:
        clean = re.sub(r"_(mt|kg|bbl|mmbtu|troy_oz|sheet)_?$", "", col_name)
        clean = clean.replace("_", " ").title()
        return clean

    def transform(self, df: DataFrame) -> DataFrame:
        """Unpivot commodity columns and parse monthly calendar periods."""
        # 1. Identify commodity columns (exclude period and technical columns)
        exclude_cols = {"period"}
        commodity_cols = [
            c for c in df.columns
            if not c.startswith("_") and c not in exclude_cols
        ]
        commodity_cols.sort()

        if not commodity_cols:
            raise ValueError(f"No commodity columns found in {self.source_table}")

        # 2. Filter valid period rows (e.g. 'YYYYMmm')
        valid_period_df = df.filter(
            F.col("period").isNotNull() & (F.col("period").rlike(r"^\d{4}M\d{2}$"))
        )

        # 3. Build stack expression for dynamic unpivoting
        # All price columns are cast to string first because some were inferred as double and others as string in Bronze
        stack_parts = []
        for c in commodity_cols:
            clean_code = re.sub(r"_(mt|kg|bbl|mmbtu|troy_oz|sheet)_?$", "", c).strip("_")
            comm_name = self._derive_commodity_name(c)
            unit_str = self._derive_unit(c)
            stack_parts.append(f"'{clean_code}', '{comm_name}', '{unit_str}', cast(`{c}` as string)")

        stack_expr = (
            f"stack({len(commodity_cols)}, {', '.join(stack_parts)}) "
            f"as (commodity_code, commodity_name, unit, raw_price)"
        )

        unpivoted_df = valid_period_df.select(
            "period",
            "_source_file",
            "_ingestion_run_id",
            "_ingestion_timestamp",
            F.expr(stack_expr),
        )

        # 4. Clean price and parse date components
        # Convert non-numeric or missing indicators ('…', empty string) to NULL
        clean_price = F.when(
            (F.col("raw_price").isNull())
            | (F.trim(F.col("raw_price")) == "")
            | (F.trim(F.col("raw_price")) == "…")
            | (F.trim(F.col("raw_price")) == "NA"),
            None,
        ).otherwise(F.col("raw_price").cast("double"))

        year_col = F.substring(F.col("period"), 1, 4).cast("int")
        month_col = F.substring(F.col("period"), 6, 2).cast("int")
        period_date_col = F.to_date(
            F.concat(
                F.substring(F.col("period"), 1, 4),
                F.lit("-"),
                F.substring(F.col("period"), 6, 2),
                F.lit("-01"),
            )
        )

        return unpivoted_df.select(
            F.col("commodity_code"),
            F.col("commodity_name"),
            F.trim(F.col("period")).alias("period_code"),
            period_date_col.alias("period_date"),
            year_col.alias("year"),
            month_col.alias("month"),
            clean_price.alias("price"),
            F.col("unit"),
            F.lit(None).cast("string").alias("series_name"),
            F.col("_source_file"),
            F.col("_ingestion_run_id"),
        ).filter(
            F.col("commodity_code").isNotNull()
            & F.col("period_date").isNotNull()
            & F.col("year").isNotNull()
        )
