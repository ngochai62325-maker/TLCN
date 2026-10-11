"""Preserve market-year semantics and raw values during vectorized unpivot."""
import re
from pyspark.sql import functions as F
from silver.transformers.faostat_source import sql_rule


class UsdaPsdTransformer:
    def preprocess(self, df):
        years = sorted(c for c in df.columns if re.fullmatch(r"col_[0-9]{4}_[0-9]{4}", c))
        if not years:
            required = {"market_year", "crop_year", "value"}
            if not required.issubset(df.columns):
                raise ValueError("USDA PSD requires wide crop-year columns or long market_year/crop_year/value")
            return self.normalize_values(df)
        for column in years:
            start, end = map(int, column.split("_")[1:])
            if end != start + 1:
                raise ValueError(f"Invalid USDA crop-year header: {column}")
        if "commodity" in df.columns:
            value = F.trim(F.col("commodity"))
            df = df.withColumn("commodity", F.when(value.isNull() | (value == ""), "Rice, Milled").otherwise(value))
        entries = [F.struct(F.lit(int(c[4:8])).alias("market_year"),
                            F.lit(c[4:8] + "/" + c[9:13]).alias("crop_year"),
                            F.col(c).cast("string").alias("value")) for c in years]
        result = (df.select(*[c for c in df.columns if c not in years],
                          F.explode(F.array(*entries)).alias("observation"))
                .select(*[c for c in df.columns if c not in years], "observation.*"))
        return self.normalize_values(result)

    @staticmethod
    def normalize_values(df):
        rate = F.trim(F.col("attribute")) == "Milling Rate (.9999)"
        raw_unit = F.col("unit_description").cast("string") if "unit_description" in df.columns else F.lit(None).cast("string")
        df = (df.withColumn("value_source", F.col("value").cast("string"))
              .withColumn("source_unit", raw_unit)
              .withColumn("_negative_value_raw", F.trim("value_source").rlike(r"^-\s*(?:[0-9]|\.)"))
              .withColumn("conversion_factor", F.when(rate, F.lit("0.0001")).otherwise(F.lit("1")).cast("decimal(18,6)")))
        # Strings on both branches keep malformed quantity input visible to
        # the shared cast-error handler even with ANSI mode enabled.
        fraction = F.expr("try_cast(value_source AS decimal(28,8)) / 10000").cast("string")
        return (df.withColumn("value", F.when(rate, fraction).otherwise(F.col("value_source")))
                .withColumn("unit_description", F.when(rate, F.lit("fraction"))
                            .otherwise(F.regexp_replace(F.trim("source_unit"), "[()]", ""))))

    def quality_rules(self):
        negative = sql_rule("PSD_RAW_NEGATIVE", "NOT coalesce(_negative_value_raw, false)", "value", "Negative source value retained for review")
        negative["action"] = "LOG"
        return [negative]
