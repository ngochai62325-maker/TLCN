"""Preserve market-year semantics and raw values during vectorized unpivot."""
import re
from pyspark.sql import functions as F


class UsdaPsdTransformer:
    def preprocess(self, df):
        years = sorted(c for c in df.columns if re.fullmatch(r"col_[0-9]{4}_[0-9]{4}", c))
        if not years:
            required = {"market_year", "crop_year", "value"}
            if not required.issubset(df.columns):
                raise ValueError("USDA PSD requires wide crop-year columns or long market_year/crop_year/value")
            return df
        for column in years:
            start, end = map(int, column.split("_")[1:])
            if end != start + 1:
                raise ValueError(f"Invalid USDA crop-year header: {column}")
        if "commodity" in df.columns:
            value = F.trim(F.col("commodity"))
            df = df.withColumn("commodity", F.when(value.isNull() | (value == ""), "Rice, Milled").otherwise(value))
        if "unit_description" in df.columns:
            df = df.withColumn("unit_description", F.regexp_replace(F.trim("unit_description"), "[()]", ""))
        entries = [F.struct(F.lit(int(c[4:8])).alias("market_year"),
                            F.lit(c[4:8] + "/" + c[9:13]).alias("crop_year"),
                            F.col(c).cast("string").alias("value")) for c in years]
        return (df.select(*[c for c in df.columns if c not in years],
                          F.explode(F.array(*entries)).alias("observation"))
                .select(*[c for c in df.columns if c not in years], "observation.*"))
