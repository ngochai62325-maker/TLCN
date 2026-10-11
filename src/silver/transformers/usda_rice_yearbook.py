"""Normalize source yearbook identities without inferring exporter countries."""
from pyspark.sql import functions as F


class UsdaRiceYearbookTransformer:
    def preprocess(self, df):
        if "unit_description" in df.columns:
            unit = F.upper(F.trim("unit_description"))
            df = df.withColumn("unit_description", F.when(unit == "DOLLARS PER METRIC TON", "USD/tonne").otherwise(F.col("unit_description")))
        return df.withColumn("_negative_value", F.coalesce(F.expr("try_cast(value as double)") < 0, F.lit(False)))

    def quality_rules(self):
        return [{"rule_id": "USDA_PRICE_SIGN", "sql_expr": "NOT _negative_value", "failed_column": "value", "rule": "Negative price cannot become zero through decimal rounding"}]
