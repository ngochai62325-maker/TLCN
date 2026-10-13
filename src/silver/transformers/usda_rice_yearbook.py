"""Normalize source yearbook identities without inferring exporter countries."""
from pyspark.sql import functions as F
from silver.transformers.faostat_source import source_values, sql_rule


class UsdaRiceYearbookTransformer:
    def preprocess(self, df):
        df = source_values(df, "value", "unit_description", ("NA", "N/A", "NULL", "..", "..."))
        if "unit_description" in df.columns:
            unit = F.upper(F.trim("unit_description"))
            df = df.withColumn("unit_description", F.when(unit == "DOLLARS PER METRIC TON", "USD/tonne").otherwise(F.col("unit_description")))
        return df.withColumn("_negative_value", F.coalesce(F.expr("try_cast(value as double)") < 0, F.lit(False)))

    def quality_rules(self):
        return [sql_rule("USDA_PRICE_SIGN", "NOT _negative_value", "value", "Negative price cannot become zero through decimal rounding"),
                sql_rule("USDA_PRICE_UNIT", "unit = 'USD/tonne'", "unit", "Only source-supported FOB price units are normalized"),
                sql_rule("USDA_PRICE_PERIOD", "year BETWEEN 1960 AND year(current_date()) + 1 AND reference_period IN ('January','February','March','April','May','June','July','August','September','October','November','December','YEAR','MARKETING')", "reference_period", "Preserve source reference period; do not infer calendar dates for annual/marketing quotes")]
