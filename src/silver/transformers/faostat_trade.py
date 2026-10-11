"""Trade preprocessing keeps invalid observations for shared quarantine."""
from pyspark.sql import functions as F
from silver.transformers.faostat_source import identifier, sql_rule


class FaostatTradeTransformer:
    def preprocess(self, df):
        for column in ("reporter_country_code_m49_", "partner_country_code_m49_", "item_code_cpc_", "element_code"):
            if column not in df.columns:
                raise ValueError(f"FAOSTAT Trade missing source identifier {column}; legacy FAO codes cannot substitute for M49/CPC")
            value = identifier(column)
            if "m49" in column:
                value = F.when(value.rlike(r"^[0-9]{1,3}$"), F.lpad(value, 3, "0")).otherwise(value)
            df = df.withColumn(column, value)
        product = F.col("item_code_cpc_")
        df = df.withColumn("item_code_cpc_", F.when(product == "113", "0113").otherwise(product))
        if "unit" in df.columns:
            raw_unit = F.lower(F.trim("unit"))
            df = df.withColumn("unit", F.when(raw_unit.isin("t", "tonne"), "tonne")
                               .when(raw_unit.isin("1000 us$", "1000 usd"), "1000 USD").otherwise(F.col("unit")))
        return df.withColumn("_negative_value", F.coalesce(F.expr("try_cast(value as double)") < 0, F.lit(False)))

    def quality_rules(self):
        return [sql_rule("TRADE_SIGN", "NOT _negative_value", "value", "Negative trade observations require review"),
                sql_rule("TRADE_CODES", "reporter_country_code RLIKE '^[0-9]{3}$' AND partner_country_code RLIKE '^[0-9]{3}$'", "country_code", "M49 must retain three digits"),
                sql_rule("TRADE_YEAR", "year BETWEEN 1960 AND year(current_date()) + 1", "year", "Invalid trade calendar year"),
                sql_rule("TRADE_UNIT", "(element_code IN ('5610','5910') AND unit = 'tonne') OR (element_code IN ('5622','5922') AND unit = '1000 USD')", "unit", "Trade quantities and monetary values must use distinct units")]
