"""FAOSTAT QCL observations at country × CPC × calendar year × element."""

from pyspark.sql import functions as F

from silver.transformers.faostat_source import FaostatSourceTransformer, sql_rule


class FaostatProductionTransformer(FaostatSourceTransformer):
    dataset_id = "faostat_production"

    def preprocess(self, df):
        df = self.prepare(df)
        unit = F.lower(F.col("unit"))
        element = F.col("element_code")
        area = (element == "5312") & unit.isin("ha", "hectare")
        production = (element == "5510") & unit.isin("t", "tonne", "kg")
        yield_value = (element == "5412") & unit.isin("kg/ha", "kg/hectare", "hg/ha", "t/ha")
        factor = (F.when((element == "5510") & (unit == "kg"), F.lit("0.001"))
                  .when((element == "5412") & (unit == "hg/ha"), F.lit("0.1"))
                  .when((element == "5412") & (unit == "t/ha"), F.lit("1000"))
                  .otherwise(F.lit("1")).cast("decimal(10,3)"))
        converted = F.col("value") * factor
        return (df.withColumn("_unit_mapping_valid", area | production | yield_value)
                .withColumn("_conversion_overflow", F.col("value").isNotNull() & converted.isNull())
                .withColumn("value", converted)
                .withColumn("unit", F.when(area, "hectare").when(production, "tonne")
                            .when(yield_value, "kg/hectare").otherwise(F.col("unit"))))

    def quality_rules(self):
        return self.common_rules() + [
            sql_rule("HOANG_PROD_VALUE", "NOT _negative_value AND (value IS NULL OR value >= 0)", "value", "Production measures cannot be negative, including values rounded to zero"),
            sql_rule("HOANG_PROD_UNIT", "_unit_mapping_valid AND NOT _conversion_overflow", "unit", "Unsupported element/unit pair or conversion overflow"),
            sql_rule("HOANG_PROD_ELEMENT", "(element_code = '5312' AND element = 'Area harvested') OR "
                     "(element_code = '5412' AND element = 'Yield') OR "
                     "(element_code = '5510' AND element = 'Production')", "element", "Element code/name must agree with source definitions"),
        ]
