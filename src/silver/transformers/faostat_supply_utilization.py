"""Long FAOSTAT SUA observations; preserve product, elements, flags and notes."""

from pyspark.sql import functions as F

from silver.transformers.faostat_source import FaostatSourceTransformer, sql_rule


# Code/name pairs observed in both local snapshot and live Bronze Trino query.
SUA_ELEMENTS = {"5016": "Loss", "5023": "Processed", "5071": "Stock Variation",
                "5113": "Opening stocks", "5141": "Food supply quantity (tonnes)",
                "5165": "Other uses (non-food)", "5166": "Residuals",
                "5510": "Production", "5520": "Feed", "5525": "Seed",
                "5610": "Import quantity", "5910": "Export quantity"}


class FaostatSupplyUtilizationTransformer(FaostatSourceTransformer):
    dataset_id = "faostat_supply_utilization"

    def preprocess(self, df):
        df = self.prepare(df)
        valid_unit = F.lower(F.col("unit")).isin("t", "tonne")
        return (df.withColumn("_unit_mapping_valid", valid_unit)
                .withColumn("unit", F.when(valid_unit, "tonne").otherwise(F.col("unit"))))

    def quality_rules(self):
        pairs = " OR ".join(f"(element_code = '{code}' AND element = '{name}')"
                            for code, name in SUA_ELEMENTS.items())
        return self.common_rules() + [
            sql_rule("HOANG_SUA_VALUE", "NOT _negative_value OR element_code = '5071'", "value", "Published contract permits negative Stock Variation only; Residuals needs approval"),
            sql_rule("HOANG_SUA_UNIT", "_unit_mapping_valid", "unit", "Only observed tonne units are supported for SUA"),
            sql_rule("HOANG_SUA_ELEMENT", pairs, "element", "Unknown or conflicting SUA element definition"),
        ]
