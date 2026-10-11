"""Monthly/annual FAOSTAT producer prices and price indices; no FX conversion."""

from pyspark.sql import functions as F

from silver.transformers.faostat_source import FaostatSourceTransformer, identifier, text_column, sql_rule


MONTH_NAMES = ("January", "February", "March", "April", "May", "June", "July",
               "August", "September", "October", "November", "December")


class FaostatMonthlyPriceTransformer(FaostatSourceTransformer):
    dataset_id = "faostat_monthly_price"
    # These are diagnostic proposal fields until the shared contract is approved.
    derived_columns = ("time_grain", "month", "currency", "price_kind")

    def preprocess(self, df):
        if not {"months_code", "months"}.issubset(df.columns):
            raise ValueError("faostat_monthly_price: missing Bronze Months Code/Months")
        df = self.prepare(df).withColumn("months_code", identifier("months_code"))
        df = df.withColumn("months", text_column("months"))
        code = F.col("months_code")
        monthly = code.isin(*[str(7001 + i) for i in range(12)])
        annual = code == "7021"
        df = df.withColumn("month", F.when(monthly, F.expr("try_cast(months_code as int)") - 7000))
        df = df.withColumn("time_grain", F.when(annual, "annual").when(monthly, "monthly"))
        element = F.col("element_code")
        currency = F.when(element == "5530", "LCU").when(element == "5531", "SLC").when(element == "5532", "USD")
        return (df.withColumn("currency", currency)
                .withColumn("price_kind", F.when(element == "5539", "producer_price_index")
                            .when(element.isin("5530", "5531", "5532"), "producer_price")))

    def quality_rules(self):
        month_pairs = " OR ".join(f"(month_code = '{7001+i}' AND month_name = '{name}')"
                                  for i, name in enumerate(MONTH_NAMES))
        return self.common_rules() + [
            sql_rule("HOANG_PRICE_VALUE", "NOT _negative_value AND (value IS NULL OR value >= 0)", "value", "Producer prices and indices cannot be negative"),
            sql_rule("HOANG_PRICE_DATE", f"(month_code = '7021' AND month_name = 'Annual value' AND month IS NULL) OR ({month_pairs})", "month_code", "Annual/monthly codes must agree with source labels"),
            sql_rule("HOANG_PRICE_UNIT", "(element_code = '5539' AND unit IS NULL) OR "
                     "(element_code IN ('5530','5531','5532') AND unit = currency)", "unit", "Index must not be treated as monetary price; source currencies remain distinct"),
            sql_rule("HOANG_PRICE_ELEMENT", "(element_code = '5530' AND element = 'Producer Price (LCU/tonne)') OR "
                     "(element_code = '5531' AND element = 'Producer Price (SLC/tonne)') OR "
                     "(element_code = '5532' AND element = 'Producer Price (USD/tonne)') OR "
                     "(element_code = '5539' AND element = 'Producer Price Index (2014-2016 = 100)')", "element", "Unknown or conflicting price element definition"),
        ]
