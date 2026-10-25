"""Unpivot Pink Sheet including missing and invalid monthly observations."""
import re
import logging
from pyspark.sql import functions as F
from silver.transformers.faostat_source import source_values

# Suffixes are produced by the Bronze adapter from the workbook unit row.
UNITS = {"mt": "USD/tonne", "kg": "USD/kg", "bbl": "USD/barrel",
         "mmbtu": "USD/MMBtu", "troy_oz": "USD/troy_oz",
         "cubic_meter": "USD/cubic_meter", "dmtu": "USD/dmtu",
         "cents_sheet": "cents/sheet", "sheet": "cents/sheet",
         "2010_100": "index(2010=100)"}


class WorldbankPinksheetTransformer:
    def preprocess(self, df):
        if "period" not in df.columns:
            raise ValueError("World Bank missing period column")
        columns = sorted(c for c in df.columns if not c.startswith("_") and c != "period")
        # Excel formatting extends the sheet beyond real series. Bronze names
        # missing headers Commodity_N. Remove only proven entirely-null synthetic
        # columns; a named series with missing prices is still an observation.
        synthetic = [c for c in columns if re.fullmatch(r"commodity_[0-9]+", c)]
        if synthetic:
            counts = df.agg(*[F.count(F.col(c)).alias(c) for c in synthetic]).first().asDict()
            empty = {c for c in synthetic if counts[c] == 0}
            columns = [c for c in columns if c not in empty]
            logging.getLogger("silver.worldbank").info("Excluded empty synthetic workbook columns: %s", sorted(empty))
        if not columns:
            raise ValueError("World Bank has no commodity columns")
        entries = []
        for column in columns:
            name = column.rstrip("_")
            unit, unit_token = None, None
            for suffix in sorted(UNITS, key=len, reverse=True):
                match = re.search(r"_+" + suffix + r"$", name)
                if match:
                    name = name[:match.start()].rstrip("_")
                    unit = UNITS[suffix]
                    unit_token = suffix
                    break
            entries.append(F.struct(F.lit(name).alias("commodity_code"),
                                    F.lit(name.replace("_", " ").title()).alias("commodity_name"),
                                    F.lit(unit).cast("string").alias("unit"),
                                    F.lit(unit_token).cast("string").alias("_source_unit_token"),
                                    F.col(column).cast("string").alias("price")))
        keep = [c for c in df.columns if c.startswith("_") or c == "period"]
        result = df.select(*keep, F.explode(F.array(*entries)).alias("observation")).select(*keep, "observation.*")
        result = source_values(result, "price", "_source_unit_token", ("�", "…", "...", "..", "NA"))
        result = result.withColumn("period", F.upper(F.trim("period")))
        result = result.withColumn("year", F.expr("try_cast(substring(period, 1, 4) as int)"))
        result = result.withColumn("month", F.expr("try_cast(substring(period, 6, 2) as int)"))
        result = result.withColumn("period_date", F.when(F.col("period").rlike(r"^[0-9]{4}M[0-9]{2}$"),
            F.expr("try_cast(concat(substring(period,1,4),'-',substring(period,6,2),'-01') as date)")))
        # The workbook uses Unicode replacement characters as missing markers.
        result = result.withColumn("price", F.when(F.trim("price").isin("�", "…", "...", "..", "NA"), F.lit(None)).otherwise(F.col("price")))
        return result.withColumn("series_name", F.lit(None).cast("string"))

    def quality_rules(self):
        return [{"rule_id": "WB_UNIT", "sql_expr": "unit IS NOT NULL", "failed_column": "unit", "rule": "Unknown unit suffix requires review"}]
