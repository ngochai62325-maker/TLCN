"""Apply only verified, dated, exact NSO geography entries; reject overlap."""
from datetime import date

from pyspark.sql import functions as F


def verified_entries(entries):
    selected = [e for e in entries if e.get("status") in ("APPROVED", "VERIFIED")]
    spans = {}
    for entry in selected:
        required = ("source_table", "raw_label", "canonical_name", "code", "code_scheme",
                    "geography_level", "valid_from", "valid_to", "source_reference")
        if any(not entry.get(k) for k in required):
            raise ValueError("Verified geography mapping needs code, dates and source reference")
        start, end = date.fromisoformat(entry["valid_from"]), date.fromisoformat(entry["valid_to"])
        if start > end:
            raise ValueError("Invalid geography validity interval")
        key = entry["source_table"], entry["raw_label"]
        for first, last in spans.get(key, []):
            if start <= last and first <= end:
                raise ValueError("Overlapping exact geography mappings would multiply observations")
        spans.setdefault(key, []).append((start, end))
    return selected


def apply_historical_geography(df, entries):
    selected = verified_entries(entries)
    if not selected:
        return df
    # Explicit schema also permits all NULL parent fields.
    schema = "source_table string, raw_label string, canonical_name string, code string, code_scheme string, geography_level string, parent_geography string, valid_from string, valid_to string, source_reference string"
    keys = [part.split()[0] for part in schema.split(", ")]
    mapping = df.sparkSession.createDataFrame([tuple(e.get(k) for k in keys) for e in selected], schema).alias("m")
    base = df.alias("b")
    condition = ((F.col("b.source_table") == F.col("m.source_table")) &
                 (F.col("b.geography_name_raw") == F.col("m.raw_label")) &
                 (F.make_date(F.col("b.year"), F.lit(1), F.lit(1)) >= F.to_date("m.valid_from")) &
                 (F.make_date(F.col("b.year"), F.lit(12), F.lit(31)) <= F.to_date("m.valid_to")))
    matched = F.col("m.code").isNotNull()
    replacements = {"geography_name": "canonical_name", "geography_code": "code", "geography_code_scheme": "code_scheme",
                    "geography_level": "geography_level", "parent_geography": "parent_geography",
                    "geography_valid_from": "valid_from", "geography_valid_to": "valid_to", "geography_source_reference": "source_reference"}
    columns = []
    for c in df.columns:
        if c in replacements:
            value = F.col("m." + replacements[c])
            if c.startswith("geography_valid_"):
                value = F.to_date(value)
            columns.append(F.when(matched, value).otherwise(F.col("b." + c)).alias(c))
        elif c == "mapping_status":
            columns.append(F.when(matched, F.lit("verified")).otherwise(F.col("b." + c)).alias(c))
        elif c == "province_code":
            columns.append(F.when(matched & (F.col("m.geography_level") == "province"), F.col("m.code"))
                           .otherwise(F.col("b." + c)).alias(c))
        else:
            columns.append(F.col("b." + c))
    return base.join(F.broadcast(mapping), condition, "left").select(*columns)
