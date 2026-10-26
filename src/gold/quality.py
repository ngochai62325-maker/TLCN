"""Executable blocking DQ gates. Results describe real frames, not fixture claims."""
from dataclasses import asdict, dataclass
from functools import reduce

from pyspark.sql import functions as F, types as T

from gold.facts import AUDITS
from gold.keys import spark_key
from gold.rules import PRICE_UNITS


@dataclass
class Result:
    rule_id: str
    scope: str
    implementation: str
    expected: object
    actual: object
    status: str
    input_snapshots: dict
    error_details: str = ""
    action_taken: str = "verified"


class Quality:
    def __init__(self, pins):
        self.pins = pins
        self.results = []

    def record(self, rule, scope, implementation, expected, actual, ok=True, detail=""):
        result = Result(rule, scope, implementation, expected, actual, "ACCEPT" if ok else "FAIL", self.pins, detail, "verified" if ok else "publication blocked; observations preserved")
        self.results.append(asdict(result))
        if not ok:
            raise ValueError(f"{rule} {scope}: {detail or implementation}; actual={actual!r}")

    def zero(self, rule, scope, implementation, bad):
        n = bad.limit(1).count()
        self.record(rule, scope, implementation, 0, n, n == 0)

    def same(self, rule, scope, left, right, description="EXCEPT ALL in both directions"):
        # Exact distributed multiset equality, not a checksum or count-only comparison.
        different = left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count()
        self.record(rule, scope, description, 0, different, different == 0)


def validate_input(source, df, contract, pin, proof, dq):
    required = {c["name"] for c in contract.columns if not c.get("nullable", True)} | set(contract.business_key) | set(AUDITS) | {"_dq_warnings"}
    expected = {c["name"]: T._parse_datatype_string(c["data_type"]) for c in contract.columns}
    expected.update({c: T.StringType() for c in ("_ingestion_run_id", "_ingestion_batch_id", "_source_id", "_source_file", "_source_checksum")})
    expected.update({c: T.LongType() for c in ("_ingestion_chunk_id", "_source_snapshot_id")})
    expected.update({c: T.TimestampType() for c in ("_ingestion_timestamp", "_silver_processed_at")})
    expected["_dq_warnings"] = T.ArrayType(T.StructType([T.StructField(c, T.StringType()) for c in ("rule_id", "error_message", "failed_column")]))
    actual = {f.name: f.dataType.simpleString() for f in df.schema}
    schema_ok = actual == {k: v.simpleString() for k, v in expected.items()}
    dq.record("DQ01", source, "exact pinned Silver columns/types", True, schema_ok, schema_ok)
    dq.zero("DQ02", source, "required fields/BK/lineage non-null", df.where(reduce(lambda a, b: a | b, (F.col(c).isNull() for c in sorted(required)))))
    dq.zero("DQ02", source, "full source business key uniqueness", df.groupBy(*contract.business_key).count().where("count > 1"))
    dq.zero("DQ14", source, "exact source ID and 64-hex checksum", df.where((F.col("_source_id") != source) | ~F.col("_source_checksum").rlike("^[0-9a-f]{64}$")))
    for rule in contract.data_quality_rules:
        if rule.get("action", "QUARANTINE") == "QUARANTINE":
            dq.zero("DQ01", source, f"contract {rule['rule_id']} (SQL UNKNOWN fails)", df.where(~F.coalesce(F.expr(rule["sql_expr"]), F.lit(False))))
    count = df.count()
    expected_count = int(proof["commit_verified"]["rows"])
    complete = (count > 0 and count == expected_count == int(proof["dedup_count"])
                and proof["prepared_count"] == proof["valid_count"] + proof["quarantine_count"]
                and proof["valid_count"] == proof["dedup_count"] + proof["duplicate_count"])
    dq.record("DQ01", source, "Silver persisted proof and physical completeness", expected_count, count, complete)
    if source == "faostat_monthly_price":
        monthly = (F.col("time_grain") == "monthly") & F.col("month").between(1, 12) & (F.col("month_code").cast("int") == F.col("month") + 7000)
        annual = (F.col("time_grain") == "annual") & (F.col("month_code") == "7021") & F.col("month").isNull()
        dq.zero("DQ07", source, "monthly/annual disjoint exhaustive partition", df.where(~F.coalesce(monthly | annual, F.lit(False))))
        expected_unit = F.create_map(*(F.lit(x) for pair in PRICE_UNITS.items() for x in pair))[F.col("element_code")]
        expected_currency = F.create_map(F.lit("5530"), F.lit("LCU"), F.lit("5531"), F.lit("SLC"), F.lit("5532"), F.lit("USD"))[F.col("element_code")]
        expected_silver_unit = F.when(F.col("element_code") != "5539", expected_currency)
        dq.zero("DQ08", source, "price code/unit/currency admission", df.where(expected_unit.isNull() | ~F.col("currency").eqNullSafe(expected_currency) | ~F.col("unit").eqNullSafe(expected_silver_unit)))
    if source == "worldbank_pinksheet":
        dq.zero("DQ07", source, "true first-of-month date and year/month", df.where((F.dayofmonth("period_date") != 1) | (F.year("period_date") != F.col("year")) | (F.month("period_date") != F.col("month"))))
    if source == "thitruongnongsan":
        dq.zero("DQ07", source, "daily year agrees with actual date", df.where(F.year("date") != F.col("year")))
    return count


def validate_table(name, df, spec, dimensions, dq):
    expected_types = {c.name: T._parse_datatype_string(c.spark_type).simpleString() for c in spec.columns}
    actual_types = {f.name: f.dataType.simpleString() for f in df.schema}
    dq.record("DQ08", name, "canonical Gold field types", expected_types, actual_types, expected_types == actual_types)
    required = [c.name for c in spec.columns if c.required]
    dq.zero("DQ05", name, "all required Gold fields non-null", df.where(reduce(lambda a, b: a | b, (F.col(c).isNull() for c in required))))
    for unique in (spec.primary_key, *spec.unique):
        dq.zero("DQ03", name, f"unique {unique}", df.groupBy(*unique).count().where("count > 1"))
    for col in spec.columns:
        if col.reference:
            parent, pk = col.reference
            if parent not in dimensions:
                raise ValueError(f"Missing dimension: {parent}")
            dq.zero("DQ04", name, f"FK {col.name} -> {parent}.{pk}", df.select(F.col(col.name).alias("fk")).distinct().join(dimensions[parent].select(F.col(pk).alias("fk")), ["fk"], "left_anti"))
    if name.startswith("fact_"):
        from gold.facts import TARGETS
        admitted_sources = [s for s, targets in TARGETS.items() if name in targets]
        dq.zero("DQ11", name, "only declared source branches, no extra-source observations", df.where(~F.col("_source_id").isin(*admitted_sources)))
        # Hash collision / wrong identity binding is observable against retained tuples.
        expected_key = F.udf(lambda s, j: __import__("gold.keys", fromlist=["key"]).key("observation", [s, __import__("json").loads(j)]), T.StringType())(F.col("_source_id"), F.col("source_business_key_json"))
        dq.zero("DQ03", name, "K1 recomputed from exact retained source tuple", df.where(F.col("observation_key") != expected_key))
        dq.zero("DQ14", name, "real Silver identity not upstream placeholder", df.where((F.col("silver_read_snapshot_id") <= 0) | (F.length("silver_table_uuid") < 32) | F.col("dq_warnings_json").isNull()))
        expected_source = spark_key("dim_source_system", F.col("_source_id"))
        dq.zero("DQ04", name, "source FK agrees with provenance", df.where(F.col("source_system_key") != expected_source))
        for fk, parent, pk in [("observation_type_key", "dim_observation_type", "observation_type_key"), ("price_series_key", "dim_price_series", "price_series_key")]:
            if fk in df.columns:
                check = df.select(fk, F.col("source_system_key").alias("fact_source")).distinct().join(dimensions[parent].select(pk, F.col("source_system_key").alias("dimension_source")), fk)
                dq.zero("DQ04", name, f"{fk} source attribution", check.where("fact_source <> dimension_source"))
    elif name == "dim_year":
        dq.zero("DQ07", name, "valid calendar year, decade and Gregorian leap rule", df.where("year_key NOT BETWEEN 1 AND 9999 OR decade <> 10 * floor(year_key / 10) OR is_leap_year <> (year_key % 400 = 0 OR (year_key % 4 = 0 AND year_key % 100 <> 0))"))
    elif name == "dim_month":
        dq.zero("DQ07", name, "month key/calendar/quarter match", df.where("month_number NOT BETWEEN 1 AND 12 OR month_key <> 100 * year_key + month_number OR year(month_start) <> year_key OR month(month_start) <> month_number OR day(month_start) <> 1 OR quarter_number <> 1 + floor((month_number - 1) / 3)"))
    elif name == "dim_date":
        dq.zero("DQ07", name, "day key and valid actual date", df.where("date_key <> year(full_date) * 10000 + month(full_date) * 100 + day(full_date) OR month_key <> year(full_date) * 100 + month(full_date) OR day_of_month <> day(full_date) OR iso_weekday <> pmod(dayofweek(full_date) + 5, 7) + 1"))


def reconcile(source, silver, expected_frames, outputs, bk, dq):
    keys = silver.select(spark_key("observation", *(F.col(c) for c in bk), nested_source=source).alias("observation_key"))
    actual_keys = None
    source_key = key_for_source(source)
    for name, expected in expected_frames.items():
        actual = outputs[name].where(F.col("source_system_key") == source_key).select(*expected.columns)
        dq.same("DQ15", f"{source}/{name}", expected, actual, "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways")
        ks = actual.select("observation_key")
        actual_keys = ks if actual_keys is None else actual_keys.unionByName(ks)
    dq.same("DQ06", source, keys, actual_keys, "full Silver BK hashed set with multiplicity = Gold source keys")
    dq.zero("DQ09", source, "monthly+annual/source branches are disjoint", actual_keys.groupBy("observation_key").count().where("count > 1"))


def key_for_source(source):
    from gold.keys import key
    return key("dim_source_system", [source])
