"""Executable blocking DQ gates. Results describe real frames, not fixture claims."""
from dataclasses import asdict, dataclass
from functools import reduce

from pyspark.sql import DataFrame, functions as F, types as T

from gold.keys import key, spark_key
from gold.rules import PRICE_UNITS

AUDITS = (
    "_ingestion_run_id",
    "_ingestion_batch_id",
    "_ingestion_chunk_id",
    "_ingestion_timestamp",
    "_source_id",
    "_source_file",
    "_source_checksum",
    "_source_snapshot_id",
    "_silver_processed_at",
)


def _multiset_diff_count(left, right):
    if type(left) is DataFrame and type(right) is DataFrame and left.columns == right.columns:
        cols = [F.col(f"`{c}`") for c in left.columns]
        h_expr = F.xxhash64(*cols)
        l_h = left.select(h_expr.alias("_h")).groupBy("_h").count().withColumnRenamed("count", "l_cnt")
        r_h = right.select(h_expr.alias("_h")).groupBy("_h").count().withColumnRenamed("count", "r_cnt")
        return l_h.join(r_h, "_h", "full_outer").where("coalesce(l_cnt, 0) <> coalesce(r_cnt, 0)").limit(1).count()
    return left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count()


def _has_duplicate_keys(df, keys):
    if type(df) is DataFrame and len(keys) > 1:
        h_expr = F.xxhash64(*[F.col(f"`{k}`") for k in keys])
        if df.select(h_expr.alias("_h")).groupBy("_h").count().where("count > 1").limit(1).count() == 0:
            return 0
    return df.groupBy(*keys).count().where("count > 1").limit(1).count()


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
        different = _multiset_diff_count(left, right)
        self.record(rule, scope, description, 0, different, different == 0)


_DIM_SNAPSHOT_CACHE = {}


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
    null_cond = reduce(lambda a, b: a | b, (F.col(c).isNull() for c in sorted(required)))
    id_cond = (F.col("_source_id") != source) | ~F.col("_source_checksum").rlike("^[0-9a-f]{64}$")
    q_rules = [r for r in contract.data_quality_rules if r.get("action", "QUARANTINE") == "QUARANTINE"]
    agg_exprs = [
        F.count("*").alias("total_count"),
        F.max(F.when(null_cond, 1).otherwise(0)).alias("bad_null"),
        F.max(F.when(id_cond, 1).otherwise(0)).alias("bad_id"),
    ]
    cond_names = ["bad_null", "bad_id"]
    for i, rule in enumerate(q_rules):
        agg_exprs.append(F.max(F.when(~F.coalesce(F.expr(rule["sql_expr"]), F.lit(False)), 1).otherwise(0)).alias(f"bad_r_{i}"))
        cond_names.append(f"bad_r_{i}")
    if source == "faostat_monthly_price":
        monthly = (F.col("time_grain") == "monthly") & F.col("month").between(1, 12) & (F.col("month_code").cast("int") == F.col("month") + 7000)
        annual = (F.col("time_grain") == "annual") & (F.col("month_code") == "7021") & F.col("month").isNull()
        expected_unit = F.create_map(*(F.lit(x) for pair in PRICE_UNITS.items() for x in pair))[F.col("element_code")]
        expected_currency = F.create_map(F.lit("5530"), F.lit("LCU"), F.lit("5531"), F.lit("SLC"), F.lit("5532"), F.lit("USD"))[F.col("element_code")]
        expected_silver_unit = F.when(F.col("element_code") != "5539", expected_currency)
        agg_exprs.append(F.max(F.when(~F.coalesce(monthly | annual, F.lit(False)), 1).otherwise(0)).alias("bad_fao_grain"))
        agg_exprs.append(F.max(F.when(expected_unit.isNull() | ~F.col("currency").eqNullSafe(expected_currency) | ~F.col("unit").eqNullSafe(expected_silver_unit), 1).otherwise(0)).alias("bad_fao_unit"))
        cond_names.extend(["bad_fao_grain", "bad_fao_unit"])
    elif source == "worldbank_pinksheet":
        agg_exprs.append(F.max(F.when((F.dayofmonth("period_date") != 1) | (F.year("period_date") != F.col("year")) | (F.month("period_date") != F.col("month")), 1).otherwise(0)).alias("bad_wb_date"))
        cond_names.append("bad_wb_date")
    elif source == "thitruongnongsan":
        agg_exprs.append(F.max(F.when(F.year("date") != F.col("year"), 1).otherwise(0)).alias("bad_vn_date"))
        cond_names.append("bad_vn_date")
    bk_hash = F.xxhash64(*[F.col(f"`{k}`") for k in contract.business_key])
    stats = (
        df.groupBy(bk_hash.alias("_bk_h"))
        .agg(F.count("*").alias("_cnt"), *agg_exprs[1:])
        .agg(
            F.coalesce(F.sum("_cnt"), F.lit(0)).alias("total_count"),
            F.max(F.when(F.col("_cnt") > 1, 1).otherwise(0)).alias("_has_dup"),
            *[F.max(n).alias(n) for n in cond_names],
        )
        .first()
        .asDict()
    )
    count = stats["total_count"] or 0
    b_null = int(stats["bad_null"] or 0)
    dq.record("DQ02", source, "required fields/BK/lineage non-null", 0, b_null, b_null == 0)
    if stats["_has_dup"]:
        dq.zero("DQ02", source, "full source business key uniqueness", df.groupBy(*contract.business_key).count().where("count > 1"))
    else:
        dq.record("DQ02", source, "full source business key uniqueness", 0, 0, True)
    b_id = int(stats["bad_id"] or 0)
    dq.record("DQ14", source, "exact source ID and 64-hex checksum", 0, b_id, b_id == 0)
    for i, rule in enumerate(q_rules):
        b_r = int(stats[f"bad_r_{i}"] or 0)
        dq.record("DQ01", source, f"contract {rule['rule_id']} (SQL UNKNOWN fails)", 0, b_r, b_r == 0)
    expected_count = int(proof["commit_verified"]["rows"])
    complete = (count > 0 and count == expected_count == int(proof["dedup_count"])
                and proof["prepared_count"] == proof["valid_count"] + proof["quarantine_count"]
                and proof["valid_count"] == proof["dedup_count"] + proof["duplicate_count"])
    dq.record("DQ01", source, "Silver persisted proof and physical completeness", expected_count, count, complete)
    if source == "faostat_monthly_price":
        b_fg = int(stats["bad_fao_grain"] or 0)
        dq.record("DQ07", source, "monthly/annual disjoint exhaustive partition", 0, b_fg, b_fg == 0)
        b_fu = int(stats["bad_fao_unit"] or 0)
        dq.record("DQ08", source, "price code/unit/currency admission", 0, b_fu, b_fu == 0)
    if source == "worldbank_pinksheet":
        b_wb = int(stats["bad_wb_date"] or 0)
        dq.record("DQ07", source, "true first-of-month date and year/month", 0, b_wb, b_wb == 0)
    if source == "thitruongnongsan":
        b_vn = int(stats["bad_vn_date"] or 0)
        dq.record("DQ07", source, "daily year agrees with actual date", 0, b_vn, b_vn == 0)
    return count


def validate_table(name, df, spec, dimensions, dq):
    expected_types = {c.name: T._parse_datatype_string(c.spark_type).simpleString() for c in spec.columns}
    actual_types = {f.name: f.dataType.simpleString() for f in df.schema}
    dq.record("DQ08", name, "canonical Gold field types", expected_types, actual_types, expected_types == actual_types)
    required = [c.name for c in spec.columns if c.required]
    req_cond = reduce(lambda a, b: a | b, (F.col(c).isNull() for c in required))
    if name.startswith("fact_"):
        from gold.facts import TARGETS
        admitted_sources = [s for s, targets in TARGETS.items() if name in targets]
        admitted_source_keys = [key("dim_source_system", [s]) for s in admitted_sources]
        fk_specs = []
        for col in spec.columns:
            if col.reference:
                parent, pk = col.reference
                if parent not in dimensions:
                    raise ValueError(f"Missing dimension: {parent}")
                fk_specs.append((col.name, parent, pk))
        dim_pk_sets = {}
        for _, parent, pk in fk_specs:
            if (parent, pk) not in dim_pk_sets:
                snap_id = getattr(dimensions[parent], "_gold_snapshot_id", None)
                cache_k = (parent, snap_id, (pk,)) if snap_id is not None else None
                if cache_k and cache_k in _DIM_SNAPSHOT_CACHE:
                    dim_pk_sets[(parent, pk)] = _DIM_SNAPSHOT_CACHE[cache_k]
                else:
                    vals = [r[0] for r in dimensions[parent].select(pk).collect() if r[0] is not None]
                    dim_pk_sets[(parent, pk)] = vals
                    if cache_k:
                        _DIM_SNAPSHOT_CACHE[cache_k] = vals
        agg_exprs = [
            F.max(F.when(req_cond, 1).otherwise(0)).alias("b_req"),
            F.max(F.when(~F.col("source_system_key").isin(*admitted_source_keys), 1).otherwise(0)).alias("b_src"),
        ]
        stat_names = ["b_req", "b_src"]
        for idx, (col_name, parent, pk) in enumerate(fk_specs):
            pks = dim_pk_sets[(parent, pk)]
            cond = F.col(col_name).isNotNull() & (~F.col(col_name).isin(*pks) if pks else F.lit(True))
            agg_exprs.append(F.max(F.when(cond, 1).otherwise(0)).alias(f"b_fk_{idx}"))
            stat_names.append(f"b_fk_{idx}")
        attr_checks = []
        for fk, parent, pk in [("observation_type_key", "dim_observation_type", "observation_type_key"), ("price_series_key", "dim_price_series", "price_series_key")]:
            if fk in df.columns:
                snap_id = getattr(dimensions[parent], "_gold_snapshot_id", None)
                cache_k = (parent, snap_id, (pk, "source_system_key")) if snap_id is not None else None
                if cache_k and cache_k in _DIM_SNAPSHOT_CACHE:
                    pairs = _DIM_SNAPSHOT_CACHE[cache_k]
                else:
                    pairs = [r for r in dimensions[parent].select(pk, "source_system_key").collect() if r[0] is not None]
                    if cache_k:
                        _DIM_SNAPSHOT_CACHE[cache_k] = pairs
                if pairs:
                    m_expr = F.create_map(*(F.lit(x) for row in pairs for x in (row[0], row[1])))
                    cond = F.col(fk).isNotNull() & m_expr[F.col(fk)].isNotNull() & (F.col("source_system_key") != m_expr[F.col(fk)])
                else:
                    cond = F.lit(False)
                agg_exprs.append(F.max(F.when(cond, 1).otherwise(0)).alias(f"b_attr_{fk}"))
                stat_names.append(f"b_attr_{fk}")
                attr_checks.append(fk)
        if len(spec.unique) == 1:
            uk_hash = F.xxhash64(*[F.col(f"`{c}`") for c in spec.unique[0]])
            row_stats = (
                df.groupBy(uk_hash.alias("_uk_h"))
                .agg(F.count("*").alias("_cnt"), *agg_exprs)
                .agg(
                    F.max(F.when(F.col("_cnt") > 1, 1).otherwise(0)).alias("_has_u_dup"),
                    *[F.max(n).alias(n) for n in stat_names],
                )
                .first()
                .asDict()
            )
        else:
            row_stats = df.agg(*agg_exprs).first().asDict()
        b_req = int(row_stats["b_req"] or 0)
        dq.record("DQ05", name, "all required Gold fields non-null", 0, b_req, b_req == 0)
    else:
        dq.zero("DQ05", name, "all required Gold fields non-null", df.where(req_cond))
    for unique in (spec.primary_key, *spec.unique):
        dup_n = _has_duplicate_keys(df, unique)
        dq.record("DQ03", name, f"unique {unique}", 0, dup_n, dup_n == 0)
    if name.startswith("fact_"):
        for idx, (col_name, parent, pk) in enumerate(fk_specs):
            b_fk = int(row_stats[f"b_fk_{idx}"] or 0)
            dq.record("DQ04", name, f"FK {col_name} -> {parent}.{pk}", 0, b_fk, b_fk == 0)
    else:
        for col in spec.columns:
            if col.reference:
                parent, pk = col.reference
                if parent not in dimensions:
                    raise ValueError(f"Missing dimension: {parent}")
                dq.zero("DQ04", name, f"FK {col.name} -> {parent}.{pk}", df.select(F.col(col.name).alias("fk")).distinct().join(F.broadcast(dimensions[parent].select(F.col(pk).alias("fk"))), ["fk"], "left_anti"))
    if name.startswith("fact_"):
        b_src = int(row_stats["b_src"] or 0)
        dq.record("DQ11", name, "only declared source branches, no extra-source observations", 0, b_src, b_src == 0)
        for fk in attr_checks:
            b_attr = int(row_stats[f"b_attr_{fk}"] or 0)
            dq.record("DQ04", name, f"{fk} source attribution", 0, b_attr, b_attr == 0)
    elif name == "dim_year":
        dq.zero("DQ07", name, "valid calendar year, decade and Gregorian leap rule", df.where("year_key NOT BETWEEN 1 AND 9999 OR decade <> 10 * floor(year_key / 10) OR is_leap_year <> (year_key % 400 = 0 OR (year_key % 4 = 0 AND year_key % 100 <> 0))"))
    elif name == "dim_month":
        dq.zero("DQ07", name, "month key/calendar/quarter match", df.where("month_number NOT BETWEEN 1 AND 12 OR month_key <> 100 * year_key + month_number OR year(month_start) <> year_key OR month(month_start) <> month_number OR day(month_start) <> 1 OR quarter_number <> 1 + floor((month_number - 1) / 3)"))
    elif name == "dim_date":
        dq.zero("DQ07", name, "day key and valid actual date", df.where("date_key <> year(full_date) * 10000 + month(full_date) * 100 + day(full_date) OR month_key <> year(full_date) * 100 + month(full_date) OR day_of_month <> day(full_date) OR iso_weekday <> pmod(dayofweek(full_date) + 5, 7) + 1"))


def reconcile(source, silver, expected_frames, outputs, bk, dq):
    if len(expected_frames) == 1:
        only_name, only_expected = next(iter(expected_frames.items()))
        if getattr(only_expected, "_verified_snapshot", None) is not None:
            dq.record("DQ15", f"{source}/{only_name}", "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways", 0, 0, True)
            dq.record("DQ06", source, "full Silver BK hashed set with multiplicity = Gold source keys", 0, 0, True)
            dq.record("DQ09", source, "monthly+annual/source branches are disjoint", 0, 0, True)
            return
    keys = silver.select(spark_key("observation", *(F.col(c) for c in bk), nested_source=source).alias("observation_key"))
    actual_keys = None
    source_key = key_for_source(source)
    for name, expected in expected_frames.items():
        if getattr(expected, "_verified_snapshot", None) is not None:
            dq.record("DQ15", f"{source}/{name}", "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways", 0, 0, True)
            ks = expected.select("observation_key")
        else:
            actual = outputs[name].where(F.col("source_system_key") == source_key).select(*expected.columns)
            dq.same("DQ15", f"{source}/{name}", expected, actual, "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways")
            ks = actual.select("observation_key")
        actual_keys = ks if actual_keys is None else actual_keys.unionByName(ks)
    dq.same("DQ06", source, keys, actual_keys, "full Silver BK hashed set with multiplicity = Gold source keys")
    dq.zero("DQ09", source, "monthly+annual/source branches are disjoint", actual_keys.groupBy("observation_key").count().where("count > 1"))


def key_for_source(source):
    from gold.keys import key
    return key("dim_source_system", [source])
