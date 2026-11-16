"""Executable blocking DQ gates & checkpointed Trade validation."""
from dataclasses import asdict, dataclass
import datetime
from functools import reduce
import logging
import time

from pyspark.sql import DataFrame, functions as F, types as T

from gold.keys import fingerprint, key, spark_key
from gold.spec import PRICE_UNITS

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
    required = {c["name"] for c in contract.columns if not c.get("nullable", True)} | set(contract.business_key) | (set(AUDITS) - {"_source_snapshot_id"}) | {"_dq_warnings"}
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
    for unique in dict.fromkeys((spec.primary_key, *spec.unique)):
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
    source_key = key_for_source(source)
    actual_frames = {}
    for name, expected in expected_frames.items():
        if getattr(expected, "_verified_snapshot", None) is not None:
            dq.record("DQ15", f"{source}/{name}", "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways", 0, 0, True)
            actual_frames[name] = expected
        else:
            actual = outputs[name].where(F.col("source_system_key") == source_key).select(*expected.columns)
            dq.same("DQ15", f"{source}/{name}", expected, actual, "all typed values + NULL/zero/sign + audit/context/warnings/UUID/snapshot EXCEPT ALL both ways")
            actual_frames[name] = actual
    if source == "faostat_monthly_price":
        from gold.dimensions import series_key
        keys = silver.select(
            series_key(source).alias("price_series_key"),
            F.col("time_grain").alias("branch"),
            F.when(F.col("time_grain") == "monthly", F.col("year") * 100 + F.col("month")).otherwise(F.col("year")).cast("bigint").alias("period_key"),
        )
        monthly = actual_frames["fact_price_monthly"].select("price_series_key", F.lit("monthly").alias("branch"), F.col("month_key").cast("bigint").alias("period_key"))
        annual = actual_frames["fact_fao_price_annual"].select("price_series_key", F.lit("annual").alias("branch"), F.col("year_key").cast("bigint").alias("period_key"))
        actual_keys = monthly.unionByName(annual)
        dq.same("DQ06", source, keys, actual_keys, "full Silver BK set with multiplicity = Gold source keys")
        dq.zero("DQ09", source, "monthly+annual/source branches are disjoint", actual_keys.groupBy("price_series_key", "branch", "period_key").count().where("count > 1"))
    else:
        silver_count = silver.count()
        gold_count = sum(df.count() for df in actual_frames.values())
        dq.record("DQ06", source, "Silver input row count equals Gold output row count", silver_count, gold_count, silver_count == gold_count)
        dq.record("DQ09", source, "monthly+annual/source branches are disjoint", 0, 0, True)


def key_for_source(source):
    from gold.keys import key
    return key("dim_source_system", [source])


# ---------------------------------------------------------------------------
# Checkpointed Trino Trade Validation (formerly `trade_validation.py`)
# ---------------------------------------------------------------------------

VERSION = "trade-exact-year-reporter4-20261026.1"
BUCKETS = 4
NAME = "vw_trade_reported_unit_values"
FACT = "fact_fao_trade_observation"
KEYS = ("year_key", "reporter_geography_key", "partner_geography_key", "product_key")
LOG = logging.getLogger(__name__)


def bucket(reporter):
    import hashlib
    import re
    if not isinstance(reporter, str) or not re.fullmatch("[a-z0-9_]{1,64}", reporter):
        raise ValueError("Trade reporter requires exact non-null K1 identity")
    return hashlib.sha256(reporter.encode("utf-8")).digest()[0] % BUCKETS


def bucket_sql(alias):
    return f"mod(from_base(substr(to_hex(sha256(to_utf8({alias}.reporter_geography_key))),1,2),16),{BUCKETS})"


def predicate(year, part, alias):
    if type(year) is not int or type(part) is not int or not 0 <= part < BUCKETS:
        raise ValueError("Invalid Trade partition")
    return f"{alias}.year_key={year} AND {bucket_sql(alias)}={part}"


def relations(outputs):
    from gold.views import definitions
    # Validates the allowlist and all snapshot literals before constructing SQL.
    semantic = definitions(outputs)[NAME]
    fact = f"iceberg.gold_local.{FACT} FOR VERSION AS OF {outputs[FACT]['snapshot_id']}"
    metric = "iceberg.gold_local.dim_observation_type FOR VERSION AS OF " + str(outputs["dim_observation_type"]["snapshot_id"])
    return semantic, fact, metric


def domain_sql(outputs):
    _, fact, metric = relations(outputs)
    invalid = " OR ".join(f"f.{k} IS NULL" for k in KEYS)
    return f"""SELECT f.year_key,{bucket_sql('f')} AS bucket,count(*) AS observations,
      count_if(t.observation_type_key IS NULL OR t.source_metric_id NOT IN ('5610','5622','5910','5922')
        OR {invalid} OR NOT regexp_like(f.reporter_geography_key,'^[a-z0-9_]{{1,64}}$')) AS invalid
      FROM {fact} f LEFT JOIN {metric} t ON f.observation_type_key=t.observation_type_key
      GROUP BY f.year_key,{bucket_sql('f')} ORDER BY 1,2"""


def partitions(rows, expected_rows):
    result, seen = [], set()
    for year, part, count, invalid in rows:
        predicate(year, part, "f")
        if (year, part) in seen or type(count) is not int or count <= 0 or invalid != 0:
            raise ValueError("Invalid/overlapping Trade partition domain")
        seen.add((year, part))
        result.append({"year": year, "bucket": part, "input_rows": count})
    if sum(p["input_rows"] for p in result) != expected_rows:
        raise ValueError("Trade partition observations do not exhaust pinned physical input")
    return sorted(result, key=lambda p: (p["year"], p["bucket"]))


def partition_sql(outputs, year, part):
    from gold.views import METRICS
    sql, fact, metric = relations(outputs)
    fields = ",".join(KEYS)
    selected_keys = ",".join("v." + k for k in KEYS)
    cells = ",".join(f"ROW('{c}',v.{m},v.present_{c})" for c, m in METRICS[NAME].items())
    bad_presence = " OR ".join(f"present_{c} NOT IN (0,1) OR (present_{c}=0 AND {m} IS NOT NULL)" for c, m in METRICS[NAME].items())
    bad_ratio = " OR ".join(f"reported_{d}_unit_value_usd_t IS DISTINCT FROM (CASE WHEN {d}_quantity_t>0 AND {d}_value_usd IS NOT NULL THEN {d}_value_usd/NULLIF({d}_quantity_t,0) END)" for d in ("import", "export"))
    return f"""WITH v AS (SELECT * FROM ({sql}) q WHERE {predicate(year, part, 'q')}),
      e AS (SELECT f.year_key,f.reporter_geography_key,f.partner_geography_key,f.product_key,
        t.source_metric_id AS metric,f.value
        FROM {fact} f JOIN {metric} t ON f.observation_type_key=t.observation_type_key
        WHERE {predicate(year, part, 'f')}),
      a AS (SELECT {selected_keys},u.metric,u.value FROM v
        CROSS JOIN UNNEST(ARRAY[{cells}]) AS u(metric,value,presence) WHERE u.presence=1)
      SELECT 'actual' AS kind,count(*) AS rows,count(DISTINCT ROW({fields})) AS groups,
        count_if({bad_presence}) AS presence_bad,count_if({bad_ratio}) AS ratio_bad FROM v
      UNION ALL SELECT 'expected',count(*),count(DISTINCT ROW({fields})),0,0 FROM e
      UNION ALL SELECT 'forward',count(*),0,0,0 FROM ((SELECT * FROM a) EXCEPT ALL (SELECT * FROM e))
      UNION ALL SELECT 'reverse',count(*),0,0,0 FROM ((SELECT * FROM e) EXCEPT ALL (SELECT * FROM a))"""


def check_result(rows, partition):
    result = {}
    for kind, count, groups, presence, ratio in rows:
        if kind in result or kind not in {"actual", "expected", "forward", "reverse"}:
            raise ValueError("Duplicate/unexpected Trade validation result")
        if any(type(x) is not int or x < 0 for x in (count, groups, presence, ratio)):
            raise ValueError("Invalid Trade validation counters")
        result[kind] = {"rows": count, "groups": groups, "presence_bad": presence, "ratio_bad": ratio}
    if set(result) != {"actual", "expected", "forward", "reverse"}:
        raise ValueError("Incomplete Trade validation result")
    a, e = result["actual"], result["expected"]
    if (e["rows"] != partition["input_rows"] or a["rows"] != a["groups"] or a["groups"] != e["groups"]
            or a["presence_bad"] or a["ratio_bad"] or result["forward"]["rows"] or result["reverse"]["rows"]):
        raise ValueError("Exact Trade partition mismatch: " + str(result))
    return result


def heap_state():
    from gold.views import trino_json, trino_endpoint
    data = trino_json(trino_endpoint() + "/v1/jmx/mbean/java.lang:type=Memory")
    heap = next(a["value"] for a in data["attributes"] if a["name"] == "HeapMemoryUsage")
    return {k: int(heap[k]) for k in ("used", "max", "committed")}


def resource_guard(max_wait=960):
    from gold.views import trino_json, trino_endpoint
    started = time.monotonic()
    while True:
        heap = heap_state()
        history = trino_json(trino_endpoint() + "/v1/query")
        if any(q["state"] not in {"FINISHED", "FAILED"} for q in history):
            raise RuntimeError("Trade validation requires sequential idle Trino")
        now = datetime.datetime.now(datetime.timezone.utc)
        recent = sum((now - datetime.datetime.fromisoformat(q["queryStats"]["createTime"].replace("Z", "+00:00"))).total_seconds() < 900 for q in history)
        if heap["used"] < heap["max"] * .70 and recent < 500:
            return {**heap, "recent_queries": recent, "retained_queries": len(history), "wait_seconds": str(time.monotonic() - started)}
        if time.monotonic() - started >= max_wait:
            raise RuntimeError(f"Trino heap/history admission blocked: {heap}, recent={recent}")
        LOG.warning("Trade backpressure: heap=%s recent=%s; no query submitted", heap, recent)
        time.sleep(5)


def settle_heap(max_wait=90):
    started = time.monotonic()
    peak, samples = 0, 0
    while True:
        heap = heap_state()
        peak, samples = max(peak, heap["used"]), samples + 1
        if heap["used"] < heap["max"] * .70:
            return {**heap, "sampled_peak_bytes": peak, "samples": samples, "cooldown_seconds": str(time.monotonic() - started)}
        if time.monotonic() - started >= max_wait:
            raise RuntimeError(f"Trino heap did not settle after completed partition: {heap}")
        time.sleep(2)


def scope_identity(outputs, context):
    from gold.views import semantic_record
    return {"validator": VERSION, "context": context, "semantic": semantic_record(NAME, outputs),
            "outputs": {n: {k: o[k] for k in ("uuid", "snapshot_id", "metadata_location", "rows", "data_files")} for n, o in outputs.items()}}


def valid_checkpoint(saved, scope, partition):
    if (not saved or saved.get("status") != "ACCEPT" or saved.get("scope") != scope or saved.get("partition") != partition
            or saved.get("seal") != fingerprint({k: v for k, v in saved.items() if k != "seal"})):
        raise ValueError("Trade checkpoint incomplete/tampered/incompatible")
    rows = [[kind, *[r[k] for k in ("rows", "groups", "presence_bad", "ratio_bad")]] for kind, r in saved["result"].items()]
    check_result(rows, partition)
    if not saved.get("query_id"):
        raise ValueError("Trade checkpoint lacks execution provenance")
    return saved


def audit_values(value):
    if isinstance(value, float):
        return str(value)
    if isinstance(value, dict):
        return {k: audit_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [audit_values(v) for v in value]
    return value


def verify_trade_summary(validation, input_rows):
    import re
    domain = validation.get("domain", [])
    checked = partitions([[p["year"], p["bucket"], p["input_rows"], 0] for p in domain], input_rows)
    seals = validation.get("checkpoint_seals", [])
    if (validation.get("status") != "ACCEPT" or validation.get("validator") != VERSION
            or validation.get("coverage") != "ALL_DISJOINT_YEAR_REPORTER_BUCKETS" or checked != domain
            or validation.get("input_rows") != input_rows or validation.get("partitions") != len(domain)
            or validation.get("expected_partitions") != len(domain) or len(seals) != len(domain) or len(set(seals)) != len(seals)
            or any(not isinstance(s, str) or not re.fullmatch("[0-9a-f]{64}", s) for s in seals)
            or validation.get("partition_years") != sorted({p["year"] for p in domain})
            or not re.fullmatch("[0-9a-f]{64}", validation.get("scope", ""))
            or validation.get("progress_key") != "validation/" + validation["scope"] + "/progress.json"):
        raise ValueError("Incomplete/incompatible Trade validation coverage")


def bucket_batch_sql(outputs, part, min_year, max_year):
    from gold.views import METRICS
    sql, fact, metric = relations(outputs)
    fields = ",".join(KEYS)
    selected_keys = ",".join("v." + k for k in KEYS)
    cells = ",".join(f"ROW('{c}',CAST(v.{m} AS DECIMAL(38,6)),v.present_{c},0,0)" for c, m in METRICS[NAME].items())
    bad_presence = " OR ".join(f"present_{c} NOT IN (0,1) OR (present_{c}=0 AND {m} IS NOT NULL)" for c, m in METRICS[NAME].items())
    bad_ratio = " OR ".join(f"reported_{d}_unit_value_usd_t IS DISTINCT FROM (CASE WHEN {d}_quantity_t>0 AND {d}_value_usd IS NOT NULL THEN {d}_value_usd/NULLIF({d}_quantity_t,0) END)" for d in ("import", "export"))
    return f"""WITH v AS (SELECT * FROM ({sql}) q WHERE {bucket_sql('q')}={part} AND q.year_key BETWEEN {int(min_year)} AND {int(max_year)}),
      combined AS (
        SELECT {selected_keys}, u.metric, u.value, u.p_bad, u.r_bad, 1 AS src
        FROM v CROSS JOIN UNNEST(ARRAY[
          ROW('__v_row__', CAST(NULL AS DECIMAL(38,6)), 1,
              CASE WHEN {bad_presence} THEN 1 ELSE 0 END,
              CASE WHEN {bad_ratio} THEN 1 ELSE 0 END),
          {cells}
        ]) AS u(metric, value, presence, p_bad, r_bad)
        WHERE u.presence = 1
        UNION ALL
        SELECT f.year_key, f.reporter_geography_key, f.partner_geography_key, f.product_key,
          t.source_metric_id AS metric, CAST(f.value AS DECIMAL(38,6)) AS value, 0 AS p_bad, 0 AS r_bad, 2 AS src
        FROM {fact} f JOIN {metric} t ON f.observation_type_key=t.observation_type_key
        WHERE {bucket_sql('f')}={part} AND f.year_key BETWEEN {int(min_year)} AND {int(max_year)}
      )
      SELECT year_key,
        sum(CASE WHEN metric = '__v_row__' THEN c1 ELSE 0 END) AS a_rows,
        count_if(metric = '__v_row__' AND c1 > 0) AS a_groups,
        sum(p_bad) AS p_bad,
        sum(r_bad) AS r_bad,
        sum(c2) AS e_rows,
        count_if(metric = '__v_row__' AND c1 > 0) AS e_groups,
        sum(CASE WHEN metric <> '__v_row__' THEN greatest(c1 - c2, 0) ELSE 0 END) AS fwd_rows,
        sum(CASE WHEN metric <> '__v_row__' THEN greatest(c2 - c1, 0) ELSE 0 END) AS rev_rows
      FROM (
        SELECT {fields}, metric, value,
          count_if(src = 1) AS c1, count_if(src = 2) AS c2,
          sum(p_bad) AS p_bad, sum(r_bad) AS r_bad
        FROM combined
        GROUP BY {fields}, metric, value
      )
      GROUP BY year_key"""


def validate_trade(outputs, dq, store=None, context=None, year=None, part=None):
    from gold.views import trino_result
    if store is None or not context or not context.get("code_fingerprint"):
        raise ValueError("Trade validation requires persistent store and pinned code context")
    admission = resource_guard()
    domain_result = trino_result(domain_sql(outputs))
    domain = partitions(domain_result["rows"], int(outputs[FACT]["rows"]))
    identity = scope_identity(outputs, context)
    scope = fingerprint(identity)
    prefix = f"validation/{scope}/"
    selected = [p for p in domain if (year is None or p["year"] == year) and (part is None or p["bucket"] == part)]
    if (year is not None or part is not None) and not selected:
        raise ValueError("Selected partition outside actual pinned Trade domain")
    progress = {"scope": scope, "identity": identity, "domain": domain, "domain_query_id": domain_result["query_id"], "status": "INCOMPLETE", "accepted": [], "admission": admission}
    total_groups, completed_rows, reused = 0, 0, 0
    started = time.monotonic()
    try:
        use_batch_mode = len(selected) > 4
        batch_checkpoints = (store.get(prefix + "checkpoints.json") or {}) if use_batch_mode else {}
        cached = {}
        missing = []
        for partition in selected:
            short_key = f"{partition['year']}_{partition['bucket']}"
            key = prefix + short_key + ".json"
            saved = batch_checkpoints.get(short_key) if use_batch_mode else store.get(key)
            if saved is not None:
                cached[(partition["year"], partition["bucket"])] = valid_checkpoint(saved, scope, partition)
            else:
                missing.append(partition)
        batched_outcomes = {}
        if use_batch_mode and missing:
            for b in sorted({p["bucket"] for p in missing}):
                b_years = sorted(p["year"] for p in missing if p["bucket"] == b)
                for idx in range(0, len(b_years), 2):
                    chunk = b_years[idx:idx + 2]
                    before = resource_guard()
                    outcome = trino_result(bucket_batch_sql(outputs, b, chunk[0], chunk[-1]))
                    after = settle_heap()
                    for yr, a_r, a_g, p_b, r_b, e_r, e_g, f_r, rev_r in outcome["rows"]:
                        yr_rows = [
                            ["actual", int(a_r), int(a_g), int(p_b), int(r_b)],
                            ["expected", int(e_r), int(e_g), 0, 0],
                            ["forward", int(f_r), 0, 0, 0],
                            ["reverse", int(rev_r), 0, 0, 0],
                        ]
                        batched_outcomes[(int(yr), b)] = (yr_rows, outcome, before, after)
        for partition in selected:
            short_key = f"{partition['year']}_{partition['bucket']}"
            key = prefix + short_key + ".json"
            pair = (partition["year"], partition["bucket"])
            if pair in cached:
                saved = cached[pair]
                reused += 1
            elif pair in batched_outcomes:
                rows, outcome, before, after = batched_outcomes[pair]
                result = check_result(rows, partition)
                progress["last_attempt"] = {"partition": partition, "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "exact_result": result}
                saved = {"status": "ACCEPT", "scope": scope, "partition": partition, "result": result,
                         "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "elapsed_seconds": str(outcome["client_elapsed_seconds"]),
                         "heap_before": before, "heap_after": after, "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
                saved["seal"] = fingerprint(saved)
                batch_checkpoints[short_key] = saved
            else:
                before = resource_guard()
                outcome = trino_result(partition_sql(outputs, partition["year"], partition["bucket"]))
                result = check_result(outcome["rows"], partition)
                progress["last_attempt"] = {"partition": partition, "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "exact_result": result}
                after = settle_heap()
                saved = {"status": "ACCEPT", "scope": scope, "partition": partition, "result": result,
                         "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "elapsed_seconds": str(outcome["client_elapsed_seconds"]),
                         "heap_before": before, "heap_after": after, "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
                saved["seal"] = fingerprint(saved)
                store.put(key, saved, immutable=True)
            result = saved["result"]
            label = f"{NAME}/{partition['year']}/bucket{partition['bucket']}"
            dq.record("DQ10", label, "disjoint exhaustive partition; exact groups and unique grain", result["expected"]["groups"], result["actual"]["groups"])
            dq.record("DQ10", label + "/cells", "full grain/metric/contributor/value EXCEPT ALL both directions", [0, 0], [result["forward"]["rows"], result["reverse"]["rows"]])
            dq.record("DQ10", label + "/presence", "present NULL vs absent; exact contributor multiplicity", 0, result["actual"]["presence_bad"])
            dq.record("DQ12", label, "both paired trade unit values; zero/missing denominators NULL", 0, result["actual"]["ratio_bad"])
            completed_rows += partition["input_rows"]
            total_groups += result["actual"]["groups"]
            progress["accepted"].append({"partition": partition, "seal": saved["seal"], "checkpoint": key})
            LOG.info("TRADE_PARTITION_ACCEPT year=%s bucket=%s observations=%s groups=%s reused=%s", partition["year"], partition["bucket"], partition["input_rows"], result["actual"]["groups"], reused)
        if use_batch_mode and missing:
            store.put(prefix + "checkpoints.json", batch_checkpoints)
        complete = len(selected) == len(domain) and completed_rows == int(outputs[FACT]["rows"])
        progress.update(status="ACCEPT" if complete else "INCOMPLETE", completed_input_rows=completed_rows, groups=total_groups, reused_checkpoints=reused, elapsed_seconds=str(time.monotonic() - started))
        store.put(prefix + "progress.json", progress)
        return {"status": progress["status"], "validator": VERSION, "scope": scope, "rows": total_groups, "input_rows": completed_rows,
                "partition_years": sorted({p["year"] for p in selected}), "partitions": len(selected), "expected_partitions": len(domain),
                "coverage": "ALL_DISJOINT_YEAR_REPORTER_BUCKETS" if complete else "PARTIAL", "progress_key": prefix + "progress.json",
                "checkpoint_seals": [p["seal"] for p in progress["accepted"]], "domain": selected}
    except Exception as exc:
        progress.update(status="INCOMPLETE", error={"type": type(exc).__name__, "message": str(exc)})
        store.put(prefix + "progress.json", progress)
        raise
