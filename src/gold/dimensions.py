"""Eight dimensions using exact source identities; only narrow dictionaries shuffle."""
import calendar
from datetime import date, timedelta
from functools import reduce

from pyspark.sql import functions as F, types as T

from gold.keys import canonical_json, key, spark_json, spark_key, spark_value_json
from gold.rules import metric_rule, PRICE_UNITS
from gold.spec import ROOT

ZERO_GEOGRAPHY = "0" * 64
METRIC_SOURCES = ("faostat_production", "faostat_supply_utilization", "faostat_trade", "usda_psd")
SERIES_COLUMNS = {
    "worldbank_pinksheet": ("commodity_code",),
    "faostat_monthly_price": ("country_code", "commodity_code", "element_code"),
    "usda_rice_yearbook": ("location", "commodity_class", "statistic_type"),
    "thitruongnongsan": ("market", "commodity", "price_type", "unit", "currency", "source"),
}


def union(frames):
    return reduce(lambda a, b: a.unionByName(b), frames)


def assert_fd(df, natural, attrs, label):
    """NULL is a real distinct attribute, unlike countDistinct(column)."""
    if df.select(*natural, *attrs).distinct().groupBy(*natural).count().where("count > 1").limit(1).count():
        raise ValueError(f"Functional dependency/fan-out: {label}")


def geo_columns(source, role=None):
    if source.startswith("faostat_"):
        identity = f"{role}_country_code" if role else "country_code"
        label = f"{role}_country_name" if role else "country_name"
        return F.lit("faostat_m49"), F.lit("faostat"), F.col(identity), label, "source_area"
    ns, identity, level = {
        "usda_psd": ("usda_psd_country", "country", "source_location"),
        "usda_rice_yearbook": ("usda_yearbook_location", "location", "source_location"),
        "thitruongnongsan": ("vn_market_label", "market", "market_label"),
        "nso": ("nso_raw_label", "source_geography_key", None),
    }[source]
    return F.lit(ns), F.col("source_table") if source == "nso" else F.lit(source), F.col(identity), identity, level


def geography_key(source, role=None):
    if source == "worldbank_pinksheet":
        return F.lit(ZERO_GEOGRAPHY)
    ns, scope, identity, _, _ = geo_columns(source, role)
    return spark_key("dim_source_geography", ns, scope, identity)


def product_columns(source):
    column = {"usda_psd": "commodity", "usda_rice_yearbook": "commodity_class", "thitruongnongsan": "commodity"}.get(source, "commodity_code")
    return F.lit("faostat_cpc" if source.startswith("faostat_") else source), F.col(column)


def product_key(source):
    return spark_key("dim_source_product", *product_columns(source))


def series_key(source):
    return spark_key("dim_price_series", *(F.col(c) for c in SERIES_COLUMNS[source]), nested_source=source)


def sources_dimension(spark):
    import yaml
    from silver.local_profile import DATASETS
    registry = yaml.safe_load((ROOT / "config/ingestion/source_registry.yaml").read_text(encoding="utf-8"))["sources"]
    return spark.createDataFrame([(key("dim_source_system", [s]), s, registry[s]["provider"], registry[s]["dataset"]) for s in DATASETS], "source_system_key string, source_id string, provider string, dataset_label string")


def calendar_dimensions(spark, inputs):
    years = []
    months = []
    for source, df in inputs.items():
        if source in {"usda_psd", "usda_rice_yearbook"}:
            continue
        bounds = df.agg(F.min("year"), F.max("year")).first()
        if bounds[0] is None or not 1 <= bounds[0] <= bounds[1] <= 9999:
            raise ValueError(f"Invalid calendar bounds: {source}")
        years.extend(bounds)
        if source in {"faostat_monthly_price", "worldbank_pinksheet", "thitruongnongsan"}:
            subset = df.where("time_grain = 'monthly'") if source == "faostat_monthly_price" else df
            m = F.month("date") if source == "thitruongnongsan" else F.col("month")
            row = subset.agg(F.min(F.col("year") * 12 + m - 1), F.max(F.col("year") * 12 + m - 1)).first()
            if row[0] is not None:
                months.extend(row)
    yd = spark.createDataFrame([(y, y // 10 * 10, calendar.isleap(y)) for y in range(min(years), max(years) + 1)], "year_key int, decade int, is_leap_year boolean")
    md = spark.createDataFrame([(n // 12 * 100 + n % 12 + 1, n // 12, n % 12 + 1, n % 12 // 3 + 1, date(n // 12, n % 12 + 1, 1)) for n in range(min(months), max(months) + 1)], "month_key int, year_key int, month_number int, quarter_number int, month_start date")
    lo, hi = inputs["thitruongnongsan"].agg(F.min("date"), F.max("date")).first()
    if lo is None or hi is None or (hi - lo).days > 36600:
        raise ValueError("Missing/excessive VN daily calendar range")
    dd = spark.createDataFrame([(d.year * 10000 + d.month * 100 + d.day, d, d.year * 100 + d.month, d.isoweekday(), d.day) for d in (lo + timedelta(days=n) for n in range((hi - lo).days + 1))], "date_key int, full_date date, month_key int, iso_weekday int, day_of_month int")
    return {"dim_year": yd, "dim_month": md, "dim_date": dd}


def geography_dimension(spark, inputs):
    frames = []
    for source, df in inputs.items():
        if source == "worldbank_pinksheet":
            continue
        for role in ("reporter", "partner") if source == "faostat_trade" else (None,):
            ns, scope, identity, label, level = geo_columns(source, role)
            labels = [("raw", "geography_raw"), ("candidate", "geography_name_candidate")] if source == "nso" else [("source", label)]
            for label_role, label_col in labels:
                frames.append(df.select(ns.alias("identity_namespace"), scope.alias("identity_scope"), identity.alias("source_identity"),
                    (F.col("geography_level") if source == "nso" else F.lit(level)).alias("source_level"),
                    (F.col("geography_status") if source == "nso" else F.lit("source_identity_only")).alias("interpretation_status"),
                    F.lit(label_role).alias("role"), F.col(label_col).alias("label")).distinct())
    raw = union(frames)
    nk = ["identity_namespace", "identity_scope", "source_identity"]
    assert_fd(raw, nk, ["source_level", "interpretation_status"], "geography classification")
    labels = raw.groupBy(*nk, "source_level", "interpretation_status").agg(F.sort_array(F.collect_set(F.when(F.col("label").isNotNull(), F.struct("role", "label")))).alias("labels"))
    labels = labels.withColumn("source_labels_json", spark_value_json(F.col("labels"))).drop("labels")
    labels = labels.withColumn("geography_key", spark_key("dim_source_geography", *(F.col(c) for c in nk)))
    technical = spark.createDataFrame([(ZERO_GEOGRAPHY, "__technical__", "not_geographic", "not_applicable", canonical_json([{"role": "technical", "label": "not_applicable"}]), "not_applicable", "not_applicable")], "geography_key string, identity_namespace string, identity_scope string, source_identity string, source_labels_json string, source_level string, interpretation_status string")
    return labels.unionByName(technical)


def product_dimension(inputs):
    frames = []
    for source, df in inputs.items():
        if source == "nso":
            continue
        ns, identity = product_columns(source)
        label = "commodity_name" if source.startswith("faostat_") or source == "worldbank_pinksheet" else "commodity_class" if source == "usda_rice_yearbook" else "commodity"
        kind = "cpc_source_code" if source.startswith("faostat_") else {"usda_psd": "source_commodity", "usda_rice_yearbook": "price_class", "worldbank_pinksheet": "benchmark", "thitruongnongsan": "market_commodity"}[source]
        frames.append(df.select(ns.alias("identity_namespace"), identity.alias("source_identity"), F.col(label).alias("label"), F.lit(kind).alias("identity_kind")).distinct())
        if source == "usda_rice_yearbook":
            assert_fd(df, ["commodity_class"], ["commodity"], "Yearbook class/commodity")
    grouped = union(frames).groupBy("identity_namespace", "source_identity", "identity_kind").agg(F.sort_array(F.collect_set("label")).alias("labels"))
    return grouped.withColumn("source_labels_json", spark_value_json(F.col("labels"))).drop("labels").withColumn("product_key", spark_key("dim_source_product", F.col("identity_namespace"), F.col("source_identity")))


def observation_dimension(spark, inputs):
    rows = []
    for source in METRIC_SOURCES:
        metric = "attribute" if source == "usda_psd" else "element_code"
        label = "attribute" if source == "usda_psd" else "element_name" if source == "faostat_trade" else "element"
        dictionary = inputs[source].select(F.col(metric).alias("metric"), F.col(label).alias("label"), "unit").distinct()
        assert_fd(dictionary, ["metric"], ["unit"], f"{source} metric/unit")
        # At most 34 reviewed metrics; bounded dictionary collect, never full observations.
        values = dictionary.limit(129).collect()
        if len(values) > 128:
            raise ValueError("Metric dictionary drift")
        definitions = {}
        for row in values:
            gold_unit, factor, rule = metric_rule(source, row.metric, row.unit)
            definitions.setdefault(row.metric, [row.unit, gold_unit, factor, rule, set()])[4].add(row.label)
        for metric_id, (unit, gold_unit, factor, rule, labels) in definitions.items():
            rows.append((key("dim_observation_type", [source, metric_id]), key("dim_source_system", [source]), metric_id, canonical_json(sorted(labels)), unit, gold_unit, factor, rule))
    return spark.createDataFrame(rows, "observation_type_key string, source_system_key string, source_metric_id string, source_labels_json string, silver_unit string, gold_unit string, gold_conversion_factor decimal(10,4), aggregation_rule string")


def price_series_dimension(inputs):
    frames = []
    for source, cols in SERIES_COLUMNS.items():
        df = inputs[source]
        if source == "faostat_monthly_price":
            unit = F.create_map(*(F.lit(x) for pair in PRICE_UNITS.items() for x in pair))[F.col("element_code")]
            currency, kind, price_type, info = F.col("currency"), F.col("price_kind"), F.col("element"), F.lit(None)
        elif source == "worldbank_pinksheet":
            unit = F.col("unit")
            currency = F.when(unit.startswith("USD/"), F.lit("USD"))
            kind = F.when(unit.startswith("index("), F.lit("index")).otherwise(F.lit("benchmark_price"))
            price_type, info = F.lit("benchmark"), F.lit(None)
        elif source == "usda_rice_yearbook":
            unit, currency, kind, price_type, info = F.col("unit"), F.lit("USD"), F.lit("reported_export_price"), F.col("statistic_type"), F.lit(None)
        else:
            unit, currency, kind, price_type, info = F.col("unit"), F.col("currency"), F.lit("market_quote"), F.col("price_type"), F.col("source")
        # Distinct before hashing and grouping; retains every defining attribute to detect drift.
        projected = df.select(series_key(source).alias("price_series_key"), F.lit(key("dim_source_system", [source])).alias("source_system_key"), spark_json(*(F.col(c) for c in cols)).alias("series_identity_json"), geography_key(source).alias("geography_key"), product_key(source).alias("product_key"), price_type.cast("string").alias("price_type_source"), unit.alias("unit"), currency.cast("string").alias("currency"), kind.alias("price_kind"), info.cast("string").alias("information_source")).distinct()
        assert_fd(projected, ["price_series_key"], [c for c in projected.columns if c != "price_series_key"], f"{source} series")
        frames.append(projected)
    return union(frames)


def build_dimensions(spark, inputs):
    yield "dim_source_system", sources_dimension(spark)
    yield from calendar_dimensions(spark, inputs).items()
    yield "dim_source_geography", geography_dimension(spark, inputs)
    yield "dim_source_product", product_dimension(inputs)
    yield "dim_observation_type", observation_dimension(spark, inputs)
    yield "dim_price_series", price_series_dimension(inputs)
