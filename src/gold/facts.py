"""G02–G10: lossless per-source fact projections, with no physical pivot."""
from pyspark.sql import functions as F, types as T

from gold import BUILD_ORDER
from gold.dimensions import geography_key, product_key, series_key
from gold.keys import key, spark_key
from gold.rules import metric_rule, checked_decimal

TARGETS = {
    "worldbank_pinksheet": ("fact_price_monthly",),
    "faostat_production": ("fact_fao_production_observation",),
    "faostat_supply_utilization": ("fact_fao_sua_observation",),
    "faostat_trade": ("fact_fao_trade_observation",),
    "usda_psd": ("fact_usda_psd_observation",),
    "nso": ("fact_nso_observation",),
    "thitruongnongsan": ("fact_vn_market_quote_daily",),
    "faostat_monthly_price": ("fact_price_monthly", "fact_fao_price_annual"),
    "usda_rice_yearbook": ("fact_usda_price_reference",),
}


def provenance(source, business_key, pin=None):
    return [
        F.lit(key("dim_source_system", [source])).alias("source_system_key"),
        F.col("missing_reason"),
    ]


def normalized_value(source, precision, scale):
    factor = F.when(F.col("unit").isin("1000 USD", "1000 MT", "1000 HA"), F.lit(1000)).otherwise(F.lit(1))
    return (F.col("value") * factor).cast(T.DecimalType(precision, scale)).alias("value")


def fao_observation(source, df, bk, pin):
    trade = source == "faostat_trade"
    geography = [geography_key(source, r).alias(f"{r}_geography_key") for r in ("reporter", "partner")] if trade else [geography_key(source).alias("geography_key")]
    return df.select(*provenance(source, bk, pin), F.col("year").alias("year_key"), *geography, product_key(source).alias("product_key"), spark_key("dim_observation_type", F.lit(source), F.col("element_code")).alias("observation_type_key"), normalized_value(source, 22 if trade else 18, 2))


def psd_observation(df, bk, pin):
    return df.select(*provenance("usda_psd", bk, pin), geography_key("usda_psd").alias("geography_key"), product_key("usda_psd").alias("product_key"), spark_key("dim_observation_type", F.lit("usda_psd"), F.col("attribute")).alias("observation_type_key"), "market_year", "crop_year", normalized_value("usda_psd", 22, 4))


def nso_observation(df, bk, pin):
    direct = "source_table source_geography_key source_column measure season statistic_kind period_raw release_status quality_status review_reason value unit value_source".split()
    return df.select(*provenance("nso", bk, pin), F.col("year").alias("year_key"), geography_key("nso").alias("geography_key"), *direct, F.col("conversion_factor").alias("silver_conversion_factor"))


def wb_monthly(df, bk, pin):
    return df.select(*provenance("worldbank_pinksheet", bk, pin), (F.col("year") * 100 + F.col("month")).alias("month_key"), series_key("worldbank_pinksheet").alias("price_series_key"), F.col("price").cast("decimal(20,4)").alias("value"))


def vn_daily(df, bk, pin):
    return df.select(*provenance("thitruongnongsan", bk, pin), (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).alias("date_key"), series_key("thitruongnongsan").alias("price_series_key"), F.col("price_value").alias("value"))


def fao_prices(df, bk, pin):
    monthly = df.where("time_grain = 'monthly'").select(*provenance("faostat_monthly_price", bk, pin), (F.col("year") * 100 + F.col("month")).alias("month_key"), series_key("faostat_monthly_price").alias("price_series_key"), F.col("value").cast("decimal(20,4)").alias("value"))
    annual = df.where("time_grain = 'annual'").select(*provenance("faostat_monthly_price", bk, pin), F.col("year").alias("year_key"), series_key("faostat_monthly_price").alias("price_series_key"), "value")
    return monthly, annual


def yearbook_reference(df, bk, pin):
    return df.select(*provenance("usda_rice_yearbook", bk, pin), series_key("usda_rice_yearbook").alias("price_series_key"), F.col("year").alias("reference_year"), "reference_period", "value")


def transform(source, df, business_key, pin):
    if source in {"faostat_production", "faostat_supply_utilization", "faostat_trade"}:
        frames = (fao_observation(source, df, business_key, pin),)
    elif source == "faostat_monthly_price":
        frames = fao_prices(df, business_key, pin)
    else:
        frames = ({"worldbank_pinksheet": wb_monthly, "usda_psd": psd_observation, "nso": nso_observation, "thitruongnongsan": vn_daily, "usda_rice_yearbook": yearbook_reference}[source](df, business_key, pin),)
    return dict(zip(TARGETS[source], frames))


def monthly_snapshot_frame(source, branch, inputs, business_keys, pins, completed_sources):
    """Preserve both branches on a completed-batch replay (physical NOOP).

    First G02 builds only WB; first G09 adds FAO. After G09 succeeded, replaying
    G02 must not temporarily remove FAO and create two unnecessary snapshots.
    """
    fao, wb = "faostat_monthly_price", "worldbank_pinksheet"
    sibling = wb if source == fao else fao if fao in completed_sources else None
    if sibling:
        other = transform(sibling, inputs[sibling], business_keys[sibling], pins[sibling])["fact_price_monthly"]
        return branch.unionByName(other)
    return branch
