"""Read-only NSO long-form proposal; the published NSO contract is not ready.

Never fabricate columns lost during Bronze multi-file ingestion. Province codes
and matrix units require reviewed source references; unknowns remain explicit.
"""

from functools import reduce
import re

from pyspark.sql import functions as F

from silver.engine.quality import SilverQualityEngine
from silver.transformers.faostat_source import sql_rule


NATIONAL_MEASURES = {
    "t_ng_di_n_t_ch_ngh_n_ha_": ("area", "annual", "1000 hectare", "hectare"),
    "di_n_t_ch_l_a_ng_xu_n_ngh_n_ha_": ("area", "winter_spring", "1000 hectare", "hectare"),
    "di_n_t_ch_l_a_h_thu_ngh_n_ha_": ("area", "summer_autumn", "1000 hectare", "hectare"),
    "di_n_t_ch_l_a_m_a_ngh_n_ha_": ("area", "mua", "1000 hectare", "hectare"),
    "t_ng_s_n_lu_ng_ngh_n_t_n_": ("production", "annual", "1000 tonne", "tonne"),
    "s_n_lu_ng_l_a_ng_xu_n_ngh_n_t_n_": ("production", "winter_spring", "1000 tonne", "tonne"),
    "s_n_lu_ng_l_a_h_thu_ngh_n_t_n_": ("production", "summer_autumn", "1000 tonne", "tonne"),
    "s_n_lu_ng_l_a_m_a_ngh_n_t_n_": ("production", "mua", "1000 tonne", "tonne"),
}

MATRIX_FILES = {
    f"V06.{number}.csv": (measure, season)
    for start, season in [(13, "annual"), (16, "winter_spring"),
                          (19, "summer_autumn_autumn_winter"), (22, "mua")]
    for number, measure in zip(range(start, start + 3), ("area", "yield", "production"))
}

# Exact national/regional labels from snapshots, including damaged labels.
# No fuzzy repair or guessed province identity is performed.
NATIONAL_LABELS = ("CẢ NƯỚC", "Cả nước", "C? NU?C")
REGION_LABELS = ("Đồng bằng sông Hồng", "Ð?ng b?ng sông H?ng",
                 "Trung du và miền núi phía Bắc", "Trung du và mi?n núi phía B?c",
                 "Bắc Trung Bộ và Duyên hải miền Trung", "B?c Trung B? và Duyên h?i mi?n Trung",
                 "Tây Nguyên", "Đông Nam Bộ", "Ðông Nam B?",
                 "Đồng bằng sông Cửu Long", "Ð?ng b?ng sông C?u Long")


class NsoVietnamTransformer:
    dataset_id = "nso_vietnam"
    business_keys = ["source_table", "geography_name_raw", "geography_level",
                     "year", "season", "measure", "statistic_kind"]

    def preprocess(self, df):
        if "_source_file" not in df.columns:
            raise ValueError("NSO requires per-file _source_file lineage")
        df = df.withColumn("source_table", F.regexp_extract("_source_file", r"(V06\.[0-9]+\.csv)$", 1))
        matrix_columns = [c for c in df.columns if re.fullmatch(r"col_\d{4}|so_b_\d{4}", c)]
        if "t_nh_th_nh_ph_" not in df.columns or not matrix_columns:
            if df.filter(F.col("source_table").isin(*MATRIX_FILES)).limit(1).count():
                raise ValueError("BLOCKED: NSO Bronze is missing province/year matrix columns from V06.13..24; repair Bronze schema with ingestion owner before Silver")
        unknown = df.filter(~F.col("source_table").isin("V06.12.csv", *MATRIX_FILES)).limit(1).count()
        if unknown:
            raise ValueError("NSO contains an unprofiled source file; no file semantics were inferred")
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(F.struct(
                *[F.col(c) for c in sorted(df.columns)]), options={"ignoreNullFields": "false"}))
        audit = [c for c in df.columns if c.startswith("_")]
        pieces = []
        national = df.filter(F.col("source_table") == "V06.12.csv")
        if national.limit(1).count():
            needed = {"nam", "gi_tr_v_ch_s_ph_t_tri_n", *NATIONAL_MEASURES}
            missing = needed - set(df.columns)
            if missing:
                raise ValueError(f"NSO V06.12 missing source columns: {sorted(missing)}")
            for column, (measure, season, source_unit, unit) in NATIONAL_MEASURES.items():
                pieces.append(national.select(*audit, "source_table",
                    F.lit("Cả nước").alias("geography_name_raw"),
                    F.lit("national").alias("geography_level"),
                    F.col("nam").alias("period_raw"),
                    F.col("gi_tr_v_ch_s_ph_t_tri_n").alias("statistic_raw"),
                    F.lit(measure).alias("measure"), F.lit(season).alias("season"),
                    F.lit(source_unit).alias("source_unit"), F.lit(unit).alias("quantity_unit"),
                    F.col(column).cast("string").alias("value_raw")))
        if matrix_columns and "t_nh_th_nh_ph_" in df.columns:
            for filename, (measure, season) in MATRIX_FILES.items():
                subset = df.filter(F.col("source_table") == filename)
                entries = [F.struct(F.lit(c).alias("period_raw"), F.col(c).cast("string").alias("value_raw"))
                           for c in sorted(matrix_columns)]
                subset = subset.withColumn("observation", F.explode(F.array(*entries)))
                label = F.trim(F.col("t_nh_th_nh_ph_"))
                level = F.when(label.isin(*NATIONAL_LABELS), "national").when(label.isin(*REGION_LABELS), "region").otherwise("unknown")
                pieces.append(subset.select(*audit, "source_table", label.alias("geography_name_raw"),
                    level.alias("geography_level"), "observation.period_raw", "observation.value_raw",
                    F.lit("quantity").alias("statistic_raw"),
                    F.lit(measure).alias("measure"), F.lit(season).alias("season"),
                    F.lit(None).cast("string").alias("source_unit"),
                    F.lit(None).cast("string").alias("quantity_unit")))
        if not pieces:
            raise ValueError("NSO input has no profiled source observations")
        long = reduce(lambda left, right: left.unionByName(right), pieces)
        long = long.withColumn("year", F.regexp_extract("period_raw", r"([0-9]{4})$", 1))
        long = long.withColumn("year", F.expr("try_cast(year as int)"))
        long = long.withColumn("_period_valid", F.col("period_raw").rlike(
            r"^(?:[0-9]{4}|So b\? [0-9]{4}|Sơ bộ [0-9]{4}|col_[0-9]{4}|so_b_[0-9]{4})$"))
        long = long.withColumn("is_provisional", F.col("period_raw").rlike(r"^(?:so_b_|So b\?|Sơ bộ )"))
        raw = F.trim(F.col("value_raw"))
        long = long.withColumn("_missing_value", raw.isNull() | raw.isin("", ".."))
        long = long.withColumn("value", F.when(~F.col("_missing_value"), F.expr("try_cast(value_raw as decimal(28,8))")))
        kind = F.when(F.col("statistic_raw").isin("Giá tr?", "Giá trị", "quantity"), "quantity")
        kind = kind.when(F.col("statistic_raw") == "Ch? s? phát tri?n (Nam tru?c =100) - %", "development_index")
        long = long.withColumn("statistic_kind", kind)
        return (long.withColumn("unit", F.when(F.col("statistic_kind") == "development_index", "percent")
                                .otherwise(F.col("quantity_unit")))
                .withColumn("value", F.when((F.col("statistic_kind") == "quantity") & F.col("source_unit").isNotNull(),
                                            F.col("value") * 1000).otherwise(F.col("value")))
                .withColumn("country_code", F.lit("704"))
                .withColumn("commodity_name", F.lit("Rice"))
                .withColumn("province_code", F.lit(None).cast("string"))
                .withColumn("mapping_status", F.when(F.col("geography_level") == "unknown", "needs_review").otherwise("source_label")))

    transform = preprocess

    def quality_rules(self):
        return [
            sql_rule("HOANG_NSO_DATE", "_period_valid AND year BETWEEN 1960 AND year(current_date()) + 1", "year", "Missing or invalid reference year"),
            sql_rule("HOANG_NSO_GEOGRAPHY", "geography_level IN ('national','region')", "geography_name_raw", "Province mapping has not been approved; preserve ambiguous names for review"),
            sql_rule("HOANG_NSO_UNIT", "unit IS NOT NULL AND statistic_kind IS NOT NULL", "unit", "Matrix units/statistic kind require source confirmation"),
            sql_rule("HOANG_NSO_VALUE", "(_missing_value OR value IS NOT NULL) AND (value IS NULL OR value >= 0)", "value", "Malformed or negative observation; missing values remain null"),
        ]

    def validate(self, df):
        return SilverQualityEngine(self.quality_rules()).apply_rules(df)
