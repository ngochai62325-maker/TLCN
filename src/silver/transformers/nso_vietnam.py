"""NSO observations: exact source inventory, verified units, explicit unknown geography."""
import json
import re
from functools import reduce
from pathlib import Path
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
NATIONAL_LABELS = ("CẢ NƯỚC", "Cả nước", "C? NU?C")
REGION_LABELS = ("Đồng bằng sông Hồng", "Ð?ng b?ng sông H?ng",
                 "Trung du và miền núi phía Bắc", "Trung du và mi?n núi phía B?c",
                 "Bắc Trung Bộ và Duyên hải miền Trung", "B?c Trung B? và Duyên h?i mi?n Trung",
                 "Tây Nguyên", "Đông Nam Bộ", "Ðông Nam B?",
                 "Đồng bằng sông Cửu Long", "Ð?ng b?ng sông C?u Long")
INVENTORY_PATH = Path(__file__).resolve().parents[3] / "config/silver/nso_source_tables.json"
GEOGRAPHY_PATH = INVENTORY_PATH.with_name("nso_geography_candidates.json")


class NsoVietnamTransformer:
    dataset_id = "nso_vietnam"
    # Refining a geography mapping later must not create a second source observation.
    business_keys = ["source_table", "geography_name_raw", "year", "season", "measure", "statistic_kind"]

    def __init__(self, source_inventory=None, geography_candidates=None):
        self.inventory = source_inventory if source_inventory is not None else json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        candidates = geography_candidates if geography_candidates is not None else json.loads(GEOGRAPHY_PATH.read_text(encoding="utf-8"))
        self.geography_aliases = {}
        for raw, candidate in candidates.items():
            if (not isinstance(raw, str) or not raw.strip() or not isinstance(candidate, list)
                    or len(candidate) != 2 or not isinstance(candidate[0], str)
                    or not candidate[0].strip() or candidate[1] not in {"national", "region", "province"}):
                raise ValueError(f"Invalid NSO geography candidate: {raw!r}")
            name, level = candidate[0].strip(), candidate[1]
            # Accept exact source labels and their correctly accented equivalents.
            # A candidate name does not constitute a verified historical code.
            for alias in (raw.strip(), name):
                previous = self.geography_aliases.get(alias)
                if previous is not None and previous != (name, level):
                    raise ValueError(f"Ambiguous NSO geography alias: {alias!r}")
                self.geography_aliases[alias] = (name, level)

    def _geography_lookup(self):
        entries = []
        for alias, (name, level) in sorted(self.geography_aliases.items()):
            entries.extend([F.lit(alias), F.struct(F.lit(name).alias("name"), F.lit(level).alias("level"))])
        if not entries:
            return F.lit(None).cast("struct<name:string,level:string>")
        return F.element_at(F.create_map(*entries), F.trim(F.col("geography_name_raw")))

    def validate_bronze_schema(self, df, files=None):
        for file in self.inventory if files is None else files:
            missing = set(self.inventory[file]["source_columns"]) - set(df.columns)
            if missing:
                raise ValueError(f"NSO {file} missing columns {sorted(missing)}; repair Bronze schema and replay original CSVs")

    def _source_records(self, df):
        if "_source_file" not in df.columns:
            raise ValueError("NSO requires per-file _source_file lineage")
        df = df.withColumn("source_table", F.coalesce(F.regexp_extract("_source_file", r"(?:^|[/\\])(V06\.[0-9]+\.csv)$", 1), F.lit("")))
        files = sorted(r[0] for r in df.select("source_table").distinct().limit(14).collect())
        unknown = set(files) - set(self.inventory)
        if unknown:
            raise ValueError(f"NSO contains unprofiled source files: {sorted(unknown)}")
        self.validate_bronze_schema(df, files)
        empty, width = F.lit(False), F.lit(0)
        for file in files:
            spec = self.inventory[file]
            unprofiled = [c for c in df.columns if re.fullmatch(r"(?:col_|so_b_)\d{4}", c)
                          and c not in spec.get("measure_columns", [])]
            if unprofiled:
                unexpected = reduce(lambda a, b: a | b, (F.col(c).isNotNull() for c in unprofiled))
                if df.filter((F.col("source_table") == file) & unexpected).limit(1).count():
                    raise ValueError(f"NSO {file} has unprofiled period columns {unprofiled}; update source inventory before processing")
            all_null = reduce(lambda a, b: a & b, (F.col(c).isNull() for c in spec["source_columns"]))
            empty = empty | ((F.col("source_table") == file) & all_null)
            width = F.when(F.col("source_table") == file, 8 if file == "V06.12.csv" else len(spec["measure_columns"])).otherwise(width)
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns) if c != "source_table"]), {"ignoreNullFields": "false"}))
        return df.withColumn("_all_source_null", empty).withColumn("_candidate_count", width), files

    def excluded_records(self, df):
        records, _ = self._source_records(df)
        return records.filter("_all_source_null").withColumn("dq_errors", F.array(F.struct(
            F.lit("NSO_EMPTY_SOURCE_RECORD").alias("rule_id"),
            F.lit("All source fields NULL; no observations generated").alias("error_message"),
            F.lit("source_record").alias("failed_column"))))

    def observation_audit(self, df):
        records, _ = self._source_records(df)
        stats = records.agg(F.sum("_candidate_count").alias("observation_count"),
            F.sum(F.when(F.col("_all_source_null"), F.col("_candidate_count")).otherwise(0)).alias("excluded_count"),
            F.sum(F.when(F.col("_all_source_null"), 1).otherwise(0)).alias("excluded_record_count")).first()
        return {k: v or 0 for k, v in stats.asDict().items()}

    def preprocess(self, df):
        records, files = self._source_records(df)
        if not files:
            exemplar = df.sparkSession.createDataFrame([("V06.12.csv",)], "_source_file string")
            for c in sorted(set(df.columns) | set(self.inventory["V06.12.csv"]["source_columns"])):
                if c != "_source_file":
                    dtype = df.schema[c].dataType if c in df.columns else "string"
                    exemplar = exemplar.withColumn(c, F.lit(None).cast(dtype))
            return self.preprocess(exemplar).limit(0)
        records = records.filter("NOT _all_source_null")
        audit = [c for c in records.columns if c.startswith("_") and c not in {"_all_source_null", "_candidate_count"}]
        pieces = []

        def entry(column, measure, season, source_unit, unit, factor, reference):
            return F.struct(F.lit(column).alias("source_column"), F.col(column).cast("string").alias("value_raw"),
                F.lit(measure).alias("measure"), F.lit(season).alias("season"),
                F.lit(source_unit).alias("source_unit"), F.lit(unit).alias("unit"),
                F.lit(factor).cast("decimal(18,6)").alias("conversion_factor"), F.lit(reference).alias("unit_source_reference"))

        if "V06.12.csv" in files:
            entries = [entry(c, m, s, u, v, 1000, "V06.12 source header") for c, (m, s, u, v) in NATIONAL_MEASURES.items()]
            national = records.filter(F.col("source_table") == "V06.12.csv").withColumn("o", F.explode(F.array(*entries)))
            pieces.append(national.select(*audit, "source_table", F.lit("Cả nước").alias("geography_name_raw"),
                F.col("nam").cast("string").alias("period_raw"), F.col("gi_tr_v_ch_s_ph_t_tri_n").alias("statistic_raw"), "o.*"))
        matrix_files = [file for file in files if file != "V06.12.csv"]
        if matrix_files:
            expression = None
            for file in matrix_files:
                spec = self.inventory[file]
                entries = [entry(c, spec["measure"], spec["season"], spec["source_unit"], spec["unit"], spec["conversion_factor"], spec["source_reference"]) for c in spec["measure_columns"]]
                condition = F.col("source_table") == file
                expression = F.when(condition, F.array(*entries)) if expression is None else expression.when(condition, F.array(*entries))
            matrix = records.filter(F.col("source_table").isin(*matrix_files)).withColumn("o", F.explode(expression))
            pieces.append(matrix.select(*audit, "source_table", F.col("t_nh_th_nh_ph_").alias("geography_name_raw"),
                F.lit("quantity").alias("statistic_raw"), "o.*").withColumn("period_raw", F.col("source_column")))
        long = reduce(lambda a, b: a.unionByName(b), pieces)
        raw = F.trim(F.col("value_raw"))
        index = F.col("statistic_raw").isin("Ch? s? phát tri?n (Nam tru?c =100) - %", "Chỉ số phát triển (Năm trước =100) - %")
        quantity = F.col("statistic_raw").isin("Giá tr?", "Giá trị", "quantity")
        national_geo = F.trim(F.col("geography_name_raw")).isin(*NATIONAL_LABELS)
        long = (long.withColumn("_geography_candidate", self._geography_lookup())
            .withColumn("year", F.expr("try_cast(regexp_extract(period_raw, '([0-9]{4})$', 1) as int)"))
            .withColumn("period_valid", F.col("period_raw").rlike(r"^(?:[0-9]{4}|So b\? [0-9]{4}|Sơ bộ [0-9]{4}|col_[0-9]{4}|so_b_[0-9]{4})$"))
            .withColumn("is_provisional", F.col("period_raw").rlike(r"^(?:so_b_|So b\?|Sơ bộ )"))
            .withColumn("statistic_kind", F.when(quantity, "quantity").when(index, "development_index"))
            .withColumn("missing_kind", F.when(raw.isNull(), "source_null").when(raw == "", "source_blank")
                .when(F.upper(raw).isin("..", "...", "…", "NA", "N/A", "NULL"), "source_marker").otherwise("present"))
            .withColumn("is_missing", F.col("missing_kind") != "present")
            .withColumn("value_source", F.when(~F.col("is_missing"), F.expr("try_cast(value_raw as decimal(28,8))")))
            .withColumn("_negative_raw", raw.rlike(r"^-\s*(?:[0-9]|\.)"))
            .withColumn("source_unit", F.when(index, "percent").otherwise(F.col("source_unit")))
            .withColumn("unit", F.when(index, "percent").otherwise(F.col("unit")))
            .withColumn("conversion_factor", F.when(index, F.lit(1)).otherwise(F.col("conversion_factor")).cast("decimal(18,6)"))
            .withColumn("value", F.expr("try_cast(try_multiply(value_source, conversion_factor) as decimal(28,8))"))
            .withColumn("geography_level", F.when(national_geo, "national")
                .when(F.col("_geography_candidate").isNotNull(), F.col("_geography_candidate.level"))
                .when(F.trim(F.col("geography_name_raw")).isin(*REGION_LABELS), "region").otherwise("unknown"))
            .withColumn("geography_name", F.when(national_geo, "Việt Nam").otherwise(F.col("_geography_candidate.name")))
            .withColumn("geography_code", F.when(national_geo, "704"))
            .withColumn("mapping_status", F.when(national_geo, "verified").otherwise("needs_review"))
            .withColumn("country_code", F.lit("704")).withColumn("commodity_name", F.lit("Rice"))
            .withColumn("province_code", F.lit(None).cast("string")))
        return long.drop("_geography_candidate")

    transform = preprocess

    def quality_rules(self):
        rules = [
            sql_rule("NSO_DATE", "period_valid AND year BETWEEN 1960 AND year(current_date()) + 1", "year", "Invalid reference year"),
            sql_rule("NSO_IDENTITY", "geography_name_raw IS NOT NULL AND trim(geography_name_raw) <> '' AND statistic_kind IS NOT NULL", "geography_name_raw", "Missing source geography or unknown statistic"),
            sql_rule("NSO_VALUE", "(is_missing OR (value_source IS NOT NULL AND value IS NOT NULL)) AND (value_source IS NULL OR value_source >= 0) AND NOT coalesce(_negative_raw, false)", "value", "Malformed, negative or overflowing value; missing remains NULL"),
        ]
        warning = sql_rule("NSO_GEOGRAPHY_REVIEW", "mapping_status = 'verified'", "geography_name_raw", "Unverified geography retained with NULL code; review before geographic joins")
        warning["action"] = "LOG"
        return rules + [warning]

    def validate(self, df):
        return SilverQualityEngine(self.quality_rules()).apply_rules(df)
