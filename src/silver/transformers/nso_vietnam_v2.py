"""EDA-scoped NSO draft; unknown units stay pending, never inferred from ratios."""
import json
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F

from silver.framework import AUDIT_COLUMNS
from silver.transformers.faostat_source import sql_rule

INVENTORY_PATH = Path(__file__).resolve().parents[3] / "config/silver/nso_source_tables.json"
# Verified V06.12 headers. Matrix unit assertions in the old inventory are NOT used.
NATIONAL_MEASURES = {
    "t_ng_di_n_t_ch_ngh_n_ha_": ("area", "annual"),
    "di_n_t_ch_l_a_ng_xu_n_ngh_n_ha_": ("area", "winter_spring"),
    "di_n_t_ch_l_a_h_thu_ngh_n_ha_": ("area", "summer_autumn"),
    "di_n_t_ch_l_a_m_a_ngh_n_ha_": ("area", "mua"),
    "t_ng_s_n_lu_ng_ngh_n_t_n_": ("production", "annual"),
    "s_n_lu_ng_l_a_ng_xu_n_ngh_n_t_n_": ("production", "winter_spring"),
    "s_n_lu_ng_l_a_h_thu_ngh_n_t_n_": ("production", "summer_autumn"),
    "s_n_lu_ng_l_a_m_a_ngh_n_t_n_": ("production", "mua"),
}


class NsoVietnamV2Transformer:
    dataset_id = "nso_vietnam_v2"
    business_keys = ["source_table", "geography_key", "year", "measure", "season", "statistic_kind"]

    def __init__(self):
        inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        # Reuse only the layout/checksums/metadata independently checked against Raw.
        self.inventory = {file: {k: spec[k] for k in
                          ("source_columns", "measure_columns", "sha256", "measure", "season") if k in spec}
                          for file, spec in inventory.items()}

    def _records(self, df):
        missing = set(AUDIT_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"NSO V2 missing lineage columns: {sorted(missing)}")
        records = df.withColumn("source_table", F.regexp_extract("_source_file", r"(?:^|[/\\])(V06\.[0-9]+\.csv)$", 1))
        files = {r[0] for r in records.select("source_table").distinct().limit(len(self.inventory) + 1).collect()}
        if files - set(self.inventory):
            raise ValueError(f"BLOCKED_FOR_APPROVAL: unprofiled NSO files {sorted(map(repr, files))}")
        files = sorted(files)
        layouts, checksums = {}, []
        for file in files:
            spec = self.inventory[file]
            missing = set(spec["source_columns"]) - set(df.columns)
            if missing:
                raise ValueError(f"NSO V2 {file} missing source columns: {sorted(missing)}")
            layouts.setdefault(tuple(spec["source_columns"]), []).append(file)
            checksums.extend([F.lit(file), F.lit(spec["sha256"])])
        structural, width, drift = F.lit(False), F.lit(0), F.lit(False)
        for fields, group in layouts.items():
            selected = F.col("source_table").isin(group)
            blank = lambda c: F.coalesce(F.trim(F.col(c).cast("string")), F.lit("")) == ""
            # Raw has a single terminal NUL-only footer; Bronze may decode it as NULL.
            empty = reduce(lambda a, b: a & b, [blank(c) for c in fields])
            nul_footer = (F.col(fields[0]) == "\x00") & reduce(lambda a, b: a & b, [blank(c) for c in fields[1:]])
            structural = structural | (selected & (empty | F.coalesce(nul_footer, F.lit(False))))
            width = F.when(selected, len(NATIONAL_MEASURES) if group == ["V06.12.csv"] else len(fields) - 1).otherwise(width)
            # Unioned columns belonging to other profiled files must be empty here.
            extras = [c for c in df.columns if not c.startswith("_") and c not in fields]
            if extras:
                drift = drift | (selected & reduce(lambda a, b: a | b, [~blank(c) for c in extras]))
        checksum_changed = (F.col("_source_checksum") != F.element_at(F.create_map(*checksums), F.col("source_table"))) if checksums else F.lit(False)
        if records.filter(checksum_changed | drift).limit(1).count():
            raise ValueError("BLOCKED_FOR_APPROVAL: source checksum/schema changed; profile the release before MERGE")
        if "_source_payload" not in records.columns:
            records = records.withColumn("_source_payload", F.to_json(F.struct(*[F.col(c) for c in df.columns]), {"ignoreNullFields": "false"}))
        return records.withColumn("_structural", structural).withColumn("_width", width), files

    def observation_audit(self, df):
        records, _ = self._records(df)
        stats = records.agg(F.sum("_width").alias("observation_count"),
                            F.sum(F.when(F.col("_structural"), F.col("_width")).otherwise(0)).alias("excluded_count"),
                            F.sum(F.col("_structural").cast("long")).alias("excluded_record_count")).first()
        return {k: v or 0 for k, v in stats.asDict().items()}

    def excluded_records(self, df):
        records, _ = self._records(df)
        return records.filter("_structural").withColumn("dq_errors", F.array(F.struct(
            F.lit("NSO_V2_STRUCTURAL").alias("rule_id"),
            F.lit("Blank or NUL-only source record; no observation generated").alias("error_message"),
            F.lit("source_record").alias("failed_column"))))

    def preprocess(self, df):
        records, files = self._records(df)
        if not files:
            # Build the same expression/schema on zero rows, without fabricated data.
            files = ["V06.12.csv"]
            for c in self.inventory[files[0]]["source_columns"]:
                if c not in records.columns:
                    records = records.withColumn(c, F.lit(None).cast("string"))
        records = records.filter("NOT _structural")
        audit = [c for c in records.columns if c.startswith("_") and c not in {"_structural", "_width"}]
        pieces = []
        groups = {}
        for file in files:
            groups.setdefault(tuple(self.inventory[file]["source_columns"]), []).append(file)
        for group in groups.values():
            spec = self.inventory[group[0]]
            national = group == ["V06.12.csv"]
            columns = NATIONAL_MEASURES if national else spec["measure_columns"]
            metadata = []
            for file in group:
                if not national:
                    metadata.extend([F.lit(file), F.struct(F.lit(self.inventory[file]["measure"]).alias("measure"),
                                                         F.lit(self.inventory[file]["season"]).alias("season"))])
            mapped = F.element_at(F.create_map(*metadata), F.col("source_table")) if metadata else None
            entries = []
            for c in columns:
                measure, season = (tuple(F.lit(v) for v in NATIONAL_MEASURES[c]) if national else (mapped["measure"], mapped["season"]))
                period = F.col("nam") if national else F.lit(("So b? " if c.startswith("so_b_") else "") + c[-4:])
                entries.append(F.struct(F.lit(c).alias("source_column"), F.col(c).cast("string").alias("value_raw"),
                                        period.alias("period_raw"), measure.alias("measure"), season.alias("season")))
            piece = records.filter(F.col("source_table").isin(group)).withColumn("o", F.explode(F.array(*entries)))
            pieces.append(piece.select(*audit, "source_table", "o.*",
                                       (F.lit(None).cast("string") if national else F.col("t_nh_th_nh_ph_")).alias("geography_raw"),
                                       (F.col("gi_tr_v_ch_s_ph_t_tri_n") if national else F.lit(None).cast("string")).alias("_statistic_raw")))
        out = reduce(lambda a, b: a.unionByName(b), pieces)
        national = F.col("source_table") == "V06.12.csv"
        kind = F.when(~national | (F.col("_statistic_raw") == "Giá tr?"), "quantity").when(
            F.col("_statistic_raw") == "Ch? s? phát tri?n (Nam tru?c =100) - %", "development_index")
        raw = F.trim(F.col("value_raw"))
        missing = F.when(raw.isNull(), "source_null").when(raw == "", "source_blank").when(raw == "..", "source_marker")
        out = out.withColumn("statistic_kind", kind).withColumn("missing_reason", missing).withColumn(
            "value_source", F.when(F.col("missing_reason").isNull(), F.expr("try_cast(trim(value_raw) AS decimal(28,8))")))
        # Reject unobserved markers, separators, precision loss and tiny negatives before rounding.
        out = out.withColumn("_numeric_valid", F.col("missing_reason").isNotNull() | (
            raw.rlike(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$") & F.col("value_source").isNotNull()
            & ~raw.rlike(r"^-.*[1-9]") & ~raw.rlike(r"\.\d{8}\d*[1-9]\d*$")))
        out = out.withColumn("geography_key", F.when(national, "__national_table_scope__").otherwise(F.col("geography_raw")))
        out = out.withColumn("geography_level", F.when(national | (F.col("geography_raw") == "C? NU?C"), "national").otherwise("unknown"))
        out = out.withColumn("year", F.when(F.col("period_raw").rlike(r"^(?:So b\? )?[1-9]\d{3}$"),
                                            F.expr("try_cast(right(period_raw, 4) AS int)")))
        out = out.withColumn("release_status", F.when(F.col("period_raw").startswith("So b? "), "provisional").otherwise("unconfirmed"))
        index = F.col("statistic_kind") == "development_index"
        out = out.withColumn("unit_raw", F.when(national, F.when(index, "%").when(F.col("measure") == "area", "Nghìn ha").otherwise("Nghìn t?n")))
        out = out.withColumn("unit", F.when(national, F.when(index, "percent").when(F.col("measure") == "area", "hectare").otherwise("tonne")))
        out = out.withColumn("value", F.when(national, F.expr(
            "try_cast(value_source * CASE WHEN statistic_kind = 'development_index' THEN 1 ELSE 1000 END AS decimal(28,8))")))
        out = out.withColumn("quality_status", F.when(F.col("missing_reason").isNotNull(), "MISSING").when(national, "NORMALIZED").otherwise("PENDING_REVIEW"))
        out = out.withColumn("review_reason", F.when(~national, F.when(F.col("geography_level") == "national", "unit_metadata_unverified")
                            .otherwise("unit_metadata_unverified;historical_geography_unmapped")))
        identity_valid = F.col("year").isNotNull() & F.col("statistic_kind").isNotNull() & (F.length(F.trim("geography_key")) > 0)
        conflicts = (out.filter(identity_valid & F.col("_numeric_valid")).groupBy(*self.business_keys)
                     .agg(F.countDistinct(F.struct("source_column", "period_raw", "value_raw", "_statistic_raw")).alias("variants")))
        if conflicts.filter("variants > 1").limit(1).count():
            raise ValueError("BLOCKED_FOR_APPROVAL: conflicting source observations; ingestion time is not release precedence")
        return out

    @staticmethod
    def quality_rules():
        rules = [sql_rule("NSO_V2_NUMERIC", "_numeric_valid", "value_raw", "Invalid/negative/overflowing numeric source or precision loss")]
        rules.extend(sql_rule(f"NSO_V2_LINEAGE_{c}", f"`{c}` IS NOT NULL AND length(trim(cast(`{c}` AS STRING))) > 0",
                              c, "Original Bronze lineage is required") for c in AUDIT_COLUMNS)
        return rules
