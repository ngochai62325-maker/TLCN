"""Canonical NSO transformer: approved reference units, historical geography and diagnostics."""
import json
import hashlib
import logging
from collections import defaultdict
from decimal import Decimal, localcontext
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F, types as T

from silver.framework import AUDIT_COLUMNS, BronzeReadContext
from silver.transformers.faostat_source import sql_rule

INVENTORY_PATH = Path(__file__).resolve().parents[3] / "config/silver/nso_v2_metadata.json"
METADATA_VERSION = "nso-v2-20261015.1"
METADATA_SHA256 = "a076880448eaf84593e75a20d0fc6156302d26c053b07e260c3b3916d4da335d"
logger = logging.getLogger(__name__)


# Geography identity is derived from exact decoded source labels, never candidates.
def source_geography_key(raw):
    return "__national_table_scope__" if raw is None else "raw:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def discrepancy_evidence(rows, policy):
    """Diagnostic comparisons of compact, bounded source-scale observations.

    Exact equality across publications has zero tolerance. Arithmetic checks use
    a documented half-step envelope; it is not a claim about producer rounding.
    No measurement, aggregation grain, unit or missing value is changed here.
    """
    keys = NsoTransformer.business_keys
    values = {tuple(r[k] for k in keys): r["value_source"] for r in rows}
    warnings = defaultdict(list)
    half = Decimal(policy["native_rounding_half_step"])
    if half != Decimal("0.05") or Decimal(policy["cross_table_equality_tolerance"]) != 0:
        raise ValueError("Unapproved NSO diagnostic tolerance")

    def warn(rule, members, detail):
        event = {"rule_id": rule, "policy_version": policy["version"], "detail": detail,
                 "members": [{"key": list(k), "value_source": str(values[k])} for k in members]}
        for key in members:
            warnings[key].append(event)

    with localcontext() as ctx:
        ctx.prec = 50
        national = source_geography_key(None)
        country = source_geography_key("C? NU?C")
        for key, value in sorted(values.items()):
            table, geo, year, measure, season, kind = key
            if table == "V06.12.csv" and season == "annual":
                peer = ("V06.13.csv" if measure == "area" else "V06.15.csv", country, year, measure, season, kind)
                if peer in values and value != values[peer]:
                    warn("NSO_CROSS_TABLE", [key, peer], {"difference_source_scale": str(value - values[peer]), "tolerance": "0"})
                seasonal = [(table, national, year, measure, s, kind) for s in ("winter_spring", "summer_autumn", "main_season")]
                if all(k in values for k in seasonal):
                    delta = value - sum((values[k] for k in seasonal), Decimal(0))
                    bound = half * (1 + len(seasonal))
                    if abs(delta) > bound:
                        warn("NSO_SEASON_TOTAL", [key, *seasonal], {"difference_source_scale": str(delta), "envelope": str(bound)})
            # Same geography/season, distinct table series. No geographic summing.
            if measure == "yield" and table != "V06.12.csv":
                number = int(table[4:6])
                area = (f"V06.{number-1}.csv", geo, year, "area", season, kind)
                production = (f"V06.{number+1}.csv", geo, year, "production", season, kind)
                if area in values and production in values and values[area] > half:
                    # Native units: 1000 tonne / 1000 ha * 10 = quintal/ha.
                    low = 10 * max(Decimal(0), values[production] - half) / (values[area] + half)
                    high = 10 * (values[production] + half) / (values[area] - half)
                    if high < value - half or low > value + half:
                        warn("NSO_AREA_YIELD_PRODUCTION", [area, key, production],
                             {"implied_yield_interval": [str(low), str(high)], "reported_yield_half_step": str(half)})
    return dict(warnings)


NATIONAL_MEASURES = {
    "t_ng_di_n_t_ch_ngh_n_ha_": ("area", "annual"),
    "di_n_t_ch_l_a_ng_xu_n_ngh_n_ha_": ("area", "winter_spring"),
    "di_n_t_ch_l_a_h_thu_ngh_n_ha_": ("area", "summer_autumn"),
    "di_n_t_ch_l_a_m_a_ngh_n_ha_": ("area", "main_season"),
    "t_ng_s_n_lu_ng_ngh_n_t_n_": ("production", "annual"),
    "s_n_lu_ng_l_a_ng_xu_n_ngh_n_t_n_": ("production", "winter_spring"),
    "s_n_lu_ng_l_a_h_thu_ngh_n_t_n_": ("production", "summer_autumn"),
    "s_n_lu_ng_l_a_m_a_ngh_n_t_n_": ("production", "main_season"),
}


class NsoTransformer:
    dataset_id = "nso"
    business_keys = ["source_table", "source_geography_key", "year", "measure", "season", "statistic_kind"]

    def __init__(self):
        if hashlib.sha256(INVENTORY_PATH.read_bytes()).hexdigest() != METADATA_SHA256:
            raise ValueError("NSO V2 metadata/evidence checksum differs from approved mapping")
        self.metadata = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        if self.metadata.get("metadata_version") != METADATA_VERSION:
            raise ValueError("NSO V2 metadata version is not approved")
        self.inventory = self.metadata["tables"]
        for spec in self.inventory.values():
            evidence = spec.get("unit_evidence", {})
            if (not spec.get("unit_reference") or not evidence.get("reference_pdf_sha256")
                    or not evidence.get("pdf_pages_one_based")
                    or evidence.get("unit_mapping_version") != self.metadata.get("unit_mapping_version")):
                raise ValueError("NSO V2 unit mapping evidence/version missing or mismatched")
        expected_units = {"area": ("hectare", "1000"), "production": ("tonne", "1000"),
                          "yield": ("kg/hectare", "100"), "development_index": ("percent", "1")}
        if any((self.metadata["units"][k]["unit"], self.metadata["units"][k]["factor"]) != v
               for k, v in expected_units.items()):
            raise ValueError("NSO V2 unit mapping differs from owner-approved policy")

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
        raise ValueError("NSO V2 requires pinned Bronze read context")

    def preprocess_with_read_context(self, df, context: BronzeReadContext):
        snapshot = context.bronze_read_snapshot_id if isinstance(context, BronzeReadContext) else None
        if (type(snapshot) is not int or not 0 < snapshot < 2**63
                or not isinstance(context.bronze_input, str) or not context.bronze_input
                or "_bronze_read_snapshot_id" not in df.columns
                or df.schema["_bronze_read_snapshot_id"].dataType != T.LongType()):
            raise ValueError("NSO V2 invalid pinned Bronze read context")
        if df.filter(~F.col("_bronze_read_snapshot_id").eqNullSafe(F.lit(snapshot).cast("long"))).limit(1).count():
            raise ValueError("NSO V2 pinned Bronze read context does not match bound data")
        out = self._preprocess(df)
        # Diagnostics use the SAME pinned read across full/incremental selections.
        # Never query current/latest. Checking the complete approved source version
        # also prevents incremental filtering from hiding a conflicting duplicate.
        full = df.sparkSession.read.format("iceberg").option("snapshot-id", str(snapshot)).load(context.bronze_input)
        diagnostic_rows = self._preprocess(full).filter(
            "statistic_kind = 'quantity' AND _numeric_valid AND year IS NOT NULL AND value_source IS NOT NULL"
        ).select(*self.business_keys, "value_source").distinct().limit(100001).collect()
        if len(diagnostic_rows) > 100000:
            raise ValueError("NSO V2 diagnostic capacity exceeded; reprofile before execution")
        evidence = discrepancy_evidence([r.asDict() for r in diagnostic_rows], self.metadata["diagnostic_policy"])
        schema = T.StructType([out.schema[k] for k in self.business_keys] + [T.StructField("diagnostic_evidence", T.StringType())])
        diagnostics = df.sparkSession.createDataFrame(
            [tuple(key) + (json.dumps(events, ensure_ascii=False, sort_keys=True),) for key, events in sorted(evidence.items())], schema)
        out = out.join(F.broadcast(diagnostics), self.business_keys, "left").fillna({"diagnostic_evidence": "[]"})
        return out.withColumn("bronze_read_snapshot_id", F.col("_bronze_read_snapshot_id")).drop("_bronze_read_snapshot_id")

    def _preprocess(self, df):
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
        missing = F.when(raw.isNull(), "source_null").when(raw == "", "source_blank").when(raw == "..", "source_marker_unspecified")
        out = out.withColumn("statistic_kind", kind).withColumn("missing_reason", missing).withColumn(
            "value_source", F.when(F.col("missing_reason").isNull(), F.expr("try_cast(trim(value_raw) AS decimal(28,8))")))
        # Reject unobserved markers, separators, precision loss and tiny negatives before rounding.
        out = out.withColumn("_numeric_valid", F.col("missing_reason").isNotNull() | (
            raw.rlike(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$") & F.col("value_source").isNotNull()
            & ~raw.rlike(r"^-.*[1-9]") & ~raw.rlike(r"\.\d{8}\d*[1-9]\d*$")))
        out = out.withColumn("source_geography_key", F.when(national, "__national_table_scope__").otherwise(
            F.when(F.length(F.trim("geography_raw")) > 0, F.concat(F.lit("raw:"), F.sha2(F.encode("geography_raw", "UTF-8"), 256)))))
        candidates = F.create_map(*[x for raw_label, (name, level) in self.metadata["geography_candidates"].items()
                                   for x in (F.lit(raw_label), F.struct(F.lit(name).alias("name"), F.lit(level).alias("level")))])
        candidate = F.element_at(candidates, F.col("geography_raw"))
        out = out.withColumn("geography_name_candidate", candidate["name"]).withColumn(
            "geography_level", F.when(national, "national").otherwise(F.coalesce(candidate["level"], F.lit("unknown"))))
        out = out.withColumn("geography_status", F.when(national, "national_scope_metadata").when(
            candidate.isNotNull(), "candidate_review").otherwise("unmapped_review"))
        out = out.withColumn("geography_code", F.lit(None).cast("string")).withColumn("boundary_version", F.lit(None).cast("string"))
        out = out.withColumn("geography_mapping_version", F.lit(self.metadata["geography_mapping_version"]))
        out = out.withColumn("geography_evidence", F.when(national, F.lit(json.dumps(self.metadata["national_scope_evidence"], ensure_ascii=False, sort_keys=True)))
                             .otherwise(F.lit("Candidate labels only; no approved historical code, boundary or hierarchy")))
        out = out.withColumn("year", F.when(F.col("period_raw").rlike(r"^(?:So b\? )?[1-9]\d{3}$"),
                                            F.expr("try_cast(right(period_raw, 4) AS int)")))
        out = out.withColumn("release_status", F.when(F.col("period_raw").startswith("So b? "), "provisional").otherwise("unconfirmed"))
        index = F.col("statistic_kind") == "development_index"
        mappings = self.metadata["units"]
        unit_key = F.when(index, "development_index").otherwise(F.col("measure"))
        unit_map = F.create_map(*[x for name, spec in mappings.items() for x in (
            F.lit(name), F.struct(F.lit(spec["unit"]).alias("unit"),
                                  F.lit(spec["factor"]).cast("decimal(8,0)").alias("factor"),
                                  F.lit(spec["source_unit_reference"]).alias("source_unit")))])
        mapping = F.element_at(unit_map, unit_key)
        headers = dict(zip(self.inventory["V06.12.csv"]["source_columns"], self.inventory["V06.12.csv"]["raw_headers"]))
        header_units = F.create_map(*[x for name in NATIONAL_MEASURES for x in (
            F.lit(name), F.lit(headers[name].rsplit("(", 1)[1].rstrip(")")))])
        out = out.withColumn("unit_raw", F.when(national, F.when(index, "%").otherwise(F.element_at(header_units, F.col("source_column")))))
        out = out.withColumn("unit", mapping["unit"]).withColumn("conversion_factor", mapping["factor"])
        out = out.withColumn("unit_source_reference", mapping["source_unit"])
        out = out.withColumn("value", F.expr("try_cast(value_source * conversion_factor AS decimal(28,8))"))
        for field in ("unit_reference", "unit_evidence"):
            lookup = F.create_map(*[x for file, spec in self.inventory.items() for x in (
                F.lit(file), F.lit(json.dumps(spec[field], ensure_ascii=False, sort_keys=True) if field == "unit_evidence" else spec[field]))])
            out = out.withColumn(field, F.element_at(lookup, F.col("source_table")))
        out = out.withColumn("unit_status", F.when(national, "source_header").otherwise("owner_approved_reference"))
        out = out.withColumn("unit_mapping_version", F.lit(self.metadata["unit_mapping_version"]))
        out = out.withColumn("metadata_version", F.lit(self.metadata["metadata_version"]))
        out = out.withColumn("quality_status", F.when(F.col("missing_reason").isNotNull(), "MISSING").otherwise("NORMALIZED"))
        out = out.withColumn("review_reason", F.when(~national, F.lit("historical_geography_unmapped")))
        identity_valid = F.col("year").isNotNull() & F.col("statistic_kind").isNotNull() & (F.length(F.trim("source_geography_key")) > 0)
        conflicts = (out.filter(identity_valid & F.col("_numeric_valid")).groupBy(*self.business_keys)
                     .agg(F.countDistinct(F.struct("source_column", "period_raw", "value_raw", "_statistic_raw")).alias("variants"),
                          F.collect_set(F.to_json(F.struct("value_raw", "source_column", "_source_checksum", "_ingestion_run_id"))).alias("source_variants")))
        groups = conflicts.filter("variants > 1")
        examples = groups.orderBy(*self.business_keys).limit(20).collect()
        if examples:
            error = ValueError("BLOCKED_FOR_APPROVAL: conflicting source observations; ingestion time is not release precedence")
            error.nso_conflict_groups = {"group_count": groups.count(), "examples": [r.asDict() for r in examples]}
            logger.error("nso_conflicting_observations %s", json.dumps(error.nso_conflict_groups, sort_keys=True))
            raise error
        return out

    @staticmethod
    def quality_rules():
        rules = [sql_rule("NSO_V2_NUMERIC", "_numeric_valid", "value_raw", "Invalid/negative/overflowing numeric source or precision loss")]
        rules.extend(sql_rule(f"NSO_V2_LINEAGE_{c}", f"`{c}` IS NOT NULL AND length(trim(cast(`{c}` AS STRING))) > 0",
                              c, "Original Bronze lineage is required") for c in AUDIT_COLUMNS)
        return rules


NsoVietnamV2Transformer = NsoTransformer
