"""Source-local FAOSTAT preparation; shared engines own DQ, dedup and writes.

The shared framework calls transform and quality_rules, preserving diagnostic
columns and Bronze lineage through contract validation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ingestion.storage.bronze_writer import sanitize_column_name
from silver.contract.loader import SilverContractLoader
from silver.engine.quality import SilverQualityEngine


def text_column(name):
    value = F.trim(F.col(name).cast("string"))
    return F.when(value != "", value)


def identifier(name):
    return F.regexp_replace(F.regexp_replace(text_column(name), "^'+", ""), r"\.0+$", "")


def sql_rule(rule_id, sql, column, message):
    # Make the source rule's SQL UNKNOWN behavior explicit at its definition.
    return {"rule_id": rule_id, "sql_expr": f"coalesce(({sql}), false)",
            "failed_column": column, "rule": message, "action": "QUARANTINE"}


class FaostatSourceTransformer:
    """Helpers shared only by Hoang's three FAOSTAT source adapters."""

    dataset_id = ""
    derived_columns = ()

    def __init__(self, contract_dir="contracts/silver"):
        self.contract = SilverContractLoader(contract_dir).load_contract(self.dataset_id)

    def prepare(self, df: DataFrame) -> DataFrame:
        required = {"area_code_m49_", "area", "item_code_cpc_", "item",
                    "year", "element_code", "element", "unit", "value"}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"{self.dataset_id}: missing Bronze columns: {missing}")
        # Snapshot the incoming observation before any normalization. Keep all
        # technical fields and source identifiers, including unknown mappings.
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(
                F.struct(*[F.col(c) for c in sorted(df.columns)]),
                options={"ignoreNullFields": "false"}))
        df = df.withColumn("_raw_value", F.col("value").cast("string"))
        for c in ("domain_code", "domain", "area", "item", "element", "unit",
                  "flag", "flag_description", "note"):
            if c in df.columns:
                df = df.withColumn(c, text_column(c))
        country = identifier("area_code_m49_")
        df = df.withColumn("area_code_m49_", F.when(
            country.rlike(r"^[0-9]{1,3}$"), F.lpad(country, 3, "0")).otherwise(country))
        product = identifier("item_code_cpc_")
        # Bronze inferred CPC 0113 as integer/double. Restore this observed
        # source identity only; never pad other CPCs or merge rice products.
        df = df.withColumn("item_code_cpc_", F.when(product == "113", "0113").otherwise(product))
        df = df.withColumn("element_code", identifier("element_code"))
        df = df.withColumn("year", F.when(text_column("year").rlike(r"^[0-9]{4}$"),
                                         F.expr("try_cast(year as int)")))
        # Convert units before rounding to the published output decimal scale.
        df = df.withColumn("value", F.expr("try_cast(value as decimal(38,12))"))
        # Inspect sign before decimal rounding (including sub-scale negatives).
        df = df.withColumn("_negative_value", F.coalesce(
            F.expr("try_cast(_raw_value as double)") < 0, F.lit(False)))
        df = df.withColumn("_country_mapping_status", F.lit("source_m49_only"))
        return df.withColumn("_value_parse_error",
            (text_column("_raw_value").isNotNull()) & F.col("value").isNull())

    def preprocess(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def project(self, prepared: DataFrame) -> DataFrame:
        """Project contract types while preserving source diagnostics and lineage."""
        value_type = next(c["data_type"] for c in self.contract.columns if c["name"] == "value")
        prepared = prepared.withColumn("_value_parse_error", F.col("_value_parse_error") |
            (F.col("value").isNotNull() & F.expr(f"try_cast(value as {value_type})").isNull()))
        columns = []
        for definition in self.contract.columns:
            source = sanitize_column_name(definition["source_column"])
            if source in prepared.columns:
                expression = F.expr(f"try_cast(`{source}` as {definition['data_type']})")
            else:
                expression = F.lit(None).cast(definition["data_type"])
            columns.append(expression.alias(definition["name"]))
        target_names = {c["name"] for c in self.contract.columns}
        extras = [c for c in prepared.columns if c not in target_names and (c.startswith("_") or c in self.derived_columns)]
        return prepared.select(*columns, *[F.col(c) for c in extras])

    def transform(self, df: DataFrame) -> DataFrame:
        return self.project(self.preprocess(df))

    def common_rules(self):
        required = [c["name"] for c in self.contract.columns if not c.get("nullable", True)]
        return [
            sql_rule("FAO_REQUIRED", " AND ".join(f"{c} IS NOT NULL" for c in required),
                     "mandatory_fields", "Contract mandatory fields must be present"),
            sql_rule("FAO_YEAR", "year BETWEEN 1960 AND year(current_date()) + 1",
                     "year", "Calendar year must be within the contract range"),
            sql_rule("FAO_M49", "country_code RLIKE '^[0-9]{3}$'", "country_code",
                     "Retain a three-digit source M49 identifier; no inferred ISO country"),
            sql_rule("FAO_CPC", "commodity_code IN ('0113', '23161.02')", "commodity_code",
                     "Unknown rice CPC mapping requires review"),
            sql_rule("FAO_PRODUCT", "(commodity_code = '0113' AND commodity_name = 'Rice') OR "
                     "(commodity_code = '23161.02' AND commodity_name = 'Rice, milled')", "commodity_name",
                     "Preserve observed rice identities; never infer paddy/milled equivalence"),
            sql_rule("FAO_NUMBER", "NOT _value_parse_error", "value",
                     "Nonempty source values must parse within the contract decimal type"),
        ]

    def quality_rules(self):
        return self.common_rules()

    def validate(self, transformed: DataFrame):
        """Use Hải's DQ engine; this helper does not write or drop observations."""
        return SilverQualityEngine(self.quality_rules()).apply_rules(transformed)
