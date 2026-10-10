"""Shared contract, DQ, quarantine and reconciliation pipeline."""
from importlib import import_module
import re

from pyspark.sql import functions as F

from silver.contract.loader import SilverContractLoader
from silver.engine.quality import SilverQualityEngine
from silver.engine.quarantine import SilverQuarantineManager
from silver.engine.merge import IcebergMergeEngine


from silver.registry import TRANSFORMERS


def source_transformer(dataset_id, spark, contract_dir="contracts/silver"):
    module_name, class_name = TRANSFORMERS[dataset_id]
    cls = getattr(import_module(f"silver.transformers.{module_name}"), class_name)
    if module_name.endswith("_transformer"):
        return cls(spark)
    if dataset_id.startswith("faostat_"):
        return cls(contract_dir=contract_dir)
    return cls()


class SilverTransformationFramework:
    def __init__(self, spark, contract_dir="contracts/silver"):
        self.spark = spark
        self.contract_loader = SilverContractLoader(contract_dir)
        self.quarantine_manager = SilverQuarantineManager(spark)
        self.merge_engine = IcebergMergeEngine(spark)

    @staticmethod
    def filter_input(df, mode="full", watermark=None, run_id=None):
        if mode not in ("full", "incremental"):
            raise ValueError(f"Unsupported read mode: {mode}")
        if mode == "incremental":
            if watermark is None and not run_id:
                raise ValueError("Incremental read requires watermark or Bronze ingestion run ID")
            if watermark is not None:
                df = df.filter(F.col("_ingestion_timestamp") > F.lit(watermark))
            if run_id:
                df = df.filter(F.col("_ingestion_run_id") == F.lit(run_id))
        return df

    def prepare(self, dataset_id, bronze, transformer=None, allow_unapproved=False):
        """Evaluate controls without writes; caller owns returned cache lifecycle."""
        contract = self.contract_loader.load_contract(dataset_id)
        if not allow_unapproved:
            contract.require_ready()
        if contract.bronze_input in ("", "UNKNOWN") or contract.silver_output in ("", "UNKNOWN"):
            raise ValueError(f"{dataset_id}: contract is not ready; no writes allowed")
        if not contract.business_key or not contract.columns:
            raise ValueError(f"{dataset_id}: contract requires schema and business key")
        transformer = transformer or source_transformer(dataset_id, self.spark, self.contract_loader.contract_dir)
        if hasattr(transformer, "target_table") and transformer.target_table != contract.silver_output:
            raise ValueError(f"{dataset_id}: transformer and contract targets differ")
        if hasattr(transformer, "business_keys") and transformer.business_keys != contract.business_key:
            raise ValueError(f"{dataset_id}: transformer and contract keys differ")
        input_count = bronze.count()
        if "_source_payload" not in bronze.columns:
            bronze = bronze.withColumn("_source_payload", F.to_json(
                F.struct(*[F.col(c) for c in sorted(bronze.columns) if c != "_bronze_iceberg_snapshot_id"]), {"ignoreNullFields": "false"}))
        transformed = transformer.transform(bronze)
        missing = [c["name"] for c in contract.columns if c["name"] not in transformed.columns]
        if missing:
            raise ValueError(f"{dataset_id}: transformer misses contract columns: {missing}")
        # Capture type failures before casts replace the source expressions.
        cast_errors = []
        for definition in contract.columns:
            name, dtype = definition["name"], definition["data_type"]
            converted = F.expr(f"try_cast(`{name}` AS {dtype})")
            cast_errors.append(F.col(name).isNotNull() & converted.isNull())
        error = F.lit(False)
        for condition in cast_errors:
            error = error | condition
        transformed = transformed.withColumn("_contract_cast_error", error)
        for definition in contract.columns:
            name, dtype = definition["name"], definition["data_type"]
            transformed = transformed.withColumn(name, F.expr(f"try_cast(`{name}` AS {dtype})"))
        rules = list(contract.data_quality_rules)
        rules.extend(transformer.quality_rules() if hasattr(transformer, "quality_rules") else [])
        required = [c["name"] for c in contract.columns if not c.get("nullable", True)]
        rules.extend([
            {"rule_id": "CONTRACT_CAST", "sql_expr": "NOT _contract_cast_error", "failed_column": "schema",
             "rule": "Nonempty values must fit contract types"},
            {"rule_id": "CONTRACT_REQUIRED", "sql_expr": " AND ".join(
                f"`{c}` IS NOT NULL AND trim(cast(`{c}` AS string)) <> ''" for c in required) or "true",
             "rule": "Contract mandatory fields must be present", "failed_column": "mandatory_fields"},
        ])
        lineage = ("_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
                   "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id")
        missing_lineage = sorted(set(lineage) - set(transformed.columns))
        if missing_lineage:
            raise ValueError(f"{dataset_id}: missing Bronze lineage columns: {missing_lineage}")
        rules.extend([
            {"rule_id": "LINEAGE_REQUIRED", "sql_expr": " AND ".join(
                f"`{c}` IS NOT NULL AND trim(cast(`{c}` AS string)) <> ''" for c in lineage),
             "failed_column": "lineage", "rule": "All Bronze lineage fields must survive transformation"},
            {"rule_id": "LINEAGE_REFERENCE", "sql_expr": "try_cast(_source_snapshot_id as bigint) > 0",
             "failed_column": "_source_snapshot_id", "rule": "Ingestion snapshot reference has not been linked",
             "severity": "WARNING", "action": "LOG"},
        ])
        transformed = transformed.cache()
        try:
            eligible_count = transformed.count()
            excluded_count = input_count * len(getattr(transformer, "excluded_columns", []))
            observation_count = eligible_count + excluded_count
            observation_audit = transformer.observation_audit(bronze) if hasattr(transformer, "observation_audit") else {}
            if observation_audit:
                excluded_count = observation_audit["excluded_count"]
                observation_count = observation_audit["observation_count"]
                if observation_count != eligible_count + excluded_count:
                    raise RuntimeError("Source observation reconciliation failed")
            valid, quarantine = SilverQualityEngine(rules).apply_rules(transformed)
            valid_count, quarantine_count = valid.count(), quarantine.count()
            dedup = self.merge_engine.deduplicate(valid, contract.business_key)
            output_count = dedup.count()
            if observation_count != valid_count + quarantine_count + excluded_count:
                raise RuntimeError("DQ reconciliation failed")
            report = {"dataset": dataset_id, "bronze_count": input_count,
                "observation_count": observation_count, "valid_count": valid_count,
                "quarantine_count": quarantine_count, "dedup_count": output_count,
                "deduplicated_count": valid_count - output_count, "excluded_count": excluded_count,
                "excluded_columns": getattr(transformer, "excluded_columns", []),
                "eligible_observation_count": eligible_count,
                "target_table": contract.silver_output, "writes_performed": False}
            report.update(observation_audit)
            report["contract_status"] = contract.status
            return contract, transformed, dedup, quarantine, report
        except Exception:
            transformed.unpersist()
            raise

    def read_input(self, bronze_table, mode="full", watermark=None, run_id=None):
        history = self.spark.sql(f"SELECT snapshot_id FROM {bronze_table}.history "
                                 "ORDER BY made_current_at DESC LIMIT 1").collect()
        snapshot = history[0][0] if history else None
        bronze = (self.spark.read.option("snapshot-id", str(snapshot)).table(bronze_table)
                  if snapshot else self.spark.table(bronze_table))
        if snapshot:
            bronze = bronze.withColumn("_bronze_iceberg_snapshot_id", F.lit(snapshot).cast("long"))
        return self.filter_input(bronze, mode, watermark, run_id)

    def run(self, dataset_id, pipeline_run_id, mode="full", watermark=None, run_id=None, transformer=None):
        if not pipeline_run_id:
            raise ValueError("A pipeline run ID is required")
        contract = self.contract_loader.load_contract(dataset_id)
        contract.require_ready()
        if contract.bronze_input in ("", "UNKNOWN"):
            raise ValueError(f"{dataset_id}: Bronze contract is not ready")
        bronze = self.read_input(contract.bronze_input, mode, watermark, run_id)
        transformer = transformer or source_transformer(dataset_id, self.spark, self.contract_loader.contract_dir)
        contract, cached, valid, quarantine, report = self.prepare(dataset_id, bronze, transformer)
        try:
            quarantine_result = self.quarantine_manager.route_quarantine(quarantine, dataset_id, pipeline_run_id, contract.business_key)
            excluded_result = {"records_changed": 0, "status": "NOOP"}
            if hasattr(transformer, "excluded_records"):
                excluded_result = self.quarantine_manager.route_quarantine(transformer.excluded_records(bronze),
                    dataset_id + "_exclusions", pipeline_run_id, ["_exclusion_record_id"])
            write_result = self.merge_engine.merge(valid, contract.silver_output, contract.business_key)
            actual = self.spark.table(contract.silver_output) if report["dedup_count"] else valid
            keys = contract.business_key
            if valid.select(*keys).join(actual.select(*keys), keys, "left_anti").limit(1).count():
                raise RuntimeError("Silver write reconciliation failed: accepted keys missing")
            changed = (write_result or {}).get("records_changed", 0) + (quarantine_result or {}).get("records_changed", 0) + excluded_result.get("records_changed", 0)
            report.update(status="SUCCESS", writes_performed=bool(changed), pipeline_run_id=pipeline_run_id, mode=mode)
            report["outcome"] = "NO_INPUT" if report["bronze_count"] == 0 else ("MERGED" if changed else "NOOP")
            report["write_result"] = write_result
            report["quarantine_result"] = quarantine_result
            report["excluded_result"] = excluded_result
            return report
        finally:
            cached.unpersist()

    def _sanitize_column_name(self, col_name):
        cleaned = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", str(col_name).strip()))
        return (f"col_{cleaned}" if cleaned and cleaned[0].isdigit() else cleaned).lower()

    def _map_type(self, type_str):
        return type_str
