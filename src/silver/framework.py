"""Single contract-driven Bronze to Silver pipeline shared by CLI and Airflow."""
import importlib
import logging
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

from ingestion.storage.bronze_writer import sanitize_column_name
from silver.contract.loader import SilverContractLoader
from silver.engine.quality import SilverQualityEngine, ERROR_TYPE
from silver.engine.quarantine import SilverQuarantineManager
from silver.engine.merge import IcebergMergeEngine

logger = logging.getLogger("silver.framework")
AUDIT_COLUMNS = (
    "_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id",
    "_ingestion_timestamp", "_source_id", "_source_file",
    "_source_checksum", "_source_snapshot_id",
)


class SilverTransformationFramework:
    def __init__(self, spark: SparkSession, contract_dir: str = None):
        self.spark = spark
        self.contract_loader = SilverContractLoader(contract_dir)
        self.quarantine_manager = SilverQuarantineManager(spark)
        self.merge_engine = IcebergMergeEngine(spark)

    def transformer(self, dataset_id):
        module_name = f"silver.transformers.{dataset_id}"
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            return None
        name = "".join(word.capitalize() for word in dataset_id.split("_")) + "Transformer"
        cls = getattr(module, name)
        if dataset_id in {"faostat_production", "faostat_monthly_price", "faostat_supply_utilization"}:
            return cls(contract_dir=self.contract_loader.contract_dir)
        return cls()

    def normalize(self, df, contract):
        """One projection prevents alias collisions and repeated withColumn plans."""
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(
                F.struct(*[F.col(c) for c in sorted(df.columns)]),
                options={"ignoreNullFields": "false"}))
        output, errors = [], []
        for definition in contract.columns:
            name = definition["name"]
            source = sanitize_column_name(definition.get("source_column", name))
            data_type = definition["data_type"]
            if source not in df.columns:
                if not definition.get("nullable", True):
                    raise ValueError(f"{contract.dataset_name}: missing required Bronze column {source}")
                output.append(F.lit(None).cast(data_type).alias(name))
                continue
            raw = F.trim(F.col(source).cast("string"))
            missing = raw.isNull() | (raw == "")
            if data_type != "string":
                missing = missing | F.upper(raw).isin("NA", "N/A", "NULL", "..", "...", "…")
            typed = F.when(~missing, F.expr(f"try_cast(`{source}` AS {data_type})"))
            if data_type == "string":
                typed = F.when(~missing, raw)
                if definition.get("preserve_whitespace", False):
                    typed = F.col(source).cast("string")
            if data_type in {"integer", "int", "long", "bigint"}:
                source_type = df.schema[source].dataType
                if isinstance(source_type, T.NumericType):
                    # Spark casts numeric fractions to integers by truncation.
                    # Treat that loss as a cast error, while allowing 2020.0.
                    typed = F.when(F.col(source) == typed.cast(source_type), typed)
            if data_type in {"double", "float"}:
                typed = F.when(~F.isnan(typed) & (F.abs(typed) != F.lit(float("inf"))), typed)
            errors.append(F.when(~missing & typed.isNull(), F.struct(
                F.lit("SYS_CAST_ERR").alias("rule_id"),
                F.lit(f"Cannot cast {source} to {data_type}").alias("error_message"),
                F.lit(name).alias("failed_column"))))
            output.append(typed.alias(name))
        names = {c["name"] for c in contract.columns}
        extras = [c for c in df.columns if c.startswith("_") and c not in names and c != "_sys_cast_errors"]
        return df.select(*output, *[F.col(c) for c in extras],
                         F.filter(F.array(*errors).cast(ERROR_TYPE), lambda x: x.isNotNull()).alias("_sys_cast_errors"))

    def run(self, dataset_id: str, pipeline_run_id: str, mode="full", run_id=None, watermark=None):
        contract = self.contract_loader.load_contract(dataset_id)
        contract.validate_executable()
        if mode not in {"full", "incremental"}:
            raise ValueError(f"Unsupported read mode: {mode}")
        if mode == "incremental" and not (run_id or watermark):
            raise ValueError("Incremental mode requires run_id or watermark")
        cached = []
        logger.info("silver_start dataset=%s pipeline_run_id=%s mode=%s", dataset_id, pipeline_run_id, mode)
        try:
            bronze = self.spark.table(contract.bronze_input)
            if mode == "incremental":
                if run_id:
                    bronze = bronze.filter(F.col("_ingestion_run_id") == F.lit(run_id))
                if watermark:
                    bronze = bronze.filter(F.col("_ingestion_timestamp") > F.lit(watermark))
            bronze_count = bronze.count()
            bronze = bronze.withColumn("_source_payload", F.to_json(
                F.struct(*[F.col(c) for c in sorted(bronze.columns)]),
                options={"ignoreNullFields": "false"}))
            transformer = self.transformer(dataset_id)
            observation_metrics = {}
            exclusions = None
            if transformer and hasattr(transformer, "observation_audit"):
                observation_metrics = transformer.observation_audit(bronze)
                exclusions = transformer.excluded_records(bronze)
            prepared = transformer.preprocess(bronze) if transformer else bronze
            normalized = self.normalize(prepared, contract)
            rules = list(contract.data_quality_rules)
            required = set(contract.business_key) | {c["name"] for c in contract.columns if not c.get("nullable", True)}
            for name in sorted(required):
                rules.append({"rule_id": f"SYS_REQUIRED_{name}", "sql_expr": f"`{name}` IS NOT NULL",
                              "failed_column": name, "rule": "Required field or business key is missing"})
            if transformer and hasattr(transformer, "quality_rules"):
                rules.extend(transformer.quality_rules())
            checked = SilverQualityEngine(rules).evaluate(normalized).persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(checked)
            stats = checked.agg(
                F.count("*").alias("prepared_count"),
                F.sum(F.when(F.size("dq_errors") > 0, 1).otherwise(0)).alias("quarantine_count"),
                F.sum(F.when(F.size("_dq_warnings") > 0, 1).otherwise(0)).alias("warning_count"),
            ).first().asDict()
            prepared_count = stats["prepared_count"]
            if observation_metrics and observation_metrics["observation_count"] != prepared_count + observation_metrics["excluded_count"]:
                raise ValueError("Observation reconciliation failed: candidates != prepared + excluded")
            quarantine_count = stats["quarantine_count"] or 0
            valid_count = prepared_count - quarantine_count
            quarantine = checked.filter(F.size("dq_errors") > 0)
            self.quarantine_manager.ensure_table()
            if exclusions is not None:
                self.quarantine_manager.route_quarantine(
                    exclusions, dataset_id + "_exclusions", pipeline_run_id, ["source_table"])
            self.quarantine_manager.route_quarantine(quarantine, dataset_id, pipeline_run_id, contract.business_key)
            valid = checked.filter(F.size("dq_errors") == 0).select(
                *[c["name"] for c in contract.columns],
                *[c for c in AUDIT_COLUMNS if c in checked.columns], "_dq_warnings")
            dedup = self.merge_engine.deduplicate(valid, contract.business_key).persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(dedup)
            dedup_count = dedup.count()
            output = dedup.withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc)))
            self.merge_engine.merge(output, target_table=contract.silver_output, business_keys=contract.business_key,
                                    partition_columns=contract.partition_columns)
            result = {"status": "SUCCESS", "dataset": dataset_id, "pipeline_run_id": pipeline_run_id,
                      "target_table": contract.silver_output, "bronze_count": bronze_count,
                      "prepared_count": prepared_count, "valid_count": valid_count,
                      "quarantine_count": quarantine_count, "warning_count": stats["warning_count"] or 0,
                      "dedup_count": dedup_count, "duplicate_count": valid_count - dedup_count}
            result.update(observation_metrics)
            logger.info("silver_complete %s", result)
            if result["warning_count"]:
                logger.warning("silver_warnings dataset=%s rows=%s; see _dq_warnings", dataset_id, result["warning_count"])
            return result
        except Exception:
            logger.exception("silver_failed dataset=%s pipeline_run_id=%s", dataset_id, pipeline_run_id)
            raise
        finally:
            for df in reversed(cached):
                df.unpersist()

    _sanitize_column_name = staticmethod(sanitize_column_name)
