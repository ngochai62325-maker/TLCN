"""Silver Layer — Contract-Driven 5-Step Transformation Framework (`iceberg.silver_local.*`).

Overview for Thesis Defense:
============================
Every dataset in Silver is processed through the exact same 5-step Template Method inside `run()`:
1. `read_bronze`: Read the pinned Iceberg snapshot from `iceberg.bronze.<source>` (supports full or incremental filter).
2. `transform`: Run source-specific `transformers/<source>.py` (`preprocess` / `preprocess_with_read_context`)
   followed by contract schema projection & type casting (`normalize()`).
3. `validate_quality`: Evaluate SQL Data Quality rules from `contracts/silver/<source>.yaml` via `SilverQualityEngine`
   to split rows into valid rows (`dq_errors == []`), warnings (`_dq_warnings`), and failed rows (`dq_errors > 0`).
4. `quarantine_bad_rows`: Route invalid rows and structural exclusions to `iceberg.silver_local.dead_letters`
   with full `_source_payload` JSON so zero raw records are silently lost.
5. `merge_to_silver`: Deduplicate valid rows by `business_key` and atomically `MERGE INTO` the target Silver
   Iceberg table via `IcebergMergeEngine` (skipping no-op merges via `xxhash64`).
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib
import json
import logging
import os
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable
import urllib.error
import urllib.request

from ingestion.storage.bronze_writer import sanitize_column_name
from silver.engine import (
    ERROR_TYPE,
    IcebergMergeEngine,
    SilverContract,
    SilverContractLoader,
    SilverQualityEngine,
    SilverQuarantineManager,
)

logger = logging.getLogger("silver.framework")
RELEASE_NAMESPACE = "silver_v2_release_20261013"
RELEASE_SOURCES = ("faostat_production", "faostat_monthly_price", "faostat_supply_utilization",
                   "faostat_trade", "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan")
AUDIT_COLUMNS = (
    "_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id",
    "_ingestion_timestamp", "_source_id", "_source_file",
    "_source_checksum", "_source_snapshot_id",
)

# ---------------------------------------------------------------------------
# Local Profile & Routing (formerly `local_profile.py`)
# ---------------------------------------------------------------------------

PROFILE = "local_nine"
NAMESPACE = "silver_local"
MAPPINGS = (
    ("faostat_production", "faostat_production", "faostat_production"),
    ("faostat_monthly_price", "faostat_monthly_price", "faostat_monthly_price"),
    ("faostat_supply_utilization", "faostat_supply_utilization", "faostat_supply_utilization"),
    ("faostat_trade", "faostat_trade", "faostat_trade"),
    ("usda_psd", "usda_psd", "usda_rice_psd"),
    ("usda_rice_yearbook", "usda_rice_yearbook", "usda_rice_yearbook"),
    ("worldbank_pinksheet", "worldbank_pinksheet", "worldbank_commodity_monthly"),
    ("thitruongnongsan", "thitruongnongsan", "thitruongnongsan"),
    ("nso", "nso", "nso_rice_statistics"),
)
BRONZE_SOURCES = tuple(m[0] for m in MAPPINGS)
DATASETS = tuple(m[1] for m in MAPPINGS)
REGISTRY_SHA256 = "c8ae17ad7e5a538ef9a6e44427bc5ec2c443dc285788c34fb2275bd8acb7b1f3"
NSO_CONTRACT_SHA256 = "9246124b891138c18d851356a95d62b2fe177adb2c73df7968fb6d7e5ee0eb8b"


def active_profile():
    profile, enabled = os.getenv("SILVER_LOCAL_PROFILE"), os.getenv("SILVER_LOCAL_ENABLED", "0")
    if not profile and enabled == "0":
        return None
    if profile != PROFILE or enabled != "1":
        raise ValueError("local_nine requires explicit SILVER_LOCAL_PROFILE and SILVER_LOCAL_ENABLED=1")
    if (os.getenv("SILVER_ACCEPTED_RELEASE") or os.getenv("SILVER_PRODUCTION_ENABLED", "0") != "0"
            or os.getenv("SILVER_CANARY_ONLY", "0") != "0" or os.getenv("SILVER_BRONZE_SCHEDULE")):
        raise ValueError("local_nine conflicts with release/production/canary/schedule flags")
    return PROFILE


def require_profile(profile, release=None):
    if profile != PROFILE or release or active_profile() != PROFILE:
        raise ValueError("Explicit activated local_nine profile required; no release/default fallback")


def mapping(dataset):
    for source, name, target in MAPPINGS:
        if dataset == name:
            return source, target
    raise ValueError(f"Dataset outside local_nine: {dataset}")


def validate_registry(registry=None):
    require_profile(PROFILE)
    from ingestion.config.registry import SourceRegistry
    registry = registry or SourceRegistry()
    registry.load()
    sources = registry.get_all_sources()
    if hashlib.sha256(Path(registry.registry_path).read_bytes()).hexdigest() != REGISTRY_SHA256:
        raise ValueError("local_nine registry checksum drift")
    for name in BRONZE_SOURCES:
        config = sources.get(name)
        if (config is None or not config.enabled or config.source_id != name
                or config.source_type.value != "LOCAL_FILE" or "fixtures" in (config.local_path or "").split("/")):
            raise ValueError(f"Missing/disabled/unapproved local_nine registry source: {name}")
    return [sources[name] for name in BRONZE_SOURCES]


def local_contract(loader, dataset):
    require_profile(PROFILE)
    source, target = mapping(dataset)
    original = loader.load_contract(dataset)
    if original.bronze_input != f"iceberg.bronze.{source}" or original.silver_output != f"iceberg.silver.{target}":
        raise ValueError(f"local_nine source/target contract routing mismatch: {dataset}")
    data = copy.deepcopy(original._data)
    if dataset == "nso":
        path = Path(loader.contract_dir) / (dataset + ".yaml")
        if original.status != "READY" or hashlib.sha256(path.read_bytes()).hexdigest() != NSO_CONTRACT_SHA256:
            raise ValueError("local_nine NSO contract requires the exact approved READY contract")
    original.validate_executable()
    data["dataset"]["silver_output"] = f"iceberg.{NAMESPACE}.{target}"
    contract = SilverContract(data)
    contract.validate_executable()
    return contract


# ---------------------------------------------------------------------------
# Airflow-to-Runner HTTP Bridge (formerly `orchestration.py`)
# ---------------------------------------------------------------------------

def trigger_spark_job(job_file="run_all_silver.py", mode="full", run_id=None,
                      dataset="all", pipeline_run_id=None, action="run", watermark=None, release=None,
                      profile=None, bronze_snapshot_id=None):
    url = os.environ.get("SPARK_API_URL", "http://spark-iceberg:5005/run")
    if release or profile:
        with urllib.request.urlopen(url.rsplit("/", 1)[0] + "/health", timeout=10) as response:
            health = json.loads(response.read().decode("utf-8"))
        if profile:
            require_profile(profile, release)
            if health.get("local_profile") != profile:
                raise RuntimeError("Spark API lacks activated local_nine routing")
        elif health.get("silver_release") != release:
            raise RuntimeError("Spark API lacks approved release routing; reload it at owner-approved cutover")
    payload = {"job": job_file, "mode": mode, "run_id": run_id,
               "dataset": dataset, "pipeline_run_id": pipeline_run_id, "action": action, "watermark": watermark,
               "release": release}
    if profile:
        payload.update(profile=profile, bronze_snapshot_id=bronze_snapshot_id)
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3600) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Spark job failed HTTP {exc.code}: {exc.read().decode('utf-8')}") from exc
    if result.get("status") != "SUCCESS" or "result" not in result:
        raise RuntimeError(f"Spark job returned no successful result: {result}")
    return result["result"]


@dataclass(frozen=True)
class BronzeReadContext:
    """Metadata of this runner's pinned read, never a catalog lookup request."""
    bronze_input: str
    bronze_read_snapshot_id: Optional[int]


@runtime_checkable
class ReadContextTransformer(Protocol):
    def preprocess_with_read_context(self, df, context: BronzeReadContext):
        ...


class SilverTransformationFramework:
    def __init__(self, spark: SparkSession, contract_dir: str = None, release=None, profile=None):
        from silver.local_profile import active_profile
        if active_profile() and not profile:
            raise ValueError("Activated local_nine requires explicit profile; no default fallback")
        if profile:
            from silver.local_profile import require_profile
            require_profile(profile, release)
        if release is not None and release != RELEASE_NAMESPACE:
            raise ValueError(f"Unapproved Silver release: {release}")
        self.release = release
        self.profile = profile
        self.spark = spark
        self.contract_loader = SilverContractLoader(contract_dir)
        self.quarantine_manager = SilverQuarantineManager(spark)
        self.merge_engine = IcebergMergeEngine(spark)

    def contract(self, dataset_id):
        """Opt-in routing; the checked-in contracts and default routing stay intact."""
        if self.profile:
            from silver.local_profile import local_contract
            return local_contract(self.contract_loader, dataset_id)
        if self.release and dataset_id not in RELEASE_SOURCES:
            raise ValueError(f"Source outside Silver V2 release: {dataset_id}")
        contract = self.contract_loader.load_contract(dataset_id)
        if self.release:
            data = copy.deepcopy(contract._data)
            catalog, _, table = contract.silver_output.rsplit(".", 2)
            data["dataset"]["silver_output"] = f"{catalog}.{self.release}.{table}"
            contract = SilverContract(data)
        contract.validate_executable()
        return contract

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
        from pyspark.sql import functions as F, types as T
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

    def run(self, dataset_id: str, pipeline_run_id: str, mode="full", run_id=None, watermark=None,
            bronze_snapshot_id=None):
        from pyspark import StorageLevel
        from pyspark.sql import functions as F, types as T
        contract = self.contract(dataset_id)
        if self.release or self.profile:
            self.quarantine_manager.target_table = contract.silver_output.rsplit(".", 1)[0] + ".dead_letters"
        if mode not in {"full", "incremental"}:
            raise ValueError(f"Unsupported read mode: {mode}")
        if mode == "incremental" and not (run_id or watermark):
            raise ValueError("Incremental mode requires run_id or watermark")
        cached = []
        stage, quarantine_committed, exclusions_committed = "bronze_read", False, False
        read_snapshot_id = None
        logger.info("silver_start dataset=%s pipeline_run_id=%s mode=%s", dataset_id, pipeline_run_id, mode)
        try:
            # --- STEP 1: read_bronze (Read pinned Bronze Iceberg snapshot) ---
            current = self.spark.sql(f"SELECT snapshot_id FROM {contract.bronze_input}.refs WHERE name = 'main'").first()
            read_snapshot_id = current[0] if current else None
            if bronze_snapshot_id is not None and read_snapshot_id != int(bronze_snapshot_id):
                raise ValueError(f"Bronze snapshot drift: {contract.bronze_input}")
            reader = self.spark.read.format("iceberg")
            if read_snapshot_id is not None:
                reader = reader.option("snapshot-id", str(read_snapshot_id))
            bronze = reader.load(contract.bronze_input)
            if mode == "incremental":
                if run_id:
                    bronze = bronze.filter(F.col("_ingestion_run_id") == F.lit(run_id))
                if watermark:
                    bronze = bronze.filter(F.col("_ingestion_timestamp") > F.lit(watermark))
            bronze_count = bronze.count() if dataset_id in {"usda_psd", "worldbank_pinksheet", "nso"} else None
            if dataset_id == "nso":
                bronze = bronze.withColumn("_source_payload", F.to_json(
                    F.struct(*[F.col(c) for c in sorted(bronze.columns)]),
                    options={"ignoreNullFields": "false"}))
            else:
                bronze = bronze.withColumn("_source_payload", F.struct(*[F.col(c) for c in sorted(bronze.columns)]))
            if dataset_id == "usda_psd":
                bronze = bronze.localCheckpoint(eager=True)

            # --- STEP 2: transform (Source-specific preprocess + Contract normalize) ---
            stage = "transformation"
            transformer = self.transformer(dataset_id)
            observation_metrics = {}
            exclusions = None
            if transformer and hasattr(transformer, "observation_audit"):
                try:
                    bronze._nso_cache_key = (contract.bronze_input, read_snapshot_id) if mode == "full" and contract.bronze_input == "iceberg.bronze.nso" else None
                except Exception:
                    pass
                observation_metrics = transformer.observation_audit(bronze)
                exclusions = transformer.excluded_records(bronze)
            if isinstance(transformer, ReadContextTransformer):
                # Only opt-in transformers receive this binding. Both values come
                # from the exact snapshot-id used by the reader above, not latest.
                context = BronzeReadContext(contract.bronze_input, read_snapshot_id)
                bound = bronze.withColumn("_bronze_read_snapshot_id", F.lit(read_snapshot_id).cast("long"))
                bound._nso_validated_files = getattr(bronze, "_nso_validated_files", None)
                bound._is_full_snapshot = (mode == "full")
                prepared = transformer.preprocess_with_read_context(bound, context)
            else:
                prepared = transformer.preprocess(bronze) if transformer else bronze
            normalized = self.normalize(prepared, contract)

            # --- STEP 3: validate_quality (Evaluate YAML contract DQ rules) ---
            rules = list(contract.data_quality_rules)
            required = set(contract.business_key) | {c["name"] for c in contract.columns if not c.get("nullable", True)}
            for name in sorted(required):
                rules.append({"rule_id": f"SYS_REQUIRED_{name}", "sql_expr": f"`{name}` IS NOT NULL",
                              "failed_column": name, "rule": "Required field or business key is missing"})
            if transformer and hasattr(transformer, "quality_rules"):
                rules.extend(transformer.quality_rules())
            stage = "quality"
            checked = SilverQualityEngine(rules).evaluate(normalized)
            # Full wide JSON is needed only by dead letters. Caching it for
            # millions of valid observations wastes memory without adding lineage.
            payload_expr = (F.col("_source_payload") if isinstance(checked.schema["_source_payload"].dataType, T.StringType)
                            else F.to_json(F.col("_source_payload"), options={"ignoreNullFields": "false"}))
            checked = checked.withColumn("_source_payload", F.when(F.size("dq_errors") > 0, payload_expr))
            checked = checked.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(checked)
            stats = checked.agg(
                F.count("*").alias("prepared_count"),
                F.sum(F.when(F.size("dq_errors") > 0, 1).otherwise(0)).alias("quarantine_count"),
                F.sum(F.when(F.size("_dq_warnings") > 0, 1).otherwise(0)).alias("warning_count"),
            ).first().asDict()
            prepared_count = stats["prepared_count"]
            if bronze_count is None:
                bronze_count = prepared_count
            if observation_metrics and observation_metrics["observation_count"] != prepared_count + observation_metrics["excluded_count"]:
                raise ValueError("Observation reconciliation failed: candidates != prepared + excluded")
            quarantine_count = stats["quarantine_count"] or 0
            valid_count = prepared_count - quarantine_count
            quarantine = checked.filter(F.size("dq_errors") > 0)

            # --- STEP 4: quarantine_bad_rows (Route failed/excluded rows to dead_letters) ---
            stage = "quarantine"
            self.quarantine_manager.ensure_table()
            if exclusions is not None:
                if observation_metrics.get("excluded_record_count", 1) > 0:
                    self.quarantine_manager.route_quarantine(
                        exclusions, dataset_id + "_exclusions", pipeline_run_id, ["source_table"])
                exclusions_committed = bool(observation_metrics.get("excluded_record_count", 0))
            if quarantine_count > 0:
                self.quarantine_manager.route_quarantine(quarantine, dataset_id, pipeline_run_id, contract.business_key)
            quarantine_committed = quarantine_count > 0
            valid = checked.filter(F.size("dq_errors") == 0).select(
                *[c["name"] for c in contract.columns],
                *[c for c in AUDIT_COLUMNS if c in checked.columns], "_dq_warnings")

            # --- STEP 5: merge_to_silver (Deduplicate by business_key & Iceberg MERGE INTO) ---
            stage = "deduplication"
            dedup_raw = self.merge_engine.deduplicate(valid, contract.business_key)
            if dedup_raw is valid:
                dedup = valid
                dedup_count = valid_count
            else:
                dedup = dedup_raw.persist(StorageLevel.MEMORY_AND_DISK)
                cached.append(dedup)
                dedup_count = dedup.count()
                try:
                    checked.unpersist()
                except Exception:
                    pass
            output = dedup.withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc)))
            stage = "silver_merge"
            self.merge_engine.merge(output, target_table=contract.silver_output, business_keys=contract.business_key,
                                    partition_columns=contract.partition_columns)
            result = {"status": "SUCCESS", "dataset": dataset_id, "pipeline_run_id": pipeline_run_id,
                      "target_table": contract.silver_output, "bronze_read_snapshot_id": read_snapshot_id, "bronze_count": bronze_count,
                      "prepared_count": prepared_count, "valid_count": valid_count,
                      "quarantine_count": quarantine_count, "warning_count": stats["warning_count"] or 0,
                      "dedup_count": dedup_count, "duplicate_count": valid_count - dedup_count}
            result.update(observation_metrics)
            logger.info("silver_complete %s", result)
            if result["warning_count"]:
                logger.warning("silver_warnings dataset=%s rows=%s; see _dq_warnings", dataset_id, result["warning_count"])
            return result
        except Exception as exc:
            # Quarantine and Silver commits are separate transactions. Preserve
            # the original exception type and expose recovery evidence to callers.
            exc.silver_failure = {"stage": stage, "quarantine_committed": quarantine_committed,
                                  "exclusions_committed": exclusions_committed, "bronze_read_snapshot_id": read_snapshot_id}
            logger.exception("silver_failed dataset=%s pipeline_run_id=%s recovery=%s", dataset_id, pipeline_run_id, exc.silver_failure)
            raise
        finally:
            for df in reversed(cached):
                try:
                    df.unpersist()
                except Exception:
                    logger.warning("silver_cache_cleanup_failed dataset=%s; preserve original pipeline outcome", dataset_id)

    _sanitize_column_name = staticmethod(sanitize_column_name)


# ---------------------------------------------------------------------------
# Readiness & Audit Checks (formerly `audit.py`)
# ---------------------------------------------------------------------------

def check_bronze(spark, contracts, profile=None):
    from pyspark.sql import functions as F
    results = {}
    for contract in contracts:
        contract.validate_executable()
        framework = SilverTransformationFramework(spark, **({"profile": profile} if profile else {}))
        source_id = contract.source_id
        source = spark.table(contract.bronze_input)
        transformer = framework.transformer(source_id)
        if transformer and hasattr(transformer, "validate_bronze_schema"):
            transformer.validate_bronze_schema(source)
        if isinstance(transformer, ReadContextTransformer):
            snapshot = spark.sql(f"SELECT snapshot_id FROM {contract.bronze_input}.refs WHERE name = 'main'").first()[0]
            source = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(contract.bronze_input)
            bound = source.withColumn("_bronze_read_snapshot_id", F.lit(snapshot).cast("long"))
            bound._is_full_snapshot = True
            bound._is_schema_probe = True
            prepared = transformer.preprocess_with_read_context(bound, BronzeReadContext(contract.bronze_input, snapshot))
        else:
            probe = source.limit(0)
            probe._is_schema_probe = True
            prepared = transformer.preprocess(probe) if transformer else probe
            if source_id in {"usda_psd", "worldbank_pinksheet"}:
                prepared = spark.createDataFrame([], prepared.schema)
        normalized = framework.normalize(prepared, contract)
        rules = contract.data_quality_rules + (transformer.quality_rules() if transformer and hasattr(transformer, "quality_rules") else [])
        SilverQualityEngine(rules).evaluate(normalized).schema
        if source.limit(1).count() == 0:
            raise ValueError(f"Bronze readiness failed: {contract.bronze_input} has no rows; ingest source data before running the Silver DAG")
        results[source_id] = {"ready": True, "table": contract.bronze_input}
    return {"status": "PASSED", "tables": results}


def audit_tables(spark, contracts):
    from pyspark.sql import functions as F, types as T
    results = {}
    for contract in contracts:
        contract.validate_executable()
        df = spark.table(contract.silver_output)
        fields = {field.name: field.dataType for field in df.schema}
        for definition in contract.columns:
            expected = T._parse_datatype_string(definition["data_type"])
            if fields.get(definition["name"]) != expected:
                raise ValueError(f"Silver audit schema mismatch: {contract.silver_output}.{definition['name']}")
        required = set(contract.business_key) | {c["name"] for c in contract.columns if not c.get("nullable", True)}
        missing = None
        for key in sorted(required):
            condition = F.col(key).isNull()
            missing = condition if missing is None else missing | condition
        blocking = [r for r in contract.data_quality_rules if r.get("action", "QUARANTINE") == "QUARANTINE"]
        checked = SilverQualityEngine(blocking).evaluate(df)
        bk_hash = F.xxhash64(*[F.col(f"`{k}`") for k in contract.business_key])
        stats = (
            checked.groupBy(bk_hash.alias("_bk_h"))
            .agg(
                F.count("*").alias("_cnt"),
                F.max(F.when(missing, 1).otherwise(0)).alias("_null"),
                F.max(F.when(F.size("dq_errors") > 0, 1).otherwise(0)).alias("_inv"),
            )
            .agg(
                F.coalesce(F.sum("_cnt"), F.lit(0)).alias("rows"),
                F.max("_null").alias("has_null"),
                F.max(F.when(F.col("_cnt") > 1, 1).otherwise(0)).alias("has_dup"),
                F.max("_inv").alias("has_invalid"),
            )
            .first()
            .asDict()
        )
        if stats["has_dup"] and len(contract.business_key) > 1:
            stats["has_dup"] = checked.groupBy(*contract.business_key).count().where("count > 1").limit(1).count()
        if stats["has_null"]:
            raise ValueError(f"Silver audit null required field: {contract.silver_output}")
        if stats["has_dup"]:
            raise ValueError(f"Silver audit duplicate keys: {contract.silver_output}")
        if stats["has_invalid"]:
            raise ValueError(f"Silver audit blocking DQ failure: {contract.silver_output}")
        results[contract.silver_output] = {"rows": stats["rows"] or 0, "status": "PASSED"}
    return {"status": "PASSED", "tables": results}
