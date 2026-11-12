"""Read-only readiness and reconciliation checks used by CLI and Airflow."""
from pyspark.sql import functions as F, types as T
from silver.framework import SilverTransformationFramework, ReadContextTransformer, BronzeReadContext
from silver.engine.quality import SilverQualityEngine


def check_bronze(spark, contracts, profile=None):
    results = {}
    for contract in contracts:
        contract.validate_executable()
        # Resolve source preprocessing and projected SQL schema, not only API health.
        framework = SilverTransformationFramework(spark, **({"profile": profile} if profile else {}))
        source_id = contract.source_id
        source = spark.table(contract.bronze_input)
        transformer = framework.transformer(source_id)
        if transformer and hasattr(transformer, "validate_bronze_schema"):
            transformer.validate_bronze_schema(source)
        if isinstance(transformer, ReadContextTransformer):
            # A readiness read has its own real pin; never fabricate NSO context
            # from source lineage or bypass its approved revision/schema guard.
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
