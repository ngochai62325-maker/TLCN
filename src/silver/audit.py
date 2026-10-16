"""Read-only readiness and reconciliation checks used by CLI and Airflow."""
from pyspark.sql import functions as F, types as T
from silver.framework import SilverTransformationFramework
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
        if profile:
            from silver.framework import ReadContextTransformer, BronzeReadContext
        if profile and isinstance(transformer, ReadContextTransformer):
            # A readiness read has its own real pin; never fabricate NSO context
            # from source lineage or bypass its approved revision/schema guard.
            snapshot = spark.sql(f"SELECT snapshot_id FROM {contract.bronze_input}.refs WHERE name = 'main'").first()[0]
            source = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(contract.bronze_input)
            bound = source.withColumn("_bronze_read_snapshot_id", F.lit(snapshot).cast("long"))
            prepared = transformer.preprocess_with_read_context(bound, BronzeReadContext(contract.bronze_input, snapshot))
        else:
            prepared = transformer.preprocess(source.limit(0)) if transformer else source.limit(0)
        normalized = framework.normalize(prepared, contract)
        rules = contract.data_quality_rules + (transformer.quality_rules() if transformer and hasattr(transformer, "quality_rules") else [])
        SilverQualityEngine(rules).evaluate(normalized).schema
        if source.isEmpty():
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
        if df.filter(missing).limit(1).count():
            raise ValueError(f"Silver audit null required field: {contract.silver_output}")
        if df.groupBy(*contract.business_key).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Silver audit duplicate keys: {contract.silver_output}")
        # Evaluate the published blocking SQL rules on persisted Silver data.
        blocking = [r for r in contract.data_quality_rules if r.get("action", "QUARANTINE") == "QUARANTINE"]
        _, invalid = SilverQualityEngine(blocking).apply_rules(df)
        if invalid.limit(1).count():
            raise ValueError(f"Silver audit blocking DQ failure: {contract.silver_output}")
        results[contract.silver_output] = {"rows": df.count(), "status": "PASSED"}
    return {"status": "PASSED", "tables": results}
