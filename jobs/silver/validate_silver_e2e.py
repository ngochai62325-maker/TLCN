"""Run, audit and replay all READY sources against the configured Iceberg catalog.

Reads Bronze without modifying ingestion metadata. Writes only the contract
Silver targets and quarantine. Output is reproducible JSON acceptance evidence.
"""
import argparse
import json
import logging
import hashlib
import os
import re
from pathlib import Path
from datetime import datetime, timezone

from run_all_silver import ROOT, datasets, selected_sources, source_lock
from silver.audit import audit_tables, check_bronze
from silver.core.spark_session import get_spark_session
from silver.framework import SilverTransformationFramework, RELEASE_NAMESPACE, AUDIT_COLUMNS
from pyspark.sql import functions as F
from ingestion.storage.bronze_writer import sanitize_column_name
from ingestion.utils.hashing import compute_file_checksum


def table_state(spark, contract):
    df = spark.table(contract.silver_output)
    identity = table_identity(spark, contract.silver_output)
    # The sum is a cheap replay check across all rows, including processing time.
    checksum = df.select(F.xxhash64(*df.columns).cast("decimal(38,0)").alias("h")).agg(F.sum("h")).first()[0]
    return {**identity, "rows": df.count(), "row_checksum": str(checksum), "schema": df.schema.jsonValue()}


def table_identity(spark, name):
    if not spark.catalog.tableExists(name):
        return {"exists": False}
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    metadata = table.operations().current()
    current = metadata.currentSnapshot()
    return {"exists": True, "uuid": str(metadata.uuid()), "location": str(metadata.location()),
            "schema_id": metadata.currentSchemaId(), "metadata_location": str(metadata.metadataFileLocation()),
            "snapshot_id": str(current.snapshotId()) if current else None}


def verify_iceberg_schema(spark, contract, identity):
    """Read the exact metadata file via PyIceberg, independently of Spark's schema."""
    from pyiceberg.table import StaticTable
    catalog = contract.silver_output.split(".")[0]
    properties = {}
    for key in ("s3.endpoint", "s3.access-key-id", "s3.secret-access-key", "s3.region"):
        value = spark.conf.get(f"spark.sql.catalog.{catalog}.{key}", None)
        if value:
            properties[key] = value
    table = StaticTable.from_metadata(identity["metadata_location"], properties=properties)
    if str(table.metadata.table_uuid) != identity["uuid"]:
        raise ValueError("PyIceberg/Spark UUID mismatch")
    snapshot = table.current_snapshot()
    if (str(snapshot.snapshot_id) if snapshot else None) != identity["snapshot_id"]:
        raise ValueError("PyIceberg/Spark current snapshot mismatch")
    fields = {f.name: str(f.field_type).replace(" ", "") for f in table.schema().fields}
    expected = {c["name"]: c["data_type"].replace("integer", "int").replace("bigint", "long") for c in contract.columns}
    for field in spark.table(contract.silver_output).schema:
        if field.name in AUDIT_COLUMNS or field.name == "_silver_processed_at":
            expected[field.name] = {"bigint": "long", "timestamp": "timestamptz", "timestamp_ntz": "timestamp"}.get(field.dataType.simpleString(), field.dataType.simpleString())
    if any(fields.get(name) != dtype for name, dtype in expected.items()):
        raise ValueError("PyIceberg/Spark schema mismatch")
    warnings = table.schema().find_field("_dq_warnings").field_type.element_type
    if [(f.name, str(f.field_type)) for f in warnings.fields] != [(n, "string") for n in ("rule_id", "error_message", "failed_column")]:
        raise ValueError("PyIceberg warning schema mismatch")
    return fields


def code_fingerprint(contract_dir, registry_path=None):
    paths = [*Path(contract_dir).glob("*.yaml"), * (ROOT / "src/silver").rglob("*.py"),
             * (ROOT / "jobs/silver").glob("*.py")]
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(path.read_bytes())
    digest.update(Path(registry_path or ROOT / "config/ingestion/source_registry.yaml").read_bytes())
    return digest.hexdigest()


def input_state(spark, contract, release, registry_path=None):
    identity = table_identity(spark, contract.bronze_input)
    if not identity["exists"] or identity["snapshot_id"] is None:
        raise ValueError(f"Missing Bronze snapshot: {contract.source_id}")
    bronze = spark.read.format("iceberg").option("snapshot-id", identity["snapshot_id"]).load(contract.bronze_input)
    if not set(AUDIT_COLUMNS).issubset(bronze.columns):
        raise ValueError("Bronze is missing required lineage fields")
    missing = F.lit(False)
    for column in AUDIT_COLUMNS:
        missing = missing | F.col(column).isNull()
    if not bronze.filter(missing).limit(1).isEmpty():
        raise ValueError("Bronze has incomplete lineage")
    identities = bronze.select("_ingestion_run_id", "_source_checksum", "_source_id", "_source_file").distinct().collect()
    if len(identities) != 1 or identities[0]._source_id != contract.source_id:
        raise ValueError(f"Release requires one complete source run: {contract.source_id}")
    row = identities[0]
    if any(row[c] is None for c in row.asDict()) or not re.fullmatch(r"[0-9a-f]{64}", row._source_checksum):
        raise ValueError("Invalid Bronze source identity/checksum")
    result = {**identity, "run_id": row._ingestion_run_id, "checksum": row._source_checksum,
              "source_file": row._source_file, "rows": bronze.count()}
    if release:
        from ingestion.config.registry import SourceRegistry
        from ingestion.storage.metadata_repository import MetadataRepository
        registry = SourceRegistry(str(registry_path) if registry_path else None); registry.load()
        raw = Path(registry.get_source(contract.source_id).local_path)
        if not raw.is_absolute():
            raw = ROOT / raw
        checksum = compute_file_checksum(str(raw))
        if checksum != row._source_checksum or raw.name != row._source_file:
            raise ValueError("Raw/Bronze artifact mismatch")
        if contract.source_id == "faostat_trade" and checksum != "697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168":
            raise ValueError("Trade artifact differs from the approved full Raw")
        with MetadataRepository()._get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT status,records_extracted,checksum FROM ingestion.ingestion_runs WHERE run_id=%s AND source_id=%s", (row._ingestion_run_id, contract.source_id))
                run = cursor.fetchone()
        if not run or tuple(run) != ("SUCCESS", result["rows"], checksum):
            raise ValueError("Bronze run not complete or checksum differs from PostgreSQL")
        result["raw_bytes"] = raw.stat().st_size
    return result


def accept_source(spark, framework, source, approved_input, trino_url=None):
    """Called while the caller holds the source lock, including failure readback."""
    contract = framework.contract(source)
    before = table_identity(spark, contract.silver_output)
    try:
        metrics = framework.run(source, "silver-release-first", bronze_snapshot_id=approved_input["snapshot_id"])
        first = table_state(spark, contract)
        if metrics["bronze_read_snapshot_id"] != int(approved_input["snapshot_id"]):
            raise ValueError("Unapproved Bronze read snapshot")
        if metrics["prepared_count"] != metrics["valid_count"] + metrics["quarantine_count"] or metrics["valid_count"] != metrics["dedup_count"] + metrics["duplicate_count"] or first["rows"] != metrics["dedup_count"]:
            raise ValueError("Actual count reconciliation failed")
        bronze = spark.read.format("iceberg").option("snapshot-id", approved_input["snapshot_id"]).load(contract.bronze_input)
        excluded = metrics.get("excluded_count", 0)
        if source == "worldbank_pinksheet":
            synthetic = [c for c in bronze.columns if re.fullmatch(r"commodity_[0-9]+", c)]
            counts = bronze.agg(*[F.count(c).alias(c) for c in synthetic]).first().asDict() if synthetic else {}
            excluded = approved_input["rows"] * sum(n == 0 for n in counts.values())
        checks = verify_values(spark, source, contract, metrics)
        schema = verify_iceberg_schema(spark, contract, first)
        audit = audit_tables(spark, [contract])
        dead = framework.quarantine_manager.target_table
        actual_quarantine = spark.table(dead).filter(F.col("source_dataset") == source).count()
        if actual_quarantine != metrics["quarantine_count"]:
            raise ValueError("Persisted quarantine count mismatch")
        for mode in ("full", "incremental"):
            replay = framework.run(source, f"silver-release-{mode}", mode=mode,
                                   run_id=approved_input["run_id"] if mode == "incremental" else None,
                                   bronze_snapshot_id=approved_input["snapshot_id"])
            if replay["bronze_count"] != approved_input["rows"] or table_state(spark, contract) != first:
                raise ValueError(f"{source}: {mode} replay is not a NOOP")
            for key in ("prepared_count", "valid_count", "quarantine_count", "dedup_count", "duplicate_count"):
                if replay[key] != metrics[key]:
                    raise ValueError(f"{source}: {mode} metric drift")
        if table_identity(spark, contract.bronze_input)["snapshot_id"] != approved_input["snapshot_id"]:
            raise ValueError("Bronze changed during acceptance")
        record = {"status": "VALIDATED_PENDING_OWNER", "metrics": metrics, "silver_before": before,
                  "silver": first, "bronze_snapshot_id": approved_input["snapshot_id"], "input": approved_input,
                  "actual_counts": {**{k: metrics[k] for k in ("prepared_count", "valid_count", "quarantine_count", "dedup_count", "duplicate_count", "warning_count")}, "excluded_count": excluded},
                  "audit": audit, "value_checks": checks, "iceberg_schema": schema,
                  "replay_verified": True, "incremental_replay_verified": True,
                  "business_keys": contract.business_key, "quarantine_table": dead,
                  "bronze_table": contract.bronze_input, **trino_distributions(spark, source, contract)}
        if trino_url:
            from verify_silver_trino import verify_record
            record["trino"] = verify_record(trino_url, source, record)
        return record
    except Exception as exc:
        recovery = {**getattr(exc, "silver_failure", {}), "target_before": before,
                    "retry_requires_catalog_readback": True, "commit_outcome": "UNKNOWN"}
        try:
            recovery["target_after"] = table_identity(spark, contract.silver_output)
            recovery["catalog_readback"] = "SUCCEEDED"
        except Exception as read_error:
            recovery["catalog_readback"] = str(read_error)
        exc.silver_failure = recovery
        raise


def trino_distributions(spark, source, contract):
    """Small expectations for readback; bound large distinct-key checks by year."""
    output = spark.table(contract.silver_output)
    value = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
    units = [r.asDict() for r in output.groupBy("unit").agg(F.count("*").alias("rows"), F.count(value).alias("present"), F.sum(value).cast("string").alias("sum_value")).collect()]
    flags = {c: [r.asDict() for r in output.groupBy(F.col(c).alias("flag")).count().withColumnRenamed("count", "rows").collect()] for c in ("flag", "flag_description", "note", "missing_reason") if c in output.columns}
    partitions = ([r.asDict() for r in output.groupBy("year").count().withColumnRenamed("count", "rows").collect()]
                  if "year" in contract.business_key and output.count() > 100_000 else [])
    return {"unit_distribution": units, "flag_distributions": flags, "key_partitions": partitions}


def verify_values(spark, source, contract, metrics):
    """Check source facts on persisted outputs, independently of DQ metrics."""
    output, bronze = spark.table(contract.silver_output), spark.table(contract.bronze_input)
    audit = ["_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
             "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id"]
    assert all(output.schema[c].dataType == bronze.schema[c].dataType for c in audit)
    assert output.select(*audit).distinct().join(bronze.select(*audit).distinct(), audit, "left_anti").isEmpty()
    if source.startswith("faostat_"):
        # Use source text dimensions and original audit identity to verify that
        # flags/descriptions/notes stayed attached to their source observations.
        mapping = ([("reporter_country_name", "reporter_countries"), ("partner_country_name", "partner_countries"),
                    ("commodity_name", "item"), ("element_name", "element")] if source == "faostat_trade" else
                   [("country_name", "area"), ("commodity_name", "item"), ("element", "element")])
        if source == "faostat_monthly_price":
            mapping.append(("month_name", "months"))
        keys = [*audit, "year", *[name for name, _ in mapping]]
        provenance = [c for c in ("flag", "flag_description", "note") if c in output.columns and c in bronze.columns]
        expected = bronze.select(*audit, F.col("year").cast("int").alias("year"),
                                 *[F.trim(F.col(raw).cast("string")).alias(name) for name, raw in mapping],
                                 *[F.when(F.trim(F.col(c).cast("string")) != "", F.trim(F.col(c).cast("string"))).alias("expected_" + c) for c in provenance])
        compared = output.join(expected, keys, "left")
        assert all(compared.filter(~F.col(c).eqNullSafe(F.col("expected_" + c))).isEmpty() for c in provenance)
    assert output.filter("missing_reason IS NOT NULL").filter((F.col("price") if source == "worldbank_pinksheet" else F.col("price_value") if source == "thitruongnongsan" else F.col("value")).isNotNull()).isEmpty()
    value_column = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
    dtype = next(c["data_type"] for c in contract.columns if c["name"] == value_column)
    if source != "usda_psd":
        raw = "regexp_replace(value_raw, ',', '')" if source == "thitruongnongsan" else "value_raw"
        if source == "faostat_production":
            raw = f"try_cast({raw} as decimal(38,12)) * CASE WHEN element_code = '5510' AND lower(unit_raw) = 'kg' THEN 0.001 WHEN element_code = '5412' AND lower(unit_raw) = 'hg/ha' THEN 0.1 WHEN element_code = '5412' AND lower(unit_raw) = 't/ha' THEN 1000 ELSE 1 END"
        assert output.filter(~F.col(value_column).eqNullSafe(F.expr(f"try_cast({raw} AS {dtype})"))).isEmpty()
    if source == "faostat_production":
        assert output.filter("(element_code = '5312' AND unit <> 'hectare') OR (element_code = '5510' AND unit <> 'tonne') OR (element_code = '5412' AND unit <> 'kg/hectare')").isEmpty()
    elif source == "faostat_monthly_price":
        assert output.filter("(time_grain = 'annual' AND month IS NOT NULL) OR (price_kind = 'producer_price_index' AND currency IS NOT NULL)").isEmpty()
    elif source == "faostat_supply_utilization":
        expected = bronze.filter("element_code = 5166 AND try_cast(value AS decimal(28,8)) < 0").count()
        residuals = output.filter("element_code = '5166' AND value < 0")
        assert residuals.count() == expected
        assert residuals.filter(F.size("_dq_warnings") == 0).isEmpty()
    elif source == "faostat_trade":
        assert output.filter("(element_code IN ('5610','5910') AND unit <> 'tonne') OR (element_code IN ('5622','5922') AND unit <> '1000 USD')").isEmpty()
    elif source == "usda_psd":
        assert output.filter("attribute = 'Milling Rate (.9999)' AND value IS NOT NULL AND (unit <> 'fraction' OR value <> value_source / 10000)").isEmpty()
        assert output.filter("attribute <> 'Milling Rate (.9999)' AND value <> try_cast(value_source AS decimal(18,4))").isEmpty()
    elif source == "usda_rice_yearbook":
        assert output.filter("unit <> 'USD/tonne'").isEmpty()
    elif source == "worldbank_pinksheet":
        assert output.filter("unit IS NULL OR month NOT BETWEEN 1 AND 12").isEmpty()
        # Source coverage includes every named series in the configured Monthly Prices sheet.
        series = [c for c in bronze.columns if not c.startswith("_") and c != "period" and not c.startswith("commodity_")]
        assert metrics["prepared_count"] == bronze.count() * len(series)
    elif source == "thitruongnongsan":
        assert output.filter("unit <> 'VND/kg' OR currency <> 'VND'").isEmpty()
        assert output.filter("price_value = 0").count() == bronze.filter(F.expr(f"try_cast(`{sanitize_column_name('Giá')}` AS decimal(18,2)) = 0")).count()
    return {"lineage_roundtrip": True, "source_values_units_flags": "PASSED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--dataset", default="all")
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--release", choices=[RELEASE_NAMESPACE])
    parser.add_argument("--preflight", action="store_true", help="Read-only shared release preparation")
    parser.add_argument("--execute", action="store_true", help="Explicitly authorize the requested release write")
    parser.add_argument("--approved-preflight", type=Path)
    parser.add_argument("--trino-url", default="http://trino:8080")
    parser.add_argument("--registry", type=Path, default=ROOT / "config/ingestion/source_registry.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    definitions = datasets(ROOT / "contracts/silver")
    sources = selected_sources(args.dataset, definitions, args.release)
    if args.release and not (args.preflight or args.audit_only or (args.execute and args.approved_preflight)):
        parser.error("Release writes require --execute --approved-preflight; use --preflight first")
    if args.preflight and (args.execute or args.audit_only):
        parser.error("Preflight cannot execute or audit a release")
    spark = get_spark_session("SilverEndToEndValidation")
    spark.sparkContext.setLogLevel("WARN")
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "status": "RUNNING", "datasets": {}}
    try:
        framework = SilverTransformationFramework(spark, str(ROOT / "contracts/silver"), release=args.release)
        contracts = [framework.contract(s) for s in sources]
        report["release"] = args.release
        report["code_fingerprint"] = code_fingerprint(ROOT / "contracts/silver", args.registry)
        report["bronze_readiness"] = check_bronze(spark, contracts)
        if args.release:
            # Fail before ANY write if any approved source is missing or incomplete.
            report["inputs"], report["targets"], report["legacy_targets"] = {}, {}, {}
            for contract in contracts:
                with source_lock(contract.source_id, args.release):
                    report["inputs"][contract.source_id] = input_state(spark, contract, args.release, args.registry)
                    report["targets"][contract.source_id] = table_identity(spark, contract.silver_output)
                    report["legacy_targets"][contract.source_id] = table_identity(spark, definitions[contract.source_id].silver_output)
                    if report["targets"][contract.source_id]["exists"]:
                        audit_tables(spark, [contract])
                        verify_iceberg_schema(spark, contract, report["targets"][contract.source_id])
            if args.preflight:
                report["status"] = "PREFLIGHT_PASSED"
                return
            if not args.audit_only:
                approved = json.loads(args.approved_preflight.read_text(encoding="utf-8"))
                for key in ("release", "code_fingerprint", "inputs", "targets", "legacy_targets"):
                    if approved.get(key) != report[key]:
                        raise ValueError(f"Approved preflight drift: {key}; obtain fresh approval")
                if approved.get("status") != "PREFLIGHT_PASSED":
                    raise ValueError("Owner-approved preflight must have passed")
                for contract in contracts:
                    source = contract.source_id
                    with source_lock(source, args.release):
                        if input_state(spark, contract, args.release, args.registry) != report["inputs"][source] or table_identity(spark, contract.silver_output) != report["targets"][source]:
                            raise ValueError(f"Source/target drift after locking: {source}")
                        report["datasets"][source] = accept_source(spark, framework, source, report["inputs"][source], args.trino_url)
                        if table_identity(spark, definitions[source].silver_output) != report["legacy_targets"][source]:
                            raise ValueError(f"Legacy target changed during release: {source}")
                report["status"] = "VALIDATED_PENDING_OWNER"
                return
        for contract in contracts:
            source = contract.source_id
            bronze_before = spark.table(contract.bronze_input + ".snapshots").orderBy(F.col("committed_at").desc()).first()
            if args.audit_only:
                metrics = None
            else:
                metrics = framework.run(source, "silver-e2e-first")
            first = table_state(spark, contract)
            audit = audit_tables(spark, [contract])
            if not args.audit_only:
                replay = framework.run(source, "silver-e2e-replay")
                second = table_state(spark, contract)
                if first != second:
                    report["replay_difference"] = {"dataset": source, "first": first, "second": second}
                    raise ValueError(f"Replay changed Silver rows/schema/snapshot: {source}")
                if metrics != dict(replay, pipeline_run_id=metrics["pipeline_run_id"]):
                    raise ValueError(f"Replay metrics differ: {source}")
            bronze_after = spark.table(contract.bronze_input + ".snapshots").orderBy(F.col("committed_at").desc()).first()
            if bronze_before.snapshot_id != bronze_after.snapshot_id:
                raise ValueError(f"Bronze changed during acceptance: {source}; rerun against stable input")
            record = {"status": "PASSED", "metrics": metrics, "silver": first,
                      "bronze_snapshot_id": bronze_before.snapshot_id, "audit": audit,
                      "replay_verified": not args.audit_only}
            report["datasets"][source] = record
            print("E2E_DATASET=" + json.dumps({"dataset": source, "status": "PASSED", "metrics": metrics}), flush=True)
        report["quarantine_counts"] = {r.source_dataset: r["count"] for r in spark.table(framework.quarantine_manager.target_table).groupBy("source_dataset").count().collect()}
        report["status"] = "PASSED"
    except Exception as exc:
        report["status"] = "FAILED"
        report["error"] = str(exc)
        report["recovery"] = getattr(exc, "silver_failure", {})
        raise
    finally:
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
        spark.stop()
        print("E2E_RESULT=" + json.dumps({"status": report["status"], "datasets": len(report["datasets"])}), flush=True)


if __name__ == "__main__":
    main()
