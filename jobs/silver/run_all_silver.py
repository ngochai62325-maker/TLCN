#!/usr/bin/env python3
"""Unified CLI entrypoint for the Silver layer (pipeline runs, schema migration, E2E validation & Trino verification)."""
import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.storage.bronze_writer import sanitize_column_name
from ingestion.utils.hashing import compute_file_checksum
from silver.engine import SilverContractLoader, get_spark_session, quote, table_identifier
from silver.framework import (
    AUDIT_COLUMNS,
    RELEASE_NAMESPACE,
    RELEASE_SOURCES,
    SilverTransformationFramework,
    audit_tables,
    check_bronze,
)

ALIASES = {"usda_export_price": "usda_rice_yearbook"}


# ---------------------------------------------------------------------------
# 1. Pipeline Helpers & Main Runner
# ---------------------------------------------------------------------------

def datasets(contract_dir):
    loader = SilverContractLoader(str(contract_dir))
    return {p.stem: loader.load_contract(p.stem) for p in sorted(Path(contract_dir).glob("*.yaml"))}


def selected_sources(dataset, contracts, release=None, profile=None, contract_dir=None):
    from silver.framework import active_profile
    if active_profile() and not profile:
        raise ValueError("Activated local_nine requires explicit profile; no default fallback")
    if profile:
        from silver.framework import require_profile, DATASETS, local_contract
        require_profile(profile, release)
        targets = dataset.split(",")
        if len(targets) != len(set(targets)) or any(k not in DATASETS for k in targets):
            raise ValueError("local_nine requires explicit approved dataset IDs")
        loader = SilverContractLoader(str(contract_dir or ROOT / "contracts/silver"))
        for target in targets:
            local_contract(loader, target)
        return targets
    if release and release != RELEASE_NAMESPACE:
        raise ValueError(f"Unapproved Silver release: {release}")
    if release and dataset == "all":
        raise ValueError("Release requires explicit source IDs; --dataset all is forbidden")
    targets = ([k for k, c in contracts.items() if c.status == "READY"] if dataset == "all"
               else [ALIASES.get(k.strip(), k.strip()) for k in dataset.split(",")])
    if len(targets) != len(set(targets)) or any(k not in contracts for k in targets):
        raise ValueError("Unknown or duplicate source ID")
    if release and any(k not in RELEASE_SOURCES for k in targets):
        raise ValueError("Source outside Silver V2 release")
    for target in targets:
        contracts[target].validate_executable()
    return targets


def source_lock(source, release, profile=None):
    if profile:
        from silver.framework import mapping, require_profile
        require_profile(profile, release)
        source = mapping(source)[0]
    if not release and not profile:
        return nullcontext()
    if not os.environ.get("INGESTION_DB_URL"):
        raise ValueError("Release requires explicit INGESTION_DB_URL; no unlocked fallback")
    from ingestion.storage.metadata_repository import MetadataRepository
    return MetadataRepository().source_lock(source)


def run_pipeline(dataset="all", mode="full", run_id=None, watermark=None,
                 pipeline_run_id=None, contract_dir=None, spark=None, release=None, profile=None,
                 bronze_snapshot_id=None):
    contract_dir = Path(contract_dir or ROOT / "contracts/silver")
    contracts = datasets(contract_dir)
    targets = selected_sources(dataset, contracts, release, profile, contract_dir)
    if dataset == "all":
        for key, contract in contracts.items():
            if contract.status != "READY":
                logging.warning("Pending Silver dataset %s: %s; excluded from all", key, contract.status)
    if any(k not in contracts for k in targets):
        raise ValueError(f"Unknown dataset {dataset}; available: {list(contracts)}")
    if mode == "incremental" and not (run_id or watermark):
        raise ValueError("Incremental mode requires --run-id or --watermark")
    if not profile:
        for target in targets:
            contracts[target].validate_executable()
    session = spark or get_spark_session("RiceLakehouseSilver")
    framework = SilverTransformationFramework(session, str(contract_dir), release=release, **({"profile": profile} if profile else {}))
    pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
    results = {}
    try:
        for target in targets:
            with source_lock(target, release, **({"profile": profile} if profile else {})):
                if release or profile:
                    table = framework.contract(target).silver_output
                    before = table_identity(session, table)
                    pinned = table_identity(session, framework.contract(target).bronze_input)["snapshot_id"]
                    if profile:
                        if bronze_snapshot_id is None or int(pinned) != int(bronze_snapshot_id):
                            raise ValueError("local_nine requires the verified batch Bronze snapshot; input drift")
                        pinned = int(bronze_snapshot_id)
                try:
                    results[target] = framework.run(target, pipeline_run_id, mode=mode, run_id=run_id, watermark=watermark,
                                                    **({"bronze_snapshot_id": pinned} if release or profile else {}))
                    if release or profile:
                        results[target]["commit_verified"] = {**table_identity(session, table), "rows": session.table(table).count()}
                except Exception as exc:
                    if release or profile:
                        recovery = {**getattr(exc, "silver_failure", {}), "target_before": before,
                                    "commit_outcome": "UNKNOWN", "retry_requires_catalog_readback": True}
                        try:
                            recovery["target_after"] = table_identity(session, table)
                        except Exception as read_error:
                            recovery["catalog_readback_error"] = str(read_error)
                        exc.silver_failure = recovery
                    raise
        return results
    finally:
        if spark is None:
            session.stop()


# ---------------------------------------------------------------------------
# 2. Empty Silver Schema Migration (formerly `migrate_empty_silver_schema.py`)
# ---------------------------------------------------------------------------

def migrate_empty_schema(dataset, execute=False, output_json=None):
    from pyspark.sql import types as T
    contract = datasets(ROOT / "contracts/silver")[dataset]
    contract.validate_executable()
    spark = get_spark_session("EmptySilverSchemaMigration")
    spark.sparkContext.setLogLevel("WARN")
    report = {"dataset": dataset, "table": contract.silver_output, "executed": execute, "actions": []}
    try:
        df = spark.table(contract.silver_output)
        if not df.isEmpty():
            raise ValueError("Refusing schema migration of a nonempty Silver table")
        fields = {f.name: f.dataType for f in df.schema}
        published = {c["name"] for c in contract.columns} | set(AUDIT_COLUMNS) | {"_dq_warnings", "_silver_processed_at"}
        for field in df.schema:
            if not field.nullable and field.name not in published and (field.name.startswith("_") or field.name == "dq_warnings"):
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ALTER COLUMN {quote(field.name)} DROP NOT NULL")
        for column in ["_ingestion_timestamp", "_silver_processed_at"]:
            legacy = column + "_legacy_ntz"
            if isinstance(fields.get(column), T.TimestampNTZType):
                if legacy in fields:
                    raise ValueError(f"Legacy column already exists: {legacy}; review previous migration")
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} RENAME COLUMN {quote(column)} TO {quote(legacy)}")
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ADD COLUMN {quote(column)} TIMESTAMP")
            elif column not in fields and legacy in fields:
                report["actions"].append(f"ALTER TABLE {table_identifier(contract.silver_output)} ADD COLUMN {quote(column)} TIMESTAMP")
        if execute:
            for action in report["actions"]:
                if not spark.table(contract.silver_output).isEmpty():
                    raise ValueError("Concurrent writer detected; stop and review migration")
                spark.sql(action)
        report["status"] = "MIGRATED" if execute and report["actions"] else "NOOP" if not report["actions"] else "PLANNED"
        if output_json:
            output_json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print("MIGRATION_RESULT=" + json.dumps(report))
        return report
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# 3. Trino Silver Verification (formerly `verify_silver_trino.py`)
# ---------------------------------------------------------------------------

def contract_expression_trino(expression):
    """Translate only the Spark constructs used by the nine pinned contracts."""
    expression = expression.replace("sha2(encode(geography_raw,'UTF-8'),256)", "lower(to_hex(sha256(to_utf8(geography_raw))))")
    expression = expression.replace("right(period_raw,4)", "substr(period_raw,-4)").replace("current_date()", "current_date")
    expression = expression.replace("try_multiply(value_source, conversion_factor)", "try(value_source * conversion_factor)")
    return re.sub(r"period_raw RLIKE ('[^']*')", r"regexp_like(period_raw,\1)", expression)


def identifier(name):
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def query(url, sql, timeout=600):
    deadline = time.monotonic() + timeout
    request = urllib.request.Request(
        url.rstrip("/") + "/v1/statement", data=sql.encode("utf-8"),
        headers={"X-Trino-User": "silver-validation", "Content-Type": "text/plain"})
    rows, columns = [], []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Trino query exceeded acceptance timeout")
        with urllib.request.urlopen(request, timeout=min(30, remaining)) as response:
            page = json.loads(response.read())
        if "error" in page:
            raise RuntimeError(f"Trino {page.get('id')}: {page['error']['message']}")
        columns = page.get("columns", columns)
        rows.extend(page.get("data", []))
        if not page.get("nextUri"):
            return [dict(zip([c["name"] for c in columns], row)) for row in rows]
        request = urllib.request.Request(page["nextUri"], headers={"X-Trino-User": "silver-validation"})


def verify_nso(url, table="iceberg.silver_local.nso_rice_statistics"):
    """Verify the current NSO contract via Trino."""
    import yaml
    contract = yaml.safe_load((ROOT / "contracts/silver/nso.yaml").read_text(encoding="utf-8"))
    target = identifier(table)
    actual = {r["Column"]: r["Type"] for r in query(url, f"DESCRIBE {target}")}
    for field in contract["schema"]["columns"]:
        expected = {"string": "varchar", "long": "bigint", "integer": "integer"}.get(field["data_type"], field["data_type"])
        if actual.get(field["name"]) != expected:
            raise ValueError(f"NSO contract schema mismatch: {field['name']}")
    expressions = []
    hard_rules = []
    for rule in contract["data_quality"]:
        expression = contract_expression_trino(rule["sql_expr"])
        alias = rule["rule_id"].lower()
        expressions.append(f"count_if(NOT coalesce(({expression}), FALSE)) AS {alias}")
        if rule.get("action", "QUARANTINE") != "LOG":
            hard_rules.append(alias)
    nonnull = [c["name"] for c in contract["schema"]["columns"] if not c["nullable"]]
    required = " OR ".join(identifier(c) + " IS NULL" for c in nonnull)
    keys = ",".join(identifier(c) for c in contract["grain"]["business_key"])
    sql = f"""SELECT count(*) AS rows, count(DISTINCT ROW({keys})) AS unique_keys,
        count_if({required}) AS invalid_required,
        count_if(geography_level='unknown') AS unknown_geography,
        count_if(quality_status='MISSING') AS missing_observations,
        count_if(value=0) AS genuine_zero,
        count_if(release_status='provisional') AS provisional,
        count_if(release_status='unconfirmed') AS unconfirmed,
        {', '.join(expressions)} FROM {target}"""
    counts = query(url, sql)[0]
    if counts["rows"] != counts["unique_keys"] or counts["invalid_required"] or any(counts[r] for r in hard_rules):
        raise ValueError(f"NSO contract/DQ violation: {counts}")
    return {"status": "PASSED", "target": table, "counts": counts,
            "scope": "retained Silver only; exclusions/quarantine need upstream admission evidence"}


def verify_record(url, source, record):
    """Reconcile persisted release data through Trino against Spark acceptance evidence."""
    if not record.get("replay_verified") or not record.get("incremental_replay_verified"):
        raise ValueError("Full AND incremental replay evidence required")
    table = identifier(record["metrics"]["target_table"])
    types = {r["Column"]: r["Type"] for r in query(url, f"DESCRIBE {table}")}
    expected = {}
    for field in record["silver"]["schema"]["fields"]:
        dtype = field["type"]
        if isinstance(dtype, str):
            expected[field["name"]] = {"string": "varchar", "long": "bigint", "timestamp": "timestamp(6) with time zone", "timestamp_ntz": "timestamp(6)"}.get(dtype, dtype)
        elif field["name"] == "_dq_warnings":
            expected[field["name"]] = "array(row(rule_id varchar, error_message varchar, failed_column varchar))"
    if types != expected:
        raise ValueError(f"Trino/Spark schema mismatch: {source}")
    keys = ",".join(identifier(k) for k in record["business_keys"])
    value = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
    partitions = record.get("key_partitions", [])
    distinct = "" if partitions else f"count(DISTINCT ROW({keys})) AS unique_keys,"
    counts = query(url, f"SELECT count(*) AS rows, {distinct} count_if(cardinality(_dq_warnings)>0) AS warnings, count_if(missing_reason IS NOT NULL AND {value} IS NOT NULL) AS invalid_missing FROM {table}")[0]
    if partitions:
        if "year" not in record["business_keys"] or len({p["year"] for p in partitions}) != len(partitions):
            raise ValueError("Invalid key partitions")
        rows, unique = 0, 0
        for partition in partitions:
            checked = query(url, f"SELECT count(*) AS rows,count(DISTINCT ROW({keys})) AS unique_keys FROM {table} WHERE year={int(partition['year'])}")[0]
            if checked["rows"] != partition["rows"]:
                raise ValueError(f"Trino partition count mismatch: {source}")
            rows += checked["rows"]
            unique += checked["unique_keys"]
        if rows != counts["rows"]:
            raise ValueError(f"Trino key partition coverage mismatch: {source}")
        counts["unique_keys"] = unique
    if counts["rows"] != record["silver"]["rows"] or counts["unique_keys"] != counts["rows"] or counts["warnings"] != record["metrics"]["warning_count"] or counts["invalid_missing"]:
        raise ValueError(f"Trino count/key/missing/warnings mismatch: {source}")
    bronze_name = record["bronze_table"]
    snapshot = int(record["bronze_snapshot_id"])
    audit = ",".join(identifier(c) for c in AUDIT_COLUMNS)
    invalid = query(url, f"SELECT count(*) AS invalid FROM (SELECT {audit} FROM {table} EXCEPT SELECT {audit} FROM {identifier(bronze_name)} FOR VERSION AS OF {snapshot})")[0]["invalid"]
    if invalid:
        raise ValueError(f"Trino Bronze lineage mismatch: {source}")
    distribution = query(url, f"SELECT unit,count(*) AS rows,count({value}) AS present,cast(sum({value}) AS varchar) AS sum_value FROM {table} GROUP BY unit ORDER BY unit")
    normalize = lambda rows: sorted([(r["unit"], int(r["rows"]), int(r["present"]), Decimal(r["sum_value"]) if r["sum_value"] is not None else None) for r in rows], key=lambda r: str(r[0]))
    if normalize(distribution) != normalize(record["unit_distribution"]):
        raise ValueError(f"Trino values/units distribution mismatch: {source}")
    for column in record["flag_distributions"]:
        actual = query(url, f"SELECT {identifier(column)} AS flag,count(*) AS rows FROM {table} GROUP BY {identifier(column)}")
        normalized = lambda rows: sorted([(r["flag"], r["rows"]) for r in rows], key=lambda r: str(r[0]))
        if normalized(actual) != normalized(record["flag_distributions"][column]):
            raise ValueError(f"Trino flag distribution mismatch: {source}.{column}")
    return {"status": "PASSED", "counts": counts, "unit_distribution": distribution, "lineage": "PASSED", "schema": types}


def _run_verify_trino(args):
    if args.nso_only:
        result = {"status": "RUNNING"}
        try:
            result = verify_nso(args.url, args.namespace + ".nso_rice_statistics")
        except Exception as exc:
            result = {"status": "FAILED", "error": str(exc)}
            raise
        finally:
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("TRINO_RESULT=" + json.dumps(result))
        return
    evidence = json.loads(args.spark_report.read_text(encoding="utf-8"))
    if evidence["status"] not in {"PASSED", "VALIDATED_PENDING_OWNER"}:
        raise ValueError("A PASSED Spark report is required")
    report = {"status": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        if evidence.get("release"):
            report["datasets"] = {s: verify_record(args.url, s, r) for s, r in evidence["datasets"].items()}
            report["status"] = "VALIDATED_PENDING_OWNER"
            return
        statements = []
        for source, record in evidence["datasets"].items():
            if not record.get("metrics") or not record.get("replay_verified"):
                raise ValueError("Trino comparison requires a full/replay report, not audit-only")
            table = record["metrics"]["target_table"]
            source_literal = source.replace("'", "''")
            statements.append(f"SELECT '{source_literal}' AS dataset, count(*) AS rows FROM {identifier(table)}")
        report["datasets"] = query(args.url, " UNION ALL ".join(statements))
        for row in report["datasets"]:
            expected = evidence["datasets"][row["dataset"]]["silver"]["rows"]
            if row["rows"] != expected:
                raise ValueError(f"Spark/Trino row mismatch: {row['dataset']}: {row['rows']} != {expected}")
        if "nso" in evidence["datasets"]:
            report["nso"] = verify_nso(args.url, evidence["datasets"]["nso"]["metrics"]["target_table"])
        if "usda_psd" in evidence["datasets"]:
            table = identifier(evidence["datasets"]["usda_psd"]["metrics"]["target_table"])
            report["usda_milling_rate"] = query(args.url, f"""SELECT count(*) AS rows,
                min(value_source) AS min_source_value, max(value_source) AS max_source_value,
                min(value) AS min_fraction, max(value) AS max_fraction,
                count_if(unit <> 'fraction' OR conversion_factor <> DECIMAL '0.0001'
                    OR value NOT BETWEEN 0 AND 1
                    OR value <> cast(value_source * conversion_factor AS DECIMAL(18,4))) AS invalid_rates
                FROM {table} WHERE attribute = 'Milling Rate (.9999)'""")[0]
            if report["usda_milling_rate"]["invalid_rates"]:
                raise ValueError("USDA milling conversion violated through Trino")
            columns = {r["Column"]: r["Type"] for r in query(args.url, f"DESCRIBE {table}")}
            report["usda_value_type"] = columns["value"]
            if columns["value"] != "decimal(18,4)":
                raise ValueError("Trino cannot read USDA value at the contract precision")
        report["status"] = "PASSED"
    except Exception as exc:
        report.update(status="FAILED", error=str(exc))
        raise
    finally:
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("TRINO_RESULT=" + json.dumps(report))


# ---------------------------------------------------------------------------
# 4. E2E Silver Acceptance Validation (formerly `validate_silver_e2e.py`)
# ---------------------------------------------------------------------------

def table_identity(spark, name):
    if not spark.catalog.tableExists(name):
        return {"exists": False}
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    metadata = table.operations().current()
    current = metadata.currentSnapshot()
    return {"exists": True, "uuid": str(metadata.uuid()), "location": str(metadata.location()),
            "schema_id": metadata.currentSchemaId(), "metadata_location": str(metadata.metadataFileLocation()),
            "snapshot_id": str(current.snapshotId()) if current else None}


def table_state(spark, contract):
    from pyspark.sql import functions as F
    df = spark.table(contract.silver_output)
    identity = table_identity(spark, contract.silver_output)
    checksum = df.select(F.xxhash64(*df.columns).cast("decimal(38,0)").alias("h")).agg(F.sum("h")).first()[0]
    return {**identity, "rows": df.count(), "row_checksum": str(checksum), "schema": df.schema.jsonValue()}


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
    paths = [*Path(contract_dir).glob("*.yaml"), *(ROOT / "src/silver").rglob("*.py"),
             *(ROOT / "jobs/silver").glob("*.py")]
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(path.read_bytes())
    digest.update(Path(registry_path or ROOT / "config/ingestion/source_registry.yaml").read_bytes())
    return digest.hexdigest()


def input_state(spark, contract, release, registry_path=None):
    from pyspark.sql import functions as F
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


def trino_distributions(spark, source, contract):
    """Small expectations for readback; bound large distinct-key checks by year."""
    from pyspark.sql import functions as F
    output = spark.table(contract.silver_output)
    value = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
    units = [r.asDict() for r in output.groupBy("unit").agg(F.count("*").alias("rows"), F.count(value).alias("present"), F.sum(value).cast("string").alias("sum_value")).collect()]
    flags = {c: [r.asDict() for r in output.groupBy(F.col(c).alias("flag")).count().withColumnRenamed("count", "rows").collect()] for c in ("flag", "flag_description", "note", "missing_reason") if c in output.columns}
    partitions = ([r.asDict() for r in output.groupBy("year").count().withColumnRenamed("count", "rows").collect()]
                  if "year" in contract.business_key and output.count() > 100_000 else [])
    return {"unit_distribution": units, "flag_distributions": flags, "key_partitions": partitions}


def verify_values(spark, source, contract, metrics):
    """Check source facts on persisted outputs, independently of DQ metrics."""
    from pyspark.sql import functions as F
    output, bronze = spark.table(contract.silver_output), spark.table(contract.bronze_input)
    audit = ["_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
             "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id"]
    assert all(output.schema[c].dataType == bronze.schema[c].dataType for c in audit)
    assert output.select(*audit).distinct().join(bronze.select(*audit).distinct(), audit, "left_anti").isEmpty()
    if source.startswith("faostat_"):
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
        series = [c for c in bronze.columns if not c.startswith("_") and c != "period" and not c.startswith("commodity_")]
        assert metrics["prepared_count"] == bronze.count() * len(series)
    elif source == "thitruongnongsan":
        assert output.filter("unit <> 'VND/kg' OR currency <> 'VND'").isEmpty()
        assert output.filter("price_value = 0").count() == bronze.filter(F.expr(f"try_cast(`{sanitize_column_name('Giá')}` AS decimal(18,2)) = 0")).count()
    return {"lineage_roundtrip": True, "source_values_units_flags": "PASSED"}


def accept_source(spark, framework, source, approved_input, trino_url=None):
    """Called while the caller holds the source lock, including failure readback."""
    from pyspark.sql import functions as F
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


def _run_validate_e2e(args):
    from pyspark.sql import functions as F
    if not args.output_json:
        raise ValueError("--validate-e2e requires --output-json")
    definitions = datasets(ROOT / "contracts/silver")
    sources = selected_sources(args.dataset, definitions, args.release)
    if args.release and not (args.preflight or args.audit_only or (args.execute and args.approved_preflight)):
        raise ValueError("Release writes require --execute --approved-preflight; use --preflight first")
    if args.preflight and (args.execute or args.audit_only):
        raise ValueError("Preflight cannot execute or audit a release")
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


def main(default_dataset="all"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", "--source", dest="dataset", default=default_dataset)
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--run-id", help="Bronze ingestion run ID; required for incremental unless watermark supplied")
    parser.add_argument("--watermark", type=datetime.fromisoformat, help="Exclusive UTC Bronze timestamp watermark (ISO 8601)")
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--contract-dir", default=str(ROOT / "contracts/silver"))
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--check-bronze", action="store_true", help="Read-only readiness/schema check")
    parser.add_argument("--audit", action="store_true", help="Read-only Silver key/type audit")
    parser.add_argument("--release", choices=[RELEASE_NAMESPACE], help="Opt-in V2 routing; requires explicit source IDs")
    parser.add_argument("--profile", choices=["local_nine"], help="Explicit activated local-only routing")
    parser.add_argument("--bronze-snapshot-id", type=int, help="Verified local batch pinned Bronze snapshot")
    # Consolidated maintenance / validation sub-modes
    parser.add_argument("--migrate-schema", action="store_true", help="Migrate empty legacy Silver schema")
    parser.add_argument("--validate-e2e", action="store_true", help="Run E2E Spark Silver acceptance validation")
    parser.add_argument("--verify-trino", action="store_true", help="Verify Silver tables via Trino")
    parser.add_argument("--execute", action="store_true", help="Execute schema migration or approved release validation")
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--approved-preflight", type=Path)
    parser.add_argument("--trino-url", default="http://trino:8080")
    parser.add_argument("--registry", type=Path, default=ROOT / "config/ingestion/source_registry.yaml")
    parser.add_argument("--spark-report", type=Path)
    parser.add_argument("--nso-only", action="store_true")
    parser.add_argument("--namespace", default="iceberg.silver_local")
    parser.add_argument("--url", default="http://trino:8080")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if args.verify_trino or args.nso_only or args.spark_report:
        _run_verify_trino(args)
        return
    if args.migrate_schema:
        migrate_empty_schema(args.dataset, execute=args.execute, output_json=args.output_json)
        return
    if args.validate_e2e or args.preflight or args.approved_preflight or args.audit_only:
        _run_validate_e2e(args)
        return

    if args.audit or args.check_bronze:
        session = get_spark_session("SilverAudit")
        try:
            contracts = datasets(args.contract_dir)
            framework = SilverTransformationFramework(session, args.contract_dir, release=args.release, profile=args.profile)
            selected = [framework.contract(k) for k in selected_sources(args.dataset, contracts, args.release, args.profile, args.contract_dir)]
            if not selected:
                raise ValueError("No READY datasets selected")
            result = (check_bronze(session, selected, **({"profile": args.profile} if args.profile else {}))
                      if args.check_bronze else audit_tables(session, selected))
        finally:
            session.stop()
    else:
        result = run_pipeline(args.dataset, args.mode, args.run_id, args.watermark,
                              args.pipeline_run_id, args.contract_dir, release=args.release, profile=args.profile,
                              bronze_snapshot_id=args.bronze_snapshot_id)
    encoded = json.dumps(result, ensure_ascii=False, default=str, indent=2)
    print("SILVER_RESULT=" + json.dumps(result, default=str))
    if args.output_json:
        args.output_json.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
