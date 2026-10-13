"""Full-input Silver validation; optional real ingestion on compose.silver-test.yml.

Default: disposable Hadoop catalog. --rebuild: guarded TEST REST/MinIO/PG/Trino.
Neither mode resets data, invokes Airflow, or accepts shared service endpoints.
"""
import copy
import argparse
import importlib
import json
import logging
import os
import sys
import tempfile
import csv
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))
from ingestion.config.registry import SourceRegistry
from ingestion.storage.bronze_writer import sanitize_column_name
from silver.audit import audit_tables
from silver.contract.loader import SilverContractLoader
from silver.framework import SilverTransformationFramework
from ingestion.utils.hashing import compute_file_checksum

SOURCES = ("faostat_production", "faostat_monthly_price", "faostat_supply_utilization", "faostat_trade",
           "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan")


def table_state(spark, table):
    df = spark.table(table)
    current = spark.sql(f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1").first()
    signature = df.select(F.xxhash64(*sorted(df.columns)).cast("decimal(38,0)").alias("h")).agg(F.sum("h")).first()[0]
    return {"rows": df.count(), "snapshot_id": current[0] if current else None,
            "checksum": str(signature), "schema": df.schema.json()}


def raw_count(config):
    """Independent CSV/workbook observation count, separate from adapter parsing."""
    path = Path(config.local_path)
    if path.suffix == ".csv":
        with path.open(encoding=config.encoding or "utf-8-sig", newline="") as stream:
            return sum(1 for _ in csv.reader(stream)) - 1
    if config.source_id == "usda_psd":
        # Source artifact is HTML despite the .xls extension.
        tables = pd.read_html(str(path))
        return len(tables[0])
    import openpyxl
    with path.open("rb") as stream:
        workbook = openpyxl.load_workbook(stream, read_only=True, data_only=True)
        sheet = workbook[config.extra.get("sheet_name") or ("Monthly Prices" if config.source_id == "worldbank_pinksheet" else "price_lua_gao")]
        rows = list(sheet.iter_rows(values_only=True))
        workbook.close()
    if config.source_id == "worldbank_pinksheet":
        import re
        return sum(bool(re.fullmatch(r"[0-9]{4}M[0-9]{2}", str(row[0]))) for row in rows)
    return sum(any(v is not None for v in row) for row in rows[1:])


def configure_rebuild(spark_builder, temporary, configs, resume=False):
    from ingestion.storage.metadata_repository import MetadataRepository
    from ingestion.storage.minio_storage import MinioStorage
    from ingestion.storage.bronze_writer import BronzeIcebergWriter
    from ingestion.core.ingestion_engine import IngestionEngine
    from jobs.silver.verify_silver_trino import query

    expected = {"TLCN_ISOLATED_REBUILD": "1", "MINIO_ENDPOINT": "minio-test:9000",
                "ICEBERG_REST_URI": "http://rest-test:8181", "TRINO_HOST": "trino-test"}
    if any(os.environ.get(k) != v for k, v in expected.items()) or not os.environ.get("INGESTION_DB_URL", "").endswith("/silver_rebuild_test"):
        raise ValueError("Rebuild requires the independent compose.silver-test.yml endpoints and database")
    metadata = MetadataRepository()
    metadata.initialize()
    storage = MinioStorage()
    storage.ensure_bucket("silver-rebuild-warehouse")
    # Freshness is a prerequisite, never achieved here by DROP/reset.
    with metadata._get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM ingestion.ingestion_runs")
            if cursor.fetchone()[0] and not resume:
                raise ValueError("Test metadata is not empty; choose a NEW test project")
    writer = BronzeIcebergWriter(catalog="rebuild_test")
    registry_data = yaml.safe_load((ROOT / "config/ingestion/source_registry.yaml").read_text(encoding="utf-8"))
    registry_data["sources"] = {k: v for k, v in registry_data["sources"].items() if k in configs}
    for key, config in configs.items():
        registry_data["sources"][key]["local_path"] = config.local_path
        registry_data["sources"][key]["local_fallback"] = config.local_path
    registry_path = temporary / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry_data), encoding="utf-8")
    registry = SourceRegistry(str(registry_path))
    engine = IngestionEngine(registry=registry, metadata_repo=metadata, minio_storage=storage, bronze_writer=writer)
    builder = (spark_builder.config("spark.sql.catalog.rebuild_test", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.catalog.rebuild_test.type", "rest")
               .config("spark.sql.catalog.rebuild_test.uri", "http://rest-test:8181")
               .config("spark.sql.catalog.rebuild_test.warehouse", "s3://silver-rebuild-warehouse/")
               .config("spark.sql.catalog.rebuild_test.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
               .config("spark.sql.catalog.rebuild_test.s3.endpoint", "http://minio-test:9000")
               .config("spark.sql.catalog.rebuild_test.s3.path-style-access", "true")
               .config("spark.sql.catalog.rebuild_test.s3.access-key-id", "rebuild_test")
               .config("spark.sql.catalog.rebuild_test.s3.secret-access-key", "rebuild-test-only-password"))
    return builder, engine, query


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
    parser.add_argument("--dataset", choices=SOURCES)
    parser.add_argument("--raw-root", type=Path, default=ROOT / "data/raw")
    parser.add_argument("--rebuild", action="store_true", help="Actual ingestion; independent test services only")
    parser.add_argument("--resume", action="store_true", help="Continue the SAME isolated drill; never reset existing test data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with tempfile.TemporaryDirectory(prefix="silver-snapshots-") as temporary:
        temporary = Path(temporary)
        builder = (SparkSession.builder.master("local[2]").appName("SilverLocalSnapshots")
                 .config("spark.sql.defaultCatalog", "spark_catalog")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                 .config("spark.sql.catalog.snapshot_test", "org.apache.iceberg.spark.SparkCatalog")
                 .config("spark.sql.catalog.snapshot_test.type", "hadoop")
                 .config("spark.sql.catalog.snapshot_test.warehouse", (temporary / "warehouse").as_uri()))
        registry = SourceRegistry(str(ROOT / "config/ingestion/source_registry.yaml"))
        registry.load()
        selected = [args.dataset] if args.dataset else list(SOURCES)
        configs = {key: replace(registry.get_source(key), local_path=str(args.raw_root / Path(registry.get_source(key).local_path).relative_to("data/raw"))) for key in selected}
        # Fail before any write if even one full input artifact is absent.
        hashes = {key: compute_file_checksum(config.local_path) for key, config in configs.items()}
        counts = {key: raw_count(config) for key, config in configs.items()}
        engine, trino_query = None, None
        if args.rebuild:
            builder, engine, trino_query = configure_rebuild(builder, temporary, configs, args.resume)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        loader = SilverContractLoader(str(ROOT / "contracts/silver"))
        contracts = temporary / "contracts"
        contracts.mkdir()
        reports = {}
        try:
            catalog = "rebuild_test" if args.rebuild else "snapshot_test"
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
            for key in selected:
                path = ROOT / "contracts/silver" / f"{key}.yaml"
                contract = loader.load_contract(path.stem)
                if contract.status != "READY":
                    raise ValueError(f"Selected source is not READY: {key}")
                config = configs[key]
                module, cls = config.adapter_class.rsplit(".", 1)
                adapter = getattr(importlib.import_module(module), cls)()
                data = copy.deepcopy(contract._data)
                data["dataset"]["bronze_input"] = f"{catalog}.bronze.{path.stem}"
                data["dataset"]["silver_output"] = f"{catalog}.silver.{path.stem}"
                (contracts / path.name).write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
                bronze_table = data["dataset"]["bronze_input"]
                run_identity = "snapshot-run"
                if engine:
                    ingestion = engine.run_with_readiness_check(key)
                    if ingestion.status.value not in ({"SUCCESS", "SKIPPED"} if args.resume else {"SUCCESS"}):
                        raise ValueError(f"Bronze rebuild failed {key}: {ingestion.to_dict()}")
                    if ingestion.status.value == "SUCCESS":
                        run_identity = ingestion.run_id
                        assert ingestion.records_extracted == counts[key]
                    else:
                        identities = spark.table(bronze_table).select("_ingestion_run_id").distinct().limit(2).collect()
                        if len(identities) != 1:
                            raise ValueError(f"Resume needs a single complete Bronze run: {key}")
                        run_identity = identities[0][0]
                for chunk in (() if engine else adapter.extract_full(config, str(temporary / "staging"))):
                    # Reproduce sanitized, nullable Bronze fields. Source adapters
                    # already perform their existing parsing; Silver does all DQ.
                    frame = chunk.data
                    names = [sanitize_column_name(c) for c in frame.columns]
                    if len(names) != len(set(names)):
                        raise ValueError(f"Bronze sanitizer collision: {path.stem}")
                    rows = [tuple(None if pd.isna(v) else str(v) for v in row)
                            for row in frame.itertuples(index=False, name=None)]
                    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in names])
                    df = spark.createDataFrame(rows, schema)
                    df = (df.withColumn("_ingestion_run_id", F.lit("snapshot-run"))
                          .withColumn("_ingestion_timestamp", F.lit(datetime.now(timezone.utc)))
                          .withColumn("_source_checksum", F.lit(hashes[key]))
                          .withColumn("_ingestion_batch_id", F.lit("snapshot-batch"))
                          .withColumn("_ingestion_chunk_id", F.lit(chunk.chunk_id).cast("long"))
                          .withColumn("_source_id", F.lit(path.stem))
                          .withColumn("_source_snapshot_id", F.lit(0).cast("long")))
                    if "_source_file" not in df.columns:
                        df = df.withColumn("_source_file", F.lit(Path(config.local_path).name))
                    if spark.catalog.tableExists(bronze_table):
                        df.writeTo(bronze_table).append()
                    else:
                        df.writeTo(bronze_table).using("iceberg").create()
                framework = SilverTransformationFramework(spark, str(contracts))
                framework.quarantine_manager.target_table = f"{catalog}.silver.dead_letters"
                before_bronze = table_state(spark, bronze_table)
                assert before_bronze["rows"] == counts[key]
                assert spark.table(bronze_table).filter(F.col("_source_checksum") != hashes[key]).isEmpty()
                result = framework.run(path.stem, "snapshot-validation")
                assert result["prepared_count"] == result["valid_count"] + result["quarantine_count"]
                target = data["dataset"]["silver_output"]
                before = table_state(spark, target)
                assert before["rows"] == result["dedup_count"]
                result["value_checks"] = verify_values(spark, key, SilverContractLoader(str(contracts)).load_contract(key), result)
                for mode in ("full", "incremental"):
                    replay = framework.run(path.stem, f"snapshot-replay-{mode}", mode=mode, run_id=run_identity if mode == "incremental" else None)
                    assert before == table_state(spark, target), f"{key} {mode} was not a native NOOP"
                    assert result["dedup_count"] == replay["dedup_count"]
                assert before_bronze == table_state(spark, bronze_table)
                if engine:
                    assert engine.run_with_readiness_check(key).status.value == "SKIPPED"
                    assert before_bronze == table_state(spark, bronze_table)
                    trino_url = "http://trino-test:8080"
                    trino = trino_query(trino_url, f"SELECT count(*) AS rows FROM {target}")[0]
                    assert trino["rows"] == before["rows"]
                    types = {r["Column"]: r["Type"] for r in trino_query(trino_url, f"DESCRIBE {target}")}
                    for column in contract.columns:
                        expected_type = {"string": "varchar", "long": "bigint", "int": "integer"}.get(column["data_type"], column["data_type"])
                        assert types[column["name"]].replace(" ", "") == expected_type.replace(" ", ""), (key, column, types[column["name"]])
                    assert types["_ingestion_timestamp"] == "timestamp(6) with time zone"
                    value = "price" if key == "worldbank_pinksheet" else "price_value" if key == "thitruongnongsan" else "value"
                    # Verification totals per unit, never a business aggregation across units.
                    checks = trino_query(trino_url, f"SELECT unit, count(*) AS rows, count({value}) AS present, sum({value}) AS total FROM {target} GROUP BY unit")
                    expected = {r.unit: r.asDict() for r in spark.table(target).groupBy("unit").agg(
                        F.count("*").alias("rows"), F.count(value).alias("present"), F.sum(value).alias("total")).collect()}
                    from decimal import Decimal
                    assert all(r["rows"] == expected[r["unit"]]["rows"] and r["present"] == expected[r["unit"]]["present"]
                               and (Decimal(str(r["total"])) if r["total"] is not None else None) == expected[r["unit"]]["total"] for r in checks)
                    assert len(checks) == len(expected)
                    result["trino"] = "PASSED"
                result.update(input_kind="full_raw_artifact", raw_records=counts[key], full_noop=True, incremental_noop=True,
                              bronze_rebuild="PASSED" if engine else "NOT_RUN", input_sha256=hashes[key])
                reports[path.stem] = result
                print("SNAPSHOT_RESULT=" + json.dumps(result), flush=True)
            ready = [SilverContractLoader(str(contracts)).load_contract(p.stem) for p in sorted(contracts.glob("*.yaml"))]
            if not ready:
                raise ValueError(f"No READY snapshots selected: {args.dataset}")
            assert audit_tables(spark, ready)["status"] == "PASSED"
            assert all(compute_file_checksum(config.local_path) == hashes[key] for key, config in configs.items())
            print("SNAPSHOT_SUMMARY=" + json.dumps({"status": "PASSED", "datasets": reports, "bronze_rebuild": "PASSED" if engine else "NOT_RUN", "nso_v2": "DRAFT_NOT_SELECTED"}), flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
