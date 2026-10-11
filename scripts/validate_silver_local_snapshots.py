"""Offline E2E validation from local source snapshots into a TEMPORARY catalog.

Run inside the isolated Spark image with the repository mounted read-only.
This script never connects to REST, MinIO, PostgreSQL or Airflow. Trade uses
the checked-in validation ZIP because the production rice_trade_data.csv is absent.
"""
import copy
import argparse
import importlib
import json
import logging
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from ingestion.config.registry import SourceRegistry
from ingestion.storage.bronze_writer import sanitize_column_name
from silver.audit import audit_tables
from silver.contract.loader import SilverContractLoader
from silver.framework import SilverTransformationFramework


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="all")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with tempfile.TemporaryDirectory(prefix="silver-snapshots-") as temporary:
        temporary = Path(temporary)
        spark = (SparkSession.builder.master("local[2]").appName("SilverLocalSnapshots")
                 .config("spark.sql.defaultCatalog", "spark_catalog")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                 .config("spark.sql.catalog.snapshot_test", "org.apache.iceberg.spark.SparkCatalog")
                 .config("spark.sql.catalog.snapshot_test.type", "hadoop")
                 .config("spark.sql.catalog.snapshot_test.warehouse", (temporary / "warehouse").as_uri()).getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        registry = SourceRegistry(str(ROOT / "config/ingestion/source_registry.yaml"))
        registry.load()
        loader = SilverContractLoader(str(ROOT / "contracts/silver"))
        contracts = temporary / "contracts"
        contracts.mkdir()
        reports = {}
        try:
            spark.sql("CREATE NAMESPACE snapshot_test.bronze")
            spark.sql("CREATE NAMESPACE snapshot_test.silver")
            for path in sorted((ROOT / "contracts/silver").glob("*.yaml")):
                contract = loader.load_contract(path.stem)
                if contract.status != "READY":
                    continue
                if args.dataset != "all" and path.stem != args.dataset:
                    continue
                config = registry.get_source(path.stem)
                if path.stem == "faostat_trade":
                    fixture = str(ROOT / "data/fixtures/trade_matrix_fixture.zip")
                    config = replace(config, local_path=fixture, local_fallback=fixture)
                else:
                    config = replace(config, local_path=str(ROOT / config.local_path))
                module, cls = config.adapter_class.rsplit(".", 1)
                adapter = getattr(importlib.import_module(module), cls)()
                data = copy.deepcopy(contract._data)
                data["dataset"]["bronze_input"] = f"snapshot_test.bronze.{path.stem}"
                data["dataset"]["silver_output"] = f"snapshot_test.silver.{path.stem}"
                (contracts / path.name).write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
                bronze_table = data["dataset"]["bronze_input"]
                for chunk in adapter.extract_full(config, str(temporary / "staging")):
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
                          .withColumn("_ingestion_timestamp", F.lit(datetime(2026, 10, 11, tzinfo=timezone.utc)))
                          .withColumn("_source_checksum", F.lit(chunk.checksum))
                          .withColumn("_ingestion_batch_id", F.lit("snapshot-batch"))
                          .withColumn("_ingestion_chunk_id", F.lit(chunk.chunk_id).cast("long"))
                          .withColumn("_source_id", F.lit(path.stem))
                          .withColumn("_source_snapshot_id", F.lit(1).cast("long")))
                    if "_source_file" not in df.columns:
                        df = df.withColumn("_source_file", F.lit(Path(config.local_path).name))
                    if spark.catalog.tableExists(bronze_table):
                        df.writeTo(bronze_table).append()
                    else:
                        df.writeTo(bronze_table).using("iceberg").create()
                framework = SilverTransformationFramework(spark, str(contracts))
                framework.quarantine_manager.target_table = "snapshot_test.silver.dead_letters"
                result = framework.run(path.stem, "snapshot-validation")
                assert result["prepared_count"] == result["valid_count"] + result["quarantine_count"]
                target = data["dataset"]["silver_output"]
                before = spark.table(target).count()
                replay = framework.run(path.stem, "snapshot-replay", mode="incremental", run_id="snapshot-run")
                assert before == spark.table(target).count() == result["dedup_count"] == replay["dedup_count"]
                result["input_kind"] = "validation_zip" if path.stem == "faostat_trade" else "local_snapshot"
                reports[path.stem] = result
                print("SNAPSHOT_RESULT=" + json.dumps(result), flush=True)
            ready = [SilverContractLoader(str(contracts)).load_contract(p.stem) for p in sorted(contracts.glob("*.yaml"))]
            if not ready:
                raise ValueError(f"No READY snapshots selected: {args.dataset}")
            assert audit_tables(spark, ready)["status"] == "PASSED"
            print("SNAPSHOT_SUMMARY=" + json.dumps({"status": "PASSED", "datasets": reports, "pending": ["nso_vietnam"]}), flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
