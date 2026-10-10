"""Actual Iceberg semantics in a fresh network-isolated Hadoop warehouse."""
from copy import deepcopy
import json
from pathlib import Path

import pytest
import pandas as pd
import hashlib
from pyspark.sql import functions as F
from silver.contract.loader import SilverContract
from silver.framework import SilverTransformationFramework
from silver.framework import source_transformer
from silver.contract.loader import SilverContractLoader
from silver.engine.merge import IcebergMergeEngine
from silver.engine.quarantine import SilverQuarantineManager
from ingestion.storage.bronze_writer import sanitize_column_name
from tests.integration.test_faostat_nso_silver import isolated_iceberg
from tests.unit.silver.faostat_nso_fixtures import observations, sql_rows


def snapshot_count(spark, table):
    return spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").first().n


def test_noop_stale_correction_and_schema_guard(isolated_iceberg):
    spark = isolated_iceberg
    table = "hoang_test.fixtures.controls"
    engine = IcebergMergeEngine(spark)
    def batch(value, timestamp):
        return spark.sql(f"SELECT 'key' AS id, {value} AS value, TIMESTAMP '{timestamp}' AS _ingestion_timestamp")
    first = batch(10, "2026-10-02 00:00:00")
    assert engine.merge(first, table, ["id"])["records_changed"] == 1
    initial_snapshots = snapshot_count(spark, table)
    assert engine.merge(first, table, ["id"])["status"] == "NOOP"
    assert engine.merge(batch(1, "2026-10-01 00:00:00"), table, ["id"])["status"] == "NOOP"
    assert snapshot_count(spark, table) == initial_snapshots
    assert engine.merge(batch(20, "2026-10-03 00:00:00"), table, ["id"])["records_changed"] == 1
    assert spark.table(table).first().value == 20
    with pytest.raises(ValueError, match="null business keys"):
        engine.merge(first.withColumn("id", F.lit(None).cast("string")), table, ["id"])
    with pytest.raises(ValueError, match="Schema change"):
        engine.merge(first.withColumn("unexpected", F.lit("new")), table, ["id"])
    assert spark.table(table).first().value == 20


def test_shared_validation_job_uses_only_explicit_isolated_targets(isolated_iceberg):
    from jobs.silver.validate_shared_controls import validate
    spark = isolated_iceberg
    target = "hoang_test.fixtures.shared_reliability"
    dead = "hoang_test.fixtures.shared_reliability_dead"
    report = validate(spark, target, dead)
    assert report["status"] == "VALIDATION_PASSED" and report["fixture_only"]
    assert report["target_rows"] == report["quarantine_rows"] == 1
    # Repeating the entire validation job changes neither the data nor snapshot.
    repeated = validate(spark, target, dead)
    assert repeated["snapshot_after"] == report["snapshot_after"]


def test_quarantine_duplicate_batch_and_retry_noop(isolated_iceberg):
    spark = isolated_iceberg
    manager = SilverQuarantineManager(spark, "hoang_test.fixtures.controls_dead")
    manager.ensure_table()
    dead = spark.sql("SELECT 'key' AS id, array(named_struct('rule_id','BAD','error_message','bad','failed_column','value')) AS dq_errors")
    manager.route_quarantine(dead.unionByName(dead), "fixture", "run", ["id"])
    snapshots = snapshot_count(spark, manager.target_table)
    manager.route_quarantine(dead, "fixture", "retry", ["id"])
    assert spark.table(manager.target_table).count() == 1
    assert snapshot_count(spark, manager.target_table) == snapshots
    assert spark.table(manager.target_table).first().pipeline_run_id == "run"
    first = dead.withColumn("_bronze_iceberg_snapshot_id", F.lit(1))
    second = dead.withColumn("_bronze_iceberg_snapshot_id", F.lit(2))
    other = SilverQuarantineManager(spark, "hoang_test.fixtures.snapshot_dead")
    other.route_quarantine(first, "fixture", "first", ["id"])
    other.route_quarantine(second, "fixture", "second", ["id"])
    assert spark.table(other.target_table).count() == 1


def test_framework_full_incremental_retry_and_pinned_lineage(isolated_iceberg, monkeypatch):
    spark = isolated_iceberg
    bronze = "hoang_test.fixtures.production_bronze"
    target = "hoang_test.fixtures.production_pipeline"
    raw = observations(spark, {}, {"value": "-1"})
    raw.write.format("iceberg").saveAsTable(bronze)
    framework = SilverTransformationFramework(spark)
    data = deepcopy(framework.contract_loader.load_contract("faostat_production")._data)
    data["dataset"].update(bronze_input=bronze, silver_output=target)
    monkeypatch.setattr(framework.contract_loader, "load_contract", lambda _: SilverContract(data))
    framework.quarantine_manager = SilverQuarantineManager(spark, "hoang_test.fixtures.production_pipeline_dead")
    actual_merge = framework.merge_engine.merge
    def failed_write(*args, **kwargs):
        raise RuntimeError("Injected failure after quarantine commit")
    monkeypatch.setattr(framework.merge_engine, "merge", failed_write)
    with pytest.raises(RuntimeError, match="Injected failure"):
        framework.run("faostat_production", "failed")
    assert spark.table(framework.quarantine_manager.target_table).count() == 1
    monkeypatch.setattr(framework.merge_engine, "merge", actual_merge)
    first = framework.run("faostat_production", "full")
    assert first["bronze_count"] == first["observation_count"] == 2
    assert first["valid_count"] == first["quarantine_count"] == 1
    snapshots = snapshot_count(spark, target)
    second = framework.run("faostat_production", "retry", mode="incremental", run_id="bronze-run")
    assert second["write_result"]["status"] == "NOOP"
    assert snapshot_count(spark, target) == snapshots
    assert spark.table(target).first()._bronze_iceberg_snapshot_id is not None
    assert spark.table(target).first()._source_snapshot_id == "42"


def test_nso_lossless_union_recovery_in_temporary_iceberg(isolated_iceberg):
    spark = isolated_iceberg
    evidence = json.loads(Path("docs/silver/bronze_inventory_evidence.json").read_text(encoding="utf-8"))
    assert len(evidence["nso_files"]) == 13
    table = "hoang_test.fixtures.nso_recovered"
    combined = None
    for file in evidence["nso_files"]:
        assert file["raw_matches"] and all(match["identical_to_local"] for match in file["raw_matches"])
        path = Path("data/raw/nso") / file["file"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == file["checksum"]
        # Use the ingestion adapter's pandas CSV semantics. Spark's CSV reader
        # skips the all-empty footer row and would change physical reconciliation.
        frame = pd.read_csv(path, encoding="latin-1", dtype=str)
        frame.columns = [sanitize_column_name(c) for c in frame.columns]
        df = sql_rows(spark, frame.where(pd.notna(frame), None).to_dict("records"))
        df = df.withColumn("_source_file", F.lit(file["file"]))
        df = df.withColumn("_source_checksum", F.lit(file["checksum"]))
        assert df.count() == file["rows"]
        combined = df if combined is None else combined.unionByName(df, allowMissingColumns=True)
    combined.write.format("iceberg").saveAsTable(table)
    recovered = spark.table(table)
    assert recovered.count() == 833
    assert recovered.exceptAll(combined).isEmpty() and combined.exceptAll(recovered).isEmpty()
    year_columns = [c for c in recovered.columns if c.startswith("col_") or c.startswith("so_b_")]
    cells = recovered.select(sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in year_columns).alias("cells"))
    assert cells.agg(F.sum("cells")).first()[0] == sum(f["lost_measurement_cells"] for f in evidence["nso_files"]) == 22500
    # This proves raw values are recoverable, not that province/unit mappings are approved.


@pytest.mark.parametrize("source", ["faostat_production", "faostat_monthly_price", "faostat_supply_utilization",
    "faostat_trade", "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan"])
def test_every_ready_source_full_and_incremental_actual_iceberg(isolated_iceberg, monkeypatch, source):
    spark = isolated_iceberg
    evidence = json.loads(Path("docs/silver/bronze_inventory_evidence.json").read_text(encoding="utf-8"))
    sample = evidence["tables"][source]["sample"]
    raw = sql_rows(spark, sample)
    bronze, target = f"hoang_test.fixtures.{source}", f"hoang_test.fixtures.output_{source}"
    raw.write.format("iceberg").saveAsTable(bronze)
    original_load = SilverContractLoader.load_contract
    def contract_for_test(loader, dataset):
        data = deepcopy(original_load(loader, dataset)._data)
        data["dataset"].update(bronze_input=bronze, silver_output=target)
        return SilverContract(data)
    monkeypatch.setattr(SilverContractLoader, "load_contract", contract_for_test)
    transformer = source_transformer(source, spark)
    if hasattr(type(transformer), "source_table"):
        monkeypatch.setattr(type(transformer), "source_table", property(lambda _: bronze))
        monkeypatch.setattr(type(transformer), "target_table", property(lambda _: target))
    original_quarantine_init = SilverQuarantineManager.__init__
    monkeypatch.setattr(SilverQuarantineManager, "__init__", lambda self, session, target_table=None:
        original_quarantine_init(self, session, f"hoang_test.fixtures.retries_{source}"))
    def execute(mode, run):
        if hasattr(transformer, "execute"):
            return transformer.execute(mode=mode, run_id=run)
        return SilverTransformationFramework(spark).run(source, "pipeline", mode=mode, run_id=run)
    first = execute("full", None)
    assert first["status"] == "SUCCESS" and first["bronze_count"] == len(sample)
    assert spark.table(target).count() == first["dedup_count"]
    snapshots = snapshot_count(spark, target)
    retry = execute("incremental", sample[0]["_ingestion_run_id"])
    assert retry["write_result"]["status"] == "NOOP"
    assert retry["writes_performed"] is False and retry["outcome"] == "NOOP"
    assert snapshot_count(spark, target) == snapshots
    assert spark.table(target).count() == first["dedup_count"]
