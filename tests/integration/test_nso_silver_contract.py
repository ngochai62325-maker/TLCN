"""Real B4 export reconciliation and source pipeline in isolated Iceberg only."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from silver.contract.loader import SilverContract
from silver.framework import SilverTransformationFramework
from silver.engine.quarantine import SilverQuarantineManager
from silver.transformers.nso_vietnam import NsoVietnamTransformer, NATIONAL_MEASURES
from tests.integration.test_faostat_nso_silver import isolated_iceberg


def export(spark):
    parquet = Path(".pytest_cache/nso_e1_bronze.parquet")
    profile = json.loads(Path("docs/silver/nso_e1_bronze_profile.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(parquet.read_bytes()).hexdigest() == profile["local_parquet_sha256"]
    assert profile["snapshot"] == 3499764399995403202
    return spark.read.parquet(str(parquet))


def snapshots(spark, table):
    return spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()


def test_pinned_real_snapshot_schema_lineage_and_observation_reconciliation(isolated_iceberg):
    spark = isolated_iceberg
    raw = export(spark).withColumn("_bronze_iceberg_snapshot_id", F.lit(3499764399995403202))
    transformer = NsoVietnamTransformer()
    framework = SilverTransformationFramework(spark)
    contract, cached, valid, dead, report = framework.prepare("nso_vietnam", raw, transformer, allow_unapproved=True)
    try:
        assert report["bronze_count"] == 833
        assert report["observation_count"] == 23428
        assert report["eligible_observation_count"] == 23060
        assert report["excluded_count"] == 368 and report["excluded_record_count"] == 13
        assert report["valid_count"] == report["dedup_count"] == 560
        assert report["quarantine_count"] == 22500
        assert report["observation_count"] == report["valid_count"] + report["quarantine_count"] + report["excluded_count"]
        assert cached.filter("source_table != 'V06.12.csv' AND value_source IS NOT NULL").count() == 21554
        assert cached.filter("missing_kind='source_marker'").count() == 946
        assert cached.filter("missing_kind='source_null'").count() == 0
        assert cached.select("_source_checksum").distinct().count() == 13
        assert cached.select(*contract.business_key).distinct().count() == cached.count()
        assert valid.filter("gold_ready").count() == 0
        lineage = ["_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
                   "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id"]
        assert cached.select(*lineage).distinct().exceptAll(raw.select(*lineage).distinct()).isEmpty()
        assert cached.filter("_source_snapshot_id != 0").count() == 0
        assert cached.filter("_bronze_iceberg_snapshot_id != 3499764399995403202").count() == 0
        assert {c["name"]:c["data_type"] for c in contract.columns} == {
            c["name"]:valid.schema[c["name"]].dataType.simpleString() for c in contract.columns}
    finally:
        cached.unpersist()


def test_actual_iceberg_full_incremental_noop_correction_and_exclusion_audit(isolated_iceberg, monkeypatch):
    spark = isolated_iceberg
    raw = export(spark)
    # Representative real rows: quantity/index, matrix missing and empty records.
    national = raw.filter("_source_file='V06.12.csv' AND (nam='1990' OR nam IS NULL)")
    matrix = raw.filter("_source_file='V06.13.csv' AND (t_nh_th_nh_ph_='Hà Tây' OR t_nh_th_nh_ph_ IS NULL)")
    batch = national.unionByName(matrix)
    bronze, target, dead = ["hoang_test.fixtures." + name for name in ("nso_e1_bronze", "nso_e1_silver", "nso_e1_dead")]
    batch.write.format("iceberg").saveAsTable(bronze)
    framework = SilverTransformationFramework(spark)
    data = deepcopy(framework.contract_loader.load_contract("nso_vietnam")._data)
    # This override exists only in this network-disabled test, never on disk or
    # on the production loader. It authorizes fixture pipeline execution only.
    data.update(contract_status="READY", critical_blockers=[])
    data["dataset"].update(bronze_input=bronze, silver_output=target)
    monkeypatch.setattr(framework.contract_loader, "load_contract", lambda _: SilverContract(data))
    framework.quarantine_manager = SilverQuarantineManager(spark, dead)
    first = framework.run("nso_vietnam", "isolated-full")
    assert first["valid_count"] == 16 and first["quarantine_count"] == 30
    assert first["excluded_count"] == 38 and first["excluded_record_count"] == 2
    assert first["excluded_result"]["records_changed"] == 2
    assert spark.table(target).count() == 16
    assert spark.table(dead).count() == 32
    old_snapshots = snapshots(spark, target), snapshots(spark, dead)
    run_id = national.filter("nam='1990'").first()._ingestion_run_id
    retry = framework.run("nso_vietnam", "isolated-retry", mode="incremental", run_id=run_id)
    assert retry["outcome"] == "NOOP" and not retry["writes_performed"]
    assert (snapshots(spark,target), snapshots(spark,dead)) == old_snapshots
    no_input = framework.run("nso_vietnam", "isolated-empty", mode="incremental", run_id="absent-run")
    assert no_input["outcome"] == "NO_INPUT" and no_input["observation_count"] == 0
    correction = national.filter("nam='1990' AND gi_tr_v_ch_s_ph_t_tri_n='Giá tr?'")
    correction = (correction.withColumn(next(iter(NATIONAL_MEASURES)), F.lit("999"))
        .withColumn("_ingestion_timestamp", F.col("_ingestion_timestamp") + F.expr("INTERVAL 1 DAY"))
        .withColumn("_ingestion_run_id", F.lit("isolated-correction")))
    correction.writeTo(bronze).append()
    changed = framework.run("nso_vietnam", "isolated-correct", mode="incremental", run_id="isolated-correction")
    assert changed["valid_count"] == 8 and changed["quarantine_count"] == 0
    assert changed["write_result"]["records_changed"] == 8 and spark.table(target).count() == 16
    assert spark.table(target).filter("measure='area' AND season='annual' AND statistic_kind='quantity'").first().value == 999000
    # Older source run cannot undo the late correction or create a new snapshot.
    before_stale = snapshots(spark,target)
    stale = framework.run("nso_vietnam", "isolated-stale", mode="incremental", run_id=run_id)
    assert stale["write_result"]["status"] == "NOOP" and snapshots(spark,target) == before_stale
