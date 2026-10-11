"""Real Iceberg writes in a fresh local catalog; never touch REST/MinIO."""
import copy
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession, functions as F

from silver.audit import audit_tables, check_bronze
from silver.contract.loader import SilverContract, SilverContractLoader
from silver.engine.merge import IcebergMergeEngine
from silver.engine.quality import SilverQualityEngine
from silver.framework import SilverTransformationFramework
from ingestion.storage.bronze_writer import sanitize_column_name
from tests.unit.silver.faostat_nso_fixtures import observations, sql_rows


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (SparkSession.builder.master("local[2]").appName("SilverPipelineIsolated")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.catalog.silver_test", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.catalog.silver_test.type", "hadoop")
               .config("spark.sql.catalog.silver_test.warehouse", tmp_path_factory.mktemp("silver-warehouse").as_uri())
               .getOrCreate())
    session.sql("CREATE NAMESPACE IF NOT EXISTS silver_test.bronze")
    session.sql("CREATE NAMESPACE IF NOT EXISTS silver_test.silver")
    yield session
    session.stop()


def source_fixture(spark, dataset):
    if dataset in {"faostat_production", "faostat_supply_utilization"}:
        return observations(spark, {"value": "12.5"}, {"value": "-1"}, {"value": "not-a-number"})
    if dataset == "faostat_monthly_price":
        base = {"element_code": "5530", "element": "Producer Price (LCU/tonne)", "unit": "LCU"}
        return observations(spark, base, dict(base, value="-1"), dict(base, value="not-a-number"))
    metadata = {"_source_file": "fixture.csv", "_ingestion_timestamp": "2026-10-01 00:00:00",
                "_ingestion_run_id": "bronze-run", "_source_checksum": "checksum"}
    if dataset == "faostat_trade":
        base = {"reporter_country_code_m49_": "'704", "reporter_countries": " Viet Nam ",
                "partner_country_code_m49_": "4.0", "partner_countries": "Afghanistan",
                "item_code_cpc_": "113.0", "item": "Rice", "year": "2020",
                "element_code": "5910", "element": "Export quantity", "unit": "t", "value": "12.5", "flag": "A"}
    elif dataset == "usda_psd":
        base = {"country": "Vietnam", "commodity": "", "attribute": "Production",
                "unit_description": "(1000 MT)", "col_2020_2021": "12.5"}
    elif dataset == "usda_rice_yearbook":
        base = {"table_number": "26", "table_name": "Export prices", "commodity_description": "RICE",
                "class_description": "5-PERCENT BROKEN", "year": "2020",
                "reference_period_description": "August", "location_description": "VIETNAM",
                "statistic_description": "FREIGHT ON BOARD PRICE", "unit_description": "DOLLARS PER METRIC TON", "value": "12.5"}
    elif dataset == "worldbank_pinksheet":
        return sql_rows(spark, [dict(metadata, period="2020M08", rice_thai_5_mt_="12.5"),
                                dict(metadata, period="2020M13", rice_thai_5_mt_="12.5"),
                                dict(metadata, period="2020M08", rice_thai_5_mt_="bad")])
    else:
        base = {sanitize_column_name(k): v for k, v in {
            "Tên_mặt_hàng": "Lúa", "Thị_trường": "An Giang", "Loại_giá": "Thu mua",
            "Đơn_vị_tính": "VNĐ/Kg", "Loại_tiền": "VNĐ", "Nguồn": "CTV",
            "Ngày": "8/3/2020 12:00:00 AM", "Giá": "12.5"}.items()}
    value = "col_2020_2021" if dataset == "usda_psd" else sanitize_column_name("Giá") if dataset == "thitruongnongsan" else "value"
    # PSD negative value is a LOG warning; use missing country as blocking failure.
    invalid = dict(base, **({"country": ""} if dataset == "usda_psd" else {value: "-1"}))
    return sql_rows(spark, [dict(metadata, **base), dict(metadata, **invalid),
                            dict(metadata, **dict(base, **{value: "bad"}))])


@pytest.mark.parametrize("dataset", ["faostat_production", "faostat_monthly_price", "faostat_supply_utilization",
                                     "faostat_trade", "usda_psd", "usda_rice_yearbook", "worldbank_pinksheet", "thitruongnongsan"])
def test_complete_pipeline_real_iceberg(spark, tmp_path, dataset):
    data = copy.deepcopy(SilverContractLoader().load_contract(dataset)._data)
    data["dataset"]["bronze_input"] = f"silver_test.bronze.{dataset}"
    data["dataset"]["silver_output"] = f"silver_test.silver.{dataset}"
    (tmp_path / f"{dataset}.yaml").write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    source_fixture(spark, dataset).writeTo(data["dataset"]["bronze_input"]).using("iceberg").create()
    framework = SilverTransformationFramework(spark, str(tmp_path))
    framework.quarantine_manager.target_table = "silver_test.silver.dead_letters"
    first = framework.run(dataset, "first")
    assert first["bronze_count"] == first["prepared_count"] == 3
    assert first["valid_count"] == first["dedup_count"] == 1
    assert first["quarantine_count"] == 2
    table = data["dataset"]["silver_output"]
    original = spark.table(table).collect()
    snapshot_before = spark.table(table + ".snapshots").count()
    second = framework.run(dataset, "second")
    assert second["dedup_count"] == 1
    assert original == spark.table(table).collect()  # Processing timestamp must stay unchanged.
    assert spark.table(table + ".snapshots").count() == snapshot_before
    assert spark.table("silver_test.silver.dead_letters").filter(F.col("source_dataset") == dataset).count() == 2
    audit = audit_tables(spark, [SilverContract(data)])
    assert audit["status"] == "PASSED"
    dead = spark.table("silver_test.silver.dead_letters").filter(F.col("source_dataset") == dataset).first()
    assert "_source_payload" in json.loads(dead.payload)
    assert dead.pipeline_run_id == "second"
    assert not any(c.startswith("_raw") for c in spark.table(table).columns)


def test_merge_rejects_stale_updates_and_keeps_new_keys(spark):
    engine = IcebergMergeEngine(spark)
    table = "silver_test.silver.freshness"
    schema = "id string, value int, _ingestion_timestamp timestamp"
    newer = spark.createDataFrame([("A", 20, datetime(2026, 10, 2))], schema)
    older = spark.createDataFrame([("A", 10, datetime(2026, 10, 1)), ("B", 10, datetime(2026, 10, 1))], schema)
    engine.merge(newer, table, ["id"])
    engine.merge(older, table, ["id"])
    assert [(r.id, r.value) for r in spark.table(table).orderBy("id").collect()] == [("A", 20), ("B", 10)]
    with pytest.raises(ValueError, match="schema mismatch"):
        engine.merge(newer.withColumn("value", F.col("value").cast("string")), table, ["id"])


def test_bronze_readiness_rejects_empty_tables_after_reset(spark):
    data = copy.deepcopy(SilverContractLoader().load_contract('usda_psd')._data)
    data['dataset']['bronze_input'] = 'silver_test.bronze.psd_empty_readiness'
    source = source_fixture(spark, 'usda_psd')
    source.limit(0).writeTo(data['dataset']['bronze_input']).using('iceberg').create()
    with pytest.raises(ValueError, match='has no rows'):
        check_bronze(spark, [SilverContract(data)])
    source.writeTo(data['dataset']['bronze_input']).append()
    assert check_bronze(spark, [SilverContract(data)])['status'] == 'PASSED'


def test_cast_overflow_and_missing_markers_under_ansi(spark):
    contract = SilverContract({"dataset": {"name": "fixture", "source_system": "TEST", "bronze_input": "b", "silver_output": "s"},
                               "grain": {"business_key": ["id"]}, "schema": {"columns": [
                                   {"name": "id", "source_column": "id", "data_type": "string"},
                                   {"name": "value", "source_column": "value", "data_type": "decimal(18,2)"}]}})
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        df = sql_rows(spark, [{"id": str(i), "value": v} for i, v in enumerate(["NA", "", None, "…", "NaN", "Infinity", "1e100", "bad", "1.25"])])
        valid, invalid = SilverQualityEngine([]).apply_rules(SilverTransformationFramework(spark).normalize(df, contract))
        assert valid.count() == 5
        assert invalid.count() == 4
        assert valid.filter(F.col("id") == "8").first().value == Decimal("1.25")
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_warning_actions_keep_records(spark):
    df = sql_rows(spark, [{"id": "A", "value": "-1"}])
    valid, invalid = SilverQualityEngine([{"rule_id": "W", "sql_expr": "try_cast(value as int) >= 0", "action": "LOG"}]).apply_rules(df)
    assert invalid.count() == 0 and valid.count() == 1
    assert valid.first()._dq_warnings[0].rule_id == "W"


def test_incremental_requires_filter_and_draft_refused(spark, tmp_path):
    framework = SilverTransformationFramework(spark)
    with pytest.raises(ValueError, match="requires run_id or watermark"):
        framework.run("usda_psd", "test", mode="incremental")
    draft = copy.deepcopy(SilverContractLoader().load_contract("nso_vietnam")._data)
    draft["contract_status"] = "NEEDS_PROFILING"
    (tmp_path / "nso_vietnam.yaml").write_text(yaml.safe_dump(draft), encoding="utf-8")
    with pytest.raises(ValueError, match="NEEDS_PROFILING"):
        SilverTransformationFramework(spark, str(tmp_path)).run("nso_vietnam", "test")


def test_incremental_read_selects_requested_bronze_run(spark, tmp_path):
    data = copy.deepcopy(SilverContractLoader().load_contract("usda_psd")._data)
    data["dataset"]["bronze_input"] = "silver_test.bronze.psd_incremental"
    data["dataset"]["silver_output"] = "silver_test.silver.psd_incremental"
    (tmp_path / "usda_psd.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    raw = sql_rows(spark, [dict(country="Vietnam", commodity="Rice, Milled", attribute="Production", unit_description="1000 MT",
                               col_2020_2021="10", _ingestion_run_id="old", _ingestion_timestamp="2026-10-01 00:00:00"),
                          dict(country="Thailand", commodity="Rice, Milled", attribute="Production", unit_description="1000 MT",
                               col_2020_2021="20", _ingestion_run_id="new", _ingestion_timestamp="2026-10-02 00:00:00")])
    raw.writeTo(data["dataset"]["bronze_input"]).using("iceberg").create()
    framework = SilverTransformationFramework(spark, str(tmp_path))
    framework.quarantine_manager.target_table = "silver_test.silver.dead_letters"
    result = framework.run("usda_psd", "inc", mode="incremental", run_id="new")
    assert result["bronze_count"] == result["dedup_count"] == 1
    assert spark.table(data["dataset"]["silver_output"]).first().country == "Thailand"
    # Empty incremental inputs still keep a typed output table and zero metrics.
    empty = framework.run("usda_psd", "empty", mode="incremental", run_id="absent")
    assert empty["prepared_count"] == empty["quarantine_count"] == empty["dedup_count"] == 0
    assert spark.table(data["dataset"]["silver_output"]).count() == 1


def test_worldbank_excludes_only_empty_synthetic_columns(spark):
    from silver.transformers.worldbank_pinksheet import WorldbankPinksheetTransformer
    raw = sql_rows(spark, [{"period": "2020M08", "rice_thai_5_mt_": None,
                            "commodity_72": None, "commodity_73": "12.5"}])
    result = WorldbankPinksheetTransformer().preprocess(raw)
    rows = result.collect()
    assert len(rows) == 2
    assert {r.commodity_code for r in rows} == {"rice_thai_5", "commodity_73"}
    assert result.filter(F.col("commodity_code") == "rice_thai_5").first().price is None


def test_integral_cast_rejects_numeric_truncation(spark):
    contract = SilverContract({"dataset": {"name": "fixture", "source_system": "TEST", "bronze_input": "b", "silver_output": "s"},
                               "grain": {"business_key": ["year"]}, "schema": {"columns": [
                                   {"name": "year", "source_column": "year", "data_type": "integer"}]}})
    raw = spark.createDataFrame([(2020.0,), (2020.5,)], "year double")
    normalized = SilverTransformationFramework(spark).normalize(raw, contract)
    valid, invalid = SilverQualityEngine([]).apply_rules(normalized)
    assert valid.count() == invalid.count() == 1
    assert valid.first().year == 2020
    assert invalid.first().dq_errors[0].rule_id == "SYS_CAST_ERR"


def test_nso_real_pipeline_replay_exclusions_and_empty_incremental(spark, tmp_path):
    from silver.transformers.nso_vietnam import NATIONAL_MEASURES
    contract = copy.deepcopy(SilverContractLoader().load_contract("nso_vietnam")._data)
    contract["dataset"]["bronze_input"] = "silver_test.bronze.nso_vietnam"
    contract["dataset"]["silver_output"] = "silver_test.silver.nso_rice_statistics"
    (tmp_path / "nso_vietnam.yaml").write_text(yaml.safe_dump(contract, allow_unicode=True), encoding="utf-8")
    fields = {"_source_file": "V06.12.csv", "_ingestion_run_id": "nso-run", "_ingestion_timestamp": "2026-10-01 00:00:00",
              "nam": "2020", "gi_tr_v_ch_s_ph_t_tri_n": "Giá tr?", **{c: "2.5" for c in NATIONAL_MEASURES}}
    empty = dict(fields, nam=None, gi_tr_v_ch_s_ph_t_tri_n=None, **{c: None for c in NATIONAL_MEASURES})
    bad = dict(fields, nam="2021", **{list(NATIONAL_MEASURES)[0]: "bad"})
    sql_rows(spark, [fields, empty, bad]).writeTo(contract["dataset"]["bronze_input"]).using("iceberg").create()
    framework = SilverTransformationFramework(spark, str(tmp_path))
    framework.quarantine_manager.target_table = "silver_test.silver.dead_letters"
    first = framework.run("nso_vietnam", "nso-first")
    assert first["bronze_count"] == 3 and first["observation_count"] == 24
    assert first["prepared_count"] == 16 and first["excluded_count"] == 8
    assert first["excluded_record_count"] == 1
    assert first["quarantine_count"] == 1 and first["dedup_count"] == 15
    original = spark.table(contract["dataset"]["silver_output"]).collect()
    framework.run("nso_vietnam", "nso-replay", mode="incremental", run_id="nso-run")
    assert original == spark.table(contract["dataset"]["silver_output"]).collect()
    dead = spark.table("silver_test.silver.dead_letters")
    assert dead.filter(F.col("source_dataset") == "nso_vietnam_exclusions").count() == 1
    assert dead.filter(F.col("source_dataset") == "nso_vietnam").count() == 1
    result = framework.run("nso_vietnam", "nso-empty", mode="incremental", run_id="absent")
    assert result["prepared_count"] == result["excluded_count"] == result["observation_count"] == 0
    assert audit_tables(spark, [SilverContract(contract)])["status"] == "PASSED"


def test_merge_preserves_extra_target_columns(spark):
    engine = IcebergMergeEngine(spark)
    table = "silver_test.silver.legacy_extras"
    original = spark.createDataFrame([("A", 1, "retained")], "id string, value int, legacy string")
    engine.merge(original, table, ["id"])
    incoming = spark.createDataFrame([("A", 2), ("B", 3)], "id string, value int")
    engine.merge(incoming, table, ["id"])
    rows = spark.table(table).orderBy("id").collect()
    assert [(r.id, r.value, r.legacy) for r in rows] == [("A", 2, "retained"), ("B", 3, None)]
