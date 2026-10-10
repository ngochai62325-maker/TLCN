"""Shared source controls must preserve rejected records and warning semantics."""
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from silver.engine.quality import SilverQualityEngine
from silver.engine.merge import IcebergMergeEngine
from silver.framework import SilverTransformationFramework, TRANSFORMERS
from silver.transformers.thitruongnongsan import ThitruongnongsanTransformer
from tests.unit.silver.faostat_nso_fixtures import sql_rows


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("SilverControls").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    yield session
    session.stop()


def test_null_predicate_fails_and_warning_stays_valid(spark):
    df = spark.sql("SELECT * FROM VALUES (1,5), (2,CAST(NULL AS INT)) AS rows(id,value)")
    rules = [{"rule_id": "REQUIRED", "sql_expr": "value > 0"},
             {"rule_id": "REVIEW", "sql_expr": "value > 10", "action": "LOG"}]
    valid, dead = SilverQualityEngine(rules).apply_rules(df)
    assert valid.count() == dead.count() == 1
    assert valid.first().dq_warnings[0].rule_id == "REVIEW"
    assert dead.first().dq_errors[0].rule_id == "REQUIRED"


def test_incremental_requires_filter_and_intersects_run_watermark(spark):
    df = spark.sql("SELECT * FROM VALUES ('a',1), ('b',2), ('a',3) AS rows(_ingestion_run_id,_ingestion_timestamp)")
    with pytest.raises(ValueError, match="requires"):
        SilverTransformationFramework.filter_input(df, "incremental")
    actual = SilverTransformationFramework.filter_input(df, "incremental", 1, "a")
    assert [row._ingestion_timestamp for row in actual.collect()] == [3]


def test_duplicate_survivor_does_not_depend_on_arrival_order(spark):
    df = spark.sql("SELECT * FROM VALUES ('a',1,'left'), ('a',1,'right') AS rows(id,_ingestion_timestamp,value)")
    engine = IcebergMergeEngine(spark)
    assert engine.deduplicate(df.repartition(1), ["id"]).first().value == engine.deduplicate(df.repartition(2), ["id"]).first().value


@pytest.mark.parametrize("unit", ["VNĐ/Kg", "VNĐ/kg", "Vnđ/Kg", "Đồng/kg"])
def test_domestic_daily_date_and_raw_error_are_preserved(spark, unit):
    df = sql_rows(spark, [{"t_n_m_t_h_ng": "Rice", "th_tr_ng": "An Giang", "lo_i_gi_": "Farmgate",
        "_n_v_t_nh": unit, "lo_i_ti_n": "VNĐ", "ngu_n": "Local", "ng_y": "8/3/2026 12:00:00 AM", "gi_": "6000"},
        {"t_n_m_t_h_ng": "Rice", "th_tr_ng": "An Giang", "lo_i_gi_": "Farmgate",
        "_n_v_t_nh": "VNĐ/Kg", "lo_i_ti_n": "VNĐ", "ngu_n": "Local", "ng_y": "7/31/2026 12:00:00 AM", "gi_": "broken"}])
    transformer = ThitruongnongsanTransformer()
    actual = transformer.transform(df)
    valid, dead = SilverQualityEngine(transformer.quality_rules()).apply_rules(actual)
    assert valid.count() == dead.count() == 1
    row = valid.first()
    assert str(row.date) == "2026-08-03" and row.unit == "VND/kg" and row.currency == "VND"
    assert dead.first()._raw_value == "broken" and dead.first()._source_payload


@pytest.mark.parametrize("source", [source for source in TRANSFORMERS if source != "nso_vietnam"])
def test_all_ready_sources_run_contract_and_source_dq(spark, source):
    # Three actual Bronze rows kept as bounded audit evidence, not shared writes.
    evidence = json.loads(Path("docs/silver/bronze_inventory_evidence.json").read_text(encoding="utf-8"))
    df = sql_rows(spark, evidence["tables"][source]["sample"])
    contract, cached, valid, dead, report = SilverTransformationFramework(spark).prepare(source, df)
    try:
        assert report["observation_count"] == report["valid_count"] + report["quarantine_count"] + report["excluded_count"]
        assert report["valid_count"] == report["dedup_count"] + report["deduplicated_count"]
        assert "_source_payload" in valid.columns and "_source_checksum" in valid.columns
        assert report["writes_performed"] is False
        assert contract.business_key
    finally:
        cached.unpersist()
