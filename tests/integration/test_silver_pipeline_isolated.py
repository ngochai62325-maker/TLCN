"""Local Iceberg tests; opt-in new-Raw E2E uses guarded isolated REST/MinIO only."""
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
from silver.framework import SilverTransformationFramework, RELEASE_NAMESPACE, RELEASE_SOURCES
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
    dead_table = "silver_test.silver.dead_letters"
    dead_before = spark.table(dead_table).orderBy("quarantine_record_id").collect()
    dead_snapshot_before = spark.sql(
        f"SELECT snapshot_id FROM {dead_table}.refs WHERE name='main'").first()[0]
    second = framework.run(dataset, "second")
    assert second["dedup_count"] == 1
    assert original == spark.table(table).collect()  # Processing timestamp must stay unchanged.
    assert spark.table(table + ".snapshots").count() == snapshot_before
    assert spark.table("silver_test.silver.dead_letters").filter(F.col("source_dataset") == dataset).count() == 2
    assert spark.table(dead_table).orderBy("quarantine_record_id").collect() == dead_before
    assert spark.sql(
        f"SELECT snapshot_id FROM {dead_table}.refs WHERE name='main'").first()[0] == dead_snapshot_before
    audit = audit_tables(spark, [SilverContract(data)])
    assert audit["status"] == "PASSED"
    dead = spark.table("silver_test.silver.dead_letters").filter(F.col("source_dataset") == dataset).first()
    assert "_source_payload" in json.loads(dead.payload)
    assert dead.pipeline_run_id == "first"
    assert not any(c.startswith("_raw") for c in spark.table(table).columns)
    # Exercise the same eight sources through opt-in routing without touching legacy.
    released = SilverTransformationFramework(spark, str(tmp_path), release=RELEASE_NAMESPACE)
    contract = released.contract(dataset)
    assert contract.business_key == SilverContract(data).business_key
    assert contract.columns == SilverContract(data).columns
    assert contract.data_quality_rules == SilverContract(data).data_quality_rules
    pinned = spark.sql(f"SELECT snapshot_id FROM {data['dataset']['bronze_input']}.refs WHERE name='main'").first()[0]
    v2 = released.run(dataset, "v2", bronze_snapshot_id=pinned)
    assert v2["dedup_count"] == first["dedup_count"]
    columns = [c for c in original[0].asDict() if c != "_silver_processed_at"]
    assert spark.table(table).select(*columns).exceptAll(spark.table(contract.silver_output).select(*columns)).isEmpty()
    assert spark.table(contract.silver_output).select(*columns).exceptAll(spark.table(table).select(*columns)).isEmpty()
    assert original == spark.table(table).collect()
    assert snapshot_before == spark.table(table + ".snapshots").count()
    assert released.quarantine_manager.target_table == f"silver_test.{RELEASE_NAMESPACE}.dead_letters"
    assert spark.table(released.quarantine_manager.target_table).filter(F.col("source_dataset") == dataset).count() == 2
    from jobs.silver.validate_silver_e2e import table_state, verify_iceberg_schema
    before = table_state(spark, contract)
    assert verify_iceberg_schema(spark, contract, before)
    for mode in ("full", "incremental"):
        released.run(dataset, "v2-replay", mode=mode, run_id="bronze-run" if mode == "incremental" else None, bronze_snapshot_id=pinned)
        assert before == table_state(spark, contract)
    with pytest.raises(ValueError, match="snapshot drift"):
        released.run(dataset, "wrong-snapshot", bronze_snapshot_id=pinned + 1)
    assert before == table_state(spark, contract)
    if dataset == "faostat_production":
        from unittest.mock import patch
        from jobs.silver.validate_silver_e2e import accept_source
        with patch.object(released.merge_engine, "merge", side_effect=RuntimeError("ambiguous commit")):
            with pytest.raises(RuntimeError) as failure:
                accept_source(spark, released, dataset, {"snapshot_id": str(pinned), "run_id": "bronze-run", "rows": 3})
        recovery = failure.value.silver_failure
        assert recovery["stage"] == "silver_merge" and recovery["quarantine_committed"]
        assert recovery["catalog_readback"] == "SUCCEEDED"
        assert recovery["target_before"]["uuid"] == recovery["target_after"]["uuid"]
        assert recovery["retry_requires_catalog_readback"]
        assert before == table_state(spark, contract)


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


def test_new_raw_incremental_revisions_e2e(spark, tmp_path, monkeypatch):
    """Real adapter/PG/MinIO/Iceberg/Trino; new objects on the isolated network only."""
    import csv
    import hashlib
    import os
    import uuid
    from urllib.parse import urlsplit, urlunsplit

    import psycopg2
    from psycopg2 import sql
    from ingestion.config.registry import SourceRegistry
    from ingestion.core.ingestion_engine import IngestionEngine
    from ingestion.storage.metadata_repository import MetadataRepository
    from ingestion.storage.minio_storage import MinioStorage
    from ingestion.storage.bronze_writer import BronzeIcebergWriter
    from jobs.silver.validate_silver_e2e import table_identity, table_state, trino_distributions, verify_values
    from jobs.silver.verify_silver_trino import verify_record, query
    from silver.framework import AUDIT_COLUMNS

    if os.getenv("TLCN_ISOLATED_REBUILD") != "1":
        pytest.skip("Requires the existing independent compose.silver-test.yml services")
    expected = {"MINIO_ENDPOINT": "minio-test:9000", "ICEBERG_REST_URI": "http://rest-test:8181",
                "TRINO_HOST": "trino-test"}
    assert all(os.environ.get(k) == v for k, v in expected.items())
    dsn = urlsplit(os.environ["INGESTION_DB_URL"])
    assert dsn.hostname == "postgres-test" and dsn.path == "/silver_rebuild_test"
    # A separate database and namespace preserve the original isolated rebuild evidence.
    suffix = uuid.uuid4().hex[:12]
    database, namespace = f"silver_incremental_{suffix}", f"incremental_{suffix}"
    connection = psycopg2.connect(os.environ["INGESTION_DB_URL"])
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    finally:
        connection.close()
    monkeypatch.setenv("INGESTION_DB_URL", urlunsplit(dsn._replace(path="/" + database)))
    metadata = MetadataRepository()
    metadata.initialize()
    storage = MinioStorage()
    writer = BronzeIcebergWriter(catalog="rebuild_test", schema=namespace)
    for key, value in {
        "": "org.apache.iceberg.spark.SparkCatalog", ".type": "rest",
        ".uri": expected["ICEBERG_REST_URI"], ".warehouse": "s3://silver-rebuild-warehouse/",
        ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO", ".s3.endpoint": "http://minio-test:9000",
        ".s3.path-style-access": "true", ".s3.access-key-id": os.environ["MINIO_ROOT_USER"],
        ".s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
    }.items():
        spark.conf.set("spark.sql.catalog.rebuild_test" + key, value)

    source = "faostat_production"
    registry_data = yaml.safe_load(Path("config/ingestion/source_registry.yaml").read_text(encoding="utf-8"))
    config = registry_data["sources"][source]
    raw_path = tmp_path / "production_revision.csv"
    config.update(local_path=str(raw_path), local_fallback=str(raw_path))
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(yaml.safe_dump({"sources": {source: config}}), encoding="utf-8")
    registry = SourceRegistry(str(registry_path))
    engine = IngestionEngine(registry=registry, metadata_repo=metadata, minio_storage=storage, bronze_writer=writer)
    data = copy.deepcopy(SilverContractLoader().load_contract(source)._data)
    bronze, target, dead = (f"rebuild_test.{namespace}.{name}" for name in
                            (source, "silver_faostat_production", "dead_letters"))
    data["dataset"].update(bronze_input=bronze, silver_output=target)
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / f"{source}.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    framework = SilverTransformationFramework(spark, str(contracts))
    framework.quarantine_manager.target_table = dead
    contract = framework.contract(source)
    # Derive the Raw fixture from the existing, verified FAOSTAT source fixture.
    fixture = observations(spark).first().asDict()
    base = {c: fixture[sanitize_column_name(c)] for c in config["expected_columns"] if c != "Year Code"}
    base["Year Code"] = fixture["year"]

    def write_raw(rows):
        with raw_path.open("w", encoding="utf-8", newline="") as stream:
            csv_writer = csv.DictWriter(stream, fieldnames=config["expected_columns"])
            csv_writer.writeheader()
            csv_writer.writerows(dict(base, Year=str(year), **{"Year Code": str(year), "Value": value})
                                 for year, value in rows)
        return hashlib.sha256(raw_path.read_bytes()).hexdigest()

    sha_before = write_raw([(2019, "9"), (2020, "12.5")])
    first = engine.run_with_readiness_check(source)
    assert first.status.value == "SUCCESS" and first.records_extracted == 2
    bronze_before = table_identity(spark, bronze)
    with metadata.source_lock(source):
        initial = framework.run(source, "revision-baseline", bronze_snapshot_id=bronze_before["snapshot_id"])
        silver_before = table_state(spark, contract)
    assert initial["dedup_count"] == silver_before["rows"] == 2

    sha_after = write_raw([(2019, "9"), (2020, "15.5"), (2021, "20"), (2021, "20"), (2022, "-1")])
    assert sha_before != sha_after
    second = engine.run_with_readiness_check(source)
    assert second.status.value == "SUCCESS" and second.records_extracted == 5
    assert second.checksum == sha_after and second.run_id != first.run_id
    # The same test session has cached the table; production launches a fresh Spark job.
    spark.catalog.refreshTable(bronze)
    bronze_after = table_identity(spark, bronze)
    assert bronze_after["uuid"] == bronze_before["uuid"]
    assert bronze_after["snapshot_id"] != bronze_before["snapshot_id"]
    assert spark.table(bronze).count() == 7
    assert spark.read.format("iceberg").option("snapshot-id", bronze_before["snapshot_id"]).load(bronze).count() == 2

    with metadata.source_lock(source):
        metrics = framework.run(source, "revision-incremental", mode="incremental", run_id=second.run_id,
                                bronze_snapshot_id=bronze_after["snapshot_id"])
        silver_after = table_state(spark, contract)
    assert {k: metrics[k] for k in ("bronze_count", "prepared_count", "valid_count", "quarantine_count",
                                  "dedup_count", "duplicate_count")} == {
        "bronze_count": 5, "prepared_count": 5, "valid_count": 4, "quarantine_count": 1,
        "dedup_count": 3, "duplicate_count": 1}
    assert metrics["bronze_read_snapshot_id"] == int(bronze_after["snapshot_id"])
    assert silver_after["uuid"] == silver_before["uuid"]
    assert silver_after["snapshot_id"] != silver_before["snapshot_id"]
    published = spark.table(target)
    assert {r.year: r.value for r in published.select("year", "value").collect()} == {
        2019: Decimal("9.00"), 2020: Decimal("15.50"), 2021: Decimal("20.00")}
    assert published.select(*contract.business_key).distinct().count() == silver_after["rows"] == 3
    assert published.filter(F.col("_ingestion_run_id") != second.run_id).isEmpty()
    assert published.filter(F.col("_source_checksum") != sha_after).isEmpty()
    assert published.select(*AUDIT_COLUMNS).exceptAll(spark.table(bronze).select(*AUDIT_COLUMNS)).isEmpty()
    assert spark.table(dead).count() == 1
    invalid = spark.table(dead).first()
    assert "-1" in invalid.payload and any(e.failed_column == "value" for e in invalid.errors)
    assert audit_tables(spark, [contract])["status"] == "PASSED"
    verify_values(spark, source, contract, metrics)

    for mode in ("incremental", "full"):
        with metadata.source_lock(source):
            framework.run(source, "revision-replay-" + mode, mode=mode,
                          run_id=second.run_id if mode == "incremental" else None,
                          bronze_snapshot_id=bronze_after["snapshot_id"])
            assert table_state(spark, contract) == silver_after
        assert spark.table(dead).count() == 1
    assert engine.run_with_readiness_check(source).status.value == "SKIPPED"
    assert table_identity(spark, bronze) == bronze_after
    record = {"metrics": metrics, "silver": silver_after, "business_keys": contract.business_key,
              "bronze_table": bronze, "bronze_snapshot_id": bronze_after["snapshot_id"],
              "replay_verified": True, "incremental_replay_verified": True,
              **trino_distributions(spark, source, contract)}
    trino = verify_record("http://trino-test:8080", source, record)
    assert trino["status"] == "PASSED"
    assert query("http://trino-test:8080", f"SELECT count(*) AS rows FROM {dead}")[0]["rows"] == 1
    print("INCREMENTAL_E2E_RESULT=" + json.dumps({
        "status": "PASSED", "source": source, "database": database, "bronze_table": bronze,
        "target": target, "raw_sha256_before": sha_before, "raw_sha256_after": sha_after,
        "bronze_run_before": first.run_id, "bronze_run_after": second.run_id,
        "bronze_snapshot_before": bronze_before["snapshot_id"], "bronze_snapshot_after": bronze_after["snapshot_id"],
        "silver_snapshot_before": silver_before["snapshot_id"], "silver_snapshot_after": silver_after["snapshot_id"],
        "metrics": metrics, "silver_rows": silver_after["rows"], "trino": trino["status"],
        "full_rerun_noop": True, "incremental_rerun_noop": True, "bronze_replay": "SKIPPED",
    }, default=str), flush=True)


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


def test_existing_schema_guard_and_partial_failure_evidence(spark, tmp_path):
    data = copy.deepcopy(SilverContractLoader().load_contract("usda_psd")._data)
    data["dataset"]["bronze_input"] = "silver_test.bronze.partial_psd"
    data["dataset"]["silver_output"] = "silver_test.silver.partial_psd"
    (tmp_path / "usda_psd.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    source_fixture(spark, "usda_psd").writeTo(data["dataset"]["bronze_input"]).using("iceberg").create()
    spark.createDataFrame([("legacy",)], "country string").writeTo(data["dataset"]["silver_output"]).using("iceberg").create()
    before = spark.table(data["dataset"]["silver_output"]).collect()
    schema_before = spark.table(data["dataset"]["silver_output"]).schema
    framework = SilverTransformationFramework(spark, str(tmp_path))
    framework.quarantine_manager.target_table = "silver_test.silver.partial_dead"
    with pytest.raises(ValueError, match="explicit migration required") as error:
        framework.run("usda_psd", "partial")
    assert error.value.silver_failure["stage"] == "silver_merge"
    assert error.value.silver_failure["quarantine_committed"] is True
    assert error.value.silver_failure["bronze_read_snapshot_id"] is not None
    assert spark.table(data["dataset"]["silver_output"]).schema == schema_before
    assert spark.table(data["dataset"]["silver_output"]).collect() == before
    assert spark.table(framework.quarantine_manager.target_table).count() > 0


def test_timestamp_and_timestamp_ntz_native_iceberg_roundtrip(spark):
    from urllib.parse import urlparse
    df = spark.sql("SELECT current_timestamp() AS aware, CAST('2026-10-12 12:00:00' AS timestamp_ntz) AS naive").cache()
    expected = df.collect()
    table = "silver_test.silver.timestamp_types"
    df.writeTo(table).using("iceberg").create()
    restored = spark.table(table)
    assert {f.name: f.dataType for f in restored.schema} == {f.name: f.dataType for f in df.schema}
    assert restored.collect() == expected
    df.unpersist()
    metadata = spark.sql(f"SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").first().file
    fields = json.loads(Path(urlparse(metadata).path).read_text())["schemas"][-1]["fields"]
    assert {f["name"]: f["type"] for f in fields} == {"aware": "timestamptz", "naive": "timestamp"}
