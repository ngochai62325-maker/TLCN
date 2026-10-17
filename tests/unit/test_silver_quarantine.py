"""Quarantine retries verified against real, temporary Iceberg tables."""
import json
import os
import uuid
from pathlib import Path

import pytest
from pyiceberg.table import StaticTable
from pyspark.sql import SparkSession

from silver.engine.quarantine import SilverQuarantineManager

SCHEMA = ("business_key string, value int, dq_errors "
          "array<struct<rule_id:string,error_message:string,failed_column:string>>")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (SparkSession.builder.master("local[2]").appName("quarantine-retry-test")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.defaultCatalog", "spark_catalog")
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.catalog.quarantine_test", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.catalog.quarantine_test.type", "hadoop")
               .config("spark.sql.catalog.quarantine_test.warehouse", tmp_path_factory.mktemp("quarantine-warehouse").as_uri())
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE NAMESPACE IF NOT EXISTS quarantine_test.retry")
    yield session
    session.stop()


@pytest.fixture
def manager(spark):
    return SilverQuarantineManager(spark, "quarantine_test.retry.dl_" + uuid.uuid4().hex)


def batch(spark, *items, error="R1"):
    return spark.createDataFrame([
        (key, value, [{"rule_id": error, "error_message": "Rejected " + error,
                       "failed_column": "value"}]) for key, value in items
    ], SCHEMA)


def native_state(manager):
    table = manager.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        manager.spark._jsparkSession, manager.target_table)
    location = table.operations().current().metadataFileLocation()
    # Read persisted metadata and Parquet independently of Spark's table cache.
    native = StaticTable.from_metadata(location)
    return {
        "uuid": str(native.metadata.table_uuid),
        "snapshot_id": str(native.current_snapshot().snapshot_id),
        "schema_id": native.metadata.current_schema_id,
        "metadata_location": location,
        "snapshot_count": len(native.metadata.snapshots),
        "records": sorted(native.scan().to_arrow().to_pylist(), key=lambda r: r["quarantine_record_id"]),
    }


def evidence(name, before, after):
    path = os.environ.get("SILVER_QUARANTINE_EVIDENCE")
    if path:
        with Path(path).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"test": name, "before": before, "after": after}, default=str) + "\n")


def test_quarantine_custom_target_table(manager):
    assert manager.target_table.startswith("quarantine_test.retry.")


def test_quarantine_idempotent_writes(spark, manager):
    data = batch(spark, ("A", 10))
    manager.route_quarantine(data, "test_ds", "run_1", ["business_key"])
    before = native_state(manager)
    for run in ("run_2", "run_3"):
        manager.route_quarantine(data, "test_ds", run, ["business_key"])
        assert native_state(manager) == before
    assert before["records"][0]["pipeline_run_id"] == "run_1"
    evidence("identical_retry_zero_commit", before, native_state(manager))


def test_new_record_inserts_once_and_preserves_existing(spark, manager):
    manager.route_quarantine(batch(spark, ("A", 10)), "test_ds", "run_1", ["business_key"])
    before = native_state(manager)
    manager.route_quarantine(batch(spark, ("B", 20)), "test_ds", "run_2", ["business_key"])
    after = native_state(manager)
    assert len(after["records"]) == 2 and before["records"][0] in after["records"]
    assert after["uuid"] == before["uuid"] and after["snapshot_id"] != before["snapshot_id"]
    manager.route_quarantine(batch(spark, ("B", 20)), "test_ds", "run_3", ["business_key"])
    assert native_state(manager) == after
    evidence("new_insert_then_physical_noop", before, after)


def test_mixed_batch_preserves_existing_audit_and_all_new_records(spark, manager):
    manager.route_quarantine(batch(spark, ("A", 10)), "test_ds", "run_1", ["business_key"])
    before = native_state(manager)
    mixed = batch(spark, ("A", 10), ("B", 20), ("C", 30), ("B", 20))
    manager.route_quarantine(mixed, "test_ds", "run_2", ["business_key"])
    after = native_state(manager)
    assert len(after["records"]) == 3 and before["records"][0] in after["records"]
    assert len({r["quarantine_record_id"] for r in after["records"]}) == 3
    manager.route_quarantine(mixed, "test_ds", "run_3", ["business_key"])
    assert native_state(manager) == after
    evidence("mixed_batch_then_physical_noop", before, after)


def test_changed_errors_keep_existing_update_policy(spark, manager):
    manager.route_quarantine(batch(spark, ("A", 10)), "test_ds", "run_1", ["business_key"])
    before = native_state(manager)
    changed = batch(spark, ("A", 10), error="R2")
    manager.route_quarantine(changed, "test_ds", "run_2", ["business_key"])
    after = native_state(manager)
    old, new = before["records"][0], after["records"][0]
    assert len(after["records"]) == 1
    for field in ("quarantine_record_id", "payload", "business_key_hash"):
        assert new[field] == old[field]
    assert new["errors"][0]["rule_id"] == "R2" and new["pipeline_run_id"] == "run_2"
    assert new["detected_at"] != old["detected_at"]
    manager.route_quarantine(changed, "test_ds", "run_3", ["business_key"])
    assert native_state(manager) == after
    evidence("changed_errors_existing_policy_then_noop", before, after)


def test_changed_payload_gets_new_identity_without_deleting_old(spark, manager):
    manager.route_quarantine(batch(spark, ("A", 10)), "test_ds", "run_1", ["business_key"])
    before = native_state(manager)
    manager.route_quarantine(batch(spark, ("A", 11)), "test_ds", "run_2", ["business_key"])
    after = native_state(manager)
    assert len(after["records"]) == 2 and before["records"][0] in after["records"]
    assert len({r["quarantine_record_id"] for r in after["records"]}) == 2
    evidence("changed_payload_new_identity", before, after)


def test_corrupt_payload_under_same_identity_fails_without_commit(spark, manager):
    data = batch(spark, ("A", 10))
    manager.route_quarantine(data, "test_ds", "run_1", ["business_key"])
    # Corrupt only this temporary test table to exercise a persisted ID mismatch.
    spark.sql(f"UPDATE {manager.target_table} SET payload = 'corrupt'")
    before = native_state(manager)
    with pytest.raises(ValueError, match="different persisted payload"):
        manager.route_quarantine(data, "test_ds", "run_2", ["business_key"])
    assert native_state(manager) == before
    evidence("identity_corruption_fails_closed", before, native_state(manager))
