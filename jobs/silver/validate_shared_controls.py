"""Explicit validation fixtures for shared write reliability; requires separate approval."""
import argparse
import json

TARGET = "iceberg.silver_validation.reliability_controls"
QUARANTINE = "iceberg.silver_validation.reliability_dead_letters"


def validate(spark, target=TARGET, quarantine_target=QUARANTINE):
    """Fixture data only; never references a business target or source ID."""
    from silver.engine.merge import IcebergMergeEngine
    from silver.engine.quarantine import SilverQuarantineManager
    engine = IcebergMergeEngine(spark)
    def batch(value, timestamp):
        return spark.sql(f"SELECT 'VALIDATION_ONLY' AS id, {value} AS value, TIMESTAMP '{timestamp}' AS _ingestion_timestamp")
    def snapshot():
        return spark.sql(f"SELECT snapshot_id FROM {target}.history ORDER BY made_current_at DESC LIMIT 1").first()[0]
    first = batch(10, "2026-10-02 00:00:00")
    engine.merge(first, target, ["id"])
    before = snapshot()
    assert engine.merge(first, target, ["id"])["status"] == "NOOP"
    assert engine.merge(batch(1, "2026-10-01 00:00:00"), target, ["id"])["status"] == "NOOP"
    assert snapshot() == before
    engine.merge(batch(20, "2026-10-03 00:00:00"), target, ["id"])
    assert spark.table(target).count() == 1 and spark.table(target).first().value == 20
    after = snapshot()
    assert engine.merge(batch(20, "2026-10-03 00:00:00"), target, ["id"])["status"] == "NOOP"
    assert snapshot() == after
    manager = SilverQuarantineManager(spark, quarantine_target)
    dead = spark.sql("SELECT 'VALIDATION_ONLY' AS id, "
        "array(named_struct('rule_id','INJECTED_VALIDATION_ERROR','error_message','fixture only','failed_column','value')) AS dq_errors")
    def interrupted():
        manager.route_quarantine(dead.unionByName(dead), "VALIDATION_ONLY", "validation-failure", ["id"])
        raise RuntimeError("Injected validation-only failure before Silver merge")
    try:
        interrupted()
    except RuntimeError as exc:
        assert str(exc) == "Injected validation-only failure before Silver merge"
    result = manager.route_quarantine(dead, "VALIDATION_ONLY", "validation-retry", ["id"])
    assert result["status"] == "NOOP" and spark.table(quarantine_target).count() == 1
    assert engine.merge(batch(20, "2026-10-03 00:00:00"), target, ["id"])["status"] == "NOOP"
    return {"status": "VALIDATION_PASSED", "fixture_only": True, "target": target,
            "quarantine_target": quarantine_target, "target_rows": 1, "quarantine_rows": 1,
            "full_rerun_noop": True, "stale_batch_noop": True, "late_correction_verified": True,
            "failed_write_retry_verified": True, "snapshot_before": before, "snapshot_after": after}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Write only after approval of both validation tables")
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({"status": "PLAN_ONLY", "writes_shared_data": False,
                          "target": TARGET, "quarantine_target": QUARANTINE}))
        return
    from silver.core.spark_session import get_spark_session
    spark = get_spark_session("ApprovedSharedReliabilityValidation")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver_validation")
        print("SILVER_VALIDATION_JSON=" + json.dumps(validate(spark)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
