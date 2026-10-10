import pytest
from pyspark.sql import SparkSession
from silver.engine.quarantine import SilverQuarantineManager

@pytest.fixture(scope="module")
def spark():
    try:
        return SparkSession.builder \
            .master("local[1]") \
            .appName("quarantine-test") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg_warehouse") \
            .getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")

def test_quarantine_custom_target_table(spark):
    manager = SilverQuarantineManager(spark, target_table="iceberg.custom_schema.dead_letters")
    assert manager.target_table == "iceberg.custom_schema.dead_letters"
    
def test_quarantine_idempotent_writes(spark):
    manager = SilverQuarantineManager(spark, target_table="iceberg.test_quarantine_idempotent")
    
    data = [
        ("A", 10, [{"rule_id": "R1", "error_message": "Err1", "failed_column": "value"}]),
    ]
    df = spark.createDataFrame(data, schema=["business_key", "value", "dq_errors"])
    
    # Run once
    manager.route_quarantine(df, dataset="test_ds", run_id="run_1", business_keys=["business_key"])
    
    count1 = spark.table("iceberg.test_quarantine_idempotent").count()
    assert count1 == 1
    
    # Run again with same payload, different run_id
    manager.route_quarantine(df, dataset="test_ds", run_id="run_2", business_keys=["business_key"])
    
    count2 = spark.table("iceberg.test_quarantine_idempotent").count()
    assert count2 == 1  # Should be idempotent!
    
    # Check that run_id is updated
    row = spark.table("iceberg.test_quarantine_idempotent").first()
    assert row["pipeline_run_id"] == "run_2"
    assert row["source_dataset"] == "test_ds"
