import os
import pytest
from silver.framework import SilverTransformationFramework

@pytest.mark.integration
def test_faostat_production_silver_framework():
    """
    Integration test for the Silver Framework.
    Requires Docker services (MinIO, Iceberg REST, Spark, Trino).
    If services are not available, it will fail or skip.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("silver-integration-test") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "rest") \
            .config("spark.sql.catalog.iceberg.uri", os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")) \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.iceberg.s3.endpoint", os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")) \
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password123") \
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")) \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception:
        pytest.skip("BLOCKED BY ENVIRONMENT: PySpark or Iceberg REST catalog not accessible.")

    # In a real environment, we would also verify if the bronze table exists before running
    try:
        spark.sql("DESCRIBE iceberg.bronze.faostat_production")
    except Exception:
        pytest.skip("BLOCKED BY ENVIRONMENT: iceberg.bronze.faostat_production does not exist.")

    framework = SilverTransformationFramework(spark=spark, contract_dir="contracts/silver")
    
    # Run 1: Initial load
    result1 = framework.run(dataset_id="faostat_production", pipeline_run_id="test_run_1")
    assert result1["status"] == "SUCCESS"
    assert result1["bronze_count"] > 0
    assert result1["valid_count"] <= result1["bronze_count"], "Valid records cannot exceed bronze records"
    assert result1["dedup_count"] <= result1["valid_count"], "Deduplicated records cannot exceed valid records"
    
    # Verify silver count matches dedup_count (since we just ran it, and the run processed the full bronze snapshot)
    silver_df = spark.table("iceberg.silver.faostat_production")
    count1 = silver_df.count()
    assert count1 == result1["dedup_count"], "Silver table count should match the deduplication count from the run"
    
    # Verify quarantine table
    try:
        quarantine_df = spark.table("iceberg.silver.dead_letters").filter("source_dataset = 'faostat_production'")
        quarantine_count = quarantine_df.count()
        assert quarantine_count == result1["quarantine_count"], "Quarantine table count does not match the returned metric"
    except Exception as e:
        # If there are 0 quarantined records, the table might not exist if it was never created, but the framework ensures schema
        if result1["quarantine_count"] > 0:
            raise e
    
    # Run 2: Idempotency (same data)
    result2 = framework.run(dataset_id="faostat_production", pipeline_run_id="test_run_2")
    assert result2["status"] == "SUCCESS"
    assert result2["dedup_count"] == result1["dedup_count"], "Idempotency failed: dedup_count changed for the identical bronze snapshot"
    
    count2 = silver_df.count()
    assert count1 == count2, "Idempotency failed: Row count in Silver changed after second run with same data."
