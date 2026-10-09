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
            .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181") \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000") \
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
    
    # Get silver count
    silver_df = spark.table("iceberg.silver.faostat_production")
    count1 = silver_df.count()
    
    # Run 2: Idempotency (same data)
    result2 = framework.run(dataset_id="faostat_production", pipeline_run_id="test_run_2")
    assert result2["status"] == "SUCCESS"
    
    count2 = silver_df.count()
    assert count1 == count2, "Idempotency failed: Row count changed after second run with same data."
