import pytest
from silver.engine.merge import IcebergMergeEngine

@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[1]") \
            .appName("silver-merge-test") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg_warehouse") \
            .getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")

def test_deduplication_basic(spark):
    data = [
        ("A", 1, 100),
        ("A", 2, 200),
        ("B", 1, 300)
    ]
    columns = ["business_key", "_ingestion_timestamp", "value"]
    df = spark.createDataFrame(data, schema=columns)
    
    engine = IcebergMergeEngine(spark)
    dedup_df = engine.deduplicate(df, business_keys=["business_key"])
    
    assert dedup_df.count() == 2
    
    rows = dedup_df.orderBy("business_key").collect()
    assert rows[0]["business_key"] == "A"
    assert rows[0]["value"] == 200 # the one with latest timestamp
    assert rows[1]["business_key"] == "B"

def test_deduplication_deterministic_tie_breaker(spark):
    # Identical business key, identical timestamp, different payload
    # tie breaker should pick based on ascending sort of the rest of the columns
    data = [
        ("A", 1, 200, "X"),
        ("A", 1, 100, "Y"),
        ("A", 1, 100, None)
    ]
    # order should be: (100, None), (100, Y), (200, X) -> ascending, so (100, None) is picked first?
    # wait, ascending nulls first
    columns = ["business_key", "_ingestion_timestamp", "value", "other"]
    df = spark.createDataFrame(data, schema=columns)
    
    engine = IcebergMergeEngine(spark)
    dedup_df = engine.deduplicate(df, business_keys=["business_key"])
    
    assert dedup_df.count() == 1
    row = dedup_df.first()
    assert row["value"] == 100
    assert row["other"] is None

def test_deduplication_identical_rows(spark):
    data = [
        ("A", 1, 100),
        ("A", 1, 100)
    ]
    df = spark.createDataFrame(data, schema=["business_key", "_ingestion_timestamp", "value"])
    
    engine = IcebergMergeEngine(spark)
    dedup_df = engine.deduplicate(df, business_keys=["business_key"])
    
    assert dedup_df.count() == 1
    row = dedup_df.first()
    assert row["value"] == 100
def test_merge_idempotency_and_updates(spark):
    engine = IcebergMergeEngine(spark)
    target_table = "iceberg.test_merge_idempotency"
    
    # Create target table by merging first batch
    data1 = [
        ("A", 1, 100),
        ("B", 1, 200)
    ]
    df1 = spark.createDataFrame(data1, schema=["business_key", "_ingestion_timestamp", "value"])
    engine.merge(df1, target_table=target_table, business_keys=["business_key"])
    
    assert spark.table(target_table).count() == 2
    
    # Merge second batch: A is updated (newer timestamp), B is identical (should be idempotent), C is new
    data2 = [
        ("A", 2, 150),
        ("B", 1, 200),
        ("C", 2, 300)
    ]
    df2 = spark.createDataFrame(data2, schema=["business_key", "_ingestion_timestamp", "value"])
    engine.merge(df2, target_table=target_table, business_keys=["business_key"])
    
    res = spark.table(target_table)
    assert res.count() == 3
    
    rows = res.orderBy("business_key").collect()
    assert rows[0]["business_key"] == "A"
    assert rows[0]["value"] == 150  # Updated
    assert rows[1]["business_key"] == "B"
    assert rows[1]["value"] == 200  # Idempotent
    assert rows[2]["business_key"] == "C"
    assert rows[2]["value"] == 300  # Inserted
