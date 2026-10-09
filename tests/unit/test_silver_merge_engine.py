import pytest
from silver.engine.merge import IcebergMergeEngine

@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[1]").appName("silver-test").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")

def test_deduplication(spark):
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
