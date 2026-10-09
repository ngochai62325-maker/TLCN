import pytest
from silver.engine.quality import SilverQualityEngine

@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[1]").appName("silver-test").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")

def test_quality_engine(spark):
    data = [
        ("A", 10, 2020),
        ("B", -5, 2020),
        ("C", 10, 2030),
        (None, 10, 2020)
    ]
    columns = ["business_key", "value", "year"]
    df = spark.createDataFrame(data, schema=columns)
    
    rules = [
        {"rule_id": "PROD_DQ_001", "rule": "Value >= 0"},
        {"rule_id": "PROD_DQ_002", "rule": "Year <= current"},
        {"rule_id": "PROD_DQ_003", "rule": "NotNull key"}
    ]
    
    # We mapped PROD_DQ_001 to value >= 0 OR value IS NULL
    # PROD_DQ_002 to year <= year(current_date()) + 1 AND year >= 1960
    # PROD_DQ_003 to business_key IS NOT NULL (for the test we used a simpler rule mapping in quality engine)
    # Let's adjust our quality engine to use the rule mapping properly
    
    engine = SilverQualityEngine(rules)
    valid_df, quarantine_df = engine.apply_rules(df)
    
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    
    # A is valid. B fails DQ_001. C fails DQ_002. None fails DQ_003.
    assert valid_count == 1
    assert quarantine_count == 3
    
    quarantine_rows = quarantine_df.collect()
    for row in quarantine_rows:
        errors = row["dq_errors"]
        assert len(errors) > 0
