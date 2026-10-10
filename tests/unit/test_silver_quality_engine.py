import pytest
from silver.engine.quality import SilverQualityEngine

@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[1]").appName("silver-test").getOrCreate()
    except ImportError:
        pytest.skip("PySpark not available")

def test_quality_engine_basic(spark):
    data = [
        ("A", 10, 2020),   # Passing
        ("B", -5, 2020),   # Failing (Value < 0)
        ("C", 10, 2030),   # Failing (Year > current)
        (None, -5, 2030),  # Multiple violations (Null key, Value < 0, Year > current)
        ("E", None, 2020)  # Passing (Value is NULL -> allowed by GEN_DQ_001)
    ]
    columns = ["business_key", "value", "year"]
    df = spark.createDataFrame(data, schema=columns)
    
    rules = [
        {"rule_id": "GEN_DQ_001", "rule": "Value >= 0", "sql_expr": "value >= 0 OR value IS NULL", "failed_column": "value"},
        {"rule_id": "GEN_DQ_002", "rule": "Year <= current", "sql_expr": "year <= 2025", "failed_column": "year"},
        {"rule_id": "GEN_DQ_003", "rule": "NotNull key", "sql_expr": "business_key IS NOT NULL", "failed_column": "business_key"}
    ]
    
    engine = SilverQualityEngine(rules)
    valid_df, quarantine_df = engine.apply_rules(df)
    
    assert valid_df.count() == 2 # A and E
    assert quarantine_df.count() == 3 # B, C, and None
    
    # Check multiple violations
    multiple_violations_row = quarantine_df.filter(quarantine_df.business_key.isNull()).first()
    assert len(multiple_violations_row["dq_errors"]) == 3

def test_quality_engine_no_rules(spark):
    data = [("A", 10)]
    df = spark.createDataFrame(data, schema=["col1", "col2"])
    
    engine = SilverQualityEngine([])
    valid_df, quarantine_df = engine.apply_rules(df)
    
    assert valid_df.count() == 1
    assert quarantine_df.count() == 0

def test_quality_engine_null_evaluation(spark):
    # If a rule expression evaluates to NULL, it should fail the record.
    # e.g., string + 1 evaluates to NULL, or comparing a column that doesn't exist?
    # Wait, missing column will throw AnalysisException.
    # Let's test a condition that returns NULL.
    data = [("A", None)]
    df = spark.createDataFrame(data, schema=["col1", "col2"])
    
    rules = [{"rule_id": "NULL_EVAL", "rule": "test", "sql_expr": "col2 > 5", "failed_column": "col2"}]
    engine = SilverQualityEngine(rules)
    valid_df, quarantine_df = engine.apply_rules(df)
    
    # col2 is NULL, so col2 > 5 is NULL. ~NULL is NULL. Our logic handles it as failure.
    assert valid_df.count() == 0
    assert quarantine_df.count() == 1

def test_quality_engine_missing_column(spark):
    data = [("A", 10)]
    df = spark.createDataFrame(data, schema=["col1", "col2"])
    
    rules = [{"rule_id": "MISSING_COL", "rule": "test", "sql_expr": "missing_col > 5", "failed_column": "missing_col"}]
    engine = SilverQualityEngine(rules)
    
    from pyspark.sql.utils import AnalysisException
    with pytest.raises(AnalysisException):
        valid_df, quarantine_df = engine.apply_rules(df)
        valid_df.count()

def test_quality_engine_invalid_sql(spark):
    data = [("A", 10)]
    df = spark.createDataFrame(data, schema=["col1", "col2"])
    
    rules = [{"rule_id": "INVALID_SQL", "rule": "test", "sql_expr": ">>invalid syntax<<", "failed_column": "col2"}]
    engine = SilverQualityEngine(rules)
    
    from pyspark.sql.utils import ParseException
    with pytest.raises(ParseException):
        valid_df, quarantine_df = engine.apply_rules(df)
        valid_df.count()

def test_quality_engine_cast_errors(spark):
    # Simulate df coming from framework with _sys_cast_errors array
    data = [
        ("A", 10, []), # No cast errors, valid
        ("B", 10, [{"rule_id": "SYS_CAST_ERR", "error_message": "Cast failed", "failed_column": "col2"}]), # Has cast error
    ]
    
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", IntegerType(), True),
        StructField("_sys_cast_errors", ArrayType(
            StructType([
                StructField("rule_id", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("failed_column", StringType(), True)
            ])
        ), True)
    ])
    
    df = spark.createDataFrame(data, schema=schema)
    
    # We apply no rules, but the cast error should still quarantine the record
    engine = SilverQualityEngine([])
    valid_df, quarantine_df = engine.apply_rules(df)
    
    assert valid_df.count() == 1
    assert valid_df.first()["col1"] == "A"
    assert "_sys_cast_errors" not in valid_df.columns
    
    assert quarantine_df.count() == 1
    assert quarantine_df.first()["col1"] == "B"
    assert len(quarantine_df.first()["dq_errors"]) == 1
    assert quarantine_df.first()["dq_errors"][0]["rule_id"] == "SYS_CAST_ERR"
