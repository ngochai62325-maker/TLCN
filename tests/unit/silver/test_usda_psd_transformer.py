import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from silver.transformers.usda_psd import UsdaPsdTransformer
from silver.engine.quality import SilverQualityEngine

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test-usda-psd").master("local[2]").getOrCreate()

def test_usda_psd_unpivot(spark: SparkSession):
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("commodity", StringType(), True),
        StructField("attribute", StringType(), True),
        StructField("unit_description", StringType(), True),
        StructField("col_1960_1961", DoubleType(), True),
        StructField("col_1961_1962", DoubleType(), True)
    ])
    
    data = [
        ("Vietnam", "Rice, Milled", "Production", "1000 MT", 100.0, 110.0),
        ("Vietnam", None, "Ending Stocks", "1000 MT", None, 50.0), # Null commodity (should default), null value
        ("Thailand", "Rice, Milled", "Production", "1000 MT", 200.0, -10.0) # Negative value
    ]
    
    df = spark.createDataFrame(data, schema)
    
    transformer = UsdaPsdTransformer()
    df_long = transformer.preprocess(df)
    
    results = df_long.collect()
    
    assert len(results) == 6
    
    # Check Vietnam 1960 (null value)
    vn_end = [r for r in results if r.country == "Vietnam" and r.attribute == "Ending Stocks" and r.market_year == 1960][0]
    assert vn_end.commodity == "Rice, Milled" # Assert defaulted
    assert vn_end.crop_year == "1960/1961"
    assert vn_end.value is None # Null preserved, not zero
    
    # Check Thailand 1961 (negative value)
    th_1961 = [r for r in results if r.country == "Thailand" and r.market_year == 1961][0]
    assert th_1961.value == -10.0
    
    # Let's test DQ rules application on this
    dq_rules = [
        {"rule_id": "PSD_DQ_001", "sql_expr": "country IS NOT NULL AND commodity IS NOT NULL AND attribute IS NOT NULL AND market_year IS NOT NULL", "failed_column": "business_key"},
        {"rule_id": "PSD_DQ_002", "sql_expr": "market_year >= 1950 AND market_year <= 2050", "failed_column": "market_year"},
        {"rule_id": "PSD_DQ_003", "sql_expr": "value IS NULL OR value >= 0", "failed_column": "value"}
    ]
    
    quality_engine = SilverQualityEngine(dq_rules)
    valid_df, quarantine_df = quality_engine.apply_rules(df_long)
    
    quarantine_results = quarantine_df.collect()
    
    # Only Thailand 1961 with -10.0 should be quarantined by PSD_DQ_003!
    assert len(quarantine_results) == 1
    bad_row = quarantine_results[0]
    assert bad_row.country == "Thailand"
    assert bad_row.market_year == 1961
    assert len(bad_row.dq_errors) == 1
    assert bad_row.dq_errors[0].rule_id == "PSD_DQ_003"
    assert bad_row.dq_errors[0].failed_column == "value"
