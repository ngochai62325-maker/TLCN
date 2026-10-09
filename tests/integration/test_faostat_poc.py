import pytest
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

from silver.framework import SilverTransformationFramework
from silver.engine.quality import SilverQualityEngine

@pytest.fixture(scope="module")
def spark():
    # Return existing session from the container
    return SparkSession.builder \
        .appName("silver-poc-audit") \
        .getOrCreate()

@pytest.fixture(scope="module")
def setup_test_data(spark):
    bronze_table = "iceberg.bronze.test_faostat_poc"
    silver_table = "iceberg.silver.test_faostat_poc"
    quarantine_table = "iceberg.silver.dead_letters"

    # Drop existing for clean slate
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    # Don't drop quarantine table if it's shared, but let's drop it for the isolated test to check counts
    spark.sql(f"DROP TABLE IF EXISTS {quarantine_table}")

    # Create dummy bronze data with columns simulating the extraction output
    # faostat_production schema:
    # "Domain Code", "Domain", "Area Code (M49)", "Area", "Element Code", "Element", "Item Code (CPC)", "Item", "Year Code", "Year", "Unit", "Value", "Flag", "Flag Description", "Note"
    # Bronze writer sanitizes to lowercase with underscores.
    
    # 1. Valid record (A)
    # 2. Invalid DQ: value < 0 (B)
    # 3. Invalid DQ: multiple rules (value < 0 AND year = 1900) (C)
    # 4. Duplicate business key in batch (D1, D2) - D2 has newer timestamp
    # 5. Type casting failure - handled by spark cast returning NULL, which then might fail DQ if NOT NULL. 
    #    Let's inject an invalid year "abc" which casts to NULL.
    
    data = [
        # Domain, Area_Code, Element_Code, Item_Code, Year, Value, _ingestion_timestamp, _source_file
        ("PROD", "004", "5312", "0111", "2020", "100.5", "2026-01-01T10:00:00Z", "file1.csv"), # Valid
        ("PROD", "008", "5312", "0111", "2020", "-50.0", "2026-01-01T10:00:00Z", "file1.csv"), # Fails DQ_001 (value < 0)
        ("PROD", "012", "5312", "0111", "1900", "-10.0", "2026-01-01T10:00:00Z", "file1.csv"), # Fails DQ_001 & DQ_002
        ("PROD", "016", "5312", "0111", "2020", "200.0", "2026-01-01T10:00:00Z", "file1.csv"), # D1
        ("PROD", "016", "5312", "0111", "2020", "250.0", "2026-01-01T11:00:00Z", "file2.csv"), # D2 (Duplicate, newer timestamp)
        ("PROD", None, "5312", "0111", "2020", "300.0", "2026-01-01T10:00:00Z", "file1.csv"), # Fails DQ_003 (country_code IS NULL)
    ]
    
    # Create DF with expected bronze sanitized column names
    cols = [
        "domain", "area_code_m49_", "element_code", "item_code_cpc_", "year", "value", 
        "_ingestion_timestamp", "_source_file"
    ]
    
    df = spark.createDataFrame(data, schema=cols)
    
    # Missing columns for contract: 'domain_code', 'element', 'item', 'unit', 'flag', 'flag_description'
    # We will add them as nulls
    for c in ['domain_code', 'element', 'item', 'unit', 'flag', 'flag_description']:
        df = df.withColumn(c, F.lit(None).cast(StringType()))
        
    df = df.withColumn("_ingestion_run_id", F.lit("run_1"))
    df = df.withColumn("_source_checksum", F.lit("chk1"))
    df = df.withColumn("_source_snapshot_id", F.lit(1))
    
    df.write.format("iceberg").mode("overwrite").saveAsTable(bronze_table)
    
    # Yield the framework and table names
    yield {
        "bronze": bronze_table,
        "silver": silver_table,
        "quarantine": quarantine_table
    }

def test_poc_end_to_end(spark, setup_test_data):
    tables = setup_test_data
    
    # We need to temporarily modify the contract file or create a test one to point to these tables
    import yaml
    with open("contracts/silver/faostat_production.yaml", "r") as f:
        contract_data = yaml.safe_load(f)
        
    contract_data["dataset"]["bronze_input"] = tables["bronze"]
    contract_data["dataset"]["silver_output"] = tables["silver"]
    
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        test_contract_path = os.path.join(td, "test_faostat.yaml")
        with open(test_contract_path, "w") as f:
            yaml.dump(contract_data, f)
            
        framework = SilverTransformationFramework(spark, contract_dir=td)
        
        # --- RUN 1 ---
        result1 = framework.run("test_faostat", "run_1")
        
        # 1. Total input rows should be 6
        assert result1["bronze_count"] == 6
        
        # 2. Valid vs Quarantine counts
        # Valid: 
        # - 004 (A)
        # - 016 (D1, D2) -> wait, deduplication happens AFTER quality engine!
        # So Valid evaluating to Quality Engine: 004, 016(D1), 016(D2) = 3 rows
        assert result1["valid_count"] == 3
        
        # Quarantined:
        # - 008 (value < 0)
        # - 012 (value < 0, year 1900)
        # - None (country_code null)
        assert result1["quarantine_count"] == 3
        
        # Dedup count:
        # 004, 016(D2) = 2 rows
        assert result1["dedup_count"] == 2
        
        # Verify Silver Table
        silver_df = spark.table(tables["silver"])
        assert silver_df.count() == 2
        
        # Verify D2 won over D1 (value should be 250.0)
        row_016 = silver_df.filter(F.col("country_code") == "016").collect()[0]
        # value is decimal, so cast to float for comparison
        assert float(row_016["value"]) == 250.0
        
        # Verify Quarantine Table
        # "One invalid source row produces one quarantine record containing all applicable errors."
        quar_df = spark.table(tables["quarantine"])
        assert quar_df.count() == 3
        
        row_012 = quar_df.filter(F.col("payload").like("%012%")).collect()[0]
        errors_012 = row_012["errors"]
        assert len(errors_012) == 2, "Row 012 should have 2 errors (value and year)"
        rule_ids = [e.rule_id for e in errors_012]
        assert "PROD_DQ_001" in rule_ids
        assert "PROD_DQ_002" in rule_ids
        
        # --- RUN 2 (Reprocessing / Idempotency) ---
        result2 = framework.run("test_faostat", "run_2")
        
        # It reads the same Bronze table (6 rows). So valid=3, quar=3, dedup=2
        # But MERGE INTO should not duplicate the Silver table rows.
        silver_df = spark.table(tables["silver"])
        assert silver_df.count() == 2
        
        # What about Quarantine? Did it duplicate?
        # The prompt says: "Audit quarantine idempotency... Implement only the behavior justified by the existing architecture... If stable quarantine keys are needed, define them carefully"
        # Wait! In run_2, the same quarantine rows are re-inserted. So quarantine table will have 6 rows!
        quar_df = spark.table(tables["quarantine"])
        assert quar_df.count() == 3, "Quarantine should be idempotent and not create duplicate failure events."
        
        # --- RUN 3 (Updating an existing business key) ---
        # Add a new row in Bronze with country 004 and updated value = 999.9, newer timestamp
        new_data = [
            ("PROD", "004", "5312", "0111", "2020", "999.9", "2026-01-02T10:00:00Z", "file3.csv")
        ]
        local_cols = [
            "domain", "area_code_m49_", "element_code", "item_code_cpc_", "year", "value", 
            "_ingestion_timestamp", "_source_file"
        ]
        new_df = spark.createDataFrame(new_data, schema=local_cols)
        for c in ['domain_code', 'element', 'item', 'unit', 'flag', 'flag_description']:
            new_df = new_df.withColumn(c, F.lit(None).cast(StringType()))
        new_df = new_df.withColumn("_ingestion_run_id", F.lit("run_3"))
        new_df = new_df.withColumn("_source_checksum", F.lit("chk3"))
        new_df = new_df.withColumn("_source_snapshot_id", F.lit(2))
        
        new_df.write.format("iceberg").mode("append").saveAsTable(tables["bronze"])
        
        framework.run("test_faostat", "run_3")
        
        silver_df = spark.table(tables["silver"])
        # Should still be 2 rows (004, 016)
        assert silver_df.count() == 2
        row_004 = silver_df.filter(F.col("country_code") == "004").collect()[0]
        assert float(row_004["value"]) == 999.9, "Value should be updated to 999.9"

