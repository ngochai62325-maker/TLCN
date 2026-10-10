"""Unit tests for Silver Layer PySpark Transformers."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from silver.transformers.faostat_trade_transformer import FaostatTradeTransformer
from silver.transformers.usda_psd_transformer import UsdaPsdTransformer
from silver.transformers.usda_export_price_transformer import UsdaExportPriceTransformer
from silver.transformers.worldbank_transformer import WorldBankTransformer


@pytest.fixture(scope="session")
def spark():
    """Create local SparkSession for unit testing."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("SilverUnitTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


class TestFaostatTradeTransformer:
    def test_transform_cleaning_and_aggregates(self, spark):
        schema = StructType([
            StructField("reporter_country_code", IntegerType(), True),
            StructField("reporter_country_code_m49_", StringType(), True),
            StructField("reporter_countries", StringType(), True),
            StructField("partner_country_code", IntegerType(), True),
            StructField("partner_country_code_m49_", StringType(), True),
            StructField("partner_countries", StringType(), True),
            StructField("item_code", IntegerType(), True),
            StructField("item_code_cpc_", StringType(), True),
            StructField("item", StringType(), True),
            StructField("element_code", IntegerType(), True),
            StructField("element", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("unit", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("flag", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_run_id", StringType(), True),
            StructField("_ingestion_timestamp", StringType(), True),
        ])

        data = [
            (
                216, "'764", "Thailand",
                231, "'840", "United States of America",
                123, "'23161.02", "Rice, milled",
                5910, "Export quantity",
                2020, "t", 5000.0, "A",
                "test.csv", "run_1", "2026-10-01 00:00:00",
            ),
            (
                5000, "'001", "World",
                231, "'840", "United States of America",
                123, "'23161.02", "Rice, milled",
                5910, "Export quantity",
                2020, "t", -10.0, "A",  # Invalid negative value
                "test.csv", "run_1", "2026-10-01 00:00:00",
            ),
        ]

        raw_df = spark.createDataFrame(data, schema=schema)
        transformer = FaostatTradeTransformer(spark)
        result_df = transformer.transform(raw_df).collect()

        assert len(result_df) == 2
        # Check standard row
        row_th = [r for r in result_df if r.reporter_country_name == "Thailand"][0]
        assert row_th.reporter_country_code == "764"  # stripped apostrophe
        assert row_th.is_reporter_aggregate is False
        assert row_th.partner_country_code == "840"
        assert row_th.commodity_code == "23161.02"
        assert row_th.value == 5000.0

        # Check aggregate row
        row_world = [r for r in result_df if r.reporter_country_name == "World"][0]
        assert row_world.is_reporter_aggregate is True
        assert row_world.value is None  # negative value converted to null


class TestUsdaPsdTransformer:
    def test_unpivot_and_metadata_preservation(self, spark):
        schema = StructType([
            StructField("commodity", StringType(), True),
            StructField("attribute", StringType(), True),
            StructField("country", StringType(), True),
            StructField("col_2020_2021", DoubleType(), True),
            StructField("col_2021_2022", DoubleType(), True),
            StructField("unit_description", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_run_id", StringType(), True),
            StructField("_ingestion_timestamp", StringType(), True),
        ])

        data = [
            (
                "", "Production", "Vietnam",
                27000.0, 27500.0, "(1000 MT)",
                "usda_psd.csv", "run_psd", "2026-10-01 00:00:00",
            ),
        ]

        raw_df = spark.createDataFrame(data, schema=schema)
        transformer = UsdaPsdTransformer(spark)
        result_df = transformer.transform(raw_df).collect()

        assert len(result_df) == 2
        # Verify unpivoted rows
        row_2020 = [r for r in result_df if r.market_year == 2020][0]
        assert row_2020.crop_year == "2020/2021"
        assert row_2020.commodity == "Rice, Milled"  # filled default
        assert row_2020.value == 27000.0
        assert row_2020.unit == "1000 MT"  # stripped parentheses


class TestWorldBankTransformer:
    def test_worldbank_unpivot_and_date_parsing(self, spark):
        schema = StructType([
            StructField("period", StringType(), True),
            StructField("rice_thai_5_mt_", StringType(), True),
            StructField("crude_oil_brent_bbl_", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_run_id", StringType(), True),
            StructField("_ingestion_timestamp", StringType(), True),
        ])

        data = [
            ("2024M08", "580.5", "80.2", "wb.csv", "run_wb", "2026-10-01 00:00:00"),
            ("2024M07", "…", "82.5", "wb.csv", "run_wb", "2026-10-01 00:00:00"),
        ]

        raw_df = spark.createDataFrame(data, schema=schema)
        transformer = WorldBankTransformer(spark)
        result_df = transformer.transform(raw_df).collect()

        # 2 periods * 2 commodities = 4 rows
        assert len(result_df) == 4

        row_rice = [r for r in result_df if r.commodity_code == "rice_thai_5" and r.period_code == "2024M08"][0]
        assert str(row_rice.period_date) == "2024-08-01"
        assert row_rice.year == 2024
        assert row_rice.month == 8
        assert row_rice.price == 580.5
        assert row_rice.unit == "USD/mt"

        # Check missing token handling
        row_missing = [r for r in result_df if r.commodity_code == "rice_thai_5" and r.period_code == "2024M07"][0]
        assert row_missing.price is None
