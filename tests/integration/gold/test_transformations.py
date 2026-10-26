"""Small real Spark fixtures, no Iceberg/catalog writes and no extra dependencies.

Run in the existing runner: GOLD_SPARK_TEST=1 spark-submit this_file.py
These tests intentionally do not claim physical Gold E2E acceptance.
"""
import os
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))


@unittest.skipUnless(os.getenv("GOLD_SPARK_TEST") == "1", "Requires explicit existing-runner Spark fixtures")
class Transformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, types as T
        from silver.contract.loader import SilverContractLoader
        from gold.dimensions import build_dimensions
        from gold.facts import transform
        from gold.spec import load_spec
        from gold.rules import PRODUCTION, TRADE, PSD, SUA
        from gold import BUILD_ORDER
        cls.spark = (SparkSession.builder.master("local[1]").appName("GoldSyntheticAcceptance")
                     .config("spark.sql.defaultCatalog", "spark_catalog")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        loader = SilverContractLoader(str(ROOT / "contracts/silver"))
        cls.contracts = {s: loader.load_contract(s) for s in BUILD_ORDER}
        cls.specs = load_spec()
        cls.inputs = {}
        warning = T.ArrayType(T.StructType([T.StructField(c, T.StringType()) for c in ("rule_id", "error_message", "failed_column")]))
        for source, contract in cls.contracts.items():
            schema = T.StructType([T.StructField(c["name"], T._parse_datatype_string(c["data_type"]), True) for c in contract.columns] + [T.StructField(c, dtype, False) for c, dtype in [("_ingestion_run_id", T.StringType()), ("_ingestion_batch_id", T.StringType()), ("_ingestion_chunk_id", T.LongType()), ("_ingestion_timestamp", T.TimestampType()), ("_source_id", T.StringType()), ("_source_file", T.StringType()), ("_source_checksum", T.StringType()), ("_source_snapshot_id", T.LongType()), ("_silver_processed_at", T.TimestampType()), ("_dq_warnings", warning)]])
            base = {}
            for field in schema:
                dtype = field.dataType
                base[field.name] = ("source_" + field.name if isinstance(dtype, T.StringType) else 2024 if isinstance(dtype, (T.IntegerType, T.LongType)) else Decimal("2.50000000") if isinstance(dtype, T.DecimalType) else date(2024, 2, 29) if isinstance(dtype, T.DateType) else datetime(2024, 3, 1) if isinstance(dtype, T.TimestampType) else [])
            base.update(_source_id=source, _source_checksum="a" * 64, _source_snapshot_id=0, missing_reason=None, year=2024, month=2, country_code="704", country_name="Vietnam", commodity_code="23161.02", commodity_name="Rice", unit="tonne")
            variants = [{}]
            if source in {"faostat_production", "faostat_trade", "faostat_supply_utilization"}:
                rules = PRODUCTION if source == "faostat_production" else TRADE if source == "faostat_trade" else {c: "tonne" for c in SUA}
                variants = [{"element_code": c, "unit": u, "element": c, "element_name": c, "reporter_country_code": "704", "partner_country_code": "156", "value": Decimal("-2.50") if c in {"5071", "5166"} else Decimal("0") if c == "5312" else None if c == "5412" else Decimal("2.50")} for c, u in rules.items()]
            elif source == "usda_psd":
                variants = [{"attribute": a, "unit": u, "country": "Vietnam", "commodity": "Rice, Milled", "market_year": 2024, "crop_year": "2024/2025", "value": Decimal("0.6250") if u == "fraction" else Decimal("2.5000")} for a, u in PSD.items()]
            elif source == "worldbank_pinksheet":
                base.update(commodity_code="rice_vietnam_5", period_date=date(2024, 2, 1), unit="USD/tonne", price=Decimal("9999999999999999.99"), series_name=None)
            elif source == "faostat_monthly_price":
                variants = [{"element_code": "5532", "element": "Producer Price (USD/tonne)", "unit": "USD", "currency": "USD", "time_grain": "monthly", "month_code": "7002", "month_name": "February", "price_kind": "producer_price", "value": Decimal("2.5000")}, {"element_code": "5532", "element": "Producer Price (USD/tonne)", "unit": "USD", "currency": "USD", "time_grain": "annual", "month_code": "7021", "month_name": "Annual value", "month": None, "price_kind": "producer_price", "value": None}, {"element_code": "5539", "element": "Producer Price Index (2014-2016 = 100)", "unit": None, "currency": None, "time_grain": "monthly", "month_code": "7002", "month_name": "February", "price_kind": "producer_price_index", "value": Decimal("0.0000")}]
            elif source == "nso":
                base.update(source_geography_key="raw-identity", geography_raw="Tỉnh nguồn", geography_name_candidate="Candidate", geography_level="province", geography_status="candidate", measure="area", season="annual", statistic_kind="quantity", unit="hectare", conversion_factor=Decimal(1000), bronze_read_snapshot_id=12, value=Decimal("2500.00000000"), value_source=Decimal("2.50000000"), diagnostic_evidence='{"source":  1}')
                variants = [{"source_table": "V06.12"}, {"source_table": "V06.13"}]
            elif source == "thitruongnongsan":
                base.update(market="Chợ nguồn", commodity="Lúa", price_type="Wholesale", unit="VND/kg", currency="VND", date=date(2024, 2, 29), price_value=Decimal("0.00"))
                variants = [{"source": "provider one"}, {"source": "provider two"}]
            elif source == "usda_rice_yearbook":
                base.update(location="VIETNAM", commodity_class="5-PERCENT BROKEN", commodity="RICE", statistic_type="FREIGHT ON BOARD PRICE", unit="USD/tonne")
                variants = [{"reference_period": "MARKETING"}, {"reference_period": "February"}, {"reference_period": "YEAR"}]
            rows = [{f.name: {**base, **v}.get(f.name) for f in schema} for v in variants]
            cls.inputs[source] = cls.spark.createDataFrame(rows, schema).cache()
        cls.dimensions = {n: cls.specs[n].project(df).cache() for n, df in build_dimensions(cls.spark, cls.inputs)}
        cls.facts = {}
        cls.pins = {s: {"uuid": "11111111-2222-3333-4444-555555555555", "snapshot_id": "123"} for s in cls.inputs}
        cls.branches = {}
        for source, df in cls.inputs.items():
            cls.branches[source] = {n: cls.specs[n].project(f) for n, f in transform(source, df, cls.contracts[source].business_key, cls.pins[source]).items()}
            for name, frame in cls.branches[source].items():
                cls.facts[name] = frame if name not in cls.facts else cls.facts[name].unionByName(frame)
        cls.facts = {n: df.cache() for n, df in cls.facts.items()}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_01_all_17_types_keys_and_foreign_keys(self):
        from gold.quality import Quality, validate_table
        dq = Quality(self.pins)
        for name, df in {**self.dimensions, **self.facts}.items():
            with self.subTest(table=name):
                validate_table(name, df, self.specs[name], self.dimensions, dq)
        self.assertTrue(dq.results)

    def test_02_all_nine_source_key_value_lineage_sets(self):
        from gold.quality import Quality, reconcile
        for source in self.inputs:
            with self.subTest(source=source):
                reconcile(source, self.inputs[source], self.branches[source], self.facts, self.contracts[source].business_key, Quality(self.pins))

    def test_03_numeric_semantics_and_source_periods(self):
        monthly = self.facts["fact_price_monthly"].collect()
        wb = next(r for r in monthly if r._source_id == "worldbank_pinksheet")
        self.assertEqual(wb.value, Decimal("9999999999999999.9900"))
        self.assertEqual(self.facts["fact_fao_price_annual"].first().value, None)
        self.assertEqual(self.facts["fact_vn_market_quote_daily"].count(), 2)
        self.assertEqual(self.facts["fact_nso_observation"].count(), 2)
        self.assertEqual(self.facts["fact_nso_observation"].first().value, Decimal("2500.00000000"))
        self.assertEqual(self.facts["fact_usda_price_reference"].count(), 3)
        self.assertNotIn("year_key", self.facts["fact_usda_psd_observation"].columns)
        self.assertNotIn("month_key", self.facts["fact_usda_price_reference"].columns)

    def test_04_label_serialization_and_repartition_keys(self):
        import json
        from gold.keys import spark_key
        from pyspark.sql import functions as F
        geo = self.dimensions["dim_source_geography"].where("identity_namespace='nso_raw_label'").first()
        self.assertEqual({x["role"] for x in json.loads(geo.source_labels_json)}, {"raw", "candidate"})
        p = self.dimensions["dim_source_product"].first()
        self.assertTrue(all(isinstance(v, str) for v in json.loads(p.source_labels_json)))
        df = self.inputs["nso"]
        bk = self.contracts["nso"].business_key
        a = df.select(spark_key("observation", *(F.col(c) for c in bk), nested_source="nso").alias("k"))
        b = df.repartition(2).select(spark_key("observation", *(F.col(c) for c in bk), nested_source="nso").alias("k"))
        self.assertEqual(a.exceptAll(b).count(), 0)

    def test_05_missing_dimension_and_duplicate_grain_fail(self):
        from gold.quality import Quality, validate_table
        name = "fact_nso_observation"
        with self.assertRaises(ValueError):
            validate_table(name, self.facts[name], self.specs[name], {}, Quality(self.pins))
        duplicated = self.facts[name].unionByName(self.facts[name])
        with self.assertRaises(ValueError):
            validate_table(name, duplicated, self.specs[name], self.dimensions, Quality(self.pins))

    def test_06_lookup_fanout_fail(self):
        from gold.dimensions import assert_fd
        df = self.spark.createDataFrame([("same", "a"), ("same", "b")], "identity string, unit string")
        with self.assertRaises(ValueError):
            assert_fd(df, ["identity"], ["unit"], "injected fanout")

    def test_07_shared_monthly_fact_replay_never_removes_fao_branch(self):
        from gold.facts import monthly_snapshot_frame
        business_keys = {s: c.business_key for s, c in self.contracts.items()}
        wb, fao = "worldbank_pinksheet", "faostat_monthly_price"
        initial = monthly_snapshot_frame(wb, self.branches[wb]["fact_price_monthly"], self.inputs, business_keys, self.pins, [])
        self.assertEqual(initial.count(), 1)
        replay = self.specs["fact_price_monthly"].project(monthly_snapshot_frame(wb, self.branches[wb]["fact_price_monthly"], self.inputs, business_keys, self.pins, [fao]))
        self.assertEqual(replay.exceptAll(self.facts["fact_price_monthly"]).count(), 0)
        self.assertEqual(self.facts["fact_price_monthly"].exceptAll(replay).count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
