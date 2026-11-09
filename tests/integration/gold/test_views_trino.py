"""Execute all four generated SELECT bodies on bounded Trino VALUES fixtures.

No CREATE VIEW/table or Iceberg data writes. Fixtures do not certify physical
full-data acceptance or publication.
"""
import os
import unittest
from decimal import Decimal
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
from gold.spec import load_spec
from gold.views import definitions, trino, trino_result, expected_schema


@unittest.skipUnless(os.getenv("GOLD_TRINO_TEST") == "1", "Requires explicit live read-only Trino fixtures")
class Views(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sqls = definitions({n: {"snapshot_id": "1"} for n in load_spec()})
        cls.sqls = {n: sql for n, sql in sqls.items()}
        for n in load_spec():
            cls.sqls = {v: sql.replace(f"iceberg.gold_local.{n} FOR VERSION AS OF 1", n) for v, sql in cls.sqls.items()}
        cls.ctes = """WITH
        dim_observation_type(observation_type_key,source_metric_id) AS (VALUES
          ('a','5312'),('p','5510'),('y','5412'),('loss','5016'),('r','5166'),
          ('iq','5610'),('iv','5622'),('eq','5910'),('ev','5922')),
        dim_source_geography(geography_key,source_labels_json,source_level,interpretation_status) AS
          (VALUES ('g1','[]','province','candidate')),
        fact_nso_observation(observation_key,geography_key,source_table,value) AS
          (VALUES ('n1','g1','V06.12',DECIMAL '2500.00'),('n2','g1','V06.13',CAST(NULL AS DECIMAL(22,2)))),
        fact_fao_production_observation(year_key,geography_key,product_key,observation_type_key,value,observation_key) AS
          (VALUES (2024,'g1','rice','a',DECIMAL '10.00','a1'),(2024,'g1','rice','p',DECIMAL '20.00','p1'),
          (2024,'g1','rice','y',CAST(NULL AS DECIMAL(22,2)),'y1'),
          (2024,'g2','rice','a',DECIMAL '30.00','a2'),(2024,'g2','rice','p',DECIMAL '30.00','p2'),
          (2024,'g3','rice','a',CAST(NULL AS DECIMAL(22,2)),'a3'),(2024,'g3','rice','p',DECIMAL '100.00','p3')),
        fact_fao_sua_observation(year_key,geography_key,product_key,observation_type_key,value,observation_key) AS
          (VALUES (2024,'g1','rice','loss',CAST(NULL AS DECIMAL(22,2)),'loss1'),(2024,'g1','rice','r',DECIMAL '-7.00','r1')),
        fact_fao_trade_observation(year_key,reporter_geography_key,partner_geography_key,product_key,observation_type_key,value,observation_key) AS
          (VALUES (2024,'g1','g2','rice','iq',DECIMAL '2.00','iq1'),(2024,'g1','g2','rice','iv',DECIMAL '1000.00','iv1'),
          (2024,'g1','g2','rice','eq',DECIMAL '0.00','eq1'),(2024,'g1','g2','rice','ev',DECIMAL '200.00','ev1'))
        """

    def query(self, name, selection="count(*)", suffix=""):
        return trino(self.ctes + f"SELECT {selection} FROM ({self.sqls[name]}) v " + suffix)

    def test_nso_publications_and_missing_survive(self):
        self.assertEqual(self.query("vw_nso_source_observations"), [[2]])
        self.assertEqual(self.query("vw_nso_source_observations", "count_if(value IS NULL)"), [[1]])

    def test_paired_yield_and_weighted_coverage(self):
        self.assertEqual(self.query("vw_fao_production_yield"), [[3]])
        self.assertEqual(self.query("vw_fao_production_yield", "computed_yield_kg_ha", "WHERE geography_key='g3'"), [[None]])
        actual = self.query("vw_fao_production_yield", "1000.0 * sum(production_t)/sum(area_ha)", "WHERE area_ha > 0 AND production_t IS NOT NULL")[0][0]
        self.assertEqual(Decimal(str(actual)), Decimal("1250"))
        self.assertEqual(self.query("vw_fao_production_yield", "present_5412,observation_5412,reported_yield_kg_ha", "WHERE geography_key='g1'"), [[1, "y1", None]])

    def test_sua_present_null_absent_signed(self):
        actual = self.query("vw_sua_supported_balance", "present_5016,loss_t,observation_5016,present_5023,processed_t,observation_5023,residuals_t")[0]
        self.assertEqual(actual[:-1], [1, None, "loss1", 0, None, None])
        self.assertEqual(Decimal(actual[-1]), Decimal("-7.00"))

    def test_trade_paired_unit_value_zero_denominator(self):
        actual = self.query("vw_trade_reported_unit_values", "reported_import_unit_value_usd_t,reported_export_unit_value_usd_t")[0]
        self.assertEqual(Decimal(actual[0]), Decimal("500"))
        self.assertIsNone(actual[1])

    def test_exact_output_types_for_canonical_gold_schemas(self):
        specs = load_spec()
        typed_ctes = []
        for name, spec in specs.items():
            def dtype(col):
                return "varchar" if col.dbml_type.startswith("varchar") else "timestamp(6) with time zone" if col.dbml_type == "timestamptz" else col.dbml_type
            typed_ctes.append(name + " AS (SELECT " + ",".join(f"CAST(NULL AS {dtype(c)}) AS {c.name}" for c in spec.columns) + ")")
        for name, sql in self.sqls.items():
            actual = trino_result("WITH " + ",".join(typed_ctes) + f" SELECT * FROM ({sql}) q LIMIT 0")["columns"]
            self.assertEqual(actual, expected_schema(name), name)

    def test_single_plan_unpivot_preserves_null_absent_signed_cells(self):
        from gold.rules import SUA
        sql = self.sqls['vw_sua_supported_balance']
        cells = ','.join(f'ROW(v.observation_{c},v.{m},v.present_{c})' for c,m in SUA.items())
        unpivot = f'SELECT u.observation_key,u.value FROM ({sql}) v CROSS JOIN UNNEST(ARRAY[{cells}]) AS u(observation_key,value,presence) WHERE u.presence=1'
        forward = trino(self.ctes + f'SELECT count(*) FROM (({unpivot}) EXCEPT ALL (SELECT observation_key,value FROM fact_fao_sua_observation))')
        reverse = trino(self.ctes + f'SELECT count(*) FROM ((SELECT observation_key,value FROM fact_fao_sua_observation) EXCEPT ALL ({unpivot}))')
        self.assertEqual(forward, [[0]])
        self.assertEqual(reverse, [[0]])

    def trade_fixture(self, duplicate=False):
        from gold.keys import key
        reporters = [key('fixture-reporter',[i]) for i in range(4)]
        # Opposite reporter/partner roles, products, skew and many observations
        # per group; values include present NULL, absent, zero and signed cents.
        rows = [(2017,reporters[0],reporters[1],'rice','iq','2.01','q1'),
                (2017,reporters[0],reporters[1],'rice','iv','1000.99','v1'),
                (2017,reporters[0],reporters[1],'rice','eq','0.00','q0'),
                (2017,reporters[0],reporters[1],'rice','ev','12.00','v0'),
                (2017,reporters[1],reporters[0],'rice','iv','-7.13','signed'),
                (2017,reporters[2],reporters[0],'paddy','iq',None,'null-q'),
                (2017,reporters[3],reporters[0],'rice','ev',None,'null-v'),
                (2018,reporters[0],reporters[1],'rice','iq','999999999999.99','precise-q'),
                (2018,reporters[0],reporters[1],'rice','iv','9999999999999.99','precise-v')]
        if duplicate:
            rows.append(rows[0])
        values = ','.join('('+','.join([str(y),*("'"+s+"'" for s in (r,p,prod,metric)),
            "CAST("+("NULL" if value is None else "'"+value+"'")+" AS DECIMAL(22,2))","'"+obs+"'"])+')'
            for y,r,p,prod,metric,value,obs in rows)
        ctes = self.ctes.split('fact_fao_trade_observation(')[0] + 'fact_fao_trade_observation(year_key,reporter_geography_key,partner_geography_key,product_key,observation_type_key,value,observation_key) AS (VALUES '+values+') '
        return ctes, rows

    def fixture_partition_sql(self, year, bucket):
        from gold.trade_validation import partition_sql
        sql = partition_sql({n:{'snapshot_id':'1'} for n in load_spec()},year,bucket)
        for n in load_spec():
            sql = sql.replace(f'iceberg.gold_local.{n} FOR VERSION AS OF 1',n)
        return sql

    def test_trade_partitions_exact_equivalent_null_zero_signed_decimal_roles(self):
        from collections import Counter
        from gold.trade_validation import bucket,check_result
        ctes, rows = self.trade_fixture()
        original = trino(ctes + 'SELECT * FROM ('+self.sqls['vw_trade_reported_unit_values']+')')
        combined=[]
        for year in (2017,2018):
            for part in range(4):
                expected=sum(y==year and bucket(r)==part for y,r,*_ in rows)
                actual=trino(ctes+'SELECT * FROM ('+self.fixture_partition_sql(year,part)+')')
                check_result(actual,{'input_rows':expected})
                from gold.trade_validation import predicate
                combined.extend(trino(ctes+'SELECT * FROM ('+self.sqls['vw_trade_reported_unit_values']+') q WHERE '+predicate(year,part,'q')))
        self.assertEqual(Counter(map(tuple,combined)),Counter(map(tuple,original)))

    def test_trade_duplicate_metric_multiplicity_detected(self):
        from gold.trade_validation import bucket,check_result
        ctes, rows=self.trade_fixture(duplicate=True)
        part=bucket(rows[0][1]); expected=sum(y==2017 and bucket(r)==part for y,r,*_ in rows)
        actual=trino(ctes+'SELECT * FROM ('+self.fixture_partition_sql(2017,part)+')')
        with self.assertRaises(ValueError): check_result(actual,{'input_rows':expected})

    def test_trade_wrong_value_or_missing_group_detected(self):
        from gold.trade_validation import bucket,check_result
        ctes, rows=self.trade_fixture(); part=bucket(rows[0][1])
        expected=sum(y==2017 and bucket(r)==part for y,r,*_ in rows)
        original=self.sqls['vw_trade_reported_unit_values']
        for altered in (original.replace('THEN f.value END','THEN f.value+DECIMAL \'0.01\' END'),
                        'SELECT * FROM ('+original+') z WHERE import_quantity_t IS NULL'):
            with patch('gold.views.definitions',return_value={'vw_trade_reported_unit_values':altered}):
                query=self.fixture_partition_sql(2017,part)
            actual=trino(ctes+'SELECT * FROM ('+query+')')
            with self.assertRaises(ValueError): check_result(actual,{'input_rows':expected})


if __name__ == "__main__":
    unittest.main(verbosity=2)
