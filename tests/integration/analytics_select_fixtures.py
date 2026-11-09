"""Small Trino VALUES fixtures, no catalog objects or Gold data reads."""
import json
from pathlib import Path
import sys
import unittest
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src'))
from analytics.queries import compile_query
from analytics.transport import execute
from gold.spec import load_spec

A,B,C='a'*64,'b'*64,'c'*64
M={'outputs':{n:{'uuid':'fixture','snapshot_id':'123'} for n in load_spec()}}
CTES=f"""WITH
types(observation_type_key,source_metric_id) AS (VALUES ('area','5312'),('prod','5510'),('yield','5412'),
 ('iq','5610'),('iv','5622'),('eq','5910'),('ev','5922'),('stock','5071')),
geo(geography_key,source_identity,source_labels_json,source_level) AS
 (VALUES ('{A}','reporter','[]','source_area'),('{C}','partner','[]','source_area')),
product(product_key,source_identity,source_labels_json) AS (VALUES ('{B}','rice','[]')),
fao(year_key,geography_key,product_key,observation_type_key,value,observation_key,source_context_json) AS
 (VALUES (2024,'{A}','{B}','area',DECIMAL '10.00','a1','{{"flag":"A"}}'),(2024,'{A}','{B}','prod',DECIMAL '20.00','p1','{{"flag":"A"}}'),
 (2024,'{A}','{B}','yield',CAST(NULL AS DECIMAL(18,2)),'y1','{{"flag":"M"}}')),
sua(year_key,geography_key,product_key,observation_type_key,value,observation_key) AS
 (VALUES (2024,'{A}','{B}','stock',DECIMAL '-2.00','s1')),
trade(year_key,reporter_geography_key,partner_geography_key,product_key,observation_type_key,value,observation_key) AS
 (VALUES (2024,'{A}','{C}','{B}','eq',DECIMAL '10.00','eq1'),(2024,'{A}','{C}','{B}','ev',DECIMAL '0.00','ev1'),
 (2024,'{A}','{C}','{B}','iq',CAST(NULL AS DECIMAL(22,2)),'iq1'),(2024,'{A}','{C}','{B}','iv',DECIMAL '50.00','iv1'))
"""


def run(name,params):
    sql,_=compile_query(name,params,M)
    for table,cte in {'dim_observation_type':'types','dim_source_geography':'geo','dim_source_product':'product',
       'fact_fao_production_observation':'fao','fact_fao_sua_observation':'sua','fact_fao_trade_observation':'trade'}.items():
        sql=sql.replace('iceberg.gold_local.'+table+' FOR VERSION AS OF 123',cte)
    result=execute(CTES+sql)
    return [dict(zip([c['name'] for c in result['columns']],row)) for row in result['rows']]


class Selects(unittest.TestCase):
    def test_paired_yield_and_present_null(self):
        row=run('fao_production',dict(geographies=[A],product=B,start_year=2024,end_year=2024))[0]
        self.assertEqual(Decimal(row['computed_yield_kg_ha']),2000)
        self.assertEqual(row['present_5412'],1)
        self.assertIsNone(row['reported_yield_kg_ha'])
        self.assertEqual(row['production_source_flag'],'A')
        self.assertEqual(row['reported_yield_source_flag'],'M')
    def test_signed_stock_variation(self):
        row=run('sua',dict(geography=A,product=B,start_year=2024,end_year=2024))[0]
        self.assertEqual(Decimal(row['stock_variation_t']),-2)
    def test_export_zero_is_a_real_unit_value(self):
        row=run('trade',dict(reporter=A,product=B,flow='export',start_year=2024,end_year=2024))[0]
        self.assertEqual(Decimal(row['reported_unit_value_usd_t']),0)
        self.assertEqual(Decimal(row['quantity_t']),10)
    def test_import_present_null_quantity(self):
        row=run('trade',dict(reporter=A,product=B,flow='import',start_year=2024,end_year=2024))[0]
        self.assertEqual(row['present_quantity'],1)
        self.assertIsNone(row['quantity_t'])
        self.assertIsNone(row['reported_unit_value_usd_t'])

if __name__=='__main__':unittest.main(verbosity=2)
