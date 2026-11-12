import json
import subprocess
from decimal import Decimal
from pathlib import Path
import pandas as pd

bundle = Path('exports/powerbi/ec421297c194_20261106T031815Z_01d03710')

def query_trino(sql):
    cmd = ['docker', 'exec', '-i', 'tlcn-local-trino-1', 'trino', '--output-format', 'ALIGNED', '--execute', sql]
    res = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    lines = [line.strip() for line in res.stdout.strip().split('\n') if line.strip() and not line.startswith('WARNING:')]
    return lines

print("=== Comparing 20 Random Rows from Each Fact CSV with Trino ===")

# 1. vn_quote.csv
df_vn = pd.read_csv(bundle / 'vn_quote.csv')
sample_vn = df_vn.dropna(subset=['price_vnd_kg']).sample(20, random_state=42)
vn_matched = 0
for _, r in sample_vn.iterrows():
    d_key = int(r['date'].replace('-', ''))
    s_key = r['series_key']
    val = float(r['price_vnd_kg'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_vn_market_quote_daily WHERE date_key = {d_key} AND price_series_key = '{s_key}';"
    out = query_trino(sql)
    # Output format is aligned table
    # header | sep | row
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - val) < 1e-2:
                vn_matched += 1
        except Exception:
            pass
print(f"1. vn_quote.csv: {vn_matched}/20 matched")

# 2. fao_production.csv
df_fao = pd.read_csv(bundle / 'fao_production.csv')
sample_fao = df_fao.dropna(subset=['production_t', 'area_ha']).sample(20, random_state=42)
fao_matched = 0
for _, r in sample_fao.iterrows():
    y = int(r['year'])
    g_key = r['geography_key']
    p_key = r['product_key']
    prod = float(r['production_t'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_fao_production_observation WHERE year_key = {y} AND geography_key = '{g_key}' AND product_key = '{p_key}' AND observation_type_key IN (SELECT observation_type_key FROM iceberg.gold_local.dim_observation_type WHERE source_metric_id = '5510');"
    out = query_trino(sql)
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - prod) < 1e-2:
                fao_matched += 1
        except Exception:
            pass
print(f"2. fao_production.csv: {fao_matched}/20 matched")

# 3. sua_components.csv
df_sua = pd.read_csv(bundle / 'sua_components.csv')
sample_sua = df_sua.dropna(subset=['value_t']).sample(20, random_state=42)
sua_matched = 0
for _, r in sample_sua.iterrows():
    y = int(r['year'])
    g_key = r['geography_key']
    p_key = r['product_key']
    elem = str(r['element_code'])
    val = float(r['value_t'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_fao_sua_observation WHERE year_key = {y} AND geography_key = '{g_key}' AND product_key = '{p_key}' AND observation_type_key IN (SELECT observation_type_key FROM iceberg.gold_local.dim_observation_type WHERE source_metric_id = '{elem}');"
    out = query_trino(sql)
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - val) < 1e-2:
                sua_matched += 1
        except Exception:
            pass
print(f"3. sua_components.csv: {sua_matched}/20 matched")

# 4. usda_psd.csv
df_psd = pd.read_csv(bundle / 'usda_psd.csv')
sample_psd = df_psd.dropna(subset=['value']).sample(20, random_state=42)
psd_matched = 0
for _, r in sample_psd.iterrows():
    my = int(r['market_year'])
    g_key = r['geography_key']
    p_key = r['product_key']
    attr = r['attribute']
    val = float(r['value'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_usda_psd_observation WHERE market_year = {my} AND geography_key = '{g_key}' AND product_key = '{p_key}' AND observation_type_key IN (SELECT observation_type_key FROM iceberg.gold_local.dim_observation_type WHERE source_metric_name = '{attr}');"
    out = query_trino(sql)
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - val) < 1e-2:
                psd_matched += 1
        except Exception:
            pass
print(f"4. usda_psd.csv: {psd_matched}/20 matched")

# 5. nso_national_annual.csv (only 35 rows total in this file)
df_nso = pd.read_csv(bundle / 'nso_national_annual.csv')
sample_nso = df_nso.dropna(subset=['production_t']).sample(20, random_state=42)
nso_matched = 0
for _, r in sample_nso.iterrows():
    y = int(r['year'])
    val = float(r['production_t'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_nso_observation WHERE year_key = {y} AND source_level = 'national' AND observation_type_key IN (SELECT observation_type_key FROM iceberg.gold_local.dim_observation_type WHERE source_metric_name = 'production' AND season = 'annual');"
    out = query_trino(sql)
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - val) < 1e-2:
                nso_matched += 1
        except Exception:
            pass
print(f"5. nso_national_annual.csv: {nso_matched}/20 matched")

# 6. trade_exports.csv (only 35 rows total in this file)
df_trade = pd.read_csv(bundle / 'trade_exports.csv')
sample_trade = df_trade.dropna(subset=['quantity_t', 'value_usd']).sample(20, random_state=42)
trade_matched = 0
for _, r in sample_trade.iterrows():
    y = int(r['year'])
    r_key = r['reporter_key']
    p_key = r['partner_key']
    prod_key = r['product_key']
    q = float(r['quantity_t'])
    v = float(r['value_usd'])
    sql = f"SELECT value FROM iceberg.gold_local.fact_fao_trade_observation WHERE year_key = {y} AND reporter_geography_key = '{r_key}' AND partner_geography_key = '{p_key}' AND product_key = '{prod_key}' AND observation_type_key IN (SELECT observation_type_key FROM iceberg.gold_local.dim_observation_type WHERE source_metric_id = '5910');"
    out = query_trino(sql)
    data_lines = [l for l in out if not l.startswith('value') and not l.startswith('---') and not l.startswith('(')]
    if data_lines:
        try:
            trino_val = float(data_lines[0].split()[0])
            if abs(trino_val - q) < 1e-2:
                trade_matched += 1
        except Exception:
            pass
print(f"6. trade_exports.csv: {trade_matched}/20 matched")
