import sys, time, requests, json, pandas as pd
sys.path.insert(0, 'src')
from ingestion.storage.bronze_writer import BronzeIcebergWriter, sanitize_column_name, TECHNICAL_METADATA_COLUMNS

writer = BronzeIcebergWriter()
df = pd.read_csv('data/raw/faostat/production_world.csv', nrows=5000, encoding='utf-8-sig')

# Clean columns
rename_map = {col: sanitize_column_name(col) for col in df.columns}
df.rename(columns=rename_map, inplace=True)
df['_ingestion_run_id'] = 'r1'
df['_ingestion_batch_id'] = 'b1'
df['_ingestion_timestamp'] = '2026-09-25 14:00:00.000000 UTC'
df['_source_id'] = 'test_5000'
df['_source_file'] = 'file.csv'
df['_source_checksum'] = 'sha'
df['_source_snapshot_id'] = 0

writer.execute_query('DROP TABLE IF EXISTS iceberg.bronze.test_5000')
writer.ensure_table('test_5000', df)

cols = list(df.columns)
quoted_cols = ', '.join(f'"{c}"' for c in cols)
val_rows = []
for _, row in df.iterrows():
    vals = []
    for col in cols:
        val = row[col]
        if pd.isna(val) or val is None:
            vals.append('NULL')
        elif isinstance(val, (int, float)) and not isinstance(val, bool):
            vals.append(str(val))
        elif isinstance(val, bool):
            vals.append('TRUE' if val else 'FALSE')
        elif col == '_ingestion_timestamp':
            vals.append(f"TIMESTAMP '{val}'")
        else:
            escaped = str(val).replace("'", "''")
            vals.append(f"'{escaped}'")
    val_rows.append(f"({', '.join(vals)})")

insert_sql = f'INSERT INTO iceberg.bronze.test_5000 ({quoted_cols}) VALUES\n' + ',\n'.join(val_rows)
print('SQL length bytes:', len(insert_sql.encode('utf-8')))

headers = {'X-Trino-User': 'admin', 'X-Trino-Catalog': 'iceberg', 'X-Trino-Schema': 'bronze'}
resp = requests.post('http://localhost:8088/v1/statement', headers=headers, data=insert_sql.encode('utf-8'))
res = resp.json()
while True:
    if 'error' in res:
        print('TRINO ERROR FOUND:', json.dumps(res['error'], indent=2))
        break
    if 'nextUri' not in res:
        print('Finished without error! stats:', res.get('stats', {}).get('state'))
        break
    res = requests.get(res['nextUri'], headers=headers).json()
