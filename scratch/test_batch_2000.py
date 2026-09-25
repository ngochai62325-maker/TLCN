import sys, time, pandas as pd
sys.path.insert(0, 'src')
from ingestion.storage.bronze_writer import BronzeIcebergWriter

writer = BronzeIcebergWriter()
df = pd.read_csv('data/raw/faostat/production_world.csv', encoding='utf-8-sig')
print(f"Total rows in dataset: {len(df)}")

writer.execute_query('DROP TABLE IF EXISTS iceberg.bronze.test_faostat_full')
t0 = time.time()
inserted = writer.write_chunk('test_faostat_full', df, 'r1', 'b1', 'sha1', batch_size=1500)
t1 = time.time()
print(f"write_chunk inserted {inserted} rows in {t1 - t0:.2f}s")
cnt = writer.get_row_count('test_faostat_full')
print(f"Table row count: {cnt}")
writer.execute_query('DROP TABLE IF EXISTS iceberg.bronze.test_faostat_full')
