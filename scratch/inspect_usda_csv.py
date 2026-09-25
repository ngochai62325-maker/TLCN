import sys
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

fpath = "data/raw/usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv"
with open(fpath, "rb") as fp:
    head = fp.read(10)
    has_bom = head.startswith(b"\xef\xbb\xbf")

df = pd.read_csv(fpath)
print(f"File: {fpath}")
print(f"Has BOM: {has_bom}")
print(f"Shape: {df.shape}")
print(f"Columns ({len(df.columns)}): {list(df.columns)}")
print(f"Dtypes:\n{df.dtypes}")
print(f"Null counts:\n{df.isnull().sum()}")
print(f"First 2 rows:\n{df.head(2).to_dict(orient='records')}")
print(f"Last 2 rows:\n{df.tail(2).to_dict(orient='records')}")
print(f"Unique Commodities: {df['COMMODITY_DESCRIPTION'].unique() if 'COMMODITY_DESCRIPTION' in df else None}")
print(f"Unique Locations: {df['LOCATION_DESCRIPTION'].unique() if 'LOCATION_DESCRIPTION' in df else None}")
print(f"Year range: {df['YEAR'].min()} to {df['YEAR'].max() if 'YEAR' in df else None}")
