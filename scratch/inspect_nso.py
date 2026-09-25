import os
import glob
import sys
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

nso_dir = "data/raw/nso"
csv_files = sorted(glob.glob(os.path.join(nso_dir, "V06.*.csv")))

print(f"Total CSV files found: {len(csv_files)}")

for fpath in csv_files:
    fname = os.path.basename(fpath)
    with open(fpath, "rb") as f:
        raw_head = f.read(200)
        has_bom = raw_head.startswith(b"\xef\xbb\xbf")

    for enc in ['latin-1', 'cp1252', 'utf-8']:
        try:
            df = pd.read_csv(fpath, encoding=enc)
            print(f"\nFile: {fname} (Size: {os.path.getsize(fpath)} bytes, Enc: {enc})")
            print(f"  Shape: {df.shape} (rows={len(df)}, cols={len(df.columns)})")
            print(f"  Columns: {list(df.columns)}")
            print(f"  First 2 rows:\n{df.head(2).to_dict(orient='records')}")
            break
        except Exception as e:
            continue
