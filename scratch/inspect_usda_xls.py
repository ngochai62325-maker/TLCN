import sys
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

fpath = "data/raw/usda/usda.xls"

with open(fpath, "r", encoding="utf-8", errors="replace") as fp:
    html_content = fp.read()

print(f"Total HTML chars: {len(html_content):,}")
print("HTML first 300 chars:")
print(html_content[:300])

# Parse tables with pandas
try:
    tables = pd.read_html(fpath, encoding="utf-8")
    print(f"\nTotal tables found by pd.read_html: {len(tables)}")
    for idx, tbl in enumerate(tables):
        print(f"\n--- TABLE {idx} ---")
        print(f"  Shape: {tbl.shape}")
        print(f"  Columns: {list(tbl.columns)[:8]}")
        print(f"  Head 3:\n{tbl.head(3)}")
except Exception as e:
    print(f"pd.read_html failed: {e}")
