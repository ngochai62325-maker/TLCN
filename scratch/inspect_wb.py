import sys
import openpyxl
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

wb = openpyxl.load_workbook("data/raw/world_bank/CMO-Historical-Data-Monthly.xlsx", read_only=True, data_only=True)
ws = wb["Monthly Prices"]

rows = list(ws.iter_rows(values_only=True))
wb.close()

print(f"Total rows in 'Monthly Prices': {len(rows)}")
for i in range(10):
    print(f"Row {i}: {rows[i][:8]}")

print("\nLast 5 rows:")
for i in range(len(rows)-5, len(rows)):
    print(f"Row {i}: {rows[i][:6]}")

# Inspect columns in Row 4 and Row 5
commodities = rows[4]
units = rows[5]
print(f"\nTotal columns in Row 4: {len(commodities)}")
rice_cols = [(idx, commodities[idx], units[idx]) for idx in range(len(commodities)) if commodities[idx] and 'rice' in str(commodities[idx]).lower()]
print(f"Rice related columns in World Bank ({len(rice_cols)}):")
for idx, c, u in rice_cols:
    print(f"  Col {idx}: {c} | Unit: {u}")
