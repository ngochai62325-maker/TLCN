import glob
import os
import sys
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

for f in sorted(glob.glob('data/raw/nso/V06.*.csv')):
    fname = os.path.basename(f)
    df = pd.read_csv(f, encoding='latin-1')
    first_col = df.columns[0]
    num_cols = len(df.columns)
    print(f"{fname:<12} | rows={len(df):<3} | cols={num_cols:<2} | first_col='{first_col}' | sample_cols={list(df.columns[:3])} ... {list(df.columns[-2:])}")
