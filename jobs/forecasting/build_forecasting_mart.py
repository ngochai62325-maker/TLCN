import argparse
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV = ROOT / "exports" / "forecast" / "gold_prices_monthly.csv"
OUT_DIR = ROOT / "exports" / "forecast"
TARGET_START = 201001
TRAIN_END = 202212
LAGS = (1, 2, 3, 6, 12)
WINDOWS = (3, 6, 12)

MART_COLUMNS = [
    "month_key", "split", "y", "target_delta",
    *(f"y_lag_{k}" for k in LAGS),
    *(f"y_roll_mean_{w}" for w in WINDOWS),
    *(f"y_roll_std_{w}" for w in WINDOWS),
    "month_of_year", "urea_lag_1", "crude_oil_lag_1",
    "thai5_lag_1", "india_parb_lag_1", "pak5_lag_1",
    "india_white_quote_missing_lag_1", "india_parb_missing_lag_1", "pak5_missing_lag_1",
    "spread_vn_thai_lag_1", "spread_vn_india_lag_1",
]


def warmup_target(raw):
    df = raw.sort_values("month_key").set_index("month_key")
    assert len(df) == 204 and df.index[0] == 200901 and df.index[-1] == 202512
    y_w = df["wb_vn5_usd_t"].copy()
    assert y_w.loc[TARGET_START:].notna().all() and list(y_w[y_w.isna()].index) == [200903]
    y_w.loc[200903] = (y_w.loc[200902] + y_w.loc[200904]) / 2
    return df, y_w


def build_mart(raw):
    df, y_w = warmup_target(raw)
    mart = pd.DataFrame(index=df.index)
    mart["y"] = df["wb_vn5_usd_t"]
    for k in LAGS:
        mart[f"y_lag_{k}"] = y_w.shift(k)
    mart["target_delta"] = mart["y"] - mart["y_lag_1"]
    for w in WINDOWS:
        mart[f"y_roll_mean_{w}"] = y_w.shift(1).rolling(w).mean()
        mart[f"y_roll_std_{w}"] = y_w.shift(1).rolling(w).std()
    mart["month_of_year"] = mart.index % 100
    mart["urea_lag_1"] = df["wb_urea_usd_t"].shift(1)
    mart["crude_oil_lag_1"] = df["wb_crude_oil_usd_bbl"].shift(1)
    mart["thai5_lag_1"] = df["usda_thai5_usd_t"].shift(1)
    mart["india_parb_lag_1"] = df["usda_india_parb5_usd_t"].shift(1)
    mart["pak5_lag_1"] = df["usda_pak5_usd_t"].shift(1)
    mart["india_white_quote_missing_lag_1"] = df["usda_india_broken5_usd_t"].shift(1).isna().astype(int)
    mart["india_parb_missing_lag_1"] = df["usda_india_parb5_usd_t"].shift(1).isna().astype(int)
    mart["pak5_missing_lag_1"] = df["usda_pak5_usd_t"].shift(1).isna().astype(int)
    mart["spread_vn_thai_lag_1"] = mart["y_lag_1"] - mart["thai5_lag_1"]
    mart["spread_vn_india_lag_1"] = mart["y_lag_1"] - mart["india_parb_lag_1"]
    mart = mart.loc[TARGET_START:].reset_index()
    mart["split"] = mart["month_key"].map(lambda m: "train" if m <= TRAIN_END else "test")
    return mart[MART_COLUMNS]


def _same_val(a, b, tol=1e-8):
    return (pd.isna(a) and pd.isna(b)) or (pd.notna(a) and pd.notna(b) and abs(float(a) - float(b)) <= tol)


def check_dq19(raw, mart):
    df, y_w = warmup_target(raw)
    assert list(mart.columns) == MART_COLUMNS and len(mart) == 192
    assert (mart["split"] == "train").sum() == 156 and (mart["split"] == "test").sum() == 36
    assert (mart.loc[mart["split"] == "train", "month_key"] <= TRAIN_END).all()
    assert (mart.loc[mart["split"] == "test", "month_key"] > TRAIN_END).all()
    no_nan_cols = [c for c in MART_COLUMNS if c not in ("india_parb_lag_1", "pak5_lag_1", "spread_vn_india_lag_1")]
    assert mart[no_nan_cols].notna().all().all()
    assert (mart["india_parb_lag_1"].isna().astype(int) == mart["india_parb_missing_lag_1"]).all()
    assert (mart["pak5_lag_1"].isna().astype(int) == mart["pak5_missing_lag_1"]).all()

    for _, row in mart.iterrows():
        mk = int(row["month_key"])
        past = df.loc[df.index < mk]
        past_y = y_w.loc[y_w.index < mk]
        prev = past.iloc[-1]
        assert _same_val(row["y"], df.loc[mk, "wb_vn5_usd_t"])
        assert _same_val(row["target_delta"], df.loc[mk, "wb_vn5_usd_t"] - prev["wb_vn5_usd_t"])
        for k in LAGS:
            assert _same_val(row[f"y_lag_{k}"], past_y.iloc[-k])
        for w in WINDOWS:
            assert _same_val(row[f"y_roll_mean_{w}"], past_y.iloc[-w:].mean())
            assert _same_val(row[f"y_roll_std_{w}"], past_y.iloc[-w:].std())
        for m_col, r_col in [("urea_lag_1", "wb_urea_usd_t"), ("crude_oil_lag_1", "wb_crude_oil_usd_bbl"), ("thai5_lag_1", "usda_thai5_usd_t"), ("india_parb_lag_1", "usda_india_parb5_usd_t"), ("pak5_lag_1", "usda_pak5_usd_t")]:
            assert _same_val(row[m_col], prev[r_col])
        assert _same_val(row["spread_vn_thai_lag_1"], past_y.iloc[-1] - prev["usda_thai5_usd_t"])
        assert _same_val(row["spread_vn_india_lag_1"], past_y.iloc[-1] - prev["usda_india_parb5_usd_t"])

    return {
        "dq19_status": "PASS",
        "leakage_errors": 0,
        "rows": len(mart),
        "train_rows": int((mart["split"] == "train").sum()),
        "test_rows": int((mart["split"] == "test").sum()),
        "null_counts": {c: int(mart[c].isna().sum()) for c in ("india_parb_lag_1", "pak5_lag_1", "spread_vn_india_lag_1")},
        "missing_flag_counts": {c: int(mart[c].sum()) for c in ("india_white_quote_missing_lag_1", "india_parb_missing_lag_1", "pak5_missing_lag_1")},
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)

    raw = pd.read_csv(args.input_csv)
    mart_path = args.output_dir / "forecasting_mart.csv"
    report_path = args.output_dir / "dq19_mart_report.json"

    if args.verify_only:
        mart = pd.read_csv(mart_path)
    else:
        mart = build_mart(raw)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        mart.to_csv(mart_path, index=False)

    report = check_dq19(raw, mart)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
