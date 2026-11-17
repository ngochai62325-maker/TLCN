import argparse
import json
from pathlib import Path
import subprocess
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
TRINO_URL = "http://localhost:18080"
TRINO_USER = "tlcn_forecast"
RUNNER_CONTAINER = "tlcn-local-runner-1"
OUT_DIR = ROOT / "exports" / "forecast"
START_MONTH = 200901
END_MONTH = 202512

WB_SERIES = {
    "worldbank_pinksheet_rice_viet_namese_5": "wb_vn5_usd_t",
    "worldbank_pinksheet_urea": "wb_urea_usd_t",
    "worldbank_pinksheet_crude_oil_average": "wb_crude_oil_usd_bbl",
}

USDA_SERIES = {
    "usda_rice_yearbook_vietnam_5_percent_broken_freight_on_bo_dd22fa": "usda_vn5_usd_t",
    "usda_rice_yearbook_thailand_5_percent_broken_freight_on_b_10e286": "usda_thai5_usd_t",
    "usda_rice_yearbook_india_5_percent_broken_freight_on_boar_3c14de": "usda_india_broken5_usd_t",
    "usda_rice_yearbook_india_5_percent_parboiled_freight_on_b_51dde4": "usda_india_parb5_usd_t",
    "usda_rice_yearbook_pakistan_5_percent_broken_freight_on_b_5d49f7": "usda_pak5_usd_t",
}

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def query_trino(sql, trino_url=TRINO_URL, http=requests):
    resp = http.post(f"{trino_url.rstrip('/')}/v1/statement", data=sql.encode("utf-8"), headers={"X-Trino-User": TRINO_USER}, timeout=30)
    resp.raise_for_status()
    page = resp.json()
    cols, rows = [], []
    while True:
        if "error" in page:
            raise RuntimeError(page["error"].get("message", "Trino error"))
        if page.get("columns"):
            cols = [c["name"] for c in page["columns"]]
        if page.get("data"):
            rows.extend(page["data"])
        if not page.get("nextUri"):
            break
        resp = http.get(page["nextUri"], headers={"X-Trino-User": TRINO_USER}, timeout=30)
        resp.raise_for_status()
        page = resp.json()
    return pd.DataFrame(rows, columns=cols)


def get_snapshot_id(table, trino_url=TRINO_URL, http=requests):
    df = query_trino(f'SELECT snapshot_id FROM iceberg.gold_local."{table}$snapshots" ORDER BY committed_at DESC LIMIT 1', trino_url, http)
    return str(int(df.iloc[0]["snapshot_id"]))


def get_gold_batch(snapshots, container=RUNNER_CONTAINER, runner=subprocess.run):
    code = "import json,sys; sys.path.insert(0,'/workspace/src'); from gold.publication import Publication; p=Publication(); ptr=p.get('published.json'); print(json.dumps(p.get(ptr['batch']+'.json')))"
    res = runner(["docker", "exec", "-i", container, "python3", "-c", code], capture_output=True, text=True, check=True, timeout=20)
    manifest = json.loads(res.stdout)
    assert manifest["publication_status"] == "PUBLISHED" and manifest["dq"] == "ACCEPT"
    for tbl, sid in snapshots.items():
        assert str(manifest["outputs"][tbl]["snapshot_id"]) == sid
    return manifest["batch"]


def build_spine(start_month=START_MONTH, end_month=END_MONTH):
    sy, sm = divmod(start_month, 100)
    ey, em = divmod(end_month, 100)
    return [(i // 12) * 100 + (i % 12) + 1 for i in range(sy * 12 + sm - 1, ey * 12 + em)]


def extract_all(out_dir=OUT_DIR, trino_url=TRINO_URL, skip_batch_check=False, http=requests, runner=subprocess.run):
    snapshots = {t: get_snapshot_id(t, trino_url, http) for t in ("fact_price_monthly", "fact_usda_price_reference")}
    gold_batch = "" if skip_batch_check else get_gold_batch(snapshots, runner=runner)

    wb_keys = ", ".join(f"'{k}'" for k in WB_SERIES)
    df_wb = query_trino(
        f"SELECT month_key, price_series_key, value FROM iceberg.gold_local.fact_price_monthly "
        f"FOR VERSION AS OF {snapshots['fact_price_monthly']} "
        f"WHERE price_series_key IN ({wb_keys}) AND month_key BETWEEN {START_MONTH} AND {END_MONTH}",
        trino_url, http,
    )
    df_wb["value"] = pd.to_numeric(df_wb["value"])
    wb_wide = df_wb.pivot(index="month_key", columns="price_series_key", values="value").rename(columns=WB_SERIES)

    usda_keys = ", ".join(f"'{k}'" for k in USDA_SERIES)
    df_usda = query_trino(
        f"SELECT price_series_key, reference_year, reference_period, value FROM iceberg.gold_local.fact_usda_price_reference "
        f"FOR VERSION AS OF {snapshots['fact_usda_price_reference']} "
        f"WHERE price_series_key IN ({usda_keys}) AND reference_period <> 'MARKETING' "
        f"AND reference_year BETWEEN {(START_MONTH // 100) - 1} AND {END_MONTH // 100}",
        trino_url, http,
    )
    assert set(df_usda["reference_period"]).issubset(MONTH_MAP)
    m_num = df_usda["reference_period"].map(MONTH_MAP)
    df_usda["month_key"] = (df_usda["reference_year"] + (m_num <= 7).astype(int)) * 100 + m_num
    df_usda = df_usda[df_usda["month_key"].between(START_MONTH, END_MONTH)]
    df_usda["value"] = pd.to_numeric(df_usda["value"])
    usda_wide = df_usda.pivot(index="month_key", columns="price_series_key", values="value").rename(columns=USDA_SERIES)

    spine = pd.DataFrame({"month_key": build_spine()})
    spine["month_date"] = pd.to_datetime(spine["month_key"].astype(str), format="%Y%m").dt.strftime("%Y-%m-%d")
    out = spine.join(wb_wide, on="month_key").join(usda_wide, on="month_key")
    cols = ["month_key", "month_date", *WB_SERIES.values(), *USDA_SERIES.values()]
    out = out[cols]
    out["gold_batch"] = gold_batch
    out["wb_snapshot_id"] = snapshots["fact_price_monthly"]
    out["usda_snapshot_id"] = snapshots["fact_usda_price_reference"]

    eval_df = out[out["month_key"] >= 201001]
    r = float(eval_df["wb_vn5_usd_t"].corr(eval_df["usda_vn5_usd_t"]))
    mae = float((eval_df["wb_vn5_usd_t"] - eval_df["usda_vn5_usd_t"]).abs().mean())
    assert r >= 0.97

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "gold_prices_monthly.csv"
    prov_path = out_dir / "gold_extraction_provenance.json"
    out.to_csv(csv_path, index=False)
    prov = {
        "trino_url": trino_url,
        "gold_batch": gold_batch,
        "snapshots": snapshots,
        "rows": len(out),
        "wb_vs_usda_vn5_2010_2025": {"pearson_r": round(r, 6), "mae_usd_t": round(mae, 4)},
    }
    prov_path.write_text(json.dumps(prov, indent=2), encoding="utf-8")
    return prov


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--trino-url", default=TRINO_URL)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--skip-batch-check", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(extract_all(args.output_dir, args.trino_url, args.skip_batch_check), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
