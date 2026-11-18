import argparse
from datetime import datetime
import json
from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
POWERBI_ROOT = ROOT / "exports" / "powerbi"
GOLD_CSV = ROOT / "exports" / "forecast" / "gold_prices_monthly.csv"
DEFAULT_OUT_DIR = ROOT / "exports" / "forecast" / "wb_usda_audit"

START_MONTH = 200501
END_MONTH = 202512

SERIES_LABELS = {
    "wb_vn5_usd_t": "WB Target: VN 5% (Pink Sheet)",
    "usda_vn5_usd_t": "USDA: Vietnam 5% broken",
    "usda_thai5_usd_t": "USDA: Thailand 5% broken",
    "usda_india_broken5_usd_t": "USDA: India 5% broken (white)",
    "usda_india_parb5_usd_t": "USDA: India 5% parboiled",
    "usda_pak5_usd_t": "USDA: Pakistan 5% broken",
}
SERIES_COLS = list(SERIES_LABELS)

USDA_KEYS = {
    "dd22fa75fc5eb986bda7828b9677247cb2ecc27b867649f89a26d569a3f1d8f1": "usda_vn5_usd_t",
    "10e2863b860499134132bcc34ba8f275f39703c19913b911a992e853cac9b430": "usda_thai5_usd_t",
    "3c14dec8f1e531db50bcd5db575f7dcbc3658e9e702f88a3bfef885adc63fc36": "usda_india_broken5_usd_t",
    "51dde42743a3e6613d8520755016442b35d262e2e90ed533a28aef80563c07af": "usda_india_parb5_usd_t",
    "5d49f78b4920846a6537c44e8b04773a0c2ace8d9c3f88cec1af0433b469e613": "usda_pak5_usd_t",
}

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def build_spine(start_month=START_MONTH, end_month=END_MONTH):
    sy, sm = divmod(start_month, 100)
    ey, em = divmod(end_month, 100)
    months = [(i // 12) * 100 + (i % 12) + 1 for i in range(sy * 12 + sm - 1, ey * 12 + em)]
    df = pd.DataFrame({"month_key": months})
    df["month_date"] = pd.to_datetime(df["month_key"].astype(str), format="%Y%m")
    df["year"] = df["month_key"] // 100
    return df


def latest_pbi_dir(powerbi_root=POWERBI_ROOT):
    root = Path(powerbi_root)
    if not root.exists():
        raise FileNotFoundError(f"PowerBI folder not found: {root}")
    candidates = []
    for folder in root.iterdir():
        has_csvs = folder.is_dir() and (folder / "wb_benchmark.csv").is_file() and (folder / "usda_price_reference.csv").is_file()
        if not has_csvs:
            continue
        manifest_file = folder / "export_manifest.json"
        if manifest_file.is_file():
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            if manifest.get("status") != "COMPLETE":
                continue
            ts = datetime.fromisoformat(manifest["export_timestamp"].replace("Z", "+00:00")).timestamp()
        else:
            ts = folder.stat().st_mtime
        candidates.append((ts, folder))
    if not candidates:
        raise FileNotFoundError(f"No valid PowerBI export in: {root}")
    return max(candidates, key=lambda item: item[0])[1]


def load_prices(pbi_dir=None):
    pbi_dir = Path(pbi_dir) if pbi_dir else latest_pbi_dir()

    wb = pd.read_csv(pbi_dir / "wb_benchmark.csv")
    wb = wb.loc[wb["series_code"] == "rice_viet_namese_5", ["month_key", "price_usd_t"]].rename(
        columns={"price_usd_t": "wb_vn5_usd_t"}
    )
    if wb.empty or wb["month_key"].duplicated().any():
        raise ValueError(f"Invalid WB rows in {pbi_dir / 'wb_benchmark.csv'}")

    usda = pd.read_csv(pbi_dir / "usda_price_reference.csv")
    usda = usda[usda["series_key"].isin(USDA_KEYS) & (usda["reference_period"] != "MARKETING")].copy()
    m_num = usda["reference_period"].map(MONTH_MAP)
    usda["month_key"] = (usda["reference_year"] + (m_num <= 7).astype(int)) * 100 + m_num
    usda["col"] = usda["series_key"].map(USDA_KEYS)
    if usda.empty or usda.duplicated(["month_key", "col"]).any():
        raise ValueError(f"Invalid or duplicate USDA rows in {pbi_dir / 'usda_price_reference.csv'}")
    usda_wide = usda.pivot(index="month_key", columns="col", values="price_usd_t").reset_index()

    df = build_spine().merge(wb, on="month_key", how="left").merge(usda_wide, on="month_key", how="left")
    return df[["month_key", "month_date", "year", *SERIES_COLS]], pbi_dir


def compress_spans(month_keys):
    if not month_keys:
        return "-"
    idx = sorted((m // 100) * 12 + (m % 100) - 1 for m in month_keys)
    spans, start, prev = [], idx[0], idx[0]
    for x in idx[1:]:
        if x == prev + 1:
            prev = x
        else:
            spans.append((start, prev))
            start = prev = x
    spans.append((start, prev))
    fmt = lambda i: f"{i // 12:04d}-{(i % 12) + 1:02d}"
    return ", ".join(fmt(a) if a == b else f"{fmt(a)}..{fmt(b)}" for a, b in spans)


def build_missing_inventory(df):
    windows = [("2005-2009", 200501, 200912), ("2010-2025", 201001, 202512), ("2005-2025", 200501, 202512)]
    rows = []
    for label, start, end in windows:
        sub = df[df["month_key"].between(start, end)]
        total = len(sub)
        for col in SERIES_COLS:
            miss = sub.loc[sub[col].isna(), "month_key"].tolist()
            rows.append({
                "period": label,
                "series_col": col,
                "series_label": SERIES_LABELS[col],
                "total_months": total,
                "valid_months": total - len(miss),
                "missing_months": len(miss),
                "missing_pct": round(len(miss) / total * 100, 2),
                "missing_spans": compress_spans(miss),
            })
    return pd.DataFrame(rows)


def compute_pair_metrics(sub, label):
    paired = sub.dropna(subset=["wb_vn5_usd_t", "usda_vn5_usd_t"])
    base = {
        "regime": label,
        "total_months": len(sub),
        "wb_valid": int(sub["wb_vn5_usd_t"].notna().sum()),
        "usda_valid": int(sub["usda_vn5_usd_t"].notna().sum()),
        "paired_months": len(paired),
    }
    if paired.empty:
        return {**base, "pearson_r": np.nan, "mae_usd_t": np.nan, "rmse_usd_t": np.nan,
                "bias_wb_minus_usda": np.nan, "median_abs_diff": np.nan, "max_abs_diff": np.nan, "max_abs_month": None}

    diff = paired["wb_vn5_usd_t"] - paired["usda_vn5_usd_t"]
    abs_diff = diff.abs()
    idx_max = abs_diff.idxmax()
    return {
        **base,
        "pearson_r": round(float(paired["wb_vn5_usd_t"].corr(paired["usda_vn5_usd_t"])), 6) if len(paired) >= 2 else np.nan,
        "mae_usd_t": round(float(abs_diff.mean()), 4),
        "rmse_usd_t": round(float(np.sqrt((diff ** 2).mean())), 4),
        "bias_wb_minus_usda": round(float(diff.mean()), 4),
        "median_abs_diff": round(float(abs_diff.median()), 4),
        "max_abs_diff": round(float(abs_diff.loc[idx_max]), 2),
        "max_abs_month": int(paired.loc[idx_max, "month_key"]) if "month_key" in paired.columns else None,
    }


def build_yearly_table(df):
    rows = []
    for year, g in df.groupby("year"):
        m = compute_pair_metrics(g, str(year))
        paired = g.dropna(subset=["wb_vn5_usd_t", "usda_vn5_usd_t"])
        rows.append({
            "year": int(year),
            "wb_valid": m["wb_valid"],
            "usda_valid": m["usda_valid"],
            "paired_n": m["paired_months"],
            "wb_mean": round(float(paired["wb_vn5_usd_t"].mean()), 2) if not paired.empty else np.nan,
            "usda_mean": round(float(paired["usda_vn5_usd_t"].mean()), 2) if not paired.empty else np.nan,
            "bias_wb_minus_usda": round(m["bias_wb_minus_usda"], 2) if pd.notna(m["bias_wb_minus_usda"]) else np.nan,
            "mae_usd_t": round(m["mae_usd_t"], 2) if pd.notna(m["mae_usd_t"]) else np.nan,
            "max_abs_diff": m["max_abs_diff"],
            "pearson_r": round(m["pearson_r"], 4) if pd.notna(m["pearson_r"]) else np.nan,
        })
    return pd.DataFrame(rows)


def _setup_plot_style():
    plt.rcParams.update({
        "font.sans-serif": ["DejaVu Sans", "Arial"],
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


def plot_missing_heatmap(df, save_path):
    _setup_plot_style()
    mat = np.vstack([df[c].notna().astype(int).values for c in SERIES_COLS])
    fig, ax = plt.subplots(figsize=(14.5, 4.8), dpi=180)
    ax.imshow(mat, aspect="auto", cmap=ListedColormap(["#e76f51", "#2a9d8f"]), interpolation="nearest")

    ax.set_yticks(range(len(SERIES_COLS)))
    ax.set_yticklabels([SERIES_LABELS[c] for c in SERIES_COLS], fontsize=9.5)
    jan_idx = df.index[df["month_key"] % 100 == 1].tolist()
    ax.set_xticks(jan_idx)
    ax.set_xticklabels([str(df.loc[i, "year"]) for i in jan_idx], fontsize=8.5, rotation=45)

    idx_2010 = df.index[df["month_key"] == 201001][0]
    ax.axvline(idx_2010 - 0.5, color="#1d3557", linestyle="--", linewidth=2.0)
    ax.text(idx_2010 / 2 - 2, -0.68, "Giai đoạn 2005–2009 (60 tháng)", ha="center", va="bottom", fontsize=9.5, fontweight="bold", color="#1d3557")
    ax.text((idx_2010 + len(df)) / 2, -0.68, "Giai đoạn 2010–2025 (192 tháng — Target Window)", ha="center", va="bottom", fontsize=9.5, fontweight="bold", color="#1d3557")

    box = dict(boxstyle="round,pad=0.28", fc="#fff8f0", ec="#e76f51", lw=0.9)
    arrow = dict(arrowstyle="->", color="#7f1d1d", lw=1.1)
    idx_0803 = df.index[df["month_key"] == 200803][0]
    ax.annotate("WB khuyết 01–05/2008\n(USDA có đủ số)", xy=(idx_0803, 0), xytext=(idx_0803 + 16, 0.12),
                fontsize=8.2, color="#7f1d1d", fontweight="bold", va="center", bbox=box, arrowprops=arrow)

    idx_2308 = df.index[df["month_key"] == 202308][0]
    idx_2408 = df.index[df["month_key"] == 202408][0]
    ax.annotate("Ấn Độ cấm XK gạo trắng\n2023-08..2024-08 (13 tháng)", xy=((idx_2308 + idx_2408) / 2, 3), xytext=(idx_2308 - 62, 2.15),
                fontsize=8.4, color="#7f1d1d", fontweight="bold", bbox=box, arrowprops=arrow)

    handles = [Patch(facecolor="#2a9d8f", label="Có dữ liệu (Valid)"), Patch(facecolor="#e76f51", label="Khuyết dữ liệu (Missing / NA)")]
    ax.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, -0.17), ncol=2, frameon=False, fontsize=9.5)
    ax.set_title("Kiểm kê độ phủ dữ liệu 2005–2025: 5 chuỗi USDA + WB Target (Gạo VN 5%)", fontsize=12.5, fontweight="bold", pad=26)
    plt.tight_layout()
    fig.savefig(save_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_wb_vs_usda(df, save_path):
    _setup_plot_style()
    fig = plt.figure(figsize=(14.8, 9.8), dpi=180)
    gs = fig.add_gridspec(2, 2, height_ratios=[1.42, 1.0], width_ratios=[1.42, 1.0], hspace=0.35, wspace=0.22)
    ax1, ax2, ax3 = fig.add_subplot(gs[0, :]), fig.add_subplot(gs[1, 0]), fig.add_subplot(gs[1, 1])

    d08_a, d08_b = pd.Timestamp("2008-01-01"), pd.Timestamp("2008-05-31")
    dban_a, dban_b = pd.Timestamp("2023-08-01"), pd.Timestamp("2024-08-31")

    ax1.plot(df["month_date"], df["usda_vn5_usd_t"], color="#e76f51", lw=1.6, ls="--", label="USDA Rice Yearbook (Vietnam 5% broken)")
    ax1.plot(df["month_date"], df["wb_vn5_usd_t"], color="#1d3557", lw=2.0, label="World Bank Pink Sheet (rice_viet_namese_5)")
    ax1.axvspan(d08_a, d08_b, color="#f4a261", alpha=0.26, label="01–05/2008: WB khuyết, USDA có số")
    ax1.axvspan(dban_a, dban_b, color="#e63946", alpha=0.14, label="2023-08..2024-08: Ấn Độ cấm XK gạo trắng")
    ax1.axvline(pd.Timestamp("2010-01-01"), color="#457b9d", ls=":", lw=1.6)

    sub_08 = df[df["month_key"].between(200801, 200805)]
    ax1.scatter(sub_08["month_date"], sub_08["usda_vn5_usd_t"], color="#d62828", s=34, zorder=5)
    ax1.annotate("01–05/2008 (Khủng hoảng giá gạo):\nUSDA có đủ 5 tháng (390 → 1,075 USD/t)\ntrong khi WB khuyết hoàn toàn",
                 xy=(pd.Timestamp("2008-05-01"), 1075), xytext=(pd.Timestamp("2010-03-01"), 965),
                 fontsize=8.6, fontweight="bold", color="#7f1d1d",
                 bbox=dict(boxstyle="round,pad=0.35", fc="#fff8f0", ec="#f4a261", lw=1.0),
                 arrowprops=dict(arrowstyle="->", color="#d62828", lw=1.3, connectionstyle="arc3,rad=-0.1"))
    ax1.annotate("2023-08..2024-08:\nẤn Độ cấm XK gạo trắng\nUSDA India 5% broken = NA (13T)\nVN 5%: MAE = 29.23 USD/t, r = 0.7814",
                 xy=(pd.Timestamp("2023-08-01"), 633), xytext=(pd.Timestamp("2017-02-01"), 745),
                 fontsize=8.6, fontweight="bold", color="#7f1d1d",
                 bbox=dict(boxstyle="round,pad=0.35", fc="#fff1f2", ec="#e63946", lw=1.0),
                 arrowprops=dict(arrowstyle="->", color="#e63946", lw=1.3, connectionstyle="arc3,rad=-0.1"))
    ax1.text(0.99, 0.96, "2010–2025 (n = 192/192 tháng):  r = 0.973079  |  MAE = 14.1344 USD/t  |  Bias (WB − USDA) = −7.79 USD/t",
             transform=ax1.transAxes, ha="right", va="top", fontsize=9.0, fontweight="bold", color="#1d3557",
             bbox=dict(boxstyle="round,pad=0.32", fc="#eef6fb", ec="#457b9d", lw=1.0))
    ax1.text(pd.Timestamp("2005-02-01"), 580, "2005–2009\n(Nhiều missing)", fontsize=8.6, color="#457b9d", fontweight="bold")
    ax1.text(pd.Timestamp("2010-04-01"), 660, "2010–2025 (Target Forecast Window)", fontsize=8.6, color="#457b9d", fontweight="bold")
    ax1.set_title("A. Đối chiếu diễn biến giá gạo VN 5% tấm: World Bank vs USDA (2005–2025)", fontsize=11.5, fontweight="bold", loc="left", pad=32)
    ax1.set_ylabel("Giá FOB (USD / tấn)", fontsize=10)
    ax1.set_ylim(180, 1200)
    ax1.xaxis.set_major_locator(mdates.YearLocator(2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.grid(axis="y", ls="--", alpha=0.35)
    ax1.legend(loc="lower center", bbox_to_anchor=(0.5, 1.005), ncol=4, fontsize=8.4, frameon=False)

    paired = df.dropna(subset=["wb_vn5_usd_t", "usda_vn5_usd_t"]).copy()
    paired["diff"] = paired["wb_vn5_usd_t"] - paired["usda_vn5_usd_t"]
    ax2.bar(paired["month_date"], paired["diff"], width=25, color=np.where(paired["diff"] >= 0, "#2a9d8f", "#e76f51"), alpha=0.85)
    ax2.axhline(0, color="#222222", lw=1.0)
    ax2.axhline(-7.7879, color="#1d3557", ls="--", lw=1.2, label="Mean Bias 2010–2025: −7.79 USD/t")
    ax2.axvspan(d08_a, d08_b, color="#f4a261", alpha=0.22)
    ax2.axvspan(dban_a, dban_b, color="#e63946", alpha=0.14)
    ax2.annotate("2008-06: −103.00", xy=(pd.Timestamp("2008-06-01"), -103), xytext=(pd.Timestamp("2009-02-01"), -117),
                 fontsize=8.2, color="#7f1d1d", fontweight="bold", arrowprops=dict(arrowstyle="->", color="#7f1d1d", lw=1.0))
    ax2.annotate("2023-08: −91.85\n(Ấn Độ cấm XK)", xy=(pd.Timestamp("2023-08-01"), -91.85), xytext=(pd.Timestamp("2015-08-01"), -121),
                 fontsize=8.2, color="#7f1d1d", fontweight="bold", arrowprops=dict(arrowstyle="->", color="#7f1d1d", lw=1.0))
    ax2.set_title("B. Chênh lệch theo tháng (WB − USDA, USD/tấn)", fontsize=10.8, fontweight="bold", loc="left", pad=8)
    ax2.set_ylabel("WB − USDA (USD/tấn)", fontsize=9.5)
    ax2.set_ylim(-132, 65)
    ax2.xaxis.set_major_locator(mdates.YearLocator(3))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax2.grid(axis="y", ls="--", alpha=0.35)
    ax2.legend(loc="upper right", fontsize=8.4, frameon=True)

    e = df[df["month_key"] >= 201001]
    is_ban = e["month_key"].between(202308, 202408)
    ax3.scatter(e.loc[~is_ban, "usda_vn5_usd_t"], e.loc[~is_ban, "wb_vn5_usd_t"], color="#457b9d", alpha=0.72, s=28, label="2010–2025 (Ngoài giai đoạn cấm, n=179)")
    ax3.scatter(e.loc[is_ban, "usda_vn5_usd_t"], e.loc[is_ban, "wb_vn5_usd_t"], color="#e63946", edgecolor="#7f1d1d", s=48, zorder=5, label="2023-08..2024-08 (Ấn Độ cấm XK, n=13)")
    lims = [290, 690]
    ax3.plot(lims, lims, "k--", lw=1.2, label="Đường chuẩn y = x (WB = USDA)")
    slope, intercept = np.polyfit(e["usda_vn5_usd_t"], e["wb_vn5_usd_t"], 1)
    ax3.plot(lims, slope * np.array(lims) + intercept, color="#1d3557", lw=1.6, label=f"OLS: y = {slope:.3f}x + {intercept:.1f}")
    ax3.text(0.04, 0.94, "r = 0.973079\nMAE = 14.1344 USD/t\nRMSE = 18.8466 USD/t", transform=ax3.transAxes, va="top",
             fontsize=8.8, fontweight="bold", color="#1d3557", bbox=dict(boxstyle="round,pad=0.3", fc="#eef6fb", ec="#457b9d", lw=0.8))
    ax3.set_title("C. Tương quan WB vs USDA giai đoạn 2010–2025", fontsize=10.8, fontweight="bold", loc="left", pad=8)
    ax3.set_xlabel("USDA Vietnam 5% broken (USD/tấn)", fontsize=9.5)
    ax3.set_ylabel("WB rice_viet_namese_5 (USD/tấn)", fontsize=9.5)
    ax3.set_xlim(lims)
    ax3.set_ylim(lims)
    ax3.grid(True, ls="--", alpha=0.35)
    ax3.legend(loc="lower right", fontsize=7.8, frameon=True)

    fig.suptitle("Kiểm chứng chất lượng nguồn giá gạo Việt Nam 5% tấm: World Bank vs USDA", fontsize=13.5, fontweight="bold", y=0.99)
    fig.savefig(save_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def run_audit(pbi_dir=None, gold_csv=GOLD_CSV, output_dir=DEFAULT_OUT_DIR):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df, resolved_pbi = load_prices(pbi_dir)

    fc = pd.read_csv(gold_csv)
    sub_fc = df[df["month_key"] >= 200901].reset_index(drop=True)
    for col in SERIES_COLS:
        assert (sub_fc[col].isna() == fc[col].isna()).all()
        max_diff = (sub_fc[col] - fc[col]).abs().max()
        assert pd.isna(max_diff) or max_diff == 0.0

    build_missing_inventory(df).to_csv(output_dir / "missing_inventory_2005_2025.csv", index=False, encoding="utf-8-sig")

    regimes = [
        ("2005-2009 (Paired)", df[df["month_key"].between(200501, 200912)]),
        ("2010-2025 (All)", df[df["month_key"].between(201001, 202512)]),
        ("2023-08..2024-08 (India Ban)", df[df["month_key"].between(202308, 202408)]),
        ("2010-2025 (Excl India Ban)", df[df["month_key"].between(201001, 202512) & ~df["month_key"].between(202308, 202408)]),
        ("2005-2025 (Full Paired)", df),
    ]
    regime_df = pd.DataFrame([compute_pair_metrics(sub, lbl) for lbl, sub in regimes])
    assert regime_df.loc[regime_df["regime"] == "2010-2025 (All)", "pearson_r"].iloc[0] == 0.973079
    assert regime_df.loc[regime_df["regime"] == "2010-2025 (All)", "mae_usd_t"].iloc[0] == 14.1344
    regime_df.to_csv(output_dir / "wb_vs_usda_vn5_regimes.csv", index=False, encoding="utf-8-sig")

    build_yearly_table(df).to_csv(output_dir / "wb_vs_usda_vn5_yearly.csv", index=False, encoding="utf-8-sig")

    df["diff_wb_minus_usda"] = (df["wb_vn5_usd_t"] - df["usda_vn5_usd_t"]).round(2)
    df[df["month_key"].between(200711, 200808)][
        ["month_key", "wb_vn5_usd_t", "usda_vn5_usd_t", "diff_wb_minus_usda", "usda_thai5_usd_t"]
    ].to_csv(output_dir / "special_regime_2008_crisis.csv", index=False, encoding="utf-8-sig")
    df[df["month_key"].between(202307, 202409)][
        ["month_key", "wb_vn5_usd_t", "usda_vn5_usd_t", "diff_wb_minus_usda", "usda_india_broken5_usd_t", "usda_india_parb5_usd_t"]
    ].to_csv(output_dir / "special_regime_2023_2024_india_ban.csv", index=False, encoding="utf-8-sig")

    plot_missing_heatmap(df, output_dir / "missing_inventory_heatmap.png")
    plot_wb_vs_usda(df, output_dir / "wb_vs_usda_vn5_comparison.png")
    print("ALL CHECKS PASSED! Source PBI:", resolved_pbi.name, "| Outputs:", output_dir)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Audit WB vs USDA rice price completeness and cross-source consistency.")
    parser.add_argument("--pbi-dir", type=Path, default=None)
    parser.add_argument("--gold-csv", type=Path, default=GOLD_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)
    run_audit(args.pbi_dir, args.gold_csv, args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
