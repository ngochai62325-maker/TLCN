import json
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import acf, adfuller, pacf

ROOT = Path(__file__).resolve().parents[2]
MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
OUT_DIR = ROOT / "exports" / "forecast" / "eda"
SPLIT_DATE = pd.Timestamp("2023-01-01")


def load_data(mart_csv=MART_CSV):
    df = pd.read_csv(mart_csv)
    df["month_date"] = pd.to_datetime(df["month_key"].astype(str), format="%Y%m")
    df = df.sort_values("month_key").set_index("month_date").asfreq("MS")
    train_y = df.loc[df["split"] == "train", "y"]
    test_y = df.loc[df["split"] == "test", "y"]
    assert len(train_y) == 156 and len(test_y) == 36
    return df["y"], train_y, test_y


def save_summary(full_y, train_y, test_y, out_dir):
    rows = []
    for name, s in [("Train (2010-2022)", train_y), ("Test (2023-2025)", test_y), ("Full (2010-2025)", full_y)]:
        rows.append({
            "segment": name,
            "n_months": len(s),
            "mean": round(float(s.mean()), 2),
            "std": round(float(s.std()), 2),
            "min": round(float(s.min()), 2),
            "min_month": s.idxmin().strftime("%Y-%m"),
            "median": round(float(s.median()), 2),
            "max": round(float(s.max()), 2),
            "max_month": s.idxmax().strftime("%Y-%m"),
        })
    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / "target_summary_stats.csv", index=False, encoding="utf-8-sig")
    return summary


def plot_timeseries(train_y, test_y, out_path):
    fig, ax = plt.subplots(figsize=(14, 5.5), dpi=180)
    test_line = pd.concat([train_y.iloc[[-1]], test_y])
    ax.plot(train_y.index, train_y, color="#1d3557", lw=2, label="Train (2010–2022, n=156)")
    ax.plot(test_line.index, test_line, color="#e76f51", lw=2.2, label="Test (2023–2025, n=36)")

    train_max, train_dt = float(train_y.max()), train_y.idxmax()
    test_max, test_dt = float(test_y.max()), test_y.idxmax()
    exceed = test_y[test_y > train_max]

    ax.axvline(SPLIT_DATE, color="#264653", ls="--", lw=1.5)
    ax.axhline(train_max, color="#457b9d", ls=":", lw=1.4, label=f"Đỉnh Train: {train_max:.2f} USD/tấn")
    ax.axvspan(exceed.index.min() - pd.Timedelta(days=15), exceed.index.max() + pd.Timedelta(days=15),
               color="#e63946", alpha=0.14, label=f"Vượt đỉnh Train ({len(exceed)} tháng: 09/2023–03/2024)")
    ax.scatter(exceed.index, exceed, color="#e63946", s=20, zorder=4)
    ax.scatter([train_dt, test_dt], [train_max, test_max], color=["#1d3557", "#d62828"], s=52, zorder=5)

    ax.annotate(f"Đỉnh Train: {train_max:.2f} USD/tấn\n({train_dt:%m/%Y})",
                xy=(train_dt, train_max), xytext=(pd.Timestamp("2013-02-01"), 606),
                fontsize=8.8, fontweight="bold", color="#1d3557",
                bbox=dict(boxstyle="round,pad=0.3", fc="#eef6fb", ec="#457b9d"),
                arrowprops=dict(arrowstyle="->", color="#1d3557"))
    ax.annotate(f"Đỉnh Test: {test_max:.2f} USD/tấn\n({test_dt:%m/%Y} — Ấn Độ cấm XK)",
                xy=(test_dt, test_max), xytext=(pd.Timestamp("2018-04-01"), 638),
                fontsize=8.8, fontweight="bold", color="#7f1d1d",
                bbox=dict(boxstyle="round,pad=0.3", fc="#fff1f2", ec="#e63946"),
                arrowprops=dict(arrowstyle="->", color="#d62828"))

    ax.text(SPLIT_DATE - pd.Timedelta(days=60), 312, "◄── Train (2010–2022)", ha="right", fontsize=8.8, fontweight="bold", color="#1d3557")
    ax.text(SPLIT_DATE + pd.Timedelta(days=60), 312, "Test (2023–2025) ──►", ha="left", fontsize=8.8, fontweight="bold", color="#b5442a")

    ax.set_title("Biến động giá gạo xuất khẩu VN 5% tấm (World Bank, 2010–2025)", fontsize=12.5, fontweight="bold", pad=38)
    ax.set_ylabel("Giá FOB (USD / tấn)")
    ax.set_ylim(295, 680)
    ax.grid(axis="y", ls="--", alpha=0.35)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=2, frameon=False, fontsize=8.8)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def analyze_decompose(train_y, out_dir):
    dec = seasonal_decompose(train_y, model="additive", period=12)
    fig, axes = plt.subplots(4, 1, figsize=(13, 8.8), sharex=True, dpi=180)
    panels = [
        (dec.observed, "1. Chuỗi giá gốc y_t", "USD/tấn", "#1d3557"),
        (dec.trend, "2. Xu hướng — Trend (MA 12 tháng)", "USD/tấn", "#2a9d8f"),
        (dec.seasonal, "3. Thành phần mùa vụ — Seasonal", "± USD/tấn", "#e76f51"),
        (dec.resid, "4. Phần dư — Residual", "USD/tấn", "#6c757d"),
    ]
    for ax, (s, title, ylabel, color) in zip(axes, panels):
        ax.plot(s.index, s, color=color, lw=1.7)
        if title.startswith(("3.", "4.")):
            ax.axhline(0, color="#222", ls="--", lw=0.9, alpha=0.7)
        ax.set_title(title, loc="left", fontsize=10, fontweight="bold", pad=4)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.grid(axis="y", ls="--", alpha=0.35)

    fig.suptitle("Phân rã xu hướng – mùa vụ trên tập Train (2010–2022, Additive)", fontsize=12.5, fontweight="bold", y=0.985)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(out_dir / "fig2_seasonal_decompose_train.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    season_12 = pd.DataFrame({
        "month_of_year": dec.seasonal.iloc[:12].index.month,
        "seasonal_additive_usd_t": dec.seasonal.iloc[:12].round(2).values,
    }).sort_values("month_of_year")
    season_12.to_csv(out_dir / "seasonal_pattern_train.csv", index=False, encoding="utf-8-sig")
    return season_12


def run_adf(train_y, full_y, out_csv):
    cases = [
        ("train_2010_2022", "y_t", train_y),
        ("train_2010_2022", "delta_y_t (d=1)", train_y.diff().dropna()),
        ("train_2010_2022", "delta12_y_t (D=1)", train_y.diff(12).dropna()),
        ("train_2010_2022", "delta_delta12_y_t (d=1, D=1)", train_y.diff(12).diff().dropna()),
        ("full_2010_2025_ref_only", "y_t", full_y),
        ("full_2010_2025_ref_only", "delta_y_t (d=1)", full_y.diff().dropna()),
    ]
    rows = []
    for scope, label, s in cases:
        stat, pval, usedlag, nobs, crit, _ = adfuller(s, autolag="AIC")
        rows.append({
            "scope": scope, "series": label, "nobs": int(nobs), "usedlag": int(usedlag),
            "adf_stat": round(float(stat), 4), "p_value": round(float(pval), 6),
            "crit_1pct": round(float(crit["1%"]), 4), "crit_5pct": round(float(crit["5%"]), 4),
            "stationary_5pct": bool(pval < 0.05), "stationary_1pct": bool(pval < 0.01),
        })
    adf_df = pd.DataFrame(rows)
    adf_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return adf_df


def analyze_acf_pacf(train_y, out_dir, nlags=24):
    dy = train_y.diff().dropna()
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), dpi=180)
    panels = [
        (axes[0, 0], plot_acf, train_y, "ACF — Chuỗi gốc y_t (Train)", "#1d3557", (-0.65, 1.05), False),
        (axes[0, 1], plot_pacf, train_y, "PACF — Chuỗi gốc y_t (Train)", "#1d3557", (-0.65, 1.05), False),
        (axes[1, 0], plot_acf, dy, "ACF — Sai phân Δy_t (Train)", "#e76f51", (-0.38, 0.62), True),
        (axes[1, 1], plot_pacf, dy, "PACF — Sai phân Δy_t (Train)", "#e76f51", (-0.38, 0.62), True),
    ]
    for ax, fn, s, title, color, ylim, show_xlabel in panels:
        kwargs = {"method": "ywm"} if fn is plot_pacf else {}
        fn(s, lags=nlags, zero=False, ax=ax, color=color, vlines_kwargs={"colors": color}, **kwargs)
        ax.set_title(title, fontsize=10.5, fontweight="bold")
        ax.set_ylim(ylim)
        ax.grid(axis="y", ls="--", alpha=0.3)
        if show_xlabel:
            ax.set_xlabel("Lag (tháng)", fontsize=9)

    fig.suptitle("Chẩn đoán ACF / PACF trên tập Train 2010–2022 (Chống rò thông tin Test)", fontsize=12.5, fontweight="bold", y=0.985)
    plt.tight_layout(rect=[0, 0, 1, 0.955])
    fig.savefig(out_dir / "fig3_acf_pacf_train.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    acf_dy, conf = acf(dy, nlags=nlags, alpha=0.05)
    pacf_dy = pacf(dy, nlags=nlags, method="ywm")
    bound_wn = 1.96 / (len(dy) ** 0.5)
    hints = {
        "train_scope": "201001..202212 (156 months)",
        "acf_dy_lags_1_to_6": [round(float(x), 3) for x in acf_dy[1:7]],
        "pacf_dy_lags_1_to_6": [round(float(x), 3) for x in pacf_dy[1:7]],
        "sig_acf_bartlett": [k for k in range(1, nlags + 1) if abs(acf_dy[k]) > (conf[k, 1] - acf_dy[k])],
        "sig_pacf_lags": [k for k in range(1, nlags + 1) if abs(pacf_dy[k]) > bound_wn],
        "recommended_d": 1,
        "recommended_grid": {"p": [0, 1, 2], "d": [1], "q": [0, 1, 2], "P": [0, 1], "D": [0, 1], "Q": [0, 1], "s": 12},
        "top_candidates": ["(0,1,1)(0,0,0)[12]", "(1,1,1)(0,0,0)[12]", "(2,1,0)(0,0,0)[12]", "(0,1,1)(0,1,1)[12]"],
    }
    (out_dir / "sarima_order_hints.json").write_text(json.dumps(hints, indent=2, ensure_ascii=False), encoding="utf-8")
    return hints


def main():
    plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False})
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    full_y, train_y, test_y = load_data()
    summary = save_summary(full_y, train_y, test_y, OUT_DIR)
    plot_timeseries(train_y, test_y, OUT_DIR / "fig1_target_timeseries.png")
    season_12 = analyze_decompose(train_y, OUT_DIR)
    adf_df = run_adf(train_y, full_y, OUT_DIR / "adf_stationarity_tests.csv")
    hints = analyze_acf_pacf(train_y, OUT_DIR)

    print("=== TÓM TẮT ===\n", summary.to_string(index=False))
    print("\n=== ADF ===\n", adf_df[["scope", "series", "adf_stat", "p_value", "stationary_5pct", "stationary_1pct"]].to_string(index=False))
    print("\n=== MÙA VỤ (MIN/MAX) ===", season_12.loc[season_12["seasonal_additive_usd_t"].idxmin()].to_dict(), "|", season_12.loc[season_12["seasonal_additive_usd_t"].idxmax()].to_dict())
    print("\n=== GỢI Ý SARIMA ===\n", json.dumps(hints, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
