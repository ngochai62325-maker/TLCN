import argparse
from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from jobs.forecasting.metrics import build_prediction_frame, evaluate_predictions
from jobs.forecasting.validate_model_predictions import validate_model_predictions

ROOT = Path(__file__).resolve().parents[2]
MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
MODELS_DIR = ROOT / "exports" / "forecast" / "models"
OUT_DIR = ROOT / "exports" / "forecast"

MODEL_FILES = [
    "naive_predictions.csv",
    "sarima_predictions.csv",
    "xgb_predictions.csv",
]
ALL_MODELS = ["naive", "sarima", "xgb_base", "xgb_competitor"]

MODEL_STYLES = {
    "naive": {"label": "Naive", "color": "#64748b", "ls": "--"},
    "sarima": {"label": "SARIMA", "color": "#2a9d8f", "ls": "-"},
    "xgb_base": {"label": "XGBoost A (Base)", "color": "#e76f51", "ls": "-."},
    "xgb_competitor": {"label": "XGBoost B (Competitor)", "color": "#2563eb", "ls": "-"},
}

SHOCK_SPAN = (pd.Timestamp("2023-07-01"), pd.Timestamp("2024-08-31"))
SPLIT_2024 = pd.Timestamp("2024-01-01")
PERIOD_SPECS = [
    ("2023", 202301, 202312, "Giai đoạn 2023 — Cú sốc tăng giá", 2),
    ("2024-2025", 202401, 202512, "Giai đoạn 2024–2025 — Nhịp điều chỉnh", 4),
]
PERIOD_ORDER = ["2023-2025", "2023", "2024-2025"]
METRIC_SPECS = [
    ("mae", "MAE (USD/tấn)"),
    ("rmse", "RMSE (USD/tấn)"),
    ("mape", "MAPE (%)"),
]

def load_and_merge_predictions(mart_df, models_dir=MODELS_DIR, require_all=False):
    frames = []
    for filename in MODEL_FILES:
        path = Path(models_dir) / filename
        if not path.is_file():
            continue
        raw_df = pd.read_csv(path)
        validate_model_predictions(raw_df, mart_df)
        for model_name, grp in raw_df.groupby("model_name", sort=False):
            frames.append(
                build_prediction_frame(
                    month_key=grp["month_key"],
                    model_name=model_name,
                    actual_usd_t=grp["actual_usd_t"],
                    predicted_usd_t=grp["predicted_usd_t"],
                )
            )

    if not frames:
        raise FileNotFoundError(f"No model prediction CSV found in {models_dir}")

    merged = pd.concat(frames, ignore_index=True)
    expected = ALL_MODELS if require_all else None
    validate_model_predictions(merged, mart_df, expected_models=expected)

    rank = {name: i for i, name in enumerate(ALL_MODELS)}
    merged["model_rank"] = merged["model_name"].map(rank).fillna(99)
    return merged.sort_values(["model_rank", "month_key"]).drop(columns="model_rank").reset_index(drop=True)

def draw_actual_vs_pred(ax, pred_df, title, show_split=False, tick_interval=3, title_pad=6):
    df = pred_df.copy()
    df["month_date"] = pd.to_datetime(df["month_key"].astype(str), format="%Y%m")

    actual = df[["month_date", "actual_usd_t"]].drop_duplicates().sort_values("month_date")
    ax.plot(actual["month_date"], actual["actual_usd_t"], color="#111827", lw=2.4, marker="o", ms=3.5, label="Actual (WB VN 5%)")

    for model_name in df["model_name"].drop_duplicates():
        sub = df[df["model_name"] == model_name].sort_values("month_date")
        style = MODEL_STYLES.get(model_name, {"label": model_name, "ls": "-"})
        ax.plot(sub["month_date"], sub["predicted_usd_t"], lw=1.9, **style)

    span_start = max(SHOCK_SPAN[0], actual["month_date"].min())
    span_end = min(SHOCK_SPAN[1], actual["month_date"].max())
    if span_start <= span_end:
        ax.axvspan(span_start, span_end, color="#ef4444", alpha=0.12, label="Ấn Độ cấm XK (07/2023–08/2024)")

    if show_split:
        ax.axvline(SPLIT_2024, color="#475569", ls=":", lw=1.4)

    ax.set_title(title, fontsize=11, fontweight="bold", pad=title_pad)
    ax.set_ylabel("Giá FOB (USD/tấn)")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=tick_interval))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", ls="--", alpha=0.35)


def plot_actual_vs_predicted(pred_df, out_path=None):
    fig, ax = plt.subplots(figsize=(13, 5.2), dpi=180)
    draw_actual_vs_pred(
        ax,
        pred_df,
        "So sánh Actual vs Predicted — Giá gạo VN 5% tấm (2023–2025)",
        show_split=True,
        tick_interval=3,
        title_pad=38,
    )
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=3, frameon=False, fontsize=8.8)
    fig.tight_layout()
    if out_path is not None:
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
    return fig


def plot_actual_vs_predicted_by_period(pred_df, out_path=None):
    fig, axes = plt.subplots(1, len(PERIOD_SPECS), figsize=(14, 5.2), sharey=True, dpi=180, squeeze=False)
    axes = axes[0]
    for ax, (_, start_mk, end_mk, title, interval) in zip(axes, PERIOD_SPECS):
        sub = pred_df[pred_df["month_key"].between(start_mk, end_mk)]
        draw_actual_vs_pred(ax, sub, title, tick_interval=interval)
    for ax in axes[1:]:
        ax.set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle("Actual vs Predicted theo từng giai đoạn (2023 và 2024–2025)", fontsize=12.5, fontweight="bold", y=0.99)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.93), ncol=3, frameon=False, fontsize=8.8)
    fig.tight_layout(rect=[0, 0, 1, 0.85])
    if out_path is not None:
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
    return fig


def plot_metrics_by_period(metrics_df, out_path=None):
    models = metrics_df["model_name"].drop_duplicates().tolist()
    x = np.arange(len(PERIOD_ORDER))
    width = min(0.35, 0.75 / max(len(models), 1))

    fig, axes = plt.subplots(1, len(METRIC_SPECS), figsize=(14, 4.8), dpi=180, squeeze=False)
    axes = axes[0]
    for ax, (col, ylabel) in zip(axes, METRIC_SPECS):
        for idx, model_name in enumerate(models):
            sub = metrics_df[metrics_df["model_name"] == model_name].set_index("eval_period")
            vals = [float(sub.loc[p, col]) for p in PERIOD_ORDER]
            offset = (idx - (len(models) - 1) / 2) * width
            style = MODEL_STYLES.get(model_name, {"label": model_name})
            bars = ax.bar(x + offset, vals, width=width * 0.92, label=style["label"], color=style.get("color"))
            ax.bar_label(bars, fmt="%.2f", fontsize=7.5, padding=2)
        ax.set_title(ylabel, fontsize=10.5, fontweight="bold")
        ax.set_xticks(x, PERIOD_ORDER)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.18)
        ax.grid(axis="y", ls="--", alpha=0.35)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle("So sánh sai số dự báo theo giai đoạn (2023–2025, 2023, 2024–2025)", fontsize=12.5, fontweight="bold", y=0.99)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=max(len(models), 1), frameon=False, fontsize=9)
    fig.tight_layout(rect=[0, 0, 1, 0.85])
    if out_path is not None:
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
    return fig

def run_summary(mart_csv=MART_CSV, models_dir=MODELS_DIR, out_dir=OUT_DIR, fig_dir=None, require_all=False):
    plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False})
    out_dir = Path(out_dir)
    fig_dir = Path(fig_dir) if fig_dir is not None else out_dir / "summary"

    mart_df = pd.read_csv(mart_csv)
    pred_df = load_and_merge_predictions(mart_df, models_dir=models_dir, require_all=require_all)
    metrics_df = evaluate_predictions(pred_df)

    out_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)
    pred_df.to_csv(out_dir / "forecast_predictions.csv", index=False)
    metrics_df.to_csv(out_dir / "forecast_metrics.csv", index=False)

    for fig in (
        plot_actual_vs_predicted(pred_df, fig_dir / "fig1_actual_vs_predicted_2023_2025.png"),
        plot_actual_vs_predicted_by_period(pred_df, fig_dir / "fig2_actual_vs_predicted_by_period.png"),
        plot_metrics_by_period(metrics_df, fig_dir / "fig3_metrics_by_period.png"),
    ):
        plt.close(fig)

    return pred_df, metrics_df


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart-csv", type=Path, default=MART_CSV)
    parser.add_argument("--models-dir", type=Path, default=MODELS_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--fig-dir", type=Path, default=None)
    parser.add_argument("--require-all", action="store_true")
    args = parser.parse_args(argv)

    pred_df, metrics_df = run_summary(
        mart_csv=args.mart_csv,
        models_dir=args.models_dir,
        out_dir=args.output_dir,
        fig_dir=args.fig_dir,
        require_all=args.require_all,
    )
    print(f"Models merged: {pred_df['model_name'].drop_duplicates().tolist()} ({len(pred_df)} rows)")
    print(metrics_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())