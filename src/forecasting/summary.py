"""Tổng hợp dự báo đa mô hình, cập nhật bảng kết quả và trực quan hóa so sánh."""
from __future__ import annotations

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd

from forecasting.metrics import (
    PREDICTION_COLUMNS,
    build_prediction_frame,
    validate_model_predictions,
)

MODEL_FILES = [
    "naive_predictions.csv",
    "sarima_predictions.csv",
    "xgb_predictions.csv",
]
ALL_MODELS = ["naive", "sarima", "xgb_base", "xgb_competitor"]
SARIMA_MODEL_NAMES = ("sarima", "sarima_rolling_1step", "sarima_static")

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


def load_and_merge_predictions(
    mart_df: pd.DataFrame,
    models_dir: str | Path,
    require_all: bool = False,
) -> pd.DataFrame:
    """Đọc, kiểm định và hợp nhất các file dự báo của từng mô hình trong models_dir."""
    frames = []
    models_path = Path(models_dir)
    for filename in MODEL_FILES:
        path = models_path / filename
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
        raise FileNotFoundError(f"No model prediction CSV found in {models_path}")

    merged = pd.concat(frames, ignore_index=True)
    expected = ALL_MODELS if require_all else None
    validate_model_predictions(merged, mart_df, expected_models=expected)

    rank = {name: i for i, name in enumerate(ALL_MODELS)}
    merged["model_rank"] = merged["model_name"].map(rank).fillna(99)
    return merged.sort_values(["model_rank", "month_key"]).drop(columns="model_rank").reset_index(drop=True)


def draw_actual_vs_pred(
    ax,
    pred_df: pd.DataFrame,
    title: str,
    show_split: bool = False,
    tick_interval: int = 3,
    title_pad: int = 6,
) -> None:
    """Vẽ chuỗi giá thực tế và đường dự báo của các mô hình lên một trục matplotlib."""
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


def plot_actual_vs_predicted(pred_df: pd.DataFrame, out_path: str | Path | None = None):
    """Vẽ biểu đồ so sánh Actual vs Predicted toàn giai đoạn kiểm thử 2023–2025."""
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


def plot_actual_vs_predicted_by_period(pred_df: pd.DataFrame, out_path: str | Path | None = None):
    """Vẽ biểu đồ Actual vs Predicted tách theo 2 giai đoạn 2023 và 2024–2025."""
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


def plot_metrics_by_period(metrics_df: pd.DataFrame, out_path: str | Path | None = None):
    """Vẽ biểu đồ cột so sánh MAE, RMSE, MAPE của các mô hình theo từng giai đoạn."""
    models = metrics_df["model_name"].drop_duplicates().tolist()
    x = np.arange(len(PERIOD_ORDER))
    width = min(0.35, 0.75 / max(len(models), 1))

    fig, axes = plt.subplots(1, len(METRIC_SPECS), figsize=(15, 5.2), dpi=180, squeeze=False)
    axes = axes[0]
    for ax, (col, ylabel) in zip(axes, METRIC_SPECS):
        for idx, model_name in enumerate(models):
            sub = metrics_df[metrics_df["model_name"] == model_name].set_index("eval_period")
            vals = [float(sub.loc[p, col]) for p in PERIOD_ORDER]
            offset = (idx - (len(models) - 1) / 2) * width
            style = MODEL_STYLES.get(model_name, {"label": model_name})
            bars = ax.bar(x + offset, vals, width=width * 0.92, label=style["label"], color=style.get("color"))
            ax.bar_label(bars, fmt="%.2f", fontsize=6.8, padding=2)
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

def plot_wb_vs_usda_zoom(gold_df: pd.DataFrame, out_path: str | Path | None = None):
    df = gold_df[gold_df["month_key"].between(201001, 202512)].copy()
    df["month_date"] = pd.to_datetime(df["month_key"].astype(str), format="%Y%m")
    zoom = df[df["month_key"].between(202301, 202512)]

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(14, 4.8), dpi=180, gridspec_kw={"width_ratios": [1.5, 1.0]}
    )

    panels = [
        (ax1, df, "Giai đoạn 2010–2025 (192 tháng)", mdates.YearLocator(2), "%Y"),
        (ax2, zoom, "Phóng to 2023–2025 (36 tháng test)", mdates.MonthLocator(interval=4), "%m/%Y"),
    ]
    for ax, sub, title, locator, fmt in panels:
        ax.plot(sub["month_date"], sub["wb_vn5_usd_t"], color="#1d3557", lw=2.0, label="World Bank (VN 5%)")
        ax.plot(sub["month_date"], sub["usda_vn5_usd_t"], color="#e76f51", lw=1.7, ls="--", label="USDA (VN 5%)")
        ax.axvspan(SHOCK_SPAN[0], SHOCK_SPAN[1], color="#ef4444", alpha=0.12, label="Ấn Độ cấm XK (07/2023–08/2024)")
        r_val = sub["wb_vn5_usd_t"].corr(sub["usda_vn5_usd_t"])
        mae_val = (sub["wb_vn5_usd_t"] - sub["usda_vn5_usd_t"]).abs().mean()
        ax.text(
            0.03,
            0.95,
            f"r = {r_val:.4f} | MAE = {mae_val:.2f} USD/t",
            transform=ax.transAxes,
            va="top",
            fontsize=8.5,
            bbox={"boxstyle": "round,pad=0.3", "fc": "#f8fafc", "ec": "#cbd5e1"},
        )
        ax.set_title(title, fontsize=10.5, fontweight="bold")
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
        ax.set_ylim(top=ax.get_ylim()[1] * 1.09)
        ax.grid(axis="y", ls="--", alpha=0.35)

    ax1.set_ylabel("Giá FOB (USD/tấn)")
    ax2.tick_params(axis="x", rotation=25)
    handles, labels = ax1.get_legend_handles_labels()
    fig.suptitle("Biểu đồ 1: Đối chiếu giá gạo VN 5% tấm — World Bank vs USDA", fontsize=12.5, fontweight="bold", y=0.99)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=3, frameon=False, fontsize=9)
    fig.tight_layout(rect=[0, 0, 1, 0.86])
    if out_path is not None:
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
    return fig

def plot_metrics_and_importance(
    metrics_df: pd.DataFrame,
    fi_df: pd.DataFrame,
    out_path: str | Path | None = None,
    top_n: int = 10,
):
    models = metrics_df["model_name"].drop_duplicates().tolist()
    x = np.arange(len(PERIOD_ORDER))
    width = 0.19

    fig = plt.figure(figsize=(15, 9.2), dpi=180, layout="constrained")
    top_fig, bot_fig = fig.subfigures(2, 1, height_ratios=[1.0, 1.15])
    metric_axes = top_fig.subplots(1, 3)

    for ax, (col, ylabel) in zip(metric_axes, METRIC_SPECS):
        for idx, model_name in enumerate(models):
            sub = metrics_df[metrics_df["model_name"] == model_name].set_index("eval_period")
            vals = [float(sub.loc[p, col]) for p in PERIOD_ORDER]
            offset = (idx - (len(models) - 1) / 2) * width
            style = MODEL_STYLES.get(model_name, {"label": model_name})
            bars = ax.bar(x + offset, vals, width=width * 0.9, label=style["label"], color=style.get("color"))
            ax.bar_label(bars, fmt="%.2f", fontsize=6.8, padding=2)
        ax.set_title(ylabel, fontsize=10.5, fontweight="bold")
        ax.set_xticks(x, PERIOD_ORDER)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.2)
        ax.grid(axis="y", ls="--", alpha=0.35)

    handles, labels = metric_axes[0].get_legend_handles_labels()
    top_fig.legend(handles, labels, loc="outside upper center", ncol=len(models), frameon=False, fontsize=9)

    fi = fi_df[fi_df["model_name"] == "xgb_competitor"].copy()
    group_colors = {"competitor": "#2563eb", "base": "#94a3b8"}
    fi_axes = bot_fig.subplots(1, 2)
    fi_specs = [
        (fi_axes[0], "gain_share_pct", "Feature Importance — Gain Share (%)", "%.1f%%"),
        (fi_axes[1], "perm_test_rmse_mean", "Permutation Importance trên Test (ΔRMSE)", "%.2f"),
    ]
    for ax, col, title, fmt in fi_specs:
        top = fi.nlargest(top_n, col).iloc[::-1]
        colors = [group_colors.get(g, "#94a3b8") for g in top["feature_group"]]
        bars = ax.barh(top["feature"], top[col], color=colors, height=0.68)
        ax.bar_label(bars, fmt=fmt, fontsize=8, padding=3)
        ax.set_title(title, fontsize=10.5, fontweight="bold")
        ax.set_xlim(min(0, top[col].min() * 1.15), top[col].max() * 1.22)
        ax.grid(axis="x", ls="--", alpha=0.35)

    fi_legend = [
        Patch(fc="#2563eb", label="Đối thủ (competitor)"),
        Patch(fc="#94a3b8", label="Nội sinh & chi phí (base)"),
    ]
    fi_axes[0].legend(handles=fi_legend, loc="lower right", frameon=True, fontsize=8)
    fig.suptitle("Biểu đồ 3: Sai số 3 giai đoạn & Feature Importance (xgb_competitor)", fontsize=12.5, fontweight="bold")
    if out_path is not None:
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
    return fig

def upsert_forecast_predictions(
    csv_path: str | Path,
    new_pred_df: pd.DataFrame,
    replace_models: tuple[str, ...] | list[str] = SARIMA_MODEL_NAMES,
) -> pd.DataFrame:
    """Cập nhật lũy đẳng (idempotent upsert) dự báo vào forecast_predictions.csv, giữ nguyên các mô hình khác."""
    path = Path(csv_path)
    if path.is_file():
        existing = pd.read_csv(path)
        kept = existing[~existing["model_name"].isin(replace_models)].copy()
        merged = pd.concat([kept, new_pred_df], ignore_index=True)
    else:
        merged = new_pred_df.copy()

    rank = {name: i for i, name in enumerate(ALL_MODELS)}
    merged["_model_rank"] = merged["model_name"].map(rank).fillna(99)
    merged = (
        merged.sort_values(["_model_rank", "model_name", "month_key"])
        .drop(columns="_model_rank")
        .drop_duplicates(["month_key", "model_name"], keep="last")
        .reset_index(drop=True)
    )
    merged = merged[PREDICTION_COLUMNS]
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False)
    return merged


def upsert_forecast_metrics(
    csv_path: str | Path,
    new_metrics_df: pd.DataFrame,
    replace_models: tuple[str, ...] | list[str] = SARIMA_MODEL_NAMES,
) -> pd.DataFrame:
    """Cập nhật lũy đẳng (idempotent upsert) bảng sai số vào forecast_metrics.csv, giữ nguyên các mô hình khác."""
    path = Path(csv_path)
    metric_cols = ["model_name", "eval_period", "n_months", "mae", "rmse", "mape"]
    if path.is_file():
        existing = pd.read_csv(path)
        kept = existing[~existing["model_name"].isin(replace_models)].copy()
        merged = pd.concat([kept, new_metrics_df], ignore_index=True)
    else:
        merged = new_metrics_df.copy()

    model_rank = {name: i for i, name in enumerate(ALL_MODELS)}
    period_rank = {p: i for i, p in enumerate(PERIOD_ORDER)}
    merged["_model_rank"] = merged["model_name"].map(model_rank).fillna(99)
    merged["_period_rank"] = merged["eval_period"].map(period_rank).fillna(99)
    merged = (
        merged.sort_values(["_model_rank", "model_name", "_period_rank"])
        .drop(columns=["_model_rank", "_period_rank"])
        .drop_duplicates(["model_name", "eval_period"], keep="last")
        .reset_index(drop=True)
    )
    merged = merged[metric_cols]
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False)
    return merged
