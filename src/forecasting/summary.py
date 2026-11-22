"""Tổng hợp dự báo đa mô hình, cập nhật bảng kết quả và trực quan hóa so sánh."""
from __future__ import annotations

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
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
