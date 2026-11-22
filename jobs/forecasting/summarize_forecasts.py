"""CLI tổng hợp dự báo các mô hình, xuất bảng tổng hợp và vẽ biểu đồ so sánh."""
import argparse
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import evaluate_predictions  # noqa: E402
from forecasting.summary import (  # noqa: E402
    ALL_MODELS,
    METRIC_SPECS,
    MODEL_FILES,
    MODEL_STYLES,
    PERIOD_ORDER,
    PERIOD_SPECS,
    SHOCK_SPAN,
    SPLIT_2024,
    draw_actual_vs_pred,
    load_and_merge_predictions as _load_and_merge_predictions,
    plot_actual_vs_predicted,
    plot_actual_vs_predicted_by_period,
    plot_metrics_by_period,
)

MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
MODELS_DIR = ROOT / "exports" / "forecast" / "models"
OUT_DIR = ROOT / "exports" / "forecast"

__all__ = [
    "MART_CSV",
    "MODELS_DIR",
    "OUT_DIR",
    "MODEL_FILES",
    "ALL_MODELS",
    "MODEL_STYLES",
    "SHOCK_SPAN",
    "SPLIT_2024",
    "PERIOD_SPECS",
    "PERIOD_ORDER",
    "METRIC_SPECS",
    "load_and_merge_predictions",
    "draw_actual_vs_pred",
    "plot_actual_vs_predicted",
    "plot_actual_vs_predicted_by_period",
    "plot_metrics_by_period",
    "run_summary",
    "main",
]


def load_and_merge_predictions(mart_df, models_dir=MODELS_DIR, require_all=False):
    return _load_and_merge_predictions(mart_df, models_dir=models_dir, require_all=require_all)


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
