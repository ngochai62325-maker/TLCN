"""CLI huấn luyện và đánh giá mô hình XGBoost (xgb_base và xgb_competitor)."""
import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import validate_model_predictions  # noqa: E402
from forecasting.xgboost_model import (  # noqa: E402
    BASE_FEATURES,
    COMPETITOR_EXTRA_FEATURES,
    COMPETITOR_FEATURES,
    DEFAULT_PARAM_GRID,
    RANDOM_STATE,
    SCENARIOS,
    _iter_param_grid,
    _make_regressor,
    extract_feature_importance,
    run_xgboost_pipeline,
    tune_xgboost_timeseries,
)

MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
OUT_DIR = ROOT / "exports" / "forecast" / "models"

__all__ = [
    "MART_CSV",
    "OUT_DIR",
    "BASE_FEATURES",
    "COMPETITOR_EXTRA_FEATURES",
    "COMPETITOR_FEATURES",
    "SCENARIOS",
    "DEFAULT_PARAM_GRID",
    "RANDOM_STATE",
    "_iter_param_grid",
    "_make_regressor",
    "tune_xgboost_timeseries",
    "extract_feature_importance",
    "run_xgboost_pipeline",
    "main",
]


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Train and evaluate XGBoost Scenario A (xgb_base) and Scenario B (xgb_competitor)"
    )
    parser.add_argument("--mart-csv", type=Path, default=MART_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--n-perm-repeats", type=int, default=30)
    args = parser.parse_args(argv)

    mart_df = pd.read_csv(args.mart_csv)
    pred_df, metrics_df, fi_df, tuning_summary = run_xgboost_pipeline(
        mart_df,
        n_splits=args.n_splits,
        n_perm_repeats=args.n_perm_repeats,
    )
    val_report = validate_model_predictions(
        pred_df,
        mart_df,
        expected_models=("xgb_base", "xgb_competitor"),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pred_path = args.output_dir / "xgb_predictions.csv"
    metrics_path = args.output_dir / "xgb_metrics.csv"
    fi_path = args.output_dir / "xgb_feature_importance.csv"
    tuning_path = args.output_dir / "xgb_tuning_summary.json"

    pred_df.to_csv(pred_path, index=False)
    metrics_df.to_csv(metrics_path, index=False)
    fi_df.to_csv(fi_path, index=False)
    tuning_path.write_text(json.dumps(tuning_summary, indent=2), encoding="utf-8")

    print("=== VALIDATION REPORT ===")
    print(json.dumps(val_report, indent=2))
    print("\n=== TUNING SUMMARY (TRAIN 2010-2022 ONLY) ===")
    for name, info in tuning_summary.items():
        print(
            f"{name}: cv_rmse={info['cv_rmse']}, cv_mae={info['cv_mae']}, "
            f"train_rmse={info['train_in_sample_rmse']}, best_params={info['best_params']}"
        )
    print("\n=== EVALUATION METRICS (TEST 2023-2025) ===")
    print(metrics_df.to_string(index=False))
    print("\n=== TOP 10 FEATURE IMPORTANCE (xgb_competitor by gain) ===")
    top_comp = fi_df[fi_df["model_name"] == "xgb_competitor"].head(10)
    print(
        top_comp[
            ["feature", "feature_group", "gain", "gain_share_pct", "perm_train_rmse_mean", "perm_test_rmse_mean"]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
