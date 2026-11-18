import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from jobs.forecasting.metrics import (
    SUBMISSION_COLUMNS,
    build_prediction_frame,
    evaluate_predictions,
    validate_predictions,
)

ROOT = Path(__file__).resolve().parents[2]
MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
OUT_DIR = ROOT / "exports" / "forecast" / "models"


def predict_naive(mart_df):
    df = mart_df.sort_values("month_key").reset_index(drop=True)
    assert np.allclose(df.loc[1:, "y_lag_1"], df["y"].shift(1).iloc[1:], atol=1e-8, rtol=0)
    test_df = df[df["split"] == "test"]
    return build_prediction_frame(
        month_key=test_df["month_key"],
        model_name="naive",
        actual_usd_t=test_df["y"],
        predicted_usd_t=test_df["y_lag_1"],
    )


def build_submission_template(mart_df):
    test_df = mart_df[mart_df["split"] == "test"].sort_values("month_key")
    return pd.DataFrame({
        "month_key": test_df["month_key"].astype("int64"),
        "model_name": "<sarima|xgb_base|xgb_competitor>",
        "split": "test",
        "actual_usd_t": test_df["y"].astype(float),
        "predicted_usd_t": np.nan,
    })[SUBMISSION_COLUMNS]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart-csv", type=Path, default=MART_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args(argv)

    mart_df = pd.read_csv(args.mart_csv)
    pred_df = predict_naive(mart_df)
    validate_predictions(pred_df, mart_df, expected_models=("naive",))
    metrics_df = evaluate_predictions(pred_df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pred_df.to_csv(args.output_dir / "naive_predictions.csv", index=False)
    metrics_df.to_csv(args.output_dir / "naive_metrics.csv", index=False)
    build_submission_template(mart_df).to_csv(args.output_dir / "submission_template_test.csv", index=False)

    worst = pred_df.loc[pred_df["error_usd_t"].abs().nlargest(5).index]
    print("=== NAIVE METRICS (202301..202512) ===")
    print(metrics_df.to_string(index=False))
    print("\n=== TOP 5 LARGEST ERRORS ===")
    print(worst[["month_key", "actual_usd_t", "predicted_usd_t", "error_usd_t", "abs_pct_error"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
