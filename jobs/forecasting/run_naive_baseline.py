"""CLI chạy mô hình cơ sở Naive và xuất kết quả ra exports/forecast/models/."""
import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import (  # noqa: E402
    build_submission_template,
    evaluate_predictions,
    validate_predictions,
)
from forecasting.naive import predict_naive  # noqa: E402

MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
OUT_DIR = ROOT / "exports" / "forecast" / "models"

__all__ = ["MART_CSV", "OUT_DIR", "predict_naive", "main"]


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
    build_submission_template(mart_df, "<sarima|xgb_base|xgb_competitor>").to_csv(
        args.output_dir / "submission_template_test.csv", index=False
    )

    worst = pred_df.loc[pred_df["error_usd_t"].abs().nlargest(5).index]
    print("=== NAIVE METRICS (202301..202512) ===")
    print(metrics_df.to_string(index=False))
    print("\n=== TOP 5 LARGEST ERRORS ===")
    print(worst[["month_key", "actual_usd_t", "predicted_usd_t", "error_usd_t", "abs_pct_error"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
