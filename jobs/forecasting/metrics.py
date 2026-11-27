"""CLI kiểm định và tính toán sai số cho file dự báo CSV."""
import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import (  # noqa: E402
    EVAL_PERIODS,
    PREDICTION_COLUMNS,
    SUBMISSION_COLUMNS,
    build_prediction_frame,
    build_submission_template,
    compute_error_metrics,
    evaluate_predictions,
    validate_predictions,
)

DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"

__all__ = [
    "EVAL_PERIODS",
    "SUBMISSION_COLUMNS",
    "PREDICTION_COLUMNS",
    "DEFAULT_MART_CSV",
    "compute_error_metrics",
    "build_prediction_frame",
    "evaluate_predictions",
    "validate_predictions",
    "build_submission_template",
    "main",
]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate and score model submission CSV")
    parser.add_argument("--validate", type=Path, required=True, help="Path to submission CSV")
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV)
    parser.add_argument("--models", nargs="+", default=None, help="Expected model_name values")
    args = parser.parse_args(argv)

    mart_df = pd.read_csv(args.mart_csv)
    pred_df = pd.read_csv(args.validate)
    validate_predictions(pred_df, mart_df, expected_models=args.models)
    print("=== VALIDATION: PASS ===")
    print(evaluate_predictions(pred_df).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
