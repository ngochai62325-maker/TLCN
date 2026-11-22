"""CLI kiểm định schema và tính hợp lệ dữ liệu dự báo mô hình."""
import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import (  # noqa: E402
    DEFAULT_TOL,
    EXPECTED_MONTHS,
    REQUIRED_COLS,
    _require,
    validate_model_predictions,
)

DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"

__all__ = [
    "DEFAULT_MART_CSV",
    "EXPECTED_MONTHS",
    "REQUIRED_COLS",
    "DEFAULT_TOL",
    "_require",
    "validate_model_predictions",
    "main",
]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pred-csv", type=Path, required=True)
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV)
    parser.add_argument("--models", nargs="+", default=None)
    parser.add_argument("--tol", type=float, default=DEFAULT_TOL)
    args = parser.parse_args(argv)

    try:
        report = validate_model_predictions(
            pd.read_csv(args.pred_csv),
            pd.read_csv(args.mart_csv),
            expected_models=args.models,
            tol=args.tol,
        )
    except ValueError as exc:
        print(f"FAIL: {exc}")
        return 1

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
