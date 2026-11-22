"""CLI xây dựng bảng đặc trưng forecasting_mart.csv và kiểm định chống rò rỉ DQ19."""
import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.mart import (  # noqa: E402
    LAGS,
    MART_COLUMNS,
    TARGET_START,
    TRAIN_END,
    WINDOWS,
    _same_val,
    build_mart,
    check_dq19,
    warmup_target,
)

INPUT_CSV = ROOT / "exports" / "forecast" / "gold_prices_monthly.csv"
OUT_DIR = ROOT / "exports" / "forecast"

__all__ = [
    "INPUT_CSV",
    "OUT_DIR",
    "TARGET_START",
    "TRAIN_END",
    "LAGS",
    "WINDOWS",
    "MART_COLUMNS",
    "warmup_target",
    "build_mart",
    "_same_val",
    "check_dq19",
    "main",
]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)

    raw = pd.read_csv(args.input_csv)
    mart_path = args.output_dir / "forecasting_mart.csv"
    report_path = args.output_dir / "dq19_mart_report.json"

    if args.verify_only:
        mart = pd.read_csv(mart_path)
    else:
        mart = build_mart(raw)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        mart.to_csv(mart_path, index=False)

    report = check_dq19(raw, mart)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
