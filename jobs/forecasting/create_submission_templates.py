"""CLI tạo các file mẫu nộp kết quả dự báo cho SARIMA và XGBoost."""
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.metrics import build_submission_template  # noqa: E402

MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
TEMPLATE_DIR = ROOT / "exports" / "forecast" / "models" / "templates"
TEMPLATES = {
    "sarima_predictions_template.csv": ["sarima"],
    "xgb_predictions_template.csv": ["xgb_base", "xgb_competitor"],
}


def main():
    mart_df = pd.read_csv(MART_CSV)
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    for filename, models in TEMPLATES.items():
        out_path = TEMPLATE_DIR / filename
        df = build_submission_template(mart_df, models)
        df.to_csv(out_path, index=False)
        print(f"Wrote {out_path} ({len(df)} rows, models={models})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
