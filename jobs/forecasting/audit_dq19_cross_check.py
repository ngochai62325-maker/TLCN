import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.audit import (
    audit_contracts_and_merge,
    audit_mart_dq19,
    audit_sarima_logic,
    audit_xgboost_logic,
    run_dq19_cross_audit,
)

DEFAULT_RAW_CSV = ROOT / "exports" / "forecast" / "gold_prices_monthly.csv"
DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
DEFAULT_EXPORT_DIR = ROOT / "exports" / "forecast"
DEFAULT_REPORT_PATH = DEFAULT_EXPORT_DIR / "dq19_cross_audit_report.json"

__all__ = [
    "DEFAULT_RAW_CSV",
    "DEFAULT_MART_CSV",
    "DEFAULT_EXPORT_DIR",
    "DEFAULT_REPORT_PATH",
    "audit_mart_dq19",
    "audit_sarima_logic",
    "audit_xgboost_logic",
    "audit_contracts_and_merge",
    "run_dq19_cross_audit",
    "main",
]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-csv", type=Path, default=DEFAULT_RAW_CSV)
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV)
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--strict-warnings", action="store_true")
    args = parser.parse_args(argv)

    report = run_dq19_cross_audit(
        raw_csv=args.raw_csv,
        mart_csv=args.mart_csv,
        export_dir=args.export_dir,
        strict_warnings=args.strict_warnings,
        n_splits=args.n_splits,
    )
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())