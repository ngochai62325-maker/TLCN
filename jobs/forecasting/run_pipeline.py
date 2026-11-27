import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "src"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from jobs.forecasting import (
    audit_dq19_cross_check,
    build_forecasting_mart,
    extract_gold_prices,
    run_naive_baseline,
    run_sarima,
    run_xgboost_models,
    summarize_forecasts,
)

DEFAULT_OUT_DIR = ROOT / "exports" / "forecast"
DEFAULT_RAW_CSV = DEFAULT_OUT_DIR / "gold_prices_monthly.csv"
DEFAULT_MART_CSV = DEFAULT_OUT_DIR / "forecasting_mart.csv"


def run_pipeline(
    start_from="raw-csv",
    raw_csv=DEFAULT_RAW_CSV,
    mart_csv=DEFAULT_MART_CSV,
    out_dir=DEFAULT_OUT_DIR,
    trino_url=extract_gold_prices.TRINO_URL,
    skip_batch_check=False,
    skip_audit=False,
    n_splits=5,
    n_perm_repeats=30,
):
    out_dir = Path(out_dir)
    models_dir = out_dir / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    active_raw = Path(raw_csv)
    active_mart = Path(mart_csv)

    if start_from == "gold":
        extract_gold_prices.extract_all(
            out_dir=out_dir,
            trino_url=trino_url,
            skip_batch_check=skip_batch_check,
        )
        active_raw = out_dir / "gold_prices_monthly.csv"

    if start_from in ("gold", "raw-csv"):
        build_forecasting_mart.main([
            "--input-csv", str(active_raw),
            "--output-dir", str(out_dir),
        ])
        active_mart = out_dir / "forecasting_mart.csv"

    run_naive_baseline.main([
        "--mart-csv", str(active_mart),
        "--output-dir", str(models_dir),
    ])

    run_sarima.main([
        "--source", "mart",
        "--mart-csv", str(active_mart),
        "--output-dir", str(out_dir),
    ])

    run_xgboost_models.main([
        "--mart-csv", str(active_mart),
        "--output-dir", str(models_dir),
        "--n-splits", str(n_splits),
        "--n-perm-repeats", str(n_perm_repeats),
    ])

    pred_df, metrics_df = summarize_forecasts.run_summary(
        mart_csv=active_mart,
        models_dir=models_dir,
        out_dir=out_dir,
        require_all=True,
        gold_csv=active_raw if active_raw.is_file() else None,
        fi_csv=models_dir / "xgb_feature_importance.csv",
    )

    if not skip_audit and active_raw.is_file():
        audit_code = audit_dq19_cross_check.main([
            "--raw-csv", str(active_raw),
            "--mart-csv", str(active_mart),
            "--export-dir", str(out_dir),
            "--report-path", str(out_dir / "dq19_cross_audit_report.json"),
            "--n-splits", str(n_splits),
            "--strict-warnings",
        ])
        if audit_code != 0:
            raise RuntimeError("DQ19 cross-audit failed")

    return pred_df, metrics_df


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start_from", choices=["gold", "raw-csv", "mart"], default="raw-csv")
    parser.add_argument("--raw-csv", type=Path, default=DEFAULT_RAW_CSV)
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--trino-url", default=extract_gold_prices.TRINO_URL)
    parser.add_argument("--skip-batch-check", action="store_true")
    parser.add_argument("--skip-audit", action="store_true")
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--n-perm-repeats", type=int, default=30)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    pred_df, metrics_df = run_pipeline(
        start_from=args.start_from,
        raw_csv=args.raw_csv,
        mart_csv=args.mart_csv,
        out_dir=args.output_dir,
        trino_url=args.trino_url,
        skip_batch_check=args.skip_batch_check,
        skip_audit=args.skip_audit,
        n_splits=args.n_splits,
        n_perm_repeats=args.n_perm_repeats,
    )
    print(f"DONE: {len(pred_df)} prediction rows, {len(metrics_df)} metric rows -> {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())