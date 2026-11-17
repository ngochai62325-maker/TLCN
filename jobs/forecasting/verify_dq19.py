#!/usr/bin/env python3
"""Independent DQ19 anti-leakage verifier reading forecasting_mart.csv directly from disk."""
import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jobs.forecasting.build_forecasting_mart import (
    DEFAULT_INPUT_CSV as DEFAULT_RAW_CSV,
    DEFAULT_INPUT_PROVENANCE as DEFAULT_RAW_PROVENANCE,
    LAG_HORIZONS,
    MART_COLUMNS,
    ROLLING_WINDOWS,
    build_warmup_target_series,
    load_extracted_prices,
    verify_no_leakage_dq19,
)

DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
DEFAULT_MART_PROVENANCE = ROOT / "exports" / "forecast" / "forecasting_mart_provenance.json"
DEFAULT_REPORT_PATH = ROOT / "exports" / "forecast" / "dq19_mart_report.json"

INT_COLUMNS = {
    "month_key",
    "month_of_year",
    "india_white_quote_missing_lag_1",
    "india_parb_missing_lag_1",
    "pak5_missing_lag_1",
}
FLOAT_COLUMNS = {
    *(f"y_roll_mean_{w}" for w in ROLLING_WINDOWS),
    *(f"y_roll_std_{w}" for w in ROLLING_WINDOWS),
}
DECIMAL_COLUMNS = {
    "y",
    "target_delta",
    *(f"y_lag_{k}" for k in LAG_HORIZONS),
    "urea_lag_1",
    "crude_oil_lag_1",
    "thai5_lag_1",
    "india_parb_lag_1",
    "pak5_lag_1",
    "spread_vn_thai_lag_1",
    "spread_vn_india_lag_1",
}

CAUSAL_LAG1_TO_RAW = {
    "y_lag_1": "wb_vn5_usd_t",
    "urea_lag_1": "wb_urea_usd_t",
    "crude_oil_lag_1": "wb_crude_oil_usd_bbl",
    "thai5_lag_1": "usda_thai5_usd_t",
    "india_parb_lag_1": "usda_india_parb5_usd_t",
    "pak5_lag_1": "usda_pak5_usd_t",
}


def _parse_int(raw: str, col: str) -> int:
    if raw is None or raw == "" or not raw.lstrip("-").isdigit():
        raise ValueError(f"Invalid integer cell in {col}: {raw!r}")
    return int(raw)


def _parse_decimal(raw: str, col: str) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        dec = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal cell in {col}: {raw!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"Non-finite decimal cell in {col}: {raw!r}")
    return dec


def _parse_float(raw: str, col: str) -> float:
    if raw is None or raw == "":
        raise ValueError(f"Unexpected empty rolling cell in {col}")
    val = float(raw)
    if val != val or abs(val) == float("inf"):
        raise ValueError(f"Non-finite float cell in {col}: {raw!r}")
    return val


def load_mart_csv(mart_csv: Path, mart_provenance: Path | None = None) -> tuple[list[dict], str, dict | None]:
    """Read and type-parse forecasting_mart.csv from disk, verifying schema and optional SHA-256."""
    mart_csv = Path(mart_csv)
    if not mart_csv.is_file():
        raise FileNotFoundError(f"Forecasting mart CSV not found: {mart_csv}")
    mart_sha256 = hashlib.sha256(mart_csv.read_bytes()).hexdigest()

    prov = None
    if mart_provenance is not None:
        mart_provenance = Path(mart_provenance)
        if not mart_provenance.is_file():
            raise FileNotFoundError(f"Mart provenance JSON not found: {mart_provenance}")
        prov = json.loads(mart_provenance.read_text(encoding="utf-8"))
        expected_sha = (prov.get("output_csv") or {}).get("sha256")
        if expected_sha and expected_sha != mart_sha256:
            raise RuntimeError(
                f"Mart CSV checksum mismatch against provenance: {mart_sha256} != {expected_sha}"
            )

    rows = []
    with mart_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != MART_COLUMNS:
            raise RuntimeError(f"Mart CSV header drift: expected {MART_COLUMNS}, got {reader.fieldnames}")
        for raw in reader:
            if None in raw or any(v is None for v in raw.values()):
                raise RuntimeError("Malformed CSV row in forecasting_mart.csv")
            if raw["split"] not in ("train", "test"):
                raise RuntimeError(f"Invalid split value: {raw['split']!r}")
            parsed = {"split": raw["split"]}
            for col in MART_COLUMNS:
                if col == "split":
                    continue
                if col in INT_COLUMNS:
                    parsed[col] = _parse_int(raw[col], col)
                elif col in FLOAT_COLUMNS:
                    parsed[col] = _parse_float(raw[col], col)
                elif col in DECIMAL_COLUMNS:
                    parsed[col] = _parse_decimal(raw[col], col)
                else:
                    raise RuntimeError(f"Unhandled mart column: {col}")
            rows.append(parsed)
    return rows, mart_sha256, prov


def verify_non_contemporaneous_separation(rows_by_month: dict[int, dict], mart_rows: list[dict]) -> dict:
    """Confirm lag-1 features never match contemporaneous t+1 when t and t+1 differ."""
    stats = {}
    for mart_col, raw_col in CAUSAL_LAG1_TO_RAW.items():
        differing_months = 0
        for row in mart_rows:
            mk = row["month_key"]
            curr_val = rows_by_month[mk][raw_col]
            feat_val = row[mart_col]
            if feat_val != curr_val:
                differing_months += 1
        if differing_months == 0:
            raise RuntimeError(f"DQ19 failed: {mart_col} matches contemporaneous {raw_col} on every row")
        stats[mart_col] = {"raw_column": raw_col, "rows_distinct_from_target_month": differing_months}
    return stats


def run_dq19_audit(
    mart_csv: Path = DEFAULT_MART_CSV,
    raw_csv: Path = DEFAULT_RAW_CSV,
    mart_provenance: Path | None = DEFAULT_MART_PROVENANCE,
    raw_provenance: Path | None = DEFAULT_RAW_PROVENANCE,
    report_path: Path | None = DEFAULT_REPORT_PATH,
) -> dict:
    """Execute full disk-to-disk DQ19 anti-leakage verification on forecasting_mart.csv."""
    rows_by_month, raw_prov = load_extracted_prices(raw_csv, provenance_path=raw_provenance)
    warmup_y, _ = build_warmup_target_series(rows_by_month)
    mart_rows, mart_sha256, mart_prov = load_mart_csv(mart_csv, mart_provenance=mart_provenance)

    dq19_summary = verify_no_leakage_dq19(rows_by_month, warmup_y, mart_rows, float_tol=1e-8)
    separation_stats = verify_non_contemporaneous_separation(rows_by_month, mart_rows)

    feature_columns = [c for c in MART_COLUMNS if c not in ("month_key", "split", "y", "target_delta")]
    report = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "mart_csv": str(Path(mart_csv)),
        "mart_csv_sha256": mart_sha256,
        "raw_csv": str(Path(raw_csv)),
        "raw_csv_sha256": hashlib.sha256(Path(raw_csv).read_bytes()).hexdigest(),
        "gold_batch": (mart_prov or raw_prov or {}).get("gold_batch"),
        "dq19_status": dq19_summary["dq19_status"],
        "leakage_errors": dq19_summary["leakage_errors"],
        "rows_checked": len(mart_rows),
        "train_rows": dq19_summary["train_rows"],
        "test_rows": dq19_summary["test_rows"],
        "feature_columns_checked": feature_columns,
        "non_contemporaneous_checks": separation_stats,
        "null_counts": dq19_summary["null_counts"],
        "missing_flag_counts": dq19_summary["missing_flag_counts"],
    }

    if report_path is not None:
        report_path = Path(report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV, help=f"Path to forecasting_mart.csv (default: {DEFAULT_MART_CSV})")
    parser.add_argument("--raw-csv", type=Path, default=DEFAULT_RAW_CSV, help=f"Path to gold_prices_monthly.csv (default: {DEFAULT_RAW_CSV})")
    parser.add_argument("--mart-provenance", type=Path, default=DEFAULT_MART_PROVENANCE, help="Mart provenance JSON")
    parser.add_argument("--raw-provenance", type=Path, default=DEFAULT_RAW_PROVENANCE, help="Extraction provenance JSON")
    parser.add_argument("--skip-provenance-check", action="store_true", help="Skip SHA-256 verification against provenance JSONs")
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH, help=f"Output DQ19 report path (default: {DEFAULT_REPORT_PATH})")
    args = parser.parse_args(argv)

    report = run_dq19_audit(
        mart_csv=args.mart_csv,
        raw_csv=args.raw_csv,
        mart_provenance=None if args.skip_provenance_check else args.mart_provenance,
        raw_provenance=None if args.skip_provenance_check else args.raw_provenance,
        report_path=args.report_path,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
