#!/usr/bin/env python3
"""Build the anti-leakage Forecasting Data Mart (D1-D5 + DQ19) from extracted Gold prices."""
import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_CSV = ROOT / "exports" / "forecast" / "gold_prices_monthly.csv"
DEFAULT_INPUT_PROVENANCE = ROOT / "exports" / "forecast" / "gold_extraction_provenance.json"
DEFAULT_OUTPUT_DIR = ROOT / "exports" / "forecast"

WARMUP_START_MONTH_KEY = 200901
TARGET_START_MONTH_KEY = 201001
TRAIN_END_MONTH_KEY = 202212
TEST_START_MONTH_KEY = 202301
TARGET_END_MONTH_KEY = 202512

LAG_HORIZONS = (1, 2, 3, 6, 12)
ROLLING_WINDOWS = (3, 6, 12)

MART_COLUMNS = [
    "month_key",
    "split",
    "y",
    "target_delta",
    "y_lag_1",
    "y_lag_2",
    "y_lag_3",
    "y_lag_6",
    "y_lag_12",
    "y_roll_mean_3",
    "y_roll_mean_6",
    "y_roll_mean_12",
    "y_roll_std_3",
    "y_roll_std_6",
    "y_roll_std_12",
    "month_of_year",
    "urea_lag_1",
    "crude_oil_lag_1",
    "thai5_lag_1",
    "india_parb_lag_1",
    "pak5_lag_1",
    "india_white_quote_missing_lag_1",
    "india_parb_missing_lag_1",
    "pak5_missing_lag_1",
    "spread_vn_thai_lag_1",
    "spread_vn_india_lag_1",
]

REQUIRED_INPUT_COLS = (
    "month_key",
    "wb_vn5_usd_t",
    "wb_urea_usd_t",
    "wb_crude_oil_usd_bbl",
    "usda_thai5_usd_t",
    "usda_india_broken5_usd_t",
    "usda_india_parb5_usd_t",
    "usda_pak5_usd_t",
)


def _month_index(month_key: int) -> int:
    year, month = divmod(month_key, 100)
    if not (1900 <= year <= 2100 and 1 <= month <= 12):
        raise ValueError(f"Invalid YYYYMM month_key: {month_key}")
    return year * 12 + (month - 1)


def _index_to_month_key(idx: int) -> int:
    year, month_zero = divmod(idx, 12)
    return year * 100 + month_zero + 1


def _calendar_range(start_month_key: int, end_month_key: int) -> list[int]:
    lo, hi = _month_index(start_month_key), _month_index(end_month_key)
    if lo > hi:
        raise ValueError(f"Reversed range: {start_month_key}..{end_month_key}")
    return [_index_to_month_key(i) for i in range(lo, hi + 1)]


def _parse_optional_decimal(raw: str) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        dec = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric cell: {raw!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"Non-finite numeric cell: {raw!r}")
    return dec


def _format_cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if math.isnan(value) or not math.isfinite(value):
            return ""
        return f"{value:.8f}".rstrip("0").rstrip(".") if "." in f"{value:.8f}" else f"{value:.8f}"
    return str(value)


def load_extracted_prices(csv_path: Path, provenance_path: Path | None = None) -> tuple[dict[int, dict], dict | None]:
    """Load extracted monthly prices CSV and verify input SHA-256 and calendar spine."""
    csv_path = Path(csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Extracted Gold CSV not found: {csv_path}")

    input_sha256 = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    input_prov = None
    if provenance_path is not None:
        provenance_path = Path(provenance_path)
        if not provenance_path.is_file():
            raise FileNotFoundError(f"Extraction provenance JSON not found: {provenance_path}")
        input_prov = json.loads(provenance_path.read_text(encoding="utf-8"))
        expected_sha = (input_prov.get("output_csv") or {}).get("sha256")
        if expected_sha and expected_sha != input_sha256:
            raise RuntimeError(
                f"Input CSV checksum mismatch against provenance: {input_sha256} != {expected_sha}"
            )

    rows_by_month = {}
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not set(REQUIRED_INPUT_COLS).issubset(reader.fieldnames):
            raise ValueError(f"Missing required input columns in {csv_path}: {reader.fieldnames}")
        for raw in reader:
            mk = int(raw["month_key"])
            _month_index(mk)
            if mk in rows_by_month:
                raise RuntimeError(f"Duplicate month_key in extracted CSV: {mk}")
            rows_by_month[mk] = {
                col: _parse_optional_decimal(raw[col])
                for col in REQUIRED_INPUT_COLS
                if col != "month_key"
            }

    expected_months = _calendar_range(WARMUP_START_MONTH_KEY, TARGET_END_MONTH_KEY)
    if sorted(rows_by_month) != expected_months:
        raise RuntimeError(
            f"Expected dense 204-month spine {WARMUP_START_MONTH_KEY}..{TARGET_END_MONTH_KEY}, "
            f"got {len(rows_by_month)} rows"
        )
    return rows_by_month, input_prov


def build_warmup_target_series(rows_by_month: dict[int, dict]) -> tuple[dict[int, Decimal], dict]:
    """Build warm-up y series applying D2 linear interpolation ONLY at 200903."""
    target_months = _calendar_range(TARGET_START_MONTH_KEY, TARGET_END_MONTH_KEY)
    missing_target = [mk for mk in target_months if rows_by_month[mk]["wb_vn5_usd_t"] is None]
    if missing_target:
        raise RuntimeError(f"Target WB VN 5% must have 0 missing values in 2010..2025; found {missing_target}")

    warmup_months = _calendar_range(WARMUP_START_MONTH_KEY, 200912)
    missing_warmup = [mk for mk in warmup_months if rows_by_month[mk]["wb_vn5_usd_t"] is None]
    if missing_warmup != [200903]:
        raise RuntimeError(f"D2 expects only 200903 missing in 2009 warm-up; found {missing_warmup}")

    left = rows_by_month[200902]["wb_vn5_usd_t"]
    right = rows_by_month[200904]["wb_vn5_usd_t"]
    if left is None or right is None:
        raise RuntimeError("Cannot linearly interpolate 200903: missing 200902 or 200904")
    interpolated_200903 = (left + right) / Decimal(2)

    warmup_y = {}
    for mk, rec in rows_by_month.items():
        val = rec["wb_vn5_usd_t"]
        warmup_y[mk] = interpolated_200903 if mk == 200903 else val

    interp_info = {
        "month_key": 200903,
        "method": "linear_midpoint_from_neighbors",
        "left_month_key": 200902,
        "left_value": format(left, "f"),
        "right_month_key": 200904,
        "right_value": format(right, "f"),
        "interpolated_value": format(interpolated_200903, "f"),
        "used_only_for_lags_and_rolling": True,
    }
    return warmup_y, interp_info


def _shift_month(month_key: int, lag: int) -> int:
    return _index_to_month_key(_month_index(month_key) - lag)


def build_mart_rows(rows_by_month: dict[int, dict], warmup_y: dict[int, Decimal]) -> list[dict]:
    """Generate the 192 target rows (201001..202512) following decisions D1-D5."""
    target_months = _calendar_range(TARGET_START_MONTH_KEY, TARGET_END_MONTH_KEY)
    mart_rows = []
    for mk in target_months:
        t_prev = _shift_month(mk, 1)
        prev_obs = rows_by_month[t_prev]
        y_curr = rows_by_month[mk]["wb_vn5_usd_t"]
        y_prev = warmup_y[t_prev]
        if y_curr is None or y_prev is None:
            raise RuntimeError(f"Missing target or lag-1 price at {mk}")

        split = "train" if mk <= TRAIN_END_MONTH_KEY else "test"
        row = {
            "month_key": mk,
            "split": split,
            "y": y_curr,
            "target_delta": y_curr - y_prev,
        }

        for k in LAG_HORIZONS:
            row[f"y_lag_{k}"] = warmup_y[_shift_month(mk, k)]

        for w in ROLLING_WINDOWS:
            # Window covers strictly months [t+1-w .. t], never including target month t+1.
            window_vals = [float(warmup_y[_shift_month(mk, offset)]) for offset in range(w, 0, -1)]
            row[f"y_roll_mean_{w}"] = statistics.fmean(window_vals)
            row[f"y_roll_std_{w}"] = statistics.stdev(window_vals)

        thai5_prev = prev_obs["usda_thai5_usd_t"]
        india_broken_prev = prev_obs["usda_india_broken5_usd_t"]
        india_parb_prev = prev_obs["usda_india_parb5_usd_t"]
        pak5_prev = prev_obs["usda_pak5_usd_t"]

        row["month_of_year"] = mk % 100
        row["urea_lag_1"] = prev_obs["wb_urea_usd_t"]
        row["crude_oil_lag_1"] = prev_obs["wb_crude_oil_usd_bbl"]
        row["thai5_lag_1"] = thai5_prev
        row["india_parb_lag_1"] = india_parb_prev
        row["pak5_lag_1"] = pak5_prev
        row["india_white_quote_missing_lag_1"] = 1 if india_broken_prev is None else 0
        row["india_parb_missing_lag_1"] = 1 if india_parb_prev is None else 0
        row["pak5_missing_lag_1"] = 1 if pak5_prev is None else 0
        row["spread_vn_thai_lag_1"] = None if thai5_prev is None else (y_prev - thai5_prev)
        row["spread_vn_india_lag_1"] = None if india_parb_prev is None else (y_prev - india_parb_prev)
        mart_rows.append(row)
    return mart_rows


def verify_no_leakage_dq19(
    rows_by_month: dict[int, dict],
    warmup_y: dict[int, Decimal],
    mart_rows: list[dict],
    float_tol: float = 1e-10,
) -> dict:
    """Automated DQ19 verification proving every feature at target month t+1 uses only months <= t."""
    expected_months = _calendar_range(TARGET_START_MONTH_KEY, TARGET_END_MONTH_KEY)
    if [r["month_key"] for r in mart_rows] != expected_months:
        raise RuntimeError("DQ19 failed: month_key grid mismatch or duplicate/unsorted rows")

    train_rows = [r for r in mart_rows if r["split"] == "train"]
    test_rows = [r for r in mart_rows if r["split"] == "test"]
    if len(train_rows) != 156 or len(test_rows) != 36:
        raise RuntimeError(f"DQ19 failed: expected 156 train / 36 test rows, got {len(train_rows)}/{len(test_rows)}")
    if any(r["month_key"] > TRAIN_END_MONTH_KEY for r in train_rows) or any(
        r["month_key"] < TEST_START_MONTH_KEY for r in test_rows
    ):
        raise RuntimeError("DQ19 failed: temporal train/test split boundary violation")

    # Mask raw observations at or after target month t+1 during independent recomputation.
    for row in mart_rows:
        mk = row["month_key"]
        causal_raw = {m: rec for m, rec in rows_by_month.items() if m < mk}
        causal_y = {m: val for m, val in warmup_y.items() if m < mk}
        t_prev = _shift_month(mk, 1)

        if row["y"] != rows_by_month[mk]["wb_vn5_usd_t"]:
            raise RuntimeError(f"DQ19 failed at {mk}: target y mismatch")
        if row["target_delta"] != rows_by_month[mk]["wb_vn5_usd_t"] - causal_raw[t_prev]["wb_vn5_usd_t"]:
            raise RuntimeError(f"DQ19 failed at {mk}: target_delta mismatch")
        if row["month_of_year"] != mk % 100:
            raise RuntimeError(f"DQ19 failed at {mk}: month_of_year mismatch")

        for k in LAG_HORIZONS:
            if row[f"y_lag_{k}"] != causal_y[_shift_month(mk, k)]:
                raise RuntimeError(f"DQ19 failed at {mk}: y_lag_{k} causality mismatch")

        for w in ROLLING_WINDOWS:
            win = [float(causal_y[_shift_month(mk, o)]) for o in range(w, 0, -1)]
            if abs(row[f"y_roll_mean_{w}"] - statistics.fmean(win)) > float_tol:
                raise RuntimeError(f"DQ19 failed at {mk}: y_roll_mean_{w} mismatch")
            if abs(row[f"y_roll_std_{w}"] - statistics.stdev(win)) > float_tol:
                raise RuntimeError(f"DQ19 failed at {mk}: y_roll_std_{w} mismatch")

        prev = causal_raw[t_prev]
        if row["urea_lag_1"] != prev["wb_urea_usd_t"] or row["crude_oil_lag_1"] != prev["wb_crude_oil_usd_bbl"]:
            raise RuntimeError(f"DQ19 failed at {mk}: cost feature causality mismatch")
        if (
            row["thai5_lag_1"] != prev["usda_thai5_usd_t"]
            or row["india_parb_lag_1"] != prev["usda_india_parb5_usd_t"]
            or row["pak5_lag_1"] != prev["usda_pak5_usd_t"]
        ):
            raise RuntimeError(f"DQ19 failed at {mk}: competitor feature causality mismatch")

        exp_white_flag = 1 if prev["usda_india_broken5_usd_t"] is None else 0
        exp_parb_flag = 1 if prev["usda_india_parb5_usd_t"] is None else 0
        exp_pak_flag = 1 if prev["usda_pak5_usd_t"] is None else 0
        if (
            row["india_white_quote_missing_lag_1"] != exp_white_flag
            or row["india_parb_missing_lag_1"] != exp_parb_flag
            or row["pak5_missing_lag_1"] != exp_pak_flag
        ):
            raise RuntimeError(f"DQ19 failed at {mk}: missing indicator flag mismatch")

        if row["india_parb_missing_lag_1"] == 1 and row["india_parb_lag_1"] is not None:
            raise RuntimeError(f"DQ19/D5 failed at {mk}: India Parboiled missing flag set with non-null value")
        if row["pak5_missing_lag_1"] == 1 and row["pak5_lag_1"] is not None:
            raise RuntimeError(f"DQ19/D5 failed at {mk}: Pakistan missing flag set with non-null value")

        exp_spread_thai = None if prev["usda_thai5_usd_t"] is None else causal_y[t_prev] - prev["usda_thai5_usd_t"]
        exp_spread_india = (
            None if prev["usda_india_parb5_usd_t"] is None else causal_y[t_prev] - prev["usda_india_parb5_usd_t"]
        )
        if row["spread_vn_thai_lag_1"] != exp_spread_thai or row["spread_vn_india_lag_1"] != exp_spread_india:
            raise RuntimeError(f"DQ19 failed at {mk}: spread causality mismatch")

    # D5 verification: ensure zero unexpected NaNs in non-competitor columns and exact NaN preservation in competitors.
    always_complete = [
        "y",
        "target_delta",
        *(f"y_lag_{k}" for k in LAG_HORIZONS),
        *(f"y_roll_mean_{w}" for w in ROLLING_WINDOWS),
        *(f"y_roll_std_{w}" for w in ROLLING_WINDOWS),
        "month_of_year",
        "urea_lag_1",
        "crude_oil_lag_1",
        "thai5_lag_1",
        "india_white_quote_missing_lag_1",
        "india_parb_missing_lag_1",
        "pak5_missing_lag_1",
        "spread_vn_thai_lag_1",
    ]
    for col in always_complete:
        if any(r[col] is None for r in mart_rows):
            raise RuntimeError(f"Unexpected NaN in required complete column: {col}")

    null_counts = {
        col: sum(1 for r in mart_rows if r[col] is None)
        for col in MART_COLUMNS
        if any(r[col] is None for r in mart_rows)
    }
    flag_counts = {
        flag: sum(r[flag] for r in mart_rows)
        for flag in ("india_white_quote_missing_lag_1", "india_parb_missing_lag_1", "pak5_missing_lag_1")
    }
    return {
        "dq19_status": "PASS",
        "leakage_errors": 0,
        "total_rows": len(mart_rows),
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "test_india_parb_non_null_rows": sum(1 for r in test_rows if r["india_parb_lag_1"] is not None),
        "test_india_white_quote_missing_rows": sum(r["india_white_quote_missing_lag_1"] for r in test_rows),
        "null_counts": null_counts,
        "missing_flag_counts": flag_counts,
    }


def write_mart_outputs(output_dir: Path, mart_rows: list[dict], provenance: dict) -> tuple[Path, Path]:
    """Write forecasting_mart.csv and forecasting_mart_provenance.json."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    mart_path = output_dir / "forecasting_mart.csv"
    prov_path = output_dir / "forecasting_mart_provenance.json"

    with mart_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(MART_COLUMNS)
        for row in mart_rows:
            writer.writerow([_format_cell(row[col]) for col in MART_COLUMNS])

    provenance["output_csv"] = {
        "filename": mart_path.name,
        "rows": len(mart_rows),
        "columns": MART_COLUMNS,
        "sha256": hashlib.sha256(mart_path.read_bytes()).hexdigest(),
    }
    prov_path.write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return mart_path, prov_path


def build_forecasting_mart(
    input_csv: Path = DEFAULT_INPUT_CSV,
    input_provenance: Path | None = DEFAULT_INPUT_PROVENANCE,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict:
    """Build and verify the 192-row Forecasting Data Mart from extracted Gold prices."""
    rows_by_month, extract_prov = load_extracted_prices(input_csv, provenance_path=input_provenance)
    warmup_y, interp_info = build_warmup_target_series(rows_by_month)
    mart_rows = build_mart_rows(rows_by_month, warmup_y)
    dq19_summary = verify_no_leakage_dq19(rows_by_month, warmup_y, mart_rows)

    provenance = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_csv": str(Path(input_csv)),
        "input_csv_sha256": hashlib.sha256(Path(input_csv).read_bytes()).hexdigest(),
        "gold_batch": extract_prov.get("gold_batch") if extract_prov else None,
        "gold_snapshots": (
            {k: v.get("snapshot_id") for k, v in (extract_prov.get("snapshots") or {}).items()}
            if extract_prov
            else None
        ),
        "decisions_applied": {
            "D1": "Added target_delta = y - y_lag_1 alongside y for XGBoost delta learning",
            "D2": interp_info,
            "D3": "One-step-ahead forecasting horizon h=1",
            "D4": "Temporal split frozen at train 201001..202212 (156) / test 202301..202512 (36)",
            "D5": "Preserved competitor missing values as NaN with explicit *_missing_lag_1 flags",
        },
        "rolling_std_ddof": 1,
        "spread_definitions": {
            "spread_vn_thai_lag_1": "y_lag_1 - thai5_lag_1",
            "spread_vn_india_lag_1": "y_lag_1 - india_parb_lag_1",
        },
        "dq19_verification": dq19_summary,
    }
    mart_path, prov_path = write_mart_outputs(output_dir, mart_rows, provenance)
    return {
        "status": "FORECASTING_MART_BUILT",
        "mart_path": str(mart_path),
        "provenance_path": str(prov_path),
        **dq19_summary,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV, help=f"Input Gold CSV (default: {DEFAULT_INPUT_CSV})")
    parser.add_argument(
        "--input-provenance",
        type=Path,
        default=DEFAULT_INPUT_PROVENANCE,
        help=f"Input provenance JSON (default: {DEFAULT_INPUT_PROVENANCE})",
    )
    parser.add_argument("--skip-provenance-check", action="store_true", help="Do not verify input CSV against extraction provenance JSON")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help=f"Output folder (default: {DEFAULT_OUTPUT_DIR})")
    args = parser.parse_args(argv)

    summary = build_forecasting_mart(
        input_csv=args.input_csv,
        input_provenance=None if args.skip_provenance_check else args.input_provenance,
        output_dir=args.output_dir,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
