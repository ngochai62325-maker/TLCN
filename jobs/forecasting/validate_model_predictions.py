import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
EXPECTED_MONTHS = [y * 100 + m for y in range(2023, 2026) for m in range(1, 13)]
REQUIRED_COLS = ["month_key", "model_name", "split", "actual_usd_t", "predicted_usd_t"]
DEFAULT_TOL = 1e-6


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_model_predictions(
    pred_df,
    mart_df,
    expected_models=None,
    expected_months=EXPECTED_MONTHS,
    tol=DEFAULT_TOL,
):
    missing_cols = [c for c in REQUIRED_COLS if c not in pred_df.columns]
    _require(not missing_cols, f"Missing columns: {missing_cols}")
    _require(not pred_df.empty, "Prediction DataFrame is empty")

    nan_cols = [c for c in REQUIRED_COLS if pred_df[c].isna().any()]
    _require(not nan_cols, f"NaN found in columns: {nan_cols}")
    _require(pd.api.types.is_integer_dtype(pred_df["month_key"]), "month_key must be integer")

    for col in ("actual_usd_t", "predicted_usd_t"):
        vals = pd.to_numeric(pred_df[col], errors="coerce")
        _require(
            not pd.api.types.is_bool_dtype(pred_df[col]) and vals.notna().all() and np.isfinite(vals).all(),
            f"{col}: invalid numeric or Inf",
        )
    _require((pred_df["predicted_usd_t"].astype(float) > 0).all(), "predicted_usd_t must be > 0")
    _require(not pred_df.duplicated(["month_key", "model_name"]).any(), "Duplicate (month_key, model_name)")

    expected_list = list(expected_months)
    test_mart = mart_df[mart_df["split"] == "test"].sort_values("month_key")
    _require(test_mart["month_key"].astype("int64").tolist() == expected_list, "Mart test months mismatch")
    _require(test_mart["y"].notna().all(), "Mart 'y' contains NaN")

    models = pred_df["model_name"].drop_duplicates().tolist()
    if expected_models is not None:
        _require(
            set(models) == set(expected_models),
            f"Unexpected models: got {models}, expected {list(expected_models)}",
        )

    max_diff = 0.0
    mart_y = test_mart["y"].to_numpy(dtype=float)
    for name in models:
        sub = pred_df[pred_df["model_name"] == name].sort_values("month_key")
        _require((sub["split"] == "test").all(), f"{name}: non-test split found")

        months = sub["month_key"].astype("int64").tolist()
        if months != expected_list:
            missing = sorted(set(expected_list) - set(months))
            extra = sorted(set(months) - set(expected_list))
            raise ValueError(f"{name}: month mismatch (missing={missing}, extra={extra})")

        pred_y = sub["actual_usd_t"].to_numpy(dtype=float)
        diffs = np.abs(pred_y - mart_y)
        model_max_diff = float(diffs.max())
        max_diff = max(max_diff, model_max_diff)
        bad_mask = ~np.isclose(pred_y, mart_y, atol=tol, rtol=0.0)
        if bad_mask.any():
            bad_months = sub.loc[bad_mask, "month_key"].astype("int64").tolist()
            raise ValueError(
                f"{name}: actual_usd_t mismatch on {bad_months} (max_diff={model_max_diff:.6f}, tol={tol})"
            )

    return {
        "status": "PASS",
        "models": models,
        "n_rows": len(pred_df),
        "n_months_per_model": len(expected_list),
        "month_range": [expected_list[0], expected_list[-1]],
        "max_actual_diff": max_diff,
    }


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
