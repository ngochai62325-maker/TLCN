import argparse
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"

EVAL_PERIODS = [
    ("2023-2025", 202301, 202512),
    ("2023", 202301, 202312),
    ("2024-2025", 202401, 202512),
]

SUBMISSION_COLUMNS = ["month_key", "model_name", "split", "actual_usd_t", "predicted_usd_t"]
PREDICTION_COLUMNS = [*SUBMISSION_COLUMNS, "error_usd_t", "abs_pct_error"]


def compute_error_metrics(y_true, y_pred):
    actual = np.asarray(y_true, dtype=float)
    pred = np.asarray(y_pred, dtype=float)
    assert actual.size > 0 and actual.shape == pred.shape, "Invalid array shape"
    assert np.isfinite(actual).all() and np.isfinite(pred).all(), "NaN or Inf found"
    assert (actual != 0).all(), "Zero in actual values"

    err = actual - pred
    return {
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "mape": float(np.mean(np.abs(err / actual)) * 100),
    }


def build_prediction_frame(month_key, model_name, actual_usd_t, predicted_usd_t, split="test"):
    df = pd.DataFrame({
        "month_key": np.asarray(month_key, dtype="int64"),
        "model_name": model_name,
        "split": split,
        "actual_usd_t": np.asarray(actual_usd_t, dtype=float),
        "predicted_usd_t": np.asarray(predicted_usd_t, dtype=float),
    })
    df["error_usd_t"] = df["actual_usd_t"] - df["predicted_usd_t"]
    df["abs_pct_error"] = np.abs(df["error_usd_t"] / df["actual_usd_t"]) * 100
    return df[PREDICTION_COLUMNS].sort_values("month_key").reset_index(drop=True)


def evaluate_predictions(pred_df):
    rows = []
    for model_name in pred_df["model_name"].drop_duplicates():
        sub = pred_df[pred_df["model_name"] == model_name]
        for label, start_mk, end_mk in EVAL_PERIODS:
            win = sub[sub["month_key"].between(start_mk, end_mk)]
            scores = compute_error_metrics(win["actual_usd_t"], win["predicted_usd_t"])
            rows.append({
                "model_name": model_name,
                "eval_period": label,
                "n_months": len(win),
                "mae": round(scores["mae"], 4),
                "rmse": round(scores["rmse"], 4),
                "mape": round(scores["mape"], 4),
            })
    return pd.DataFrame(rows)


def validate_predictions(pred_df, mart_df, expected_models=None, tol=1e-6):
    assert set(SUBMISSION_COLUMNS).issubset(pred_df.columns), "Missing required columns"
    assert pred_df[SUBMISSION_COLUMNS].notna().all().all(), "NaN found"
    for col in ("actual_usd_t", "predicted_usd_t"):
        dtype = pred_df[col].dtype
        assert pd.api.types.is_numeric_dtype(dtype) and not pd.api.types.is_bool_dtype(dtype), f"{col}: must be numeric"
    assert np.isfinite(pred_df[["actual_usd_t", "predicted_usd_t"]]).all().all(), "Inf found"
    assert (pred_df["predicted_usd_t"] > 0).all(), "predicted_usd_t must be positive (check delta vs price)"
    assert not pred_df.duplicated(["month_key", "model_name"]).any(), "Duplicate keys found"

    test_mart = mart_df[mart_df["split"] == "test"].sort_values("month_key")
    expected_months = test_mart["month_key"].tolist()
    models = list(pred_df["model_name"].drop_duplicates())
    if expected_models is not None:
        assert set(models) == set(expected_models), f"Unexpected models: {models}"

    for name in models:
        sub = pred_df[pred_df["model_name"] == name].sort_values("month_key")
        assert (sub["split"] == "test").all(), f"{name}: contains non-test rows"
        assert sub["month_key"].tolist() == expected_months, f"{name}: month mismatch"
        assert np.allclose(sub["actual_usd_t"], test_mart["y"], atol=tol, rtol=0), f"{name}: actual_usd_t mismatch"


def build_submission_template(mart_df, model_names):
    if isinstance(model_names, str):
        model_names = [model_names]

    test_df = mart_df[mart_df["split"] == "test"].sort_values("month_key")
    months = np.asarray(test_df["month_key"], dtype="int64")
    actuals = np.asarray(test_df["y"], dtype=float)
    frames = [
        pd.DataFrame({
            "month_key": months,
            "model_name": name,
            "split": "test",
            "actual_usd_t": actuals,
            "predicted_usd_t": np.nan,
        })
        for name in model_names
    ]
    return pd.concat(frames, ignore_index=True)[SUBMISSION_COLUMNS]


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
