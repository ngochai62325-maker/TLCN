"""Các hàm tính toán chỉ số sai số và kiểm tra hợp lệ kết quả dự báo."""
from __future__ import annotations

import numpy as np
import pandas as pd

EVAL_PERIODS = [
    ("2023-2025", 202301, 202512),
    ("2023", 202301, 202312),
    ("2024-2025", 202401, 202512),
]

SUBMISSION_COLUMNS = ["month_key", "model_name", "split", "actual_usd_t", "predicted_usd_t"]
PREDICTION_COLUMNS = [*SUBMISSION_COLUMNS, "error_usd_t", "abs_pct_error"]

EXPECTED_MONTHS = [y * 100 + m for y in range(2023, 2026) for m in range(1, 13)]
REQUIRED_COLS = list(SUBMISSION_COLUMNS)
DEFAULT_TOL = 1e-6


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def compute_error_metrics(y_true, y_pred) -> dict[str, float]:
    """Tính MAE, RMSE và MAPE (%) trên thang đo giá gốc."""
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


def build_prediction_frame(
    month_key,
    model_name: str,
    actual_usd_t,
    predicted_usd_t,
    split: str = "test",
) -> pd.DataFrame:
    """Chuẩn hóa bảng dự báo kèm cột sai số tuyệt đối và phần trăm."""
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


def evaluate_predictions(pred_df: pd.DataFrame) -> pd.DataFrame:
    """Đánh giá sai số MAE, RMSE, MAPE cho từng mô hình trên các cửa sổ EVAL_PERIODS."""
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


def validate_predictions(
    pred_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    expected_models=None,
    tol: float = 1e-6,
) -> None:
    """Kiểm tra nhanh schema, giá trị dương và khớp tập test với forecasting_mart."""
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


def build_submission_template(mart_df: pd.DataFrame, model_names) -> pd.DataFrame:
    """Tạo bảng mẫu nộp dự báo trên tập test cho một hoặc nhiều mô hình."""
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


def validate_model_predictions(
    pred_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    expected_models=None,
    expected_months=EXPECTED_MONTHS,
    tol: float = DEFAULT_TOL,
) -> dict:
    """Kiểm định chặt chẽ bảng dự báo của mô hình và trả về báo cáo kiểm tra."""
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
