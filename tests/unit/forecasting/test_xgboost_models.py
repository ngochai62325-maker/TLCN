import numpy as np
import pandas as pd
import pytest

from jobs.forecasting import run_xgboost_models as xgb_mod
from jobs.forecasting.validate_model_predictions import EXPECTED_MONTHS, validate_model_predictions


def _make_synthetic_mart():
    months = [y * 100 + m for y in range(2016, 2026) for m in range(1, 13)]
    n = len(months)
    rng = np.random.default_rng(7)
    y = 400.0 + np.cumsum(rng.normal(0.5, 8.0, size=n))
    df = pd.DataFrame({"month_key": months, "y": y})
    df["split"] = np.where(df["month_key"] <= 202212, "train", "test")
    df["y_lag_1"] = df["y"].shift(1)
    df["target_delta"] = df["y"] - df["y_lag_1"]
    for k in (2, 3, 6, 12):
        df[f"y_lag_{k}"] = df["y"].shift(k)
    for w in (3, 6, 12):
        df[f"y_roll_mean_{w}"] = df["y"].shift(1).rolling(w).mean()
        df[f"y_roll_std_{w}"] = df["y"].shift(1).rolling(w).std()
    df["month_of_year"] = df["month_key"] % 100
    df["urea_lag_1"] = 300.0 + rng.normal(0, 15.0, size=n)
    df["crude_oil_lag_1"] = 70.0 + rng.normal(0, 5.0, size=n)
    df["thai5_lag_1"] = df["y_lag_1"] + 20.0 + rng.normal(0, 4.0, size=n)
    df["india_parb_lag_1"] = df["y_lag_1"] - 30.0 + rng.normal(0, 4.0, size=n)
    df["pak5_lag_1"] = df["y_lag_1"] - 10.0 + rng.normal(0, 4.0, size=n)
    df["india_white_quote_missing_lag_1"] = (df["month_key"] >= 202307).astype(int)
    df["spread_vn_thai_lag_1"] = df["y_lag_1"] - df["thai5_lag_1"]
    df["spread_vn_india_lag_1"] = df["y_lag_1"] - df["india_parb_lag_1"]

    # Inject NaNs in train competitor columns (D5) to verify native NaN handling
    df = df.iloc[12:].reset_index(drop=True)
    df.loc[:4, ["india_parb_lag_1", "spread_vn_india_lag_1"]] = np.nan
    df.loc[2, "pak5_lag_1"] = np.nan
    return df


def test_feature_sets_strict_separation():
    assert len(xgb_mod.BASE_FEATURES) == 14
    assert len(xgb_mod.COMPETITOR_EXTRA_FEATURES) == 6
    assert len(xgb_mod.COMPETITOR_FEATURES) == 20
    assert set(xgb_mod.BASE_FEATURES).isdisjoint(set(xgb_mod.COMPETITOR_EXTRA_FEATURES))
    for col in xgb_mod.BASE_FEATURES:
        lower = col.lower()
        assert not any(tok in lower for tok in ("thai", "india", "pak", "spread"))


def test_xgboost_pipeline_end_to_end_and_nan_handling():
    mart = _make_synthetic_mart()
    small_grid = {
        "max_depth": [2, 3],
        "learning_rate": [0.05],
        "n_estimators": [20],
        "min_child_weight": [3],
        "reg_alpha": [0.0],
        "reg_lambda": [1.0],
        "subsample": [0.8],
        "colsample_bytree": [0.8],
    }

    pred_df, metrics_df, fi_df, tuning_summary = xgb_mod.run_xgboost_pipeline(
        mart,
        param_grid=small_grid,
        n_splits=3,
        n_perm_repeats=5,
    )

    report = validate_model_predictions(
        pred_df,
        mart,
        expected_models=("xgb_base", "xgb_competitor"),
        expected_months=EXPECTED_MONTHS,
    )
    assert report["status"] == "PASS"
    assert report["n_rows"] == 72
    assert list(metrics_df["model_name"].unique()) == ["xgb_base", "xgb_competitor"]
    assert len(metrics_df) == 6

    # Feature importance covers both models and all features
    assert set(fi_df["model_name"].unique()) == {"xgb_base", "xgb_competitor"}
    assert len(fi_df[fi_df["model_name"] == "xgb_base"]) == 14
    assert len(fi_df[fi_df["model_name"] == "xgb_competitor"]) == 20
    assert set(fi_df.loc[fi_df["model_name"] == "xgb_base", "feature_group"].unique()) == {"base"}
    assert set(fi_df.loc[fi_df["model_name"] == "xgb_competitor", "feature_group"].unique()) == {
        "base",
        "competitor",
    }

    # Tuning summary strictly trained on train rows
    n_train = int((mart["split"] == "train").sum())
    for name in ("xgb_base", "xgb_competitor"):
        assert tuning_summary[name]["n_train_rows"] == n_train
        assert tuning_summary[name]["cv_rmse"] > 0
