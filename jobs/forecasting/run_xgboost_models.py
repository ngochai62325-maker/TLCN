import argparse
import itertools
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from jobs.forecasting.metrics import (
    build_prediction_frame,
    evaluate_predictions,
    validate_predictions,
)
from jobs.forecasting.validate_model_predictions import validate_model_predictions

ROOT = Path(__file__).resolve().parents[2]
MART_CSV = ROOT / "exports" / "forecast" / "forecasting_mart.csv"
OUT_DIR = ROOT / "exports" / "forecast" / "models"

BASE_FEATURES = [
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
]

COMPETITOR_EXTRA_FEATURES = [
    "thai5_lag_1",
    "india_parb_lag_1",
    "pak5_lag_1",
    "india_white_quote_missing_lag_1",
    "spread_vn_thai_lag_1",
    "spread_vn_india_lag_1",
]

COMPETITOR_FEATURES = [*BASE_FEATURES, *COMPETITOR_EXTRA_FEATURES]

SCENARIOS = {
    "xgb_base": BASE_FEATURES,
    "xgb_competitor": COMPETITOR_FEATURES,
}

DEFAULT_PARAM_GRID = {
    "max_depth": [2, 3, 4],
    "learning_rate": [0.03, 0.05, 0.1],
    "n_estimators": [50, 100, 150],
    "min_child_weight": [3, 5],
    "reg_alpha": [0.0, 1.0],
    "reg_lambda": [1.0, 5.0],
    "subsample": [0.8],
    "colsample_bytree": [0.8],
}

RANDOM_STATE = 42


def _iter_param_grid(param_grid):
    keys = list(param_grid.keys())
    for values in itertools.product(*(param_grid[k] for k in keys)):
        yield dict(zip(keys, values))


def _make_regressor(params, random_state=RANDOM_STATE):
    return XGBRegressor(
        objective="reg:squarederror",
        random_state=random_state,
        n_jobs=1,
        tree_method="hist",
        **params,
    )


def tune_xgboost_timeseries(
    X_train,
    y_train_delta,
    param_grid=None,
    n_splits=5,
    random_state=RANDOM_STATE,
):
    """Select compact anti-overfit hyperparameters using TimeSeriesSplit strictly on train."""
    grid = DEFAULT_PARAM_GRID if param_grid is None else param_grid
    tscv = TimeSeriesSplit(n_splits=n_splits)
    splits = list(tscv.split(X_train))

    best_params = None
    best_cv_rmse = float("inf")
    best_cv_mae = float("inf")
    n_candidates = 0

    for params in _iter_param_grid(grid):
        n_candidates += 1
        fold_rmses = []
        fold_maes = []
        for tr_idx, va_idx in splits:
            model = _make_regressor(params, random_state=random_state)
            model.fit(X_train.iloc[tr_idx], y_train_delta.iloc[tr_idx])
            preds = model.predict(X_train.iloc[va_idx])
            err = y_train_delta.iloc[va_idx].to_numpy(dtype=float) - preds
            fold_rmses.append(float(np.sqrt(np.mean(err ** 2))))
            fold_maes.append(float(np.mean(np.abs(err))))

        mean_rmse = float(np.mean(fold_rmses))
        mean_mae = float(np.mean(fold_maes))
        if mean_rmse < best_cv_rmse:
            best_cv_rmse = mean_rmse
            best_cv_mae = mean_mae
            best_params = params

    return {
        "best_params": best_params,
        "cv_rmse": round(best_cv_rmse, 4),
        "cv_mae": round(best_cv_mae, 4),
        "n_splits": n_splits,
        "n_candidates": n_candidates,
    }


def extract_feature_importance(
    model,
    model_name,
    feature_cols,
    X_train,
    y_train_delta,
    X_test,
    y_test_delta,
    n_repeats=30,
    random_state=RANDOM_STATE,
):
    """Extract XGBoost gain importance and permutation importance on train and test."""
    gain_map = model.get_booster().get_score(importance_type="gain")
    gains = np.array([float(gain_map.get(col, 0.0)) for col in feature_cols], dtype=float)
    total_gain = float(gains.sum())
    gain_shares = (gains / total_gain * 100.0) if total_gain > 0 else np.zeros_like(gains)

    perm_train = permutation_importance(
        model,
        X_train,
        y_train_delta,
        scoring="neg_root_mean_squared_error",
        n_repeats=n_repeats,
        random_state=random_state,
        n_jobs=1,
    )
    perm_test = permutation_importance(
        model,
        X_test,
        y_test_delta,
        scoring="neg_root_mean_squared_error",
        n_repeats=n_repeats,
        random_state=random_state,
        n_jobs=1,
    )

    rows = []
    for idx, col in enumerate(feature_cols):
        group = "competitor" if col in COMPETITOR_EXTRA_FEATURES else "base"
        rows.append(
            {
                "model_name": model_name,
                "feature": col,
                "feature_group": group,
                "gain": round(float(gains[idx]), 6),
                "gain_share_pct": round(float(gain_shares[idx]), 4),
                "perm_train_rmse_mean": round(float(perm_train.importances_mean[idx]), 6),
                "perm_train_rmse_std": round(float(perm_train.importances_std[idx]), 6),
                "perm_test_rmse_mean": round(float(perm_test.importances_mean[idx]), 6),
                "perm_test_rmse_std": round(float(perm_test.importances_std[idx]), 6),
            }
        )

    return pd.DataFrame(rows).sort_values(
        ["model_name", "gain", "perm_test_rmse_mean"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def run_xgboost_pipeline(
    mart_df,
    param_grid=None,
    n_splits=5,
    n_perm_repeats=30,
    random_state=RANDOM_STATE,
):
    df = mart_df.sort_values("month_key").reset_index(drop=True)
    train_df = df[df["split"] == "train"].copy().reset_index(drop=True)
    test_df = df[df["split"] == "test"].copy().reset_index(drop=True)

    # Anti-leakage checks on splits and target definition
    assert len(train_df) > 0 and len(test_df) > 0, "Empty train or test split"
    assert train_df["month_key"].max() < test_df["month_key"].min(), "Chronological split violated"
    assert np.allclose(
        train_df["y"] - train_df["y_lag_1"], train_df["target_delta"], atol=1e-8, rtol=0
    ), "target_delta mismatch on train"
    assert np.allclose(
        test_df["y"] - test_df["y_lag_1"], test_df["target_delta"], atol=1e-8, rtol=0
    ), "target_delta mismatch on test"
    assert set(BASE_FEATURES).isdisjoint(set(COMPETITOR_EXTRA_FEATURES)), "Competitor leaked into base"

    pred_frames = []
    fi_frames = []
    tuning_summary = {}

    for model_name, feature_cols in SCENARIOS.items():
        X_train = train_df[feature_cols]
        y_train_delta = train_df["target_delta"]
        X_test = test_df[feature_cols]
        y_test_delta = test_df["target_delta"]

        # Tune strictly on train (2010-2022)
        tune_res = tune_xgboost_timeseries(
            X_train=X_train,
            y_train_delta=y_train_delta,
            param_grid=param_grid,
            n_splits=n_splits,
            random_state=random_state,
        )

        # Fit frozen model on full train set (2010-2022)
        final_model = _make_regressor(tune_res["best_params"], random_state=random_state)
        final_model.fit(X_train, y_train_delta)

        # Predict 1-step-ahead on test (2023-2025): y_hat = y_lag_1 + y_hat_delta
        y_hat_delta_test = final_model.predict(X_test)
        y_hat_price_test = test_df["y_lag_1"].to_numpy(dtype=float) + y_hat_delta_test

        # Also compute train in-sample RMSE for overfit diagnostics
        y_hat_delta_train = final_model.predict(X_train)
        train_rmse = float(
            np.sqrt(np.mean((y_train_delta.to_numpy(dtype=float) - y_hat_delta_train) ** 2))
        )

        pred_frame = build_prediction_frame(
            month_key=test_df["month_key"],
            model_name=model_name,
            actual_usd_t=test_df["y"],
            predicted_usd_t=y_hat_price_test,
        )
        pred_frames.append(pred_frame)

        fi_frame = extract_feature_importance(
            model=final_model,
            model_name=model_name,
            feature_cols=feature_cols,
            X_train=X_train,
            y_train_delta=y_train_delta,
            X_test=X_test,
            y_test_delta=y_test_delta,
            n_repeats=n_perm_repeats,
            random_state=random_state,
        )
        fi_frames.append(fi_frame)

        tuning_summary[model_name] = {
            "n_train_rows": len(train_df),
            "n_test_rows": len(test_df),
            "n_features": len(feature_cols),
            "features": feature_cols,
            "train_in_sample_rmse": round(train_rmse, 4),
            **tune_res,
        }

    pred_df = pd.concat(pred_frames, ignore_index=True)
    fi_df = pd.concat(fi_frames, ignore_index=True)
    validate_predictions(pred_df, mart_df, expected_models=("xgb_base", "xgb_competitor"))
    metrics_df = evaluate_predictions(pred_df)
    return pred_df, metrics_df, fi_df, tuning_summary


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Train and evaluate XGBoost Scenario A (xgb_base) and Scenario B (xgb_competitor)"
    )
    parser.add_argument("--mart-csv", type=Path, default=MART_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--n-perm-repeats", type=int, default=30)
    args = parser.parse_args(argv)

    mart_df = pd.read_csv(args.mart_csv)
    pred_df, metrics_df, fi_df, tuning_summary = run_xgboost_pipeline(
        mart_df,
        n_splits=args.n_splits,
        n_perm_repeats=args.n_perm_repeats,
    )
    val_report = validate_model_predictions(
        pred_df,
        mart_df,
        expected_models=("xgb_base", "xgb_competitor"),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pred_path = args.output_dir / "xgb_predictions.csv"
    metrics_path = args.output_dir / "xgb_metrics.csv"
    fi_path = args.output_dir / "xgb_feature_importance.csv"
    tuning_path = args.output_dir / "xgb_tuning_summary.json"

    pred_df.to_csv(pred_path, index=False)
    metrics_df.to_csv(metrics_path, index=False)
    fi_df.to_csv(fi_path, index=False)
    tuning_path.write_text(json.dumps(tuning_summary, indent=2), encoding="utf-8")

    print("=== VALIDATION REPORT ===")
    print(json.dumps(val_report, indent=2))
    print("\n=== TUNING SUMMARY (TRAIN 2010-2022 ONLY) ===")
    for name, info in tuning_summary.items():
        print(
            f"{name}: cv_rmse={info['cv_rmse']}, cv_mae={info['cv_mae']}, "
            f"train_rmse={info['train_in_sample_rmse']}, best_params={info['best_params']}"
        )
    print("\n=== EVALUATION METRICS (TEST 2023-2025) ===")
    print(metrics_df.to_string(index=False))
    print("\n=== TOP 10 FEATURE IMPORTANCE (xgb_competitor by gain) ===")
    top_comp = fi_df[fi_df["model_name"] == "xgb_competitor"].head(10)
    print(
        top_comp[
            ["feature", "feature_group", "gain", "gain_share_pct", "perm_train_rmse_mean", "perm_test_rmse_mean"]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
