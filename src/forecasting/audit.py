from __future__ import annotations

import inspect
import json
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from statsmodels.tsa.statespace.sarimax import SARIMAX

from forecasting.data_loader import load_series_from_mart
from forecasting.mart import TRAIN_END, check_dq19, warmup_target
from forecasting.metrics import evaluate_predictions, validate_model_predictions
from forecasting.sarima import rolling_backtest_sarima, run_sarima_pipeline, split_train_test
from forecasting.xgboost_model import (
    BASE_FEATURES,
    COMPETITOR_EXTRA_FEATURES,
    RANDOM_STATE,
    SCENARIOS,
    _make_regressor,
    run_xgboost_pipeline,
    tune_xgboost_timeseries,
)

ALLOWED_FEATURE_PREFIXES = ("y_lag_", "y_roll_mean_", "y_roll_std_")
ALLOWED_FEATURE_SUFFIXES = ("_lag_1",)
ALLOWED_EXACT_FEATURES = ("month_of_year",)
FORBIDDEN_FEATURE_COLS = {"y", "target_delta", "month_key", "split"}
COMPETITOR_TOKENS = ("thai", "india", "pak", "spread")
PERTURB_MONTH = "2023-07-01"
PERTURB_NEXT_MONTH = "2023-08-01"
PERTURB_VALUE = 9999.0


def audit_mart_dq19(raw_df: pd.DataFrame, mart_df: pd.DataFrame) -> dict:
    base_report = check_dq19(raw_df, mart_df)
    df_raw, _ = warmup_target(raw_df)
    india_white_errors = 0
    month_of_year_errors = 0
    for _, row in mart_df.iterrows():
        mk = int(row["month_key"])
        prev = df_raw.loc[df_raw.index < mk].iloc[-1]
        if int(row["india_white_quote_missing_lag_1"]) != int(pd.isna(prev["usda_india_broken5_usd_t"])):
            india_white_errors += 1
        if int(row["month_of_year"]) != mk % 100:
            month_of_year_errors += 1

    if india_white_errors != 0 or month_of_year_errors != 0:
        raise AssertionError(
            f"Mart extra DQ19 failed: india_white_errors={india_white_errors}, month_of_year_errors={month_of_year_errors}"
        )

    return {
        "status": "PASS",
        "rows": base_report["rows"],
        "train_rows": base_report["train_rows"],
        "test_rows": base_report["test_rows"],
        "leakage_errors": base_report["leakage_errors"],
        "india_white_flag_errors": india_white_errors,
        "month_of_year_errors": month_of_year_errors,
    }


def audit_sarima_logic(
    mart_csv: str | Path,
    diag_json: str | Path,
    sarima_pred_csv: str | Path,
) -> dict:
    series, mart_df, meta = load_series_from_mart(mart_csv)
    test_months = int(meta["test_months"])
    train, test = split_train_test(series, test_months=test_months)

    if int(train.index[-1].strftime("%Y%m")) > TRAIN_END:
        raise AssertionError("SARIMA train split leaked into test period")

    diag_data = json.loads(Path(diag_json).read_text(encoding="utf-8"))
    order = tuple(diag_data["best_order"])
    seasonal_order = tuple(diag_data["best_seasonal_order"])
    use_log = bool(diag_data.get("use_log", True))

    rolling_src = inspect.getsource(rolling_backtest_sarima)
    pipeline_src = inspect.getsource(run_sarima_pipeline)
    if "refit=False" not in rolling_src:
        raise AssertionError("rolling_backtest_sarima must pass refit=False")
    if "grid_search_sarima(train" not in pipeline_src:
        raise AssertionError("grid_search_sarima must run strictly on train")

    y_train = np.log(train.astype(float)) if use_log else train.astype(float)
    y_test = np.log(test.astype(float)) if use_log else test.astype(float)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = SARIMAX(
            y_train,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False, maxiter=150)
        frozen_params = res.params.to_numpy(dtype=float).copy()
        max_param_drift = 0.0
        for i in range(len(y_test) - 1):
            res = res.append(y_test.iloc[i : i + 1], refit=False)
            drift = float(np.max(np.abs(res.params.to_numpy(dtype=float) - frozen_params)))
            max_param_drift = max(max_param_drift, drift)

    if max_param_drift != 0.0:
        raise AssertionError(f"SARIMA params drifted during test rollout: {max_param_drift}")

    corrupted = series.copy()
    corrupted.loc[PERTURB_MONTH:] = PERTURB_VALUE
    train_corrupted, _ = split_train_test(corrupted, test_months=test_months)
    if not train.equals(train_corrupted):
        raise AssertionError("Corrupting test modified train series")

    pred_orig = rolling_backtest_sarima(
        series,
        test_months=test_months,
        order=order,
        seasonal_order=seasonal_order,
        use_log=use_log,
        round_decimals=None,
    )
    pred_corr = rolling_backtest_sarima(
        corrupted,
        test_months=test_months,
        order=order,
        seasonal_order=seasonal_order,
        use_log=use_log,
        round_decimals=None,
    )
    diff_up_to_perturb = float((pred_orig.loc[:PERTURB_MONTH] - pred_corr.loc[:PERTURB_MONTH]).abs().max())
    diff_after_perturb = float((pred_orig.loc[PERTURB_NEXT_MONTH:] - pred_corr.loc[PERTURB_NEXT_MONTH:]).abs().max())
    if diff_up_to_perturb != 0.0:
        raise AssertionError(f"SARIMA future leak detected: diff_up_to_perturb={diff_up_to_perturb}")
    if diff_after_perturb <= 0.0:
        raise AssertionError("SARIMA failed to react to appended actual observations")

    saved_df = pd.read_csv(sarima_pred_csv).sort_values("month_key")
    validate_model_predictions(saved_df, mart_df, expected_models=["sarima"])
    saved_diff = float(
        np.max(np.abs(saved_df["predicted_usd_t"].to_numpy(dtype=float) - pred_orig.to_numpy(dtype=float)))
    )

    stationarity_n_obs = int(diag_data["stationarity"]["raw_level"]["n_obs"])
    warnings_list = []
    if stationarity_n_obs > len(train):
        warnings_list.append(
            f"sarima_diagnostics stationarity.raw_level.n_obs={stationarity_n_obs} exceeds train length {len(train)}"
        )

    return {
        "status": "PASS",
        "best_order": list(order),
        "best_seasonal_order": list(seasonal_order),
        "use_log": use_log,
        "max_param_drift_refit_false": max_param_drift,
        "counterfactual_diff_before_or_at_perturb": diff_up_to_perturb,
        "counterfactual_diff_after_perturb": round(diff_after_perturb, 4),
        "saved_vs_local_max_diff": round(saved_diff, 6),
        "stationarity_n_obs": stationarity_n_obs,
        "warnings": warnings_list,
    }


def _is_valid_lag_feature(col: str) -> bool:
    return (
        col in ALLOWED_EXACT_FEATURES
        or col.startswith(ALLOWED_FEATURE_PREFIXES)
        or col.endswith(ALLOWED_FEATURE_SUFFIXES)
    )


def audit_xgboost_logic(
    mart_df: pd.DataFrame,
    tuning_json: str | Path,
    xgb_pred_csv: str | Path,
    n_splits: int = 5,
) -> dict:
    df = mart_df.sort_values("month_key").reset_index(drop=True)
    train_df = df[df["split"] == "train"].reset_index(drop=True)
    test_df = df[df["split"] == "test"].reset_index(drop=True)

    if int(train_df["month_key"].max()) > TRAIN_END:
        raise AssertionError("XGBoost train split exceeds TRAIN_END")
    if int(test_df["month_key"].min()) <= TRAIN_END:
        raise AssertionError("XGBoost test split overlaps train period")

    for split_name, sub in (("train", train_df), ("test", test_df)):
        if not np.allclose(sub["y"] - sub["y_lag_1"], sub["target_delta"], atol=1e-8, rtol=0):
            raise AssertionError(f"target_delta mismatch on {split_name}")

    if not set(BASE_FEATURES).isdisjoint(set(COMPETITOR_EXTRA_FEATURES)):
        raise AssertionError("Competitor feature leaked into BASE_FEATURES")
    for col in BASE_FEATURES:
        if any(tok in col.lower() for tok in COMPETITOR_TOKENS):
            raise AssertionError(f"Competitor token found in BASE_FEATURES: {col}")
    for name, cols in SCENARIOS.items():
        if not set(cols).isdisjoint(FORBIDDEN_FEATURE_COLS):
            raise AssertionError(f"Forbidden column in {name}: {set(cols) & FORBIDDEN_FEATURE_COLS}")
        bad_cols = [c for c in cols if not _is_valid_lag_feature(c)]
        if bad_cols:
            raise AssertionError(f"Non-lag t+1 candidate column in {name}: {bad_cols}")

    tune_params = set(inspect.signature(tune_xgboost_timeseries).parameters)
    if any("test" in p.lower() for p in tune_params):
        raise AssertionError(f"tune_xgboost_timeseries accepts test parameter: {tune_params}")

    pipeline_src = inspect.getsource(run_xgboost_pipeline)
    if 'y_train_delta = train_df["target_delta"]' not in pipeline_src:
        raise AssertionError("XGBoost pipeline does not target target_delta")
    if 'test_df["y_lag_1"]' not in pipeline_src:
        raise AssertionError("XGBoost pipeline does not reconstruct price from y_lag_1")

    tscv = TimeSeriesSplit(n_splits=n_splits)
    folds = []
    for fold_idx, (tr_idx, va_idx) in enumerate(tscv.split(train_df), start=1):
        tr_max = int(train_df.loc[tr_idx[-1], "month_key"])
        va_min = int(train_df.loc[va_idx[0], "month_key"])
        va_max = int(train_df.loc[va_idx[-1], "month_key"])
        if not (tr_max < va_min and va_max <= TRAIN_END):
            raise AssertionError(f"TimeSeriesSplit leakage in fold {fold_idx}: tr_max={tr_max}, va={va_min}..{va_max}")
        folds.append({
            "fold": fold_idx,
            "train_range": [int(train_df.loc[tr_idx[0], "month_key"]), tr_max],
            "val_range": [va_min, va_max],
        })

    tuning_data = json.loads(Path(tuning_json).read_text(encoding="utf-8"))
    saved_df = pd.read_csv(xgb_pred_csv)
    validate_model_predictions(saved_df, mart_df, expected_models=("xgb_base", "xgb_competitor"))

    train_max_price = float(train_df["y"].max())
    scenario_reports = {}
    for name, cols in SCENARIOS.items():
        info = tuning_data[name]
        if int(info["n_train_rows"]) != len(train_df):
            raise AssertionError(f"{name}: tuning n_train_rows mismatch")
        if list(info["features"]) != list(cols):
            raise AssertionError(f"{name}: tuning features mismatch")

        mdl = _make_regressor(info["best_params"], random_state=RANDOM_STATE)
        mdl.fit(train_df[cols], train_df["target_delta"])
        pred_price = test_df["y_lag_1"].to_numpy(dtype=float) + mdl.predict(test_df[cols])

        test_corrupted = test_df.copy()
        test_corrupted["y"] = PERTURB_VALUE
        test_corrupted["target_delta"] = PERTURB_VALUE
        pred_price_corr = test_corrupted["y_lag_1"].to_numpy(dtype=float) + mdl.predict(test_corrupted[cols])
        counterfactual_diff = float(np.max(np.abs(pred_price - pred_price_corr)))
        if counterfactual_diff != 0.0:
            raise AssertionError(f"{name}: test target leak detected ({counterfactual_diff})")

        sub_saved = saved_df[saved_df["model_name"] == name].sort_values("month_key")
        saved_prices = sub_saved["predicted_usd_t"].to_numpy(dtype=float)
        saved_diff = float(np.max(np.abs(saved_prices - pred_price)))

        scenario_reports[name] = {
            "n_features": len(cols),
            "counterfactual_diff": counterfactual_diff,
            "saved_months_above_train_max": int((saved_prices > train_max_price).sum()),
            "saved_max_predicted_usd_t": round(float(saved_prices.max()), 2),
            "saved_vs_local_max_diff": round(saved_diff, 4),
        }

    return {
        "status": "PASS",
        "train_max_usd_t": train_max_price,
        "tscv_folds": folds,
        "scenarios": scenario_reports,
    }


def audit_contracts_and_merge(mart_df: pd.DataFrame, export_dir: str | Path) -> dict:
    export_path = Path(export_dir)
    models_dir = export_path / "models"
    indiv_specs = [
        (models_dir / "naive_predictions.csv", ["naive"]),
        (models_dir / "sarima_predictions.csv", ["sarima"]),
        (models_dir / "xgb_predictions.csv", ["xgb_base", "xgb_competitor"]),
    ]
    indiv_frames = []
    for path, expected in indiv_specs:
        df = pd.read_csv(path)
        validate_model_predictions(df, mart_df, expected_models=expected)
        indiv_frames.append(df)

    indiv_all = (
        pd.concat(indiv_frames, ignore_index=True)
        .sort_values(["model_name", "month_key"])
        .reset_index(drop=True)
    )
    merged_df = pd.read_csv(export_path / "forecast_predictions.csv")
    validate_model_predictions(merged_df, mart_df, expected_models=["naive", "sarima", "xgb_base", "xgb_competitor"])
    merged_sorted = merged_df.sort_values(["model_name", "month_key"]).reset_index(drop=True)

    merge_diff = float((merged_sorted["predicted_usd_t"] - indiv_all["predicted_usd_t"]).abs().max())
    if merge_diff > 1e-8:
        raise AssertionError(f"Merged predictions diverge from individual model files: {merge_diff}")

    saved_metrics = (
        pd.read_csv(export_path / "forecast_metrics.csv")
        .sort_values(["model_name", "eval_period"])
        .reset_index(drop=True)
    )
    recomputed_metrics = (
        evaluate_predictions(merged_df)
        .sort_values(["model_name", "eval_period"])
        .reset_index(drop=True)
    )
    metrics_diff = float(
        (saved_metrics[["mae", "rmse", "mape"]] - recomputed_metrics[["mae", "rmse", "mape"]]).abs().max().max()
    )
    if metrics_diff > 1e-4:
        raise AssertionError(f"Saved metrics diverge from recomputed metrics: {metrics_diff}")

    return {
        "status": "PASS",
        "merged_rows": len(merged_df),
        "metrics_rows": len(saved_metrics),
        "merge_max_diff": merge_diff,
        "metrics_max_diff": metrics_diff,
    }


def run_dq19_cross_audit(
    raw_csv: str | Path,
    mart_csv: str | Path,
    export_dir: str | Path,
    strict_warnings: bool = False,
    n_splits: int = 5,
) -> dict:
    export_path = Path(export_dir)
    models_dir = export_path / "models"
    raw_df = pd.read_csv(raw_csv)
    mart_df = pd.read_csv(mart_csv)

    report = {
        "overall_status": "PASS",
        "mart_dq19": audit_mart_dq19(raw_df, mart_df),
        "sarima": audit_sarima_logic(
            mart_csv,
            export_path / "sarima_diagnostics.json",
            models_dir / "sarima_predictions.csv",
        ),
        "xgboost": audit_xgboost_logic(
            mart_df,
            models_dir / "xgb_tuning_summary.json",
            models_dir / "xgb_predictions.csv",
            n_splits=n_splits,
        ),
        "contracts": audit_contracts_and_merge(mart_df, export_path),
    }

    if strict_warnings and report["sarima"]["warnings"]:
        report["overall_status"] = "FAIL"

    return report