"""Unit tests cho module dự báo giá gạo SARIMA trên chuỗi tổng hợp 48 tháng."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from forecasting.data_loader import load_from_raw_excel
from forecasting.sarima import (
    check_stationarity,
    evaluate_metrics,
    fit_and_forecast,
    forecast_seasonal_naive,
    rolling_backtest_sarima,
    split_train_test,
)


@pytest.fixture
def synthetic_seasonal_series() -> pd.Series:
    """Tạo chuỗi giá tổng hợp 48 tháng có xu hướng tăng nhẹ và tính mùa vụ chu kỳ 12 tháng."""
    rng = np.random.default_rng(42)
    idx = pd.date_range("2022-01-01", periods=48, freq="MS")
    t = np.arange(48, dtype=float)
    seasonal = 15.0 * np.sin(2.0 * np.pi * t / 12.0)
    noise = rng.normal(0.0, 2.0, size=48)
    values = 450.0 + 1.2 * t + seasonal + noise
    return pd.Series(values, index=idx, name="synthetic_rice_price")


def test_check_stationarity_detects_stationary_and_nonstationary(synthetic_seasonal_series):
    rng = np.random.default_rng(7)
    stationary_noise = pd.Series(rng.normal(0.0, 1.0, size=120), index=pd.date_range("2016-01-01", periods=120, freq="MS"))
    res_stat = check_stationarity(stationary_noise)
    assert res_stat["adf_is_stationary"] is True
    assert "adf_pvalue" in res_stat and "kpss_pvalue" in res_stat and "conclusion" in res_stat

    diff_res = check_stationarity(synthetic_seasonal_series.diff().dropna())
    assert isinstance(diff_res["is_stationary"], bool)


def test_split_train_test_preserves_chronological_order(synthetic_seasonal_series):
    train, test = split_train_test(synthetic_seasonal_series, test_months=12)
    assert len(train) == 36
    assert len(test) == 12
    assert train.index[-1] < test.index[0]
    with pytest.raises(ValueError):
        split_train_test(synthetic_seasonal_series, test_months=0)


def test_evaluate_metrics_exact_values():
    idx = pd.date_range("2025-01-01", periods=4, freq="MS")
    y_true = pd.Series([400.0, 500.0, 450.0, 550.0], index=idx)
    y_pred = pd.Series([410.0, 490.0, 450.0, 530.0], index=idx)
    metrics = evaluate_metrics(y_true, y_pred)
    assert metrics["MAE"] == 10.0
    assert metrics["RMSE"] == round(float(np.sqrt((100 + 100 + 0 + 400) / 4)), 2)
    assert metrics["MAPE_pct"] > 0 and metrics["sMAPE_pct"] > 0


def test_forecast_seasonal_naive_repeats_last_12_months(synthetic_seasonal_series):
    train, _ = split_train_test(synthetic_seasonal_series, test_months=12)
    naive = forecast_seasonal_naive(train, horizon=12, m=12)
    assert len(naive) == 12
    assert np.allclose(naive.values, train.iloc[-12:].values)
    assert naive.index[0] == train.index[-1] + pd.DateOffset(months=1)


def test_fit_and_forecast_returns_confidence_intervals_and_diagnostics(synthetic_seasonal_series):
    fc_df, diag = fit_and_forecast(
        synthetic_seasonal_series,
        order=(1, 1, 0),
        seasonal_order=(0, 1, 0, 12),
        horizon=6,
        use_log=True,
    )
    assert list(fc_df.columns) == ["month_start", "forecast", "lower_95", "upper_95"]
    assert len(fc_df) == 6
    assert (fc_df["lower_95"] < fc_df["forecast"]).all()
    assert (fc_df["forecast"] < fc_df["upper_95"]).all()
    assert 0.0 <= diag["ljung_box_pvalue"] <= 1.0


def test_load_from_raw_excel_verified_201_clean_months():
    series, meta = load_from_raw_excel(series_name="Rice, Viet Namese 5%", start_date="2009-04-01")
    assert len(series) == 201
    assert meta["missing_count"] == 0
    assert meta["raw_total_months"] == 265
    assert meta["raw_missing_count"] == 11


def test_rolling_backtest_sarima_returns_exact_test_horizon(synthetic_seasonal_series):
    rolling_pred = rolling_backtest_sarima(
        synthetic_seasonal_series,
        test_months=12,
        order=(1, 1, 0),
        seasonal_order=(0, 1, 0, 12),
        use_log=True,
    )
    assert len(rolling_pred) == 12
    assert rolling_pred.isna().sum() == 0
    assert (rolling_pred > 0).all()

