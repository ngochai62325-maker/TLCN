import numpy as np
import pandas as pd
import pytest

from jobs.forecasting import metrics as m_mod
from jobs.forecasting import run_naive_baseline as naive_mod


def _make_sample_mart():
    months = [y * 100 + m for y in (2022, 2023, 2024, 2025) for m in range(1, 13)]
    df = pd.DataFrame({"month_key": months, "y": [400.0 + i * 2.0 for i in range(len(months))]})
    df["y_lag_1"] = df["y"].shift(1)
    df["split"] = np.where(df["month_key"] <= 202212, "train", "test")
    return df.iloc[1:].reset_index(drop=True)


def test_compute_error_metrics():
    res = m_mod.compute_error_metrics([100.0, 200.0], [90.0, 220.0])
    assert res["mae"] == pytest.approx(15.0)
    assert res["rmse"] == pytest.approx(np.sqrt(250.0))
    assert res["mape"] == pytest.approx(10.0)


def test_naive_predict_and_evaluate():
    mart = _make_sample_mart()
    pred = naive_mod.predict_naive(mart)
    m_mod.validate_predictions(pred, mart, expected_models=("naive",))

    assert len(pred) == 36
    assert pred.loc[pred["month_key"] == 202301, "predicted_usd_t"].iloc[0] == pytest.approx(
        mart.loc[mart["month_key"] == 202212, "y"].iloc[0]
    )

    scores = m_mod.evaluate_predictions(pred)
    assert list(scores["eval_period"]) == ["2023-2025", "2023", "2024-2025"]
    assert list(scores["n_months"]) == [36, 12, 24]
    assert np.allclose(scores["mae"], 2.0)
    assert np.allclose(scores["rmse"], 2.0)


def test_validate_predictions_rejects_bad_rows():
    mart = _make_sample_mart()
    pred = naive_mod.predict_naive(mart)

    bad_nan = pred.copy()
    bad_nan.loc[0, "predicted_usd_t"] = np.nan
    with pytest.raises(AssertionError, match="NaN"):
        m_mod.validate_predictions(bad_nan, mart)

    bad_actual = pred.copy()
    bad_actual.loc[0, "actual_usd_t"] += 1.0
    with pytest.raises(AssertionError, match="actual_usd_t"):
        m_mod.validate_predictions(bad_actual, mart)
