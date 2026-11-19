import numpy as np
import pandas as pd
import pytest

from jobs.forecasting import validate_model_predictions as val_mod


def _make_mart_and_pred():
    months = val_mod.EXPECTED_MONTHS
    mart = pd.DataFrame({
        "month_key": months,
        "split": "test",
        "y": [450.0 + i for i in range(len(months))],
    })
    pred = pd.DataFrame({
        "month_key": months,
        "model_name": "sarima",
        "split": "test",
        "actual_usd_t": mart["y"].to_numpy(),
        "predicted_usd_t": mart["y"].to_numpy() - 5.0,
    })
    return mart, pred


def test_validate_pass():
    mart, pred = _make_mart_and_pred()
    report = val_mod.validate_model_predictions(pred, mart, expected_models=["sarima"])
    assert report["status"] == "PASS"
    assert report["n_months_per_model"] == 36
    assert report["max_actual_diff"] == 0.0


def test_reject_missing_month():
    mart, pred = _make_mart_and_pred()
    bad = pred.iloc[:-1].copy()
    with pytest.raises(ValueError, match="month mismatch"):
        val_mod.validate_model_predictions(bad, mart)


def test_reject_nan():
    mart, pred = _make_mart_and_pred()
    bad = pred.copy()
    bad.loc[0, "predicted_usd_t"] = np.nan
    with pytest.raises(ValueError, match="NaN"):
        val_mod.validate_model_predictions(bad, mart)


def test_reject_actual_mismatch():
    mart, pred = _make_mart_and_pred()
    bad = pred.copy()
    bad.loc[5, "actual_usd_t"] += 0.5
    with pytest.raises(ValueError, match="actual_usd_t mismatch"):
        val_mod.validate_model_predictions(bad, mart)
