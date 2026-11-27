"""Mô hình cơ sở Naive 1-step (Random Walk: y_hat(t) = y(t-1))."""
from __future__ import annotations

import numpy as np
import pandas as pd

from forecasting.metrics import build_prediction_frame


def predict_naive(mart_df: pd.DataFrame) -> pd.DataFrame:
    """Sinh dự báo Naive trên tập test từ cột y_lag_1 của forecasting_mart."""
    df = mart_df.sort_values("month_key").reset_index(drop=True)
    assert np.allclose(df.loc[1:, "y_lag_1"], df["y"].shift(1).iloc[1:], atol=1e-8, rtol=0)
    test_df = df[df["split"] == "test"]
    return build_prediction_frame(
        month_key=test_df["month_key"],
        model_name="naive",
        actual_usd_t=test_df["y"],
        predicted_usd_t=test_df["y_lag_1"],
    )
