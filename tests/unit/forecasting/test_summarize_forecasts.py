import matplotlib
matplotlib.use("Agg")
import pandas as pd
import pytest

from jobs.forecasting import summarize_forecasts as sum_mod
from jobs.forecasting.validate_model_predictions import EXPECTED_MONTHS


def _write_mart(path):
    mart = pd.DataFrame({
        "month_key": EXPECTED_MONTHS,
        "split": "test",
        "y": [450.0 + i * 2.0 for i in range(len(EXPECTED_MONTHS))],
    })
    mart.to_csv(path, index=False)
    return mart


def _write_pred(path, mart, offsets):
    frames = []
    for name, delta in offsets.items():
        frames.append(pd.DataFrame({
            "month_key": mart["month_key"],
            "model_name": name,
            "split": "test",
            "actual_usd_t": mart["y"],
            "predicted_usd_t": mart["y"] + delta,
        }))
    pd.concat(frames, ignore_index=True).to_csv(path, index=False)


def test_run_summary_partial_models(tmp_path):
    mart_csv = tmp_path / "mart.csv"
    models_dir = tmp_path / "models"
    out_dir = tmp_path / "out"
    models_dir.mkdir()
    mart = _write_mart(mart_csv)
    _write_pred(models_dir / "naive_predictions.csv", mart, {"naive": -5.0})

    pred_df, metrics_df = sum_mod.run_summary(mart_csv=mart_csv, models_dir=models_dir, out_dir=out_dir)

    assert len(pred_df) == 36
    assert len(metrics_df) == 3
    assert (out_dir / "forecast_predictions.csv").is_file()
    assert (out_dir / "forecast_metrics.csv").is_file()
    assert (out_dir / "summary" / "fig1_actual_vs_predicted_2023_2025.png").is_file()
    assert (out_dir / "summary" / "fig2_actual_vs_predicted_by_period.png").is_file()
    assert (out_dir / "summary" / "fig3_metrics_by_period.png").is_file()


def test_require_all_rejects_missing_models(tmp_path):
    mart_csv = tmp_path / "mart.csv"
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    mart = _write_mart(mart_csv)
    _write_pred(models_dir / "naive_predictions.csv", mart, {"naive": -5.0})

    with pytest.raises(ValueError, match="Unexpected models"):
        sum_mod.load_and_merge_predictions(mart, models_dir=models_dir, require_all=True)


def test_run_summary_all_four_models(tmp_path):
    mart_csv = tmp_path / "mart.csv"
    models_dir = tmp_path / "models"
    out_dir = tmp_path / "out"
    models_dir.mkdir()
    mart = _write_mart(mart_csv)
    _write_pred(models_dir / "naive_predictions.csv", mart, {"naive": -10.0})
    _write_pred(models_dir / "sarima_predictions.csv", mart, {"sarima": -6.0})
    _write_pred(models_dir / "xgb_predictions.csv", mart, {"xgb_base": -8.0, "xgb_competitor": -4.0})

    pred_df, metrics_df = sum_mod.run_summary(
        mart_csv=mart_csv, models_dir=models_dir, out_dir=out_dir, require_all=True
    )

    assert len(pred_df) == 144
    assert len(metrics_df) == 12
    assert pred_df["model_name"].drop_duplicates().tolist() == sum_mod.ALL_MODELS