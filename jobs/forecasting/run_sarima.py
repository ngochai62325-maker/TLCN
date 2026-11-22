#!/usr/bin/env python3
"""CLI chạy mô hình dự báo giá gạo hàng tháng SARIMA và tích hợp kết quả vào exports/forecast/."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from forecasting.data_loader import load_monthly_rice_price, load_series_from_mart  # noqa: E402
from forecasting.metrics import (  # noqa: E402
    build_prediction_frame,
    evaluate_predictions,
    validate_model_predictions,
    validate_predictions,
)
from forecasting.sarima import rolling_backtest_sarima, run_sarima_pipeline  # noqa: E402
from forecasting.summary import (  # noqa: E402
    SARIMA_MODEL_NAMES,
    upsert_forecast_metrics,
    upsert_forecast_predictions,
)

DEFAULT_OUT_DIR = ROOT / "exports" / "forecast"
DEFAULT_MART_CSV = DEFAULT_OUT_DIR / "forecasting_mart.csv"

__all__ = [
    "DEFAULT_OUT_DIR",
    "DEFAULT_MART_CSV",
    "SARIMA_MODEL_NAMES",
    "load_series_from_mart",
    "upsert_forecast_predictions",
    "upsert_forecast_metrics",
    "main",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["mart", "raw", "trino"], default="mart", help="Nguồn dữ liệu: 'mart' (forecasting_mart.csv), 'raw' (Excel WB) hoặc 'trino' (Gold)")
    parser.add_argument("--mart-csv", type=Path, default=DEFAULT_MART_CSV, help="Đường dẫn file forecasting_mart.csv của nhóm")
    parser.add_argument("--series", default="Rice, Viet Namese 5%", help="Tên chuỗi giá gạo trong World Bank Pink Sheet")
    parser.add_argument("--start-date", default="2010-01-01", help="Ngày bắt đầu cửa sổ huấn luyện (mặc định 2010-01-01 theo chuẩn nhóm)")
    parser.add_argument("--test-months", type=int, default=36, help="Số tháng kiểm thử (mặc định 36 tháng: 2023-01 -> 2025-12 theo chuẩn nhóm)")
    parser.add_argument("--horizon", type=int, default=12, help="Số tháng dự báo tương lai sau năm 2025")
    parser.add_argument("--no-log", action="store_true", help="Tắt phép biến đổi logarit tự nhiên trước khi khớp SARIMA")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR, help="Thư mục lưu kết quả (mặc định: exports/forecast)")
    args = parser.parse_args(argv)

    use_log = not args.no_log
    out_dir = Path(args.output_dir)
    models_dir = out_dir / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    mart_df = None
    if args.source == "mart" and Path(args.mart_csv).is_file():
        series, mart_df, meta = load_series_from_mart(Path(args.mart_csv))
        test_months = meta["test_months"]
    else:
        start_date = None if str(args.start_date).strip().lower() in ("none", "null", "") else args.start_date
        raw_source = "raw" if args.source == "mart" else args.source
        series, meta = load_monthly_rice_price(series_name=args.series, source=raw_source, start_date=start_date)
        test_months = args.test_months
        if Path(args.mart_csv).is_file():
            mart_df = pd.read_csv(args.mart_csv)

    result = run_sarima_pipeline(series, test_months=test_months, horizon=args.horizon, use_log=use_log)

    # Dự báo cuốn chiếu 1 bước (1-step Rolling) trên tập Test để so sánh công bằng với Naive (y_lag_1) và XGBoost 1-step
    rolling_raw = rolling_backtest_sarima(
        series,
        test_months=test_months,
        order=result["best_order"],
        seasonal_order=result["best_seasonal_order"],
        use_log=use_log,
        round_decimals=None,
    )
    test_series = result["test"]
    month_keys = [int(dt.strftime("%Y%m")) for dt in test_series.index]

    sarima_pred_df = build_prediction_frame(
        month_key=month_keys,
        model_name="sarima",
        actual_usd_t=test_series.to_numpy(dtype=float),
        predicted_usd_t=rolling_raw.to_numpy(dtype=float),
        split="test",
    )

    if mart_df is not None and len(test_series) == int((mart_df["split"] == "test").sum()):
        validate_predictions(sarima_pred_df, mart_df, expected_models=("sarima",))
        validate_model_predictions(sarima_pred_df, mart_df, expected_models=["sarima"])

    sarima_metrics_df = evaluate_predictions(sarima_pred_df)

    # Lưu bản riêng của SARIMA trong exports/forecast/models/ và upsert vào 2 bảng tổng hợp của nhóm
    sarima_pred_df.to_csv(models_dir / "sarima_predictions.csv", index=False)
    sarima_metrics_df.to_csv(models_dir / "sarima_metrics.csv", index=False)

    all_preds_df = upsert_forecast_predictions(out_dir / "forecast_predictions.csv", sarima_pred_df)
    all_metrics_df = upsert_forecast_metrics(out_dir / "forecast_metrics.csv", sarima_metrics_df)

    # Lưu thông tin chẩn đoán chuyên sâu của SARIMA (AIC, BIC, Ljung-Box, ADF/KPSS, dự báo tương lai 2026)
    diag = {k: v for k, v in result["full_diagnostics"].items() if k != "fitted_result"}
    bt_diag = {k: v for k, v in result["backtest_diagnostics"].items() if k != "fitted_result"}
    fc_records = result["future_forecast_df"].copy()
    fc_records["month_start"] = fc_records["month_start"].dt.strftime("%Y-%m-%d")

    diagnostics_payload = {
        "metadata": meta,
        "model_name": "sarima",
        "best_order": list(result["best_order"]),
        "best_seasonal_order": list(result["best_seasonal_order"]),
        "use_log": use_log,
        "stationarity": result["stationarity"],
        "rolling_1step_metrics": result["rolling_1step_metrics"],
        "static_multistep_metrics": result["sarima_metrics"],
        "seasonal_naive_metrics": result["seasonal_naive_metrics"],
        "eval_periods_metrics": sarima_metrics_df.to_dict("records"),
        "train_diagnostics": bt_diag,
        "full_series_diagnostics": diag,
        "top_5_candidates": [
            {
                "order": list(r["order"]),
                "seasonal_order": list(r["seasonal_order"]),
                "AIC": float(r["AIC"]),
                "BIC": float(r["BIC"]),
            }
            for r in result["grid_search"]["top_5"].to_dict("records")
        ],
        "future_forecast_2026": fc_records.to_dict("records"),
    }
    diag_path = out_dir / "sarima_diagnostics.json"
    diag_path.write_text(json.dumps(diagnostics_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(" KẾT QUẢ DỰ BÁO GIÁ GẠO HÀNG THÁNG (SARIMA — TÍCH HỢP EXPORTS/FORECAST)")
    print(f"Chuỗi dữ liệu : {meta['series_name']} ({meta['start_month']} -> {meta['end_month']}, {meta['total_months']} tháng, test={test_months} tháng)")
    print(f"Cấu hình tối ưu: SARIMA{result['best_order']}x{result['best_seasonal_order']} (use_log={use_log})")
    print(f"Phần dư (sau burn-in): resid_std={diag['resid_std']} | Ljung-Box (lag={diag['ljung_box_lag']}): p-value={diag['ljung_box_pvalue']} (white_noise={diag['residuals_white_noise']})")
    print("\n=== BẢNG TỔNG HỢP SAI SỐ CÁC MÔ HÌNH (exports/forecast/forecast_metrics.csv) ===")
    print(all_metrics_df.to_string(index=False))
    print(f"\nTổng số dòng trong forecast_predictions.csv: {len(all_preds_df)} (models={all_preds_df['model_name'].drop_duplicates().tolist()})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
