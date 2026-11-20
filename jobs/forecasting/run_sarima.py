#!/usr/bin/env python3
"""CLI chạy mô hình dự báo giá gạo hàng tháng SARIMA và xuất kết quả CSV/JSON."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from forecasting.data_loader import load_monthly_rice_price
from forecasting.sarima import run_sarima_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["raw", "trino"], default="raw", help="Nguồn dữ liệu: 'raw' (Excel WB) hoặc 'trino' (Gold)")
    parser.add_argument("--series", default="Rice, Viet Namese 5%", help="Tên chuỗi giá gạo trong World Bank Pink Sheet")
    parser.add_argument("--start-date", default="2009-04-01", help="Ngày bắt đầu cửa sổ huấn luyện (truyền 'none' để lấy từ 2003-12)")
    parser.add_argument("--test-months", type=int, default=12, help="Số tháng cuối dùng để Backtest đánh giá sai số")
    parser.add_argument("--horizon", type=int, default=12, help="Số tháng dự báo tương lai")
    parser.add_argument("--no-log", action="store_true", help="Tắt phép biến đổi logarit tự nhiên trước khi khớp SARIMA")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/outputs/forecasting", help="Thư mục lưu file kết quả")
    args = parser.parse_args()

    start_date = None if str(args.start_date).strip().lower() in ("none", "null", "") else args.start_date
    use_log = not args.no_log

    series, meta = load_monthly_rice_price(series_name=args.series, source=args.source, start_date=start_date)
    result = run_sarima_pipeline(series, test_months=args.test_months, horizon=args.horizon, use_log=use_log)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    backtest_path = out_dir / "sarima_backtest.csv"
    forecast_path = out_dir / "sarima_forecast.csv"
    metrics_path = out_dir / "sarima_metrics.json"

    bt_export = result["backtest_df"].copy()
    bt_export["month_start"] = bt_export["month_start"].dt.strftime("%Y-%m-%d")
    bt_export.to_csv(backtest_path, index=False, encoding="utf-8-sig")

    fc_export = result["future_forecast_df"].copy()
    fc_export["month_start"] = fc_export["month_start"].dt.strftime("%Y-%m-%d")
    fc_export.to_csv(forecast_path, index=False, encoding="utf-8-sig")

    diag = {k: v for k, v in result["full_diagnostics"].items() if k != "fitted_result"}
    summary_payload = {
        "metadata": meta,
        "best_order": list(result["best_order"]),
        "best_seasonal_order": list(result["best_seasonal_order"]),
        "use_log": use_log,
        "stationarity": result["stationarity"],
        "sarima_backtest_metrics": result["sarima_metrics"],
        "seasonal_naive_backtest_metrics": result["seasonal_naive_metrics"],
        "diagnostics": diag,
        "top_5_candidates": [
            {
                "order": list(r["order"]),
                "seasonal_order": list(r["seasonal_order"]),
                "AIC": float(r["AIC"]),
                "BIC": float(r["BIC"]),
            }
            for r in result["grid_search"]["top_5"].to_dict("records")
        ],
    }
    metrics_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(" KẾT QUẢ DỰ BÁO GIÁ GẠO HÀNG THÁNG (SARIMA)")
    print(f"Chuỗi dữ liệu : {meta['series_name']} ({meta['start_month']} -> {meta['end_month']}, {meta['total_months']} tháng, khuyết={meta['missing_count']})")
    print(f"Cấu hình tối ưu: SARIMA{result['best_order']}x{result['best_seasonal_order']} (use_log={use_log})")
    print(f"Backtest SARIMA ({args.test_months} tháng): MAE={result['sarima_metrics']['MAE']} | RMSE={result['sarima_metrics']['RMSE']} | MAPE={result['sarima_metrics']['MAPE_pct']}% | sMAPE={result['sarima_metrics']['sMAPE_pct']}%")
    print(f"Backtest Naive  ({args.test_months} tháng): MAE={result['seasonal_naive_metrics']['MAE']} | RMSE={result['seasonal_naive_metrics']['RMSE']} | MAPE={result['seasonal_naive_metrics']['MAPE_pct']}% | sMAPE={result['seasonal_naive_metrics']['sMAPE_pct']}%")
    print(f"Kiểm định phần dư Ljung-Box (lag={diag['ljung_box_lag']}): p-value={diag['ljung_box_pvalue']} (white_noise={diag['residuals_white_noise']})")
    print(f"Đã lưu kết quả tại: {out_dir}")


if __name__ == "__main__":
    main()
