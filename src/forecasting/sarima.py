"""Mô hình dự báo giá gạo hàng tháng SARIMA theo quy trình 5 bước Box-Jenkins.

Các hàm được viết phẳng (plain functions), ngắn gọn, có giải thích ý nghĩa thống kê bằng tiếng Việt
để sinh viên dễ học và trình bày trước Hội đồng bảo vệ đồ án.
"""
from __future__ import annotations

import itertools
import warnings
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller, kpss


def check_stationarity(series: pd.Series) -> dict:
    """Bước 1 Box-Jenkins: Kiểm định tính dừng bằng cặp kiểm định đối ngẫu ADF và KPSS.

    - ADF (Augmented Dickey-Fuller): H0 = Chuỗi KHÔNG dừng (có nghiệm đơn vị). Cần p-value < 0.05 để bác bỏ H0.
    - KPSS: H0 = Chuỗi DỪNG quanh mức trung bình. Cần p-value >= 0.05 để chấp nhận H0.
    """
    clean = series.dropna().astype(float)
    adf_stat, adf_p, adf_lags, adf_nobs, adf_crit, _ = adfuller(clean, autolag="AIC")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kpss_stat, kpss_p, kpss_lags, kpss_crit = kpss(clean, regression="c", nlags="auto")

    adf_stationary = bool(adf_p < 0.05)
    kpss_stationary = bool(kpss_p >= 0.05)
    both_stationary = adf_stationary and kpss_stationary
    if both_stationary:
        conclusion = "Chuỗi DỪNG (ADF bác bỏ nghiệm đơn vị p < 0.05 và KPSS không bác bỏ tính dừng p >= 0.05)."
    elif not adf_stationary and not kpss_stationary:
        conclusion = "Chuỗi KHÔNG DỪNG (cần lấy sai phân d=1 để khử xu hướng)."
    else:
        conclusion = "Chuỗi bán dừng / có xu hướng cục bộ (nên lấy sai phân d=1)."

    return {
        "n_obs": int(adf_nobs),
        "adf_stat": round(float(adf_stat), 4),
        "adf_pvalue": round(float(adf_p), 4),
        "adf_is_stationary": adf_stationary,
        "kpss_stat": round(float(kpss_stat), 4),
        "kpss_pvalue": round(float(kpss_p), 4),
        "kpss_is_stationary": kpss_stationary,
        "is_stationary": both_stationary,
        "conclusion": conclusion,
    }


def split_train_test(series: pd.Series, test_months: int = 12) -> tuple[pd.Series, pd.Series]:
    """Bước 2: Chia tập Huấn luyện (Train) và Kiểm thử (Test) theo đúng thứ tự thời gian.

    Tuyệt đối không dùng random shuffle để tránh rò rỉ dữ liệu tương lai (data leakage).
    """
    if test_months <= 0 or test_months >= len(series):
        raise ValueError(f"test_months={test_months} không hợp lệ với chuỗi dài {len(series)} tháng")
    train = series.iloc[:-test_months].copy()
    test = series.iloc[-test_months:].copy()
    return train, test


def evaluate_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict:
    """Tính 4 chỉ số đánh giá sai số dự báo trên thang đo giá gốc (USD/tấn): MAE, RMSE, MAPE (%), sMAPE (%)."""
    df = pd.DataFrame({"actual": y_true, "pred": y_pred}).dropna()
    actual = df["actual"].to_numpy(dtype=float)
    pred = df["pred"].to_numpy(dtype=float)
    mae = float(mean_absolute_error(actual, pred))
    rmse = float(np.sqrt(mean_squared_error(actual, pred)))
    mape = float(np.mean(np.abs((actual - pred) / actual)) * 100.0)
    smape = float(np.mean(2.0 * np.abs(actual - pred) / (np.abs(actual) + np.abs(pred))) * 100.0)
    return {
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "MAPE_pct": round(mape, 2),
        "sMAPE_pct": round(smape, 2),
    }


def forecast_seasonal_naive(train: pd.Series, horizon: int, m: int = 12) -> pd.Series:
    """Mô hình cơ sở (Baseline Seasonal Naive): dự báo giá tháng t bằng giá của đúng tháng đó năm trước (t - 12)."""
    clean_train = train.dropna()
    last_season = clean_train.iloc[-m:].to_numpy(dtype=float)
    reps = int(np.ceil(horizon / m))
    values = np.tile(last_season, reps)[:horizon]
    future_idx = pd.date_range(train.index[-1] + pd.DateOffset(months=1), periods=horizon, freq="MS")
    return pd.Series(values, index=future_idx, name="seasonal_naive")


def grid_search_sarima(
    train: pd.Series,
    p_vals: tuple = (0, 1, 2),
    d_vals: tuple = (1,),
    q_vals: tuple = (0, 1, 2),
    P_vals: tuple = (0, 1),
    D_vals: tuple = (0, 1),
    Q_vals: tuple = (0, 1),
    m: int = 12,
    use_log: bool = True,
) -> dict:
    """Bước 3 Box-Jenkins: Tìm lưới tham số SARIMA(p,d,q)(P,D,Q,m) có chỉ số AIC thấp nhất.

    Biến đổi `np.log(train)` giúp ổn định phương sai khi giá gạo có những đợt biến động mạnh.
    """
    y = np.log(train.astype(float)) if use_log else train.astype(float)
    records = []
    orders = list(itertools.product(p_vals, d_vals, q_vals))
    seasonal_orders = [(P, D, Q, m) for P, D, Q in itertools.product(P_vals, D_vals, Q_vals)]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for order in orders:
            for s_order in seasonal_orders:
                try:
                    model = SARIMAX(
                        y,
                        order=order,
                        seasonal_order=s_order,
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    res = model.fit(disp=False, maxiter=100)
                    if np.isfinite(res.aic) and np.isfinite(res.bic):
                        records.append({
                            "order": order,
                            "seasonal_order": s_order,
                            "AIC": round(float(res.aic), 2),
                            "BIC": round(float(res.bic), 2),
                        })
                except Exception:
                    continue

    if not records:
        raise RuntimeError("Không hội tụ được cấu hình SARIMA nào trên tập train")
    ranking = pd.DataFrame(records).sort_values(["AIC", "BIC"]).reset_index(drop=True)
    best = ranking.iloc[0]
    return {
        "best_order": tuple(best["order"]),
        "best_seasonal_order": tuple(best["seasonal_order"]),
        "best_aic": float(best["AIC"]),
        "best_bic": float(best["BIC"]),
        "total_evaluated": len(ranking),
        "top_5": ranking.head(5),
    }


def fit_and_forecast(
    series: pd.Series,
    order: tuple,
    seasonal_order: tuple,
    horizon: int = 12,
    use_log: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """Bước 4 & 5 Box-Jenkins: Ước lượng SARIMA, kiểm định phần dư Ljung-Box và dự báo `horizon` tháng tới.

    - Ljung-Box test tại trễ 12: H0 = Phần dư là nhiễu trắng (White Noise, không còn tự tương quan).
      Nếu `ljung_box_pvalue >= 0.05`, mô hình đã trích xuất hết thông tin trong chuỗi.
    - Nếu dùng `use_log=True`, kết quả dự báo và khoảng tin cậy 95% được biến đổi ngược bằng `np.exp()`.
    """
    y = np.log(series.astype(float)) if use_log else series.astype(float)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = SARIMAX(
            y,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False, maxiter=150)
        pred_res = res.get_forecast(steps=horizon)
        mean_pred = pred_res.predicted_mean
        conf_df = pred_res.conf_int(alpha=0.05)

    future_idx = pd.date_range(series.index[-1] + pd.DateOffset(months=1), periods=horizon, freq="MS")
    lower_raw = conf_df.iloc[:, 0].to_numpy(dtype=float)
    upper_raw = conf_df.iloc[:, 1].to_numpy(dtype=float)
    point_raw = mean_pred.to_numpy(dtype=float)

    if use_log:
        point = np.exp(point_raw)
        lower = np.exp(lower_raw)
        upper = np.exp(upper_raw)
    else:
        point, lower, upper = point_raw, lower_raw, upper_raw

    # Bỏ các kỳ đầu bị ảnh hưởng bởi khởi tạo trạng thái Kalman và sai phân khi kiểm định phần dư
    burn_in = max(
        1,
        int(getattr(res, "loglikelihood_burn", 0)),
        order[0] + order[1] + seasonal_order[1] * seasonal_order[3],
    )
    clean_resid = res.resid.iloc[burn_in:].dropna()
    lag = min(12, max(1, len(clean_resid) // 3))
    lb_df = acorr_ljungbox(clean_resid, lags=[lag], return_df=True)
    lb_pvalue = float(lb_df["lb_pvalue"].iloc[0])

    forecast_df = pd.DataFrame(
        {
            "month_start": future_idx,
            "forecast": np.round(point, 2),
            "lower_95": np.round(lower, 2),
            "upper_95": np.round(upper, 2),
        },
        index=future_idx,
    )
    diagnostics = {
        "order": tuple(order),
        "seasonal_order": tuple(seasonal_order),
        "use_log": bool(use_log),
        "aic": round(float(res.aic), 2),
        "bic": round(float(res.bic), 2),
        "ljung_box_lag": int(lag),
        "ljung_box_pvalue": round(lb_pvalue, 4),
        "residuals_white_noise": bool(lb_pvalue >= 0.05),
        "resid_mean": round(float(clean_resid.mean()), 5),
        "resid_std": round(float(clean_resid.std()), 5),
        "fitted_result": res,
    }
    return forecast_df, diagnostics


def rolling_backtest_sarima(
    series: pd.Series,
    test_months: int,
    order: tuple,
    seasonal_order: tuple,
    use_log: bool = True,
) -> pd.Series:
    """Backtest cuốn chiếu 1 bước (Rolling 1-step-ahead): dự báo từng tháng t+1 rồi cập nhật giá thực tế vào bộ lọc Kalman.

    Cách đánh giá này phản ánh đúng vận hành thực tế hàng tháng: mỗi khi có giá đóng cửa tháng mới,
    hệ thống cập nhật trạng thái (`res.append(..., refit=False)`) để dự báo tháng kế tiếp.
    """
    train, test = split_train_test(series, test_months=test_months)
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
        preds = []
        for i in range(len(test)):
            step_pred = float(res.forecast(steps=1).iloc[0])
            preds.append(float(np.exp(step_pred)) if use_log else step_pred)
            if i < len(test) - 1:
                res = res.append(y_test.iloc[i : i + 1], refit=False)

    return pd.Series(np.round(preds, 2), index=test.index, name="rolling_1step_pred")


def run_sarima_pipeline(
    series: pd.Series,
    test_months: int = 12,
    horizon: int = 12,
    use_log: bool = True,
) -> dict:
    """Chạy trọn bộ quy trình dự báo SARIMA từ kiểm định tính dừng -> chọn mô hình -> backtest -> dự báo tương lai."""
    log_s = np.log(series.dropna().astype(float))
    diff_log_s = log_s.diff().dropna()
    stationarity = {
        "raw_level": check_stationarity(series),
        "log_level": check_stationarity(log_s),
        "log_diff_1": check_stationarity(diff_log_s),
    }

    train, test = split_train_test(series, test_months=test_months)
    search = grid_search_sarima(train, use_log=use_log)
    best_order = search["best_order"]
    best_seasonal = search["best_seasonal_order"]

    # Backtest trên tập Test: cả 12-step Static lẫn 1-step Rolling
    bt_df, bt_diag = fit_and_forecast(train, best_order, best_seasonal, horizon=len(test), use_log=use_log)
    rolling_pred = rolling_backtest_sarima(series, test_months=test_months, order=best_order, seasonal_order=best_seasonal, use_log=use_log)
    naive_pred = forecast_seasonal_naive(train, horizon=len(test))
    sarima_metrics = evaluate_metrics(test, bt_df["forecast"])
    rolling_metrics = evaluate_metrics(test, rolling_pred)
    naive_metrics = evaluate_metrics(test, naive_pred)

    backtest_table = pd.DataFrame({
        "month_start": test.index,
        "actual": test.values,
        "rolling_1step_pred": rolling_pred.values,
        "sarima_pred": bt_df["forecast"].values,
        "lower_95": bt_df["lower_95"].values,
        "upper_95": bt_df["upper_95"].values,
        "seasonal_naive_pred": naive_pred.values,
    }, index=test.index)

    # Huấn luyện lại trên toàn bộ chuỗi (Train + Test) để dự báo `horizon` tháng tương lai
    future_df, full_diag = fit_and_forecast(series, best_order, best_seasonal, horizon=horizon, use_log=use_log)
    return {
        "stationarity": stationarity,
        "train": train,
        "test": test,
        "grid_search": search,
        "best_order": best_order,
        "best_seasonal_order": best_seasonal,
        "backtest_df": backtest_table,
        "backtest_diagnostics": bt_diag,
        "rolling_1step_metrics": rolling_metrics,
        "sarima_metrics": sarima_metrics,
        "seasonal_naive_metrics": naive_metrics,
        "future_forecast_df": future_df,
        "full_diagnostics": full_diag,
    }
