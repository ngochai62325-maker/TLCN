# Báo cáo Mô hình XGBoost — Dự báo Giá Xuất khẩu Gạo Việt Nam 5% tấm (2023–2025)

**Người thực hiện:** Phúc  
**Nhánh:** `feat/forecast`  
**Script tái lập:** `python -m jobs.forecasting.run_xgboost_models`  
**Môi trường tái lập chuẩn:** `xgboost 3.3.0`, `scikit-learn 1.9.0`, `pandas 3.0.3`, `numpy 2.4.6`

---

## 1. Thiết lập Thí nghiệm & Quy tắc Chống Rò rỉ Dữ liệu

- **Nguồn dữ liệu:** `exports/forecast/forecasting_mart.csv` (`192` tháng từ `201001` đến `202512`).
- **Phân chia theo thời gian:**
  - Tập huấn luyện (`train`): `201001–202212` (`156` tháng).
  - Tập kiểm thử (`test`): `202301–202512` (`36` tháng), gồm giai đoạn cú sốc Ấn Độ cấm xuất khẩu gạo trắng (`2023`) và nhịp điều chỉnh (`2024–2025`).
- **Biến mục tiêu & tái tạo giá:**
  - Khi huấn luyện: dự báo biến sai phân `target_delta` ($\Delta y_t = y_t - y_{t-1}$).
  - Khi suy luận trên `test`: $\hat{y}_t = y_{t-1} + \widehat{\Delta y}_t$ (`y_lag_1 + y_hat_delta`).
- **Tuning siêu tham số:**
  - Dùng `TimeSeriesSplit(n_splits=5)` **chỉ trên tập `train` `2010–2022`** (tuyệt đối không sử dụng tập `test` `2023–2025` để chọn siêu tham số).
  - Lưới tham số gọn chống overfit cho tập nhỏ (`216` tổ hợp): `max_depth ∈ {2,3,4}`, `learning_rate ∈ {0.03,0.05,0.1}`, `n_estimators ∈ {50,100,150}`, `min_child_weight ∈ {3,5}`, `reg_alpha ∈ {0,1}`, `reg_lambda ∈ {1,5}`, `subsample=0.8`, `colsample_bytree=0.8`.
- **Xử lý giá trị khuyết (`D5`):** Giữ nguyên `NaN` ở các biến đối thủ (`india_parb_lag_1`, `pak5_lag_1`, `spread_vn_india_lag_1` trên `train`) để XGBoost tự học nhánh mặc định.

### Tập đặc trưng của 2 kịch bản
- **Kịch bản A (`xgb_base` — 14 đặc trưng):** `y_lag_1`, `y_lag_2`, `y_lag_3`, `y_lag_6`, `y_lag_12`, `y_roll_mean_3`, `y_roll_mean_6`, `y_roll_mean_12`, `y_roll_std_3`, `y_roll_std_6`, `y_roll_std_12`, `month_of_year`, `urea_lag_1`, `crude_oil_lag_1` *(loại bỏ tuyệt đối nhóm biến đối thủ)*.
- **Kịch bản B (`xgb_competitor` — 20 đặc trưng):** 14 đặc trưng của Kịch bản A + 6 biến đối thủ (`thai5_lag_1`, `india_parb_lag_1`, `pak5_lag_1`, `india_white_quote_missing_lag_1`, `spread_vn_thai_lag_1`, `spread_vn_india_lag_1`).

---

## 2. Kết quả Tuning trên Tập Train (`2010–2022`)

| Mô hình | CV RMSE (5-fold) | CV MAE (5-fold) | Train RMSE | Siêu tham số tốt nhất |
|---|---:|---:|---:|---|
| `xgb_base` | 15.2446 | 11.3671 | 14.3437 | `max_depth=3, lr=0.03, n_estimators=50, min_child_weight=5, reg_alpha=1.0, reg_lambda=5.0` |
| `xgb_competitor` | 14.9615 | 11.2155 | 13.6010 | `max_depth=3, lr=0.03, n_estimators=50, min_child_weight=3, reg_alpha=1.0, reg_lambda=5.0` |

---

## 3. Bảng So sánh Đối chứng trên Tập Test (`2023–2025`)

| Giai đoạn | Mô hình | Số tháng | MAE (USD/tấn) | RMSE (USD/tấn) | MAPE (%) |
|---|---|---:|---:|---:|---:|
| **2023–2025 (Toàn kỳ)** | `naive` | 36 | 14.7094 | 20.1049 | 3.0078 |
| | `sarima` | 36 | 14.2894 | 18.4900 | 2.9757 |
| | `xgb_base` | 36 | 15.6603 | 21.5661 | 3.1514 |
| | **`xgb_competitor`** | **36** | **12.4959** | **18.1833** | **2.5123** |
| **2023 (Cú sốc tăng giá)** | `naive` | 12 | 18.2325 | 23.9939 | 3.3944 |
| | `sarima` | 12 | 19.5843 | 24.3368 | 3.6391 |
| | `xgb_base` | 12 | 27.2125 | 32.6537 | 5.0569 |
| | **`xgb_competitor`** | **12** | **20.6726** | **26.2411** | **3.7746** |
| **2024–2025 (Nhịp điều chỉnh)** | `naive` | 24 | 12.9479 | 17.8453 | 2.8144 |
| | `sarima` | 24 | 11.6419 | 14.7200 | 2.6440 |
| | `xgb_base` | 24 | 9.8842 | 12.8262 | 2.1987 |
| | **`xgb_competitor`** | **24** | **8.4076** | **12.3146** | **1.8812** |

### Kết luận đối chứng: *"Thêm giá đối thủ có giúp dự báo tốt hơn không?"*
- **Có — cải thiện toàn diện trên mọi giai đoạn và mọi chỉ số:**
  1. **Toàn kỳ `2023–2025`**: `xgb_competitor` giảm **20.2% MAE** (`15.66 → 12.50 USD/tấn`) và **15.7% RMSE** (`21.57 → 18.18 USD/tấn`) so với `xgb_base`, đồng thời **vượt cả `naive` (`14.71 USD/tấn`) lẫn `sarima` (`14.29 USD/tấn`)**.
  2. **Giai đoạn cú sốc `2023`**: `xgb_base` chỉ dựa trên độ trễ nội sinh nên phản ứng chậm trước cú sốc cấm xuất khẩu gạo trắng của Ấn Độ (`MAE = 27.21 USD/tấn`). Khi bổ sung giá đối thủ, `xgb_competitor` cắt giảm **24.0% MAE** xuống `20.67 USD/tấn`.
  3. **Giai đoạn điều chỉnh `2024–2025`**: cả hai mô hình XGBoost đều vượt `naive` (`12.95`) và `sarima` (`11.64`), trong đó `xgb_competitor` đạt độ chính xác tốt nhất (`MAE = 8.41 USD/tấn`, `MAPE = 1.88%`).

---

## 4. Phân tích Feature Importance (`xgb_competitor`)

Tổng 6 biến đối thủ đóng góp **24.52% tổng Gain** của mô hình:

| Đặc trưng | Nhóm | Gain | Gain Share (%) | Perm. Train (RMSE) | Perm. Test (RMSE) |
|---|---|---:|---:|---:|---:|
| `y_lag_2` | `base` | 2668.71 | 10.91% | +2.015 | +0.218 |
| `y_roll_mean_3` | `base` | 2465.70 | 10.08% | +0.994 | +0.244 |
| `y_lag_3` | `base` | 1936.74 | 7.91% | +0.435 | +0.100 |
| `pak5_lag_1` | `competitor` | 1583.14 | 6.47% | +1.021 | **+2.618** |
| `spread_vn_thai_lag_1` | `competitor` | 1311.79 | 5.36% | +0.385 | +0.174 |
| `thai5_lag_1` | `competitor` | 1205.48 | 4.93% | +0.639 | -0.005 |
| `spread_vn_india_lag_1` | `competitor` | 1009.09 | 4.12% | +0.336 | +0.073 |
| `india_parb_lag_1` | `competitor` | 889.86 | 3.64% | +0.309 | +0.036 |
| `india_white_quote_missing_lag_1` | `competitor` | 0.00 | 0.00% | 0.000 | 0.000 |

- **Đóng góp của Pakistan & Thái Lan:** `pak5_lag_1` có **Permutation Importance lớn nhất trên tập test (`+2.618 USD/tấn` RMSE)** và Gain cao nhất nhóm đối thủ (`6.47%`), cho thấy giá gạo Pakistan 5% tấm truyền dẫn cực mạnh áp lực cung cầu khu vực sang giá gạo Việt Nam; `spread_vn_thai_lag_1` (`5.36%`, perm test `+0.174`) và `spread_vn_india_lag_1` (`4.12%`, perm test `+0.073`) đóng góp dương ổn định.
- **Đóng góp của Ấn Độ & cờ thiếu gạo trắng:** `india_parb_lag_1` đóng góp `3.64%` gain và dương trên permutation test (`+0.036`). Riêng cờ `india_white_quote_missing_lag_1` không được chọn làm nút chia (`0.00%` gain) với cấu hình chính quy hóa (`max_depth=3, reg_alpha=1.0`) do trên tập train `2010–2022` chỉ có 17 tháng cờ bật ở bối cảnh khác cú sốc 2023–2024; hiệu ứng của lệnh cấm Ấn Độ được phản ánh mạnh hơn qua biến động giá thực tế của các nước đối thủ (`pak5_lag_1`, `spread_vn_thai_lag_1`, `spread_vn_india_lag_1`, `india_parb_lag_1`).

---

## 5. Xác minh & Kiểm thử

- `validate_model_predictions.py`: **PASS** (`72` dòng XGBoost, `36` tháng/model, `max_actual_diff = 0.0`).
- `summarize_forecasts.py --require-all`: Đã tổng hợp đủ 4 mô hình `naive`, `sarima`, `xgb_base`, `xgb_competitor` (`144` dòng) và cập nhật 3 biểu đồ trong `exports/forecast/summary/`.
- `audit_dq19_cross_check.py --strict-warnings`: **PASS** (`0` cảnh báo, `counterfactual_diff = 0.0`, `saved_vs_local_max_diff = 0.0`).
- `pytest tests/unit/forecasting/`: **27/27 PASSED**.
