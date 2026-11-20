# Báo cáo Mô hình XGBoost — Dự báo Giá Xuất khẩu Gạo Việt Nam 5% tấm (2023–2025)

**Người thực hiện:** Phúc  
**Nhánh:** `feat/forecast`  
**Script tái lập:** `python -m jobs.forecasting.run_xgboost_models`

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
| `xgb_base` | 15.2517 | 11.2576 | 14.3194 | `max_depth=3, lr=0.03, n_estimators=50, min_child_weight=3, reg_alpha=1.0, reg_lambda=5.0` |
| `xgb_competitor` | 14.7787 | 11.0607 | 12.6414 | `max_depth=4, lr=0.03, n_estimators=50, min_child_weight=3, reg_alpha=0.0, reg_lambda=5.0` |

---

## 3. Bảng So sánh Đối chứng trên Tập Test (`2023–2025`)

| Giai đoạn | Mô hình | Số tháng | MAE (USD/tấn) | RMSE (USD/tấn) | MAPE (%) |
|---|---|---:|---:|---:|---:|
| **2023–2025 (Toàn kỳ)** | `naive` | 36 | 14.7094 | 20.1049 | 3.0078 |
| | `xgb_base` | 36 | 15.6914 | 21.4475 | 3.1620 |
| | **`xgb_competitor`** | **36** | **12.3984** | **18.1428** | **2.4798** |
| **2023 (Cú sốc tăng giá)** | `naive` | 12 | 18.2325 | 23.9939 | 3.3944 |
| | `xgb_base` | 12 | 27.0994 | 32.5382 | 5.0320 |
| | **`xgb_competitor`** | **12** | **20.3405** | **26.2473** | **3.7027** |
| **2024–2025 (Nhịp điều chỉnh)** | `naive` | 24 | 12.9479 | 17.8453 | 2.8144 |
| | `xgb_base` | 24 | 9.9873 | 12.6738 | 2.2270 |
| | **`xgb_competitor`** | **24** | **8.4274** | **12.2181** | **1.8683** |

### Kết luận đối chứng: *"Thêm giá đối thủ có giúp dự báo tốt hơn không?"*
- **Có — cải thiện toàn diện trên mọi giai đoạn và mọi chỉ số:**
  1. **Toàn kỳ `2023–2025`**: `xgb_competitor` giảm **21.0% MAE** (`15.69 → 12.40 USD/tấn`) và **15.4% RMSE** (`21.45 → 18.14 USD/tấn`) so với `xgb_base`, đồng thời **vượt qua mô hình chuẩn `naive`** (`12.40` vs `14.71 USD/tấn`).
  2. **Giai đoạn cú sốc `2023`**: `xgb_base` chỉ dựa trên độ trễ nội sinh nên phản ứng chậm trước cú sốc cấm xuất khẩu gạo trắng của Ấn Độ (`MAE = 27.10`). Khi bổ sung giá đối thủ, `xgb_competitor` cắt giảm **24.9% MAE** (xuống `20.34 USD/tấn`).
  3. **Giai đoạn điều chỉnh `2024–2025`**: Cả hai mô hình XGBoost đều vượt xa `naive` (`12.95`), trong đó `xgb_competitor` đạt độ chính xác tốt nhất (`MAE = 8.43 USD/tấn`, `MAPE = 1.87%`).

---

## 4. Phân tích Feature Importance (`xgb_competitor`)

Tổng 6 biến đối thủ đóng góp **28.63% tổng Gain** của mô hình:

| Đặc trưng | Nhóm | Gain | Gain Share (%) | Perm. Train (RMSE) | Perm. Test (RMSE) |
|---|---|---:|---:|---:|---:|
| `y_lag_2` | `base` | 1996.89 | 11.39% | +2.812 | +0.400 |
| `y_lag_3` | `base` | 1564.36 | 8.93% | +0.915 | +0.272 |
| `y_roll_mean_3` | `base` | 1227.30 | 7.00% | +0.568 | +0.039 |
| `thai5_lag_1` | `competitor` | 1131.76 | 6.46% | +0.886 | +0.087 |
| `pak5_lag_1` | `competitor` | 1111.69 | 6.34% | +1.364 | **+2.620** |
| `spread_vn_thai_lag_1` | `competitor` | 1052.85 | 6.01% | +0.628 | +0.239 |
| `india_parb_lag_1` | `competitor` | 833.80 | 4.76% | +0.330 | +0.039 |
| `spread_vn_india_lag_1` | `competitor` | 754.79 | 4.31% | +0.518 | -0.009 |
| `india_white_quote_missing_lag_1` | `competitor` | 133.03 | 0.76% | +0.028 | -0.004 |

- **Đóng góp của Pakistan & Thái Lan:** `pak5_lag_1` có **Permutation Importance lớn nhất trên tập test (`+2.620 USD/tấn` RMSE)**, cho thấy giá gạo Pakistan 5% tấm truyền dẫn cực mạnh áp lực cung cầu khu vực sang giá gạo Việt Nam; `thai5_lag_1` và `spread_vn_thai_lag_1` đóng góp ổn định trên cả train và test.
- **Đóng góp của Ấn Độ & cờ thiếu gạo trắng:** `india_parb_lag_1` đóng góp `4.76%` gain và dương trên permutation test (`+0.039`). Riêng cờ `india_white_quote_missing_lag_1` có đóng góp trực tiếp nhỏ (`0.76%` gain) do trong tập train `2010–2022` chỉ có 17 tháng cờ bật ở bối cảnh khác cú sốc 2023–2024; hiệu ứng của lệnh cấm Ấn Độ được phản ánh mạnh mẽ hơn qua biến động giá thực tế của các nước xuất khẩu thay thế (`pak5_lag_1`, `thai5_lag_1`, `spread_vn_thai_lag_1`).

---

## 5. Xác minh & Kiểm thử

- `validate_model_predictions.py`: **PASS** (`72` dòng, `36` tháng/model, `max_actual_diff = 0.0`).
- `summarize_forecasts.py`: Đã tổng hợp `naive`, `xgb_base`, `xgb_competitor` (`108` dòng) và cập nhật 3 biểu đồ trong `exports/forecast/summary/`.
- `pytest tests/unit/forecasting/`: **16/16 PASSED**.
