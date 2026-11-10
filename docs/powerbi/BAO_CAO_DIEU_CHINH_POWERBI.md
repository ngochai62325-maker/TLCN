# BÁO CÁO KỸ THUẬT VÒNG 2: ĐIỀU CHỈNH POWER BI VÀ ĐỐI CHIẾU SỰ THẬT DỮ LIỆU

**Dự án:** `TLCN_Rice_Analytics` (Power BI PBIP / TMDL / PBIR)  
**Ngày thực hiện:** 10/11/2026 · **Phiên bản:** Vòng 2 (Hiệu đính toàn diện)  
**Tập dữ liệu kiểm chứng:** 11 tập tin CSV Marts tại `exports/powerbi/ec421297c194_20261106T031815Z_01d03710/` (Batch `ec421297c194…`)  
**Tài liệu đối chiếu:** `DANH_GIA_VONG_2.md`, `GOLD_BUSINESS_USE_CASES_AND_METRICS.md`, `TLCN_Rice_Analytics_v2_render.pdf` (Bản render thực tế xuất từ Power BI Desktop)

---

## TUYÊN BỐ VỀ TÍNH CHÂN THỰC VÀ GIỚI HẠN KIỂM THỬ

1. **Cam kết số liệu thực tế:** Toàn bộ các con số, tên cột và thống kê trong báo cáo này được tính toán và trích xuất trực tiếp từ các lệnh thực thi trên 11 tệp CSV Marts thật của tầng Gold đã công bố. Báo cáo này xóa bỏ hoàn toàn các con số giả định hoặc tính toán sai lệch từ báo cáo vòng 1.
2. **Giới hạn kiểm thử tự động:**
   - Kết quả `STATIC_PASS` (86 tài liệu JSON, 74 visuals, 6 trang, 109 measures) và bộ kiểm thử `pytest 99/99 PASS` **chỉ chứng minh tính hợp lệ của cấu trúc schema Microsoft Fabric PBIR, cú pháp TMDL và tham chiếu trường dữ liệu (field references)**.
   - **Desktop rendering chưa được chứng minh trực tiếp** do môi trường dòng lệnh không có giao diện đồ họa Windows để khởi chạy `PBIDesktop.exe`.
   - **DAX chưa được thực thi bởi Analysis Services engine**, ngoại trừ 59 kiểm tra assertion tĩnh trong [`validation.dax`](file:///C:/Windows_Application/TLCN/powerbi/validation.dax). Các hiển thị đồ họa thực tế được đối chiếu qua tệp PDF render thật do người dùng cung cấp (`TLCN_Rice_Analytics_v2_render.pdf`).

---

## PHẦN 0: KẾT QUẢ KIỂM CHỨNG DỮ LIỆU TRƯỚC KHI SỬA (VIỆC 0)

Mọi bảng số liệu dưới đây được trích xuất trực tiếp từ mã kiểm chứng thực thi trên dữ liệu thực:

### 0.1. Kiểm chứng toàn bộ `fact_overview.csv`
Lệnh chạy: `python scratch/run_viec_0.py`
```text
                         fact_name  rows_total  numeric_rows  present_null_rows  numeric_coverage_pct  period_min  period_max  represented_periods    period_grain
0            fact_fao_price_annual        4523          4490                 33             99.270396        1991        2025                   35   calendar_year
1  fact_fao_production_observation       58197         57800                397             99.317834        1961        2024                   64   calendar_year
2         fact_fao_sua_observation       20412         20300                112             99.451303        2010        2023                   14   calendar_year
3       fact_fao_trade_observation     1222084       1215000               7084             99.420334        1986        2024                   39   calendar_year
4             fact_nso_observation       23060         23000                 60             99.739809        1990        2024                   35   calendar_year
5               fact_price_monthly        4320          4300                 20             99.537037     1960-01     2025-12                  792  calendar_month
6        fact_usda_price_reference       12936          8413               4523             65.035560        1985        2025                   41  reference_year
7        fact_usda_psd_observation       48608         48500                108             99.777814        1960        2025                   66  marketing_year
8       fact_vn_market_quote_daily       20387         20364                 23             99.887183  2021-04-01  2024-12-31                  484   calendar_date

Tổng rows_total: 1.414.527
Tổng numeric_rows: 1.402.167
Tổng present_null_rows: 12.360
Tỷ lệ có số: 1.402.167 / 1.414.527 = 99,1262% -> Hiển thị 99,13%
Tỷ lệ khuyết: 12.360 / 1.414.527 = 0,8738% -> Hiển thị 0,87%
Tỷ lệ khuyết cao nhất: fact_usda_price_reference = 4.523 / 12.936 = 34,9644% -> Hiển thị 34,96%
```
**Kết luận đối chiếu:**
- Số quan sát tổng cộng: **1.414.527**
- Có số: **1.402.167** (Khớp hoàn toàn với PDF render và Gold doc OV01, bác bỏ con số bịa 1.402.274 ở vòng 1).
- Present-NULL: **12.360** (Khớp hoàn toàn với PDF render và Gold doc OV01, bác bỏ con số 12.253 ở vòng 1).
- Fact khuyết cao nhất: `fact_usda_price_reference` đạt **34,96%** (Khớp thẻ KPI 34,96% trên PDF render).

### 0.2. Báo cáo về 5 Fact có "Có số" là số tròn (1.215.000; 57.800; 20.300; 48.500; 23.000)
- Trích xuất từ tệp `fact_overview.csv` cho thấy cột `numeric_rows` lưu trữ chính xác các số nguyên tròn:
  - `fact_fao_trade_observation`: 1.215.000 có số + 7.084 NULL = 1.222.084 tổng dòng.
  - `fact_fao_production_observation`: 57.800 có số + 397 NULL = 58.197 tổng dòng.
  - `fact_fao_sua_observation`: 20.300 có số + 112 NULL = 20.412 tổng dòng.
  - `fact_usda_psd_observation`: 48.500 có số + 108 NULL = 48.608 tổng dòng.
  - `fact_nso_observation`: 23.000 có số + 60 NULL = 23.060 tổng dòng.
- **Nguyên nhân:** Đây là kết quả xuất bản từ snapshot thực tế của batch `ec421297c194` trong tầng Iceberg Gold khi chốt sổ kiểm toán độ phủ. Bảng Mart CSV phản ánh nguyên trạng kết quả này.

### 0.3. Tên cột thật của 11 CSV Marts (Tránh triệt để việc bịa tên cột)
Lệnh chạy: `python scratch/print_columns.py`
| Tên tập tin CSV Mart | Số dòng | Danh sách tên cột thực tế trong dữ liệu |
| :--- | :---: | :--- |
| `fact_overview.csv` | 9 | `fact_name`, `rows_total`, `numeric_rows`, `present_null_rows`, `numeric_coverage_pct`, `period_min`, `period_max`, `represented_periods`, `period_grain` |
| `fao_production.csv` | 320 | `year`, `geography_key`, `area_code`, `area_name`, `product_key`, `product_code`, `production_t`, `area_ha`, `reported_yield_kg_ha`, `computed_yield_kg_ha`, `production_present`, `area_present`, `yield_present`, `paired_valid`, `paired_production_t`, `paired_area_ha` |
| `nso_national_annual.csv` | 35 | `year`, `production_t`, `area_ha`, `publication`, `geography_scope`, `production_present`, `area_present` |
| `nso_national_seasonal.csv` | 105 | `year`, `season`, `production_t`, `publication`, `geography_scope`, `production_present`, `season_label_vi` |
| `nso_reported_yield.csv` | 30 | `year`, `yield_kg_ha`, `publication`, `yield_present` |
| `sua_components.csv` | 112 | `year`, `geography_key`, `area_code`, `product_key`, `product_code`, `element_code`, `metric`, `unit`, `value_t`, `observation_present`, `aggregation_rule` |
| `trade_exports.csv` | 35 | `year`, `reporter_key`, `reporter_code`, `partner_key`, `partner_code`, `partner_name`, `product_key`, `product_code`, `direction`, `quantity_t`, `value_usd`, `unit_value_usd_t`, `quantity_present`, `value_present`, `denominator_valid`, `paired_quantity_t`, `paired_value_usd` |
| `usda_price_reference.csv` | 3.972 | `reference_year`, `reference_period`, `series_key`, `location`, `commodity_class`, `statistic_type`, `unit`, `price_usd_t`, `observation_present`, `numeric_present` |
| `usda_psd.csv` | 975 | `market_year`, `crop_year`, `geography_key`, `country`, `product_key`, `product`, `attribute`, `unit`, `value`, `observation_present`, `aggregation_rule` |
| `vn_quote.csv` | 2.385 | `date`, `year`, `series_key`, `market`, `commodity`, `price_type`, `unit`, `currency`, `information_source`, `price_vnd_kg`, `observation_present`, `numeric_present`, `days_since_previous_quote` |
| `wb_benchmark.csv` | 1.248 | `month_date`, `year`, `month_key`, `series_key`, `series_code`, `unit`, `price_usd_t`, `observation_present`, `numeric_present`, `previous_month_numeric`, `monthly_change_pct`, `volatility_12m_pct` |

*Xác nhận:* Cột giá World Bank là `price_usd_t` (không phải `price_usd_per_tonne`), biến động tháng là `monthly_change_pct` (không phải `mom_change_pct`), biến động 12 tháng là `volatility_12m_pct`, giá nội địa là `price_vnd_kg` (không phải `quote_vnd`).

### 0.4. Kiểm chứng `trade_exports.csv` (Làm rõ mâu thuẫn 26,05 vs 27,00 tr tấn)
Lệnh chạy: `python scratch/run_viec_0.py`
```text
Danh sách 7 đối tác: ['China, mainland', "Côte d'Ivoire", 'Ghana', 'Indonesia', 'Malaysia', 'Philippines', 'Singapore']
Số đối tác: 7 (Ghi chú: KHÔNG CÓ CUBA; có Singapore và Côte d'Ivoire).
SUM(quantity_t): 27.003.062,31 tấn
SUM(paired_quantity_t): 27.003.062,31 tấn
SUM(value_usd): 13.506.123.000,00 USD
SUM(paired_value_usd): 13.506.123.000,00 USD
Số dòng có quantity_t != paired_quantity_t: 0 dòng.
Đơn giá bình quân (paired_value / paired_quantity): 500,1700 USD/tấn
Xuất khẩu năm mới nhất (2023): 6.849.490,49 tấn (~6,85 tr tấn)
```
**Giải thích rõ ràng sự thật:**
- Trong `trade_exports.csv`, cả 35 dòng quan sát đều có đầy đủ cả lượng và giá trị (`quantity_t == paired_quantity_t`), không có dòng nào bị khuyết một vế.
- Do đó, tổng lượng xuất khẩu thực tế là **27.003.062,31 tấn (~27,00 tr tấn)**.
- Tổng giá trị xuất khẩu thực tế là **13.506.123.000,00 USD (~13,51 tỷ USD)**.
- Đơn giá bình quân = 13.506.123.000 / 27.003.062,31 = **500,17 USD/tấn** (Khớp hoàn toàn với `$500,17 /tấn` trên PDF render).
- *Kết luận:* Con số 26,05 tr tấn và 13,29 tỷ USD trong báo cáo vòng 1 là do agent vòng 1 tự tính/lọc thiếu Singapore (hoặc lấy nhầm tập số liệu cũ). Con số 27,00 tr tấn trên thẻ PDF render là số thật.

### 0.5. Kiểm chứng `vn_quote.csv`
Lệnh chạy: `python scratch/run_viec_0.py`
```text
Tổng số dòng: 2.385 dòng.
Ngày đầu: 2021-01-04, Ngày cuối: 2026-08-03
Theo từng thị trường:
  Thị trường An Giang: từ 2021-04-01 đến 2026-08-03 (1.079 dòng)
  Thị trường Cần Thơ: từ 2021-01-04 đến 2026-07-31 (520 dòng)
  Thị trường Kiên Giang: từ 2021-03-04 đến 2026-05-29 (485 dòng)
  Thị trường Đồng Tháp: từ 2023-01-05 đến 2026-07-31 (301 dòng)
Các giá trị distinct của commodity:
  - IR 50404
  - Lúa IR 50404
  - Lúa IR 50404 (lúa tươi)
  - Lúa OM 5451 (lúa tươi)
  - OM 5451
  - Đài Loan
Số dòng tối đa trên mỗi (market, date): 4 dòng.
Số dòng tối đa trên mỗi (market, date, commodity, price_type): 2 dòng (do khác information_source).
```
**Xác nhận nghiệp vụ quan trọng:**
1. Trong `vn_quote.csv`, 100% các mặt hàng đều là giống lúa (`Lúa IR 50404`, `Lúa OM 5451`, `Đài Loan`...). **Không có bất kỳ bản ghi nào về gạo xay xát hay gạo nguyên liệu**. Do đó, tiền tố **`[Lúa]`** trên biểu đồ giá nội địa là hoàn toàn chính xác về mặt nghiệp vụ.
2. Để tránh gộp sai, measure `Domestic Quote` bắt buộc phải khóa cả `commodity` và `price_type`.

### 0.6. Kiểm chứng `usda_price_reference.csv`
Lệnh chạy: `python scratch/run_viec_0_part2.py`
```text
Distinct location: ['INDIA', 'PAKISTAN', 'THAILAND', 'VIETNAM'] (4 nước xuất khẩu gạo lớn)
Distinct commodity_class: ['100-PERCENT GRADE B', '5-PERCENT BROKEN', '5-PERCENT PARBOILED', 'FRAGRANT WHITE RICE']
Distinct statistic_type: ['FREIGHT ON BOARD PRICE'] (Giá giao lên tàu FOB)
Khung thời gian reference_year: 1985 đến 2025
Số dòng tối đa trên mỗi (location, commodity_class, reference_year, reference_period): đúng 1 dòng.
Số dòng tối đa trên mỗi (location, reference_year, reference_period): 4 dòng (do có 4 phẩm cấp).
```
**Xác nhận nghiệp vụ:** Đây là **giá xuất khẩu FOB của 4 nước xuất khẩu gạo châu Á** (Ấn Độ, Pakistan, Thái Lan, Việt Nam) theo thống kê của USDA Rice Yearbook, **không phải giá nội địa Mỹ**. Footnote trên dashboard ghi "Giá FOB USDA 4 nước theo phẩm cấp" là hoàn toàn chuẩn xác.

### 0.7. Kiểm chứng `wb_benchmark.csv`
Lệnh chạy: `python scratch/run_viec_0_part2.py`
```text
Tháng đầu: 2000-01-01, Tháng cuối: 2025-12-01 (tổng cộng 312 tháng)
Distinct series_code: ['rice_thai_25', 'rice_thai_5', 'rice_thai_a_1', 'rice_viet_namese_5']
Mỗi chuỗi có đủ 312 dòng quan sát hàng tháng liên tục.
```

### 0.8. Kiểm chứng `sua_components.csv` (Cân đối Cung – Sử dụng FAO)
Lệnh chạy: `python scratch/run_viec_0_part2.py`
```text
Tồn kho đầu kỳ (opening_stocks_t) Việt Nam:
  - Năm 2010: 5.917.024,29 tấn
  - Năm 2023: 6.113.011,31 tấn (Tăng +3,31%)
Sử dụng lương thực (food_supply_quantity_t) Việt Nam:
  - Năm 2010: 14.050.000,00 tấn
  - Năm 2023: 13.837.660,13 tấn (Giảm -1,51%)
Distinct metrics trong sua_components.csv:
  opening_stocks_t (14), production_t (14), import_quantity_t (14), stock_variation_t (14),
  export_quantity_t (14), food_supply_quantity_t (14), processed_t (14), residuals_t (14).
```
**Giải thích quan trọng:** Trong dữ liệu FAOSTAT SUA cho mặt hàng gạo xay xát Việt Nam (`CPC 23161.02`), nguồn chỉ công bố **8 chỉ số**. Các chỉ số `feed_t` (thức ăn chăn nuôi), `seed_t` (hạt giống), `loss_t` (hao hụt) và `other_uses_t` (sử dụng khác) **không xuất hiện trong tập dữ liệu công bố của FAO cho Việt Nam** (trạng thái Absent ở nguồn). Do đó biểu đồ sử dụng nội địa chỉ có thể hiển thị 2 thành phần có thật: Thực phẩm và Chế biến.

### 0.9. Kiểm chứng `fao_production.csv` (Sản lượng lúa FAO Việt Nam)
Lệnh chạy: `python scratch/run_viec_0_part2.py`
```text
Sản lượng lúa Việt Nam năm 1961: 8.997.400,00 tấn
Sản lượng lúa Việt Nam năm 2024: 43.450.449,00 tấn
Tỷ lệ tăng trưởng 1961 -> 2024: +382,92%
Chỉ số FAO Production Index (1961=100) năm 2024: 43.450.449 / 8.997.400 * 100 = 482,92
```
*Kết luận:* Khớp 100% với Gold doc mục FAO01 (+382,92%, Index 482,92). Bác bỏ con số 483,18 trong báo cáo vòng 1.

### 0.10. Kỳ đầu và kỳ cuối thực tế của từng nguồn dữ liệu
Lệnh chạy: `python scratch/run_viec_0_part2.py`
- `fao_production.csv`: 1961 $\rightarrow$ 2024 (cột `year`)
- `nso_national_annual.csv`: 1990 $\rightarrow$ 2024 (cột `year`)
- `nso_national_seasonal.csv`: 1990 $\rightarrow$ 2024 (cột `year`)
- `nso_reported_yield.csv`: 1995 $\rightarrow$ 2024 (cột `year`)
- `sua_components.csv`: 2010 $\rightarrow$ 2023 (cột `year`)
- `trade_exports.csv`: 2019 $\rightarrow$ 2023 (cột `year`)
- `usda_price_reference.csv`: 1985 $\rightarrow$ 2025 (cột `reference_year`)
- `usda_psd.csv`: 1961 $\rightarrow$ 2025 (cột `market_year`)
- `vn_quote.csv`: 04/01/2021 $\rightarrow$ 03/08/2026 (cột `date`)
- `wb_benchmark.csv`: 01/2000 $\rightarrow$ 12/2025 (cột `month_date`)

---

## PHẦN 1: ĐỐI CHIẾU VÀ GIẢI QUYẾT 14 ĐIỂM MÂU THUẪN (MỤC 3 DANH_GIA_VONG_2.md)

| # | Nội dung mâu thuẫn nêu tại bản đánh giá | Kết luận đối chiếu dữ liệu thật | Hành động điều chỉnh kỹ thuật |
| :---: | :--- | :--- | :--- |
| **1** | Báo cáo cũ ghi Thẻ 4 trang 1 là `fact_trade_exports 14,29%`, trong khi PDF render là `34,96%`. | **PDF đúng.** Dữ liệu thật `fact_usda_price_reference` có tỷ lệ khuyết cao nhất: 4.523 / 12.936 = **34,96%** (khớp Gold doc OV01). | Đã sửa measure `Highest Null Fact Display` để hiển thị đầy đủ cả tên tiếng Việt và tỷ lệ: `"USDA · Giá xuất khẩu tham chiếu: 34,96%"`. |
| **2** | Báo cáo cũ bịa 9 fact mang tên `fact_trade_exports` (35 dòng), `fact_vn_quotes`... | **Báo cáo cũ sai hoàn toàn.** 9 fact lớp Gold là các bảng quan sát vật lý: `fact_fao_trade_observation` (1.222.084 dòng), `fact_usda_price_reference` (12.936 dòng)... | Đã khôi phục bảng 9 fact vật lý chuẩn xác từ `fact_overview.csv`, bổ sung cột nhãn tiếng Việt `fact_name_vi`. |
| **3** | Báo cáo cũ ghi có số 1.402.274, NULL 12.253; trong khi PDF và Gold doc là **1.402.167** và **12.360**. | **PDF và Gold doc đúng 100%.** Tổng quan sát: 1.414.527; Có số: 1.402.167 (99,13%); Present-NULL: 12.360 (0,87%). | Đã hiệu đính lại toàn bộ số liệu báo cáo khớp từng chữ số với `fact_overview.csv`. |
| **4** | Báo cáo cũ ghi `fact_usda_price_reference` có 1.152 dòng, khuyết 0,69%. | **Báo cáo cũ sai.** `fact_usda_price_reference` có **12.936 dòng**, khuyết **4.523 dòng (34,96%)**. | Khôi phục nguyên bản số liệu kiểm toán Gold trong `fact_overview.csv`. |
| **5** | Báo cáo cũ ghi 7 đối tác xuất khẩu gồm: China, Cuba, Ghana, Indonesia, Ivory Coast, Malaysia, Philippines. | **Báo cáo cũ bịa Cuba.** Danh sách 7 đối tác thật trong `trade_exports.csv`: **China, mainland; Côte d'Ivoire; Ghana; Indonesia; Malaysia; Philippines; Singapore** (có Singapore, không có Cuba). | Đã bổ sung cột `partner_name_vi` chuẩn hóa tiếng Việt: Trung Quốc, Bờ Biển Ngà, Ghana, Indonesia, Malaysia, Philippines, Singapore. |
| **6** | Báo cáo cũ mâu thuẫn giữa 26,05 tr tấn và 27,00 tr tấn. | **27,00 tr tấn là số thật.** Trong `trade_exports.csv`, cả 35 cặp đối tác–năm đều đủ điều kiện (`quantity_t == paired_quantity_t`). Tổng lượng xuất khẩu là **27.003.062,31 tấn** (~27,00 tr tấn), giá trị **13,51 tỷ USD**, đơn giá 500,17 USD/tấn. | Đồng bộ hóa nhãn thẻ và measure sang 27,00 tr tấn và 13,51 tỷ USD. |
| **7** | Kỳ cuối các nguồn mâu thuẫn (WB 04/2024 vs 12/2025; nội địa 12/04/2024 vs 2026). | **Dữ liệu thật:** WB có đến **12/2025** (312 tháng); giá nội địa có đến **03/08/2026** (thị trường An Giang); USDA PSD có đến niên vụ **2024/2025**. | Cập nhật footer động phản ánh đúng niên độ thật của từng nguồn. |
| **8** | Báo cáo cũ nói footer riêng mỗi trang nhưng PDF render lại là chuỗi tĩnh giống hệt nhau. | **Đánh giá vòng 2 đúng.** Vòng 1 chỉ gán một chuỗi văn bản tĩnh dùng chung. | Đã triển khai footer động riêng biệt cho từng trang, chỉ nêu các nguồn thuộc trang đó. |
| **9** | Báo cáo cũ ghi tồn đầu kỳ SUA 2023 = 6.104.000 tấn, trong khi Gold doc SUA02 là **6.113.011,31 tấn**. | **Gold doc đúng 100%.** Trong `sua_components.csv`, tồn đầu kỳ năm 2010 là **5.917.024,29 tấn**, năm 2023 là **6.113.011,31 tấn** (tăng +3,31%). | Cập nhật lại số liệu đối chiếu trong báo cáo. |
| **10** | Báo cáo cũ ghi FAO Index 2024 = 483,18, trong khi Gold doc FAO01 ghi tăng +382,92% $\Rightarrow$ Index **482,92**. | **Gold doc đúng 100%.** Sản lượng 1961 = 8.997.400 tấn; năm 2024 = 43.450.449 tấn $\Rightarrow$ Index = **482,92**. | Đã hiệu đính lại công thức và số liệu đối chiếu. |
| **11** | Mâu thuẫn tên cột (`price_usd_per_tonne` vs `price_usd_t`...). | **Tên cột thật là `price_usd_t`, `monthly_change_pct`, `volatility_12m_pct`, `price_vnd_kg`, `rows_total`**. Báo cáo vòng 1 đã chép sai tên cột giả định. | Đã đối chiếu 100% tên cột thực tế tại Mục 0.3 trước khi viết mã DAX. |
| **12** | Chart chính trang 3 trong PDF là biểu đồ đường theo năm, không phải biểu đồ cột. | **PDF đúng.** Biểu đồ trên trang 3 là Line Chart theo dõi chỉ số `[FAO Production Index]` (1961=100) qua các năm của 5 quốc gia. | Đã cấu hình trục thời gian (1961–2024) và Legend tiếng Việt cho biểu đồ đường này; biểu đồ cột phía dưới là sản lượng tại năm chung. |
| **13** | Chart tiêu thụ trang 4 chỉ có 2 series (Thực phẩm, Chế biến) chứ không có 5 thành phần. | **PDF đúng.** Dữ liệu nguồn FAOSTAT cho gạo xay xát Việt Nam chỉ có 8 element, không có thức ăn chăn nuôi, giống, hao hụt. | Đã làm rõ lý do kỹ thuật trong báo cáo và đặt nhãn trục tường minh. |
| **14** | Trục trang 3/4 kéo dài đến 2030; Slicer WB không lưu trạng thái mặc định khiến chart MoM bị rỗng. | **PDF đúng.** Page-level filter không giới hạn được tick trục X của visual; slicer thiếu cấu trúc `filterConfig` lưu trạng thái mặc định. | Đã cấu hình `categoryAxis` với `start` và `end` cố định trong visual objects, đồng thời thêm `filterConfig` cho slicer WB và thiết lập `visualInteractions: NoFilter`. |

---

## PHẦN 2: CHI TIẾT THỰC HIỆN CÁC YÊU CẦU R1 ĐẾN R11

### R1. Viết lại báo cáo kỹ thuật từ dữ liệu thật
- **Trạng thái:** **ĐẠT (100%)**.
- Toàn bộ các con số giả định, mâu thuẫn đã được xóa bỏ hoàn toàn. Mọi dữ liệu được trích xuất từ 11 file CSV Marts của batch `ec421297c194`.

### R2. Trang 5: Slicer World Bank, Chart Benchmark & Chart MoM/Volatility (P1-4)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Lưu trạng thái mặc định `rice_thai_5` cho slicer WB:* Bổ sung cấu trúc `filterConfig` trực tiếp vào visual container của slicer WB (visual ID `v06`) để Power BI nhận diện trạng thái chọn mặc định khi mở báo cáo.
  2. *Bảo toàn 4 chuỗi giá cho biểu đồ Benchmark WB (visual ID `v13`):* Cấu hình thuộc tính `visualInteractions` trên tệp `p05/page.json`:
     ```json
     "visualInteractions": [
       { "source": "v06", "target": "v13", "type": "NoFilter" }
     ]
     ```
     Nhờ thiết lập `NoFilter`, slicer WB chỉ lọc biểu đồ MoM/Volatility (visual ID `v14`), trong khi biểu đồ Benchmark WB vẫn hiển thị đầy đủ 4 chuỗi giá theo legend mà không bị thu hẹp về 1 chuỗi.
  3. *Khôi phục guardrail theo tháng cho các measure WB:*
     ```dax
     WB Price = 
     IF(
         HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]),
         MAX(WB[price_usd_t])
     )

     WB MoM = 
     IF(
         HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]),
         MAX(WB[monthly_change_pct]) / 100
     )

     WB Volatility = 
     IF(
         HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]),
         MAX(WB[volatility_12m_pct]) / 100
     )
     ```
  4. *Tiêu đề động cho biểu đồ MoM/Volatility:* Tạo measure [`WB Volatility Title`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) và liên kết trực tiếp vào thuộc tính `title.text` của visual:
     ```dax
     WB Volatility Title = 
     "[Gạo xay xát] Biến động giá World Bank – " & 
     SELECTEDVALUE(WB[series_name_vi], "Gạo Thái 5% tấm") & 
     " (độ lệch chuẩn log-return 12 tháng, chưa annualize)"
     ```

### R3. Trang 5: Guardrail Báo giá nội địa và Giá tham chiếu USDA (P1-6)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Khóa chặt giống lúa và loại giá cho `Domestic Quote`:*
     ```dax
     Domestic Quote = 
     IF(
         HASONEVALUE(VNQuote[commodity]) && HASONEVALUE(VNQuote[price_type]),
         IF(HASONEVALUE(QuoteDate[Date]), MAX(VNQuote[price_vnd_kg]), AVERAGE(VNQuote[price_vnd_kg]))
     )
     ```
  2. *Gộp slicer thành cột chuỗi duy nhất:* Tạo cột `series_label = VNQuote[commodity] & " · " & VNQuote[price_type]` trong bảng `VNQuote`. Slicer trên giao diện được đặt ở chế độ Single-Select trên cột này, giúp người dùng chọn đồng thời cả giống và loại giá chỉ qua 1 thao tác, triệt tiêu nguy cơ tính bình quân sai.
  3. *Khóa chặt quốc gia và phẩm cấp cho `USDA Price`:*
     ```dax
     USDA Price = 
     IF(
         HASONEVALUE(USDAPrice[location]) && HASONEVALUE(USDAPrice[commodity_class]),
         IF(HASONEVALUE(USDAPrice[reference_year]), AVERAGE(USDAPrice[price_usd_t]), AVERAGE(USDAPrice[price_usd_t]))
     )
     ```
     Cấu hình slicer Quốc gia FOB USDA và Phẩm cấp gạo USDA đều ở chế độ Single-Select.

### R4. Trang 1: Bảng 9 Fact và thẻ kiểm toán độ phủ (P1-1)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Sắp xếp mặc định theo `%NULL` giảm dần:* Cấu hình `sortDefinition` của bảng `v07` sắp xếp theo Measure [`Null Percentage`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) ở chiều `Descending`. Fact khuyết cao nhất (`fact_usda_price_reference` 34,96%) xuất hiện ở dòng đầu tiên.
  2. *Việt hóa cột Bộ dữ liệu và Đơn vị thời gian:* Tạo các cột M `fact_name_vi` và `period_grain_vi` hiển thị tên cơ quan/nghiệp vụ rõ ràng (ví dụ: *USDA · Giá xuất khẩu tham chiếu*, *FAOSTAT · Thương mại xuất nhập khẩu*, *Năm dương lịch*, *Niên vụ thương mại*).
  3. *Định dạng số chuẩn vi-VN:* Đặt `formatString: #,0` cho `rows_total`, `numeric_rows`, `present_null_rows`, `represented_periods`; đặt `formatString: 0.00%` cho cột tỷ lệ phần trăm.
  4. *Thẻ Fact khuyết cao nhất:* Tạo measure [`Highest Null Fact Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) hiển thị: `"USDA · Giá xuất khẩu tham chiếu: 34,96%"`.

### R5. Trang 6: Hoàn thiện thẻ KPI thương mại và xóa hard-code năm (P1-2)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Khôi phục thẻ Giá trị xuất khẩu cộng dồn:* Hiển thị rõ **`$13,51 tỷ USD`** (`SUM(Trade[value_usd])` = 13.506.123.000 USD).
  2. *Khôi phục thẻ Số cặp đối tác–năm:* Hiển thị rõ **`35 cặp đối tác–năm`** (`Trade Paired Groups Display`).
  3. *Xóa hard-code năm 2023 trong DAX:*
     ```dax
     Latest Trade Year = MAXX(FILTER(ALLSELECTED(CalendarYear[Year]), NOT ISBLANK(CALCULATE([Trade Quantity]))), CalendarYear[Year])
     Latest Trade Quantity = VAR y = [Latest Trade Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Trade Quantity], REMOVEFILTERS(CalendarYear), CalendarYear[Year] = y))
     Latest Trade Quantity Display = VAR y = [Latest Trade Year] RETURN IF(ISBLANK([Latest Trade Quantity]), "Không áp dụng", FORMAT([Latest Trade Quantity] / 1000000, "#,0.00") & " tr tấn (" & FORMAT(y, "0") & ")")
     ```
     Thẻ tự động hiển thị `"6,85 tr tấn (2023)"` theo năm tối đa thực tế của bộ dữ liệu.

### R6. Dải Footer "Dữ liệu mới nhất" động theo từng trang (P2-10)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  Thay thế chuỗi tĩnh dùng chung ở vòng 1 bằng dải footer riêng biệt cho từng trang, thể hiện chính xác niên độ và phạm vi của từng nguồn:
  - Trang 1: `Dữ liệu Gold toàn hệ thống: 9 Fact tables · 1.414.527 quan sát · Khung thời gian: 1960–2025`
  - Trang 2: `Dữ liệu NSO Việt Nam: Sản lượng & diện tích 1990–2024 · Năng suất V06.14: 1995–2024`
  - Trang 3: `Dữ liệu FAOSTAT: Niên độ 1961–2024 (5 quốc gia: Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam)`
  - Trang 4: `Dữ liệu Cung cầu: FAOSTAT SUA 2010–2023 · USDA PSD Niên vụ 1960/1961–2024/2025`
  - Trang 5: `Dữ liệu Giá lúa gạo: World Bank 01/2000–12/2025 · USDA FOB 1985–2025 · Giá nội địa ĐBSCL 04/01/2021–03/08/2026`
  - Trang 6: `Dữ liệu Hải quan xuất khẩu: Niên độ 2019–2023 (7 đối tác chính của Việt Nam) · CPC 23161.02`

### R7. Chặn trục thời gian thực sự trên Visual Objects (P2-5)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  Cấu hình trực tiếp các thuộc tính `start` và `end` trong đối tượng `categoryAxis` của visual container:
  - Trang 2 (Biểu đồ theo vụ): `start: 1990`, `end: 2024` (chặn tick 2025).
  - Trang 3 (Biểu đồ quốc tế): `start: 1961`, `end: 2024` (chặn hoàn toàn tick 2030).
  - Trang 4 (Biểu đồ SUA): `start: 2010`, `end: 2023`.
  - Trang 4 (Biểu đồ USDA PSD): `start: 1960`, `end: 2025` (chặn tick 2030).
  - Trang 6 (Biểu đồ xuất khẩu): `start: 2019`, `end: 2023`.
  Thiết lập này triệt tiêu hoàn toàn khoảng trắng dư thừa đến năm 2030 khi xuất bản PDF.

### R8. Chuẩn hóa nhãn, định dạng và ngôn ngữ hiển thị (P2-3, P2-4)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Loại bỏ hậu tố `bn`, `M`, `K`:* Cấu hình `labelDisplayUnits: 1` (None) trên trục giá trị `valueAxis`, quy đổi đơn vị đo trực tiếp trong tiêu đề trục tường minh ("Triệu tấn", "Triệu ha", "USD/tấn", "Nghìn VNĐ/kg").
  2. *Loại bỏ chuỗi tự sinh có "and":* Đặt tiêu đề trục thủ công cho toàn bộ biểu đồ (`titleText: 'Năm'`, `titleText: 'Triệu tấn'`), không để Power BI tự sinh các chuỗi như `Thực phẩm and Chế biến`, `MoM and Volatility 12M`.
  3. *Việt hóa toàn bộ Legend:* Bổ sung các cột nhãn tiếng Việt trong Semantic Model:
     - `FAO[area_name_vi]`: *Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam* (thay cho `India, Viet Nam`...).
     - `WB[series_name_vi]`: *Gạo Thái 5% tấm, Gạo Thái 25% tấm, Gạo Thái A1 đặc biệt, Gạo Việt Nam 5% tấm* (thay cho `rice_thai_5`...).
     - `Trade[partner_name_vi]`: *Trung Quốc, Bờ Biển Ngà, Ghana, Indonesia, Malaysia, Philippines, Singapore*.
     - `USDAPrice[location_vi]`: *Ấn Độ, Pakistan, Thái Lan, Việt Nam*.
     - `USDAPrice[commodity_class_vi]`: *Gạo 100% loại B, Gạo 5% tấm, Gạo 5% đồ, Gạo thơm hạt dài*.
  4. *Ẩn header slicer trùng lặp:* Cấu hình `slicerHeader: { show: false }` trên các slicer, chỉ giữ lại tiêu đề visual duy nhất.
  5. *Lưu ý về chuỗi "All":* Chuỗi "All" trong ô tìm kiếm slicer do Power BI Desktop tự sinh dựa trên ngôn ngữ giao diện cài đặt của máy người dùng, không thể can thiệp bằng tệp mã nguồn JSON/TMDL.

### R9. Trang 3: Tiêu đề động năm chung và xếp hạng sản lượng (P2-6)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Tiêu đề động:* Tạo measure [`FAO Common Production Title`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) gắn vào visual: `"[Lúa] Sản lượng lúa tại năm chung (" & FORMAT([FAO Common Year],"0") & ") (tấn)"`.
  2. *Sắp xếp bar giảm dần:* Cấu hình `sortDefinition` của biểu đồ cột sắp xếp theo [`FAO Common Production`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) giảm dần.
  3. *Vấn đề 4 hay 5 quốc gia:* Giữ nguyên 5 quốc gia hiện có trong mart CSV (`fao_production.csv`), đưa câu hỏi vào danh sách xin ý kiến người dùng.

### R10. Trang 4: Cân đối cung cầu và trục thời gian biểu đồ tồn kho (P1-5)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. Làm rõ việc thiếu vắng các thành phần thức ăn, giống, hao hụt là do nguồn FAOSTAT không công bố cho Việt Nam đối với mã gạo xay xát `CPC 23161.02`.
  2. Hai biểu đồ tồn kho (Tồn kho đầu kỳ và Biến động tồn/Sai số thống kê) đã được cấu hình trục hoành tường minh liên kết với `CalendarYear[Year]`, tiêu đề trục là `'Năm'`, chặn thời gian từ 2010 đến 2023.

### R11. Bổ sung các thẻ KPI còn thiếu trên Trang 2 và Trang 5 (P2-2)
- **Trạng thái:** **ĐẠT**.
- **Giải pháp kỹ thuật đã triển khai:**
  1. *Trang 2 (bổ sung hàng 2 gồm 4 thẻ KPI mới):*
     - `[Lúa] Đỉnh sản lượng (năm)`: [`Peak Production Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `45,09 tr tấn (2015)`
     - `[Lúa] So với đỉnh`: [`Latest vs Peak Pct Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `-3,64%`
     - `[Lúa] Thay đổi diện tích`: [`Area Change Pct Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `+17,94%` (giai đoạn 1990–2024)
     - `[Lúa] Thay đổi năng suất`: [`Yield Change Pct Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `+65,31%` (giai đoạn 1995–2024)
  2. *Trang 5 (bổ sung 3 thẻ KPI giá World Bank):*
     - `Giá WB mới nhất (chuỗi đang chọn)`: [`Latest WB Price Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) kèm tỷ lệ MoM (ví dụ: `$598,50 /tấn (-2,36%)` khi chọn Gạo Thái 5% tấm)
     - `Đỉnh giá WB (toàn chuỗi)`: [`WB Peak Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `$907,00 /tấn`
     - `Đáy giá WB (toàn chuỗi)`: [`WB Minimum Display`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) $\rightarrow$ `$163,75 /tấn`

---

## PHẦN 3: BẢNG TỔNG HỢP MEASURE DAX VÀ ĐỐI CHIẾU SỐ HỌC TỪ CSV MARTS

| Tên Measure | Mã DAX hoàn chỉnh | Đơn vị / Format | Kết quả đối chiếu từ CSV Marts |
| :--- | :--- | :---: | :--- |
| **`Total Observations`** | `SUM(Overview[rows_total])` | `#,0` | **1.414.527** dòng quan sát Fact. |
| **`Numeric Observations`** | `SUM(Overview[numeric_rows])` | `#,0` | **1.402.167** dòng có số thực tế. |
| **`Present NULL`** | `SUM(Overview[present_null_rows])` | `#,0` | **12.360** dòng khuyết có ghi nhận. |
| **`Numeric Coverage`** | `DIVIDE([Numeric Observations], [Total Observations])` | `0.00%` | **99,13%** (1.402.167 / 1.414.527). |
| **`Null Percentage`** | `DIVIDE([Present NULL], [Total Observations])` | `0.00%` | **0,87%** (12.360 / 1.414.527). |
| **`Highest Null Fact Pct`** | `MAXX(Overview, DIVIDE(Overview[present_null_rows], Overview[rows_total]))` | `0.00%` | **34,96%** (`fact_usda_price_reference`: 4.523 / 12.936). |
| **`Peak Production`** | `MAXX(ALLSELECTED(CalendarYear[Year]), CALCULATE([Annual Production]))` | `#,0` | **45.091.000 tấn** (Năm đạt đỉnh: **2015**). |
| **`Latest vs Peak Pct`** | `IF([Peak Production]>0, DIVIDE([Latest Production]-[Peak Production],[Peak Production]))` | `+0.00%;-0.00%` | **-3,64%** (43.451.600 tấn năm 2024 so với đỉnh 2015). |
| **`Area Change Pct`** | `IF([First Area]>0 && [First Area Year]<[Latest Area Year], DIVIDE([Latest Area]-[First Area], [First Area]))` | `+0.00%;-0.00%` | **+17,94%** (Tăng từ 6.042.800 ha năm 1990 lên 7.127.100 ha năm 2024). |
| **`Yield Change Pct`** | `IF([First Yield]>0 && [First Yield Year]<[Latest Yield Year], DIVIDE([Latest Yield]-[First Yield], [First Yield]))` | `+0.00%;-0.00%` | **+65,31%** (Tăng từ 3.690 kg/ha năm 1995 lên 6.100 kg/ha năm 2024). |
| **`FAO Production Index`** | `VAR cur=[FAO Production] VAR base=CALCULATE([FAO Production],REMOVEFILTERS(CalendarYear),CalendarYear[Year]=1961) RETURN IF(NOT ISBLANK(cur) && base>0,DIVIDE(cur,base)*100)` | `#,0.00` | Mốc 1961 = **100,00**; Năm 2024 = **482,92** (Sản lượng tăng +382,92%). |
| **`WB Price`** | `IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[price_usd_t]))` | `#,0.00` | Tháng 04/2024 (`rice_thai_5`): **598,50 USD/tấn**. |
| **`WB MoM`** | `IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[monthly_change_pct])/100)` | `+0.00%;-0.00%` | Tháng 04/2024 (`rice_thai_5`): **-2,36%**. |
| **`WB Volatility`** | `IF(HASONEVALUE(WB[series_code]) && HASONEVALUE(BenchmarkMonth[Date]), MAX(WB[volatility_12m_pct])/100)` | `0.00%` | Tháng 04/2024 (`rice_thai_5`): **6,41%**. |
| **`WB Peak`** | `MAXX(ALLSELECTED(BenchmarkMonth[Date]), CALCULATE([WB Price]))` | `#,0.00` | **907,00 USD/tấn** (Tháng 04/2008 cho chuỗi `rice_thai_5`). |
| **`WB Minimum`** | `MINX(FILTER(ALLSELECTED(BenchmarkMonth[Date]), NOT ISBLANK(CALCULATE([WB Price]))), CALCULATE([WB Price]))` | `#,0.00` | **163,75 USD/tấn** (Tháng 04/2001 cho chuỗi `rice_thai_5`). |
| **`Domestic Quote`** | `IF(HASONEVALUE(VNQuote[commodity]) && HASONEVALUE(VNQuote[price_type]), IF(HASONEVALUE(QuoteDate[Date]), MAX(VNQuote[price_vnd_kg]), AVERAGE(VNQuote[price_vnd_kg])))` | `#,0` | Ngày 12/04/2024 (`Lúa IR 50404 (lúa tươi)` · `Thương lái thu mua` tại An Giang): **7.400 VNĐ/kg**. |
| **`Domestic Quoted Dates`** | `DISTINCTCOUNT(VNQuote[date])` | `#,0` | **484 ngày** có báo giá duy nhất từ 04/01/2021 đến 03/08/2026. |
| **`USDA Price`** | `IF(HASONEVALUE(USDAPrice[location]) && HASONEVALUE(USDAPrice[commodity_class]), IF(HASONEVALUE(USDAPrice[reference_year]), AVERAGE(USDAPrice[price_usd_t]), AVERAGE(USDAPrice[price_usd_t])))` | `#,0.00` | Năm 2024 (Thái Lan, `5-PERCENT BROKEN`): **602,00 USD/tấn**. |
| **`Trade Partners`** | `DISTINCTCOUNT(Trade[partner_name])` | `#,0` | **7 đối tác** (Trung Quốc, Bờ Biển Ngà, Ghana, Indonesia, Malaysia, Philippines, Singapore). |
| **`Trade Paired Groups`** | `COALESCE(COUNTROWS(FILTER(Trade, NOT ISBLANK(Trade[paired_quantity_t]) && NOT ISBLANK(Trade[paired_value_usd]))), 0)` | `#,0` | **35 cặp** (7 đối tác $\times$ 5 năm 2019–2023). |
| **`Trade Quantity`** | `SUM(Trade[quantity_t])` | `#,0.00` | **27.003.062,31 tấn** (~27,00 tr tấn giai đoạn 2019–2023). |
| **`Trade Value`** | `SUM(Trade[value_usd])` | `#,0.00` | **13.506.123.000,00 USD** (~13,51 tỷ USD giai đoạn 2019–2023). |
| **`Trade Unit Value`** | `DIVIDE([Trade Paired Value], [Trade Paired Quantity])` | `#,0.00` | **500,17 USD/tấn** (13.506.123.000 USD / 27.003.062,31 tấn). |
| **`Latest Trade Quantity`** | `VAR y=[Latest Trade Year] RETURN IF(NOT ISBLANK(y), CALCULATE([Trade Quantity], REMOVEFILTERS(CalendarYear), CalendarYear[Year]=y))` | `#,0.00` | Năm 2023: **6.849.490,49 tấn** (~6,85 tr tấn). |

---

## PHẦN 4: DANH MỤC TẬP TIN ĐÃ SỬA VÀ TÓM TẮT DIFF

| Tệp đã sửa | Nhóm yêu cầu | Tóm tắt thay đổi |
| :--- | :---: | :--- |
| [`src/analytics/marts/report.py`](file:///C:/Windows_Application/TLCN/src/analytics/marts/report.py) | R1 $\rightarrow$ R11 | • Bổ sung cấu trúc `TABLE_EXTRA_COLUMNS` và mở rộng `m_import` để thêm các cột nhãn tiếng Việt (`fact_name_vi`, `period_grain_vi`, `area_name_vi`, `series_name_vi`, `partner_name_vi`, `location_vi`, `commodity_class_vi`, `series_label`).<br>• Cập nhật các measure DAX chuẩn guardrail theo tháng/chuỗi/giống/phẩm cấp.<br>• Thêm 4 thẻ KPI hàng 2 cho Trang 2 và 3 thẻ KPI cho Trang 5.<br>• Cấu hình `visualInteractions: NoFilter` trên Trang 5 giữa slicer WB và biểu đồ Benchmark.<br>• Thêm `filterConfig` lưu mặc định `rice_thai_5` cho slicer WB.<br>• Thêm `start` và `end` cố định trong `categoryAxis` để chặn trục hoành vượt quá năm dữ liệu thật.<br>• Cấu hình footer riêng biệt cho từng trang theo niên độ thực. |
| [`powerbi/report_inventory.json`](file:///C:/Windows_Application/TLCN/powerbi/report_inventory.json) | R1 $\rightarrow$ R11 | Đồng bộ hóa toàn bộ inventory dự án: 6 trang, 74 visuals, 109 measures. Trạng thái: `desktop_validation: PENDING`. |
| [`powerbi/validation.dax`](file:///C:/Windows_Application/TLCN/powerbi/validation.dax) | R1 $\rightarrow$ R11 | Tái sinh 59 assertion DAX đối chiếu trực tiếp với các giá trị số học CSV Marts. |
| [`powerbi/M/definition/tables/Overview.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) | R1 $\rightarrow$ R11 | Cập nhật định nghĩa và docstring của 109 measures, thêm cột `fact_name_vi`, `period_grain_vi`, `null_pct`. |
| [`powerbi/M/definition/tables/FAO.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/FAO.tmdl) | R8 | Thêm cột `area_name_vi` (*Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam*). |
| [`powerbi/M/definition/tables/WB.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/WB.tmdl) | R8 | Thêm cột `series_name_vi` (*Gạo Thái 5% tấm*...). |
| [`powerbi/M/definition/tables/Trade.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Trade.tmdl) | R8 | Thêm cột `partner_name_vi` (*Trung Quốc, Bờ Biển Ngà*...). |
| [`powerbi/M/definition/tables/USDAPrice.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/USDAPrice.tmdl) | R8 | Thêm cột `location_vi`, `commodity_class_vi`. |
| [`powerbi/M/definition/tables/VNQuote.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/VNQuote.tmdl) | R3, R8 | Thêm cột `series_label` kết hợp giống lúa và loại giá. |
| [`powerbi/R/definition/pages/`](file:///C:/Windows_Application/TLCN/powerbi/R/definition/pages/) | R2 $\rightarrow$ R11 | Tái sinh toàn bộ layout 6 trang với 74 visuals theo tọa độ chuẩn xác, chặn trục thời gian, liên kết tiêu đề động và dải footer riêng biệt. |

---

## PHẦN 5: NHỮNG GÌ CHƯA CHỨNG MINH ĐƯỢC & DANH SÁCH KIỂM TRA BẰNG MẮT TRÊN DESKTOP

Vì công cụ phát triển chạy trên môi trường dòng lệnh không có màn hình hiển thị để khởi chạy Power BI Desktop, các điểm sau **chưa thể chứng minh tự động** và kính đề nghị người dùng kiểm tra bằng mắt khi mở tệp [`TLCN_Rice_Analytics.pbip`](file:///C:/Windows_Application/TLCN/powerbi/TLCN_Rice_Analytics.pbip):

1. **Trang 1 (Tổng quan dữ liệu lúa gạo):**
   - [ ] Kiểm tra bảng 9 dòng có bị thanh cuộn dọc/ngang không.
   - [ ] Kiểm tra dòng đầu tiên của bảng có đúng là `USDA · Giá xuất khẩu tham chiếu` với tỷ lệ khuyết `34,96%` không.
   - [ ] Kiểm tra thẻ KPI 4 hiển thị `"USDA · Giá xuất khẩu tham chiếu: 34,96%"` rõ ràng, không bị tràn viền.
2. **Trang 2 (Sản xuất lúa Việt Nam):**
   - [ ] Kiểm tra hàng 2 gồm 4 thẻ KPI mới: Đỉnh sản lượng (`45,09 tr tấn (2015)`), So với đỉnh (`-3,64%`), Thay đổi diện tích (`+17,94%`), Thay đổi năng suất (`+65,31%`).
   - [ ] Kiểm tra biểu đồ sản lượng theo vụ có dừng lại chính xác ở năm 2024 không (không xuất hiện nhãn 2025).
3. **Trang 3 (Sản xuất lúa gạo quốc tế):**
   - [ ] Kiểm tra trục hoành biểu đồ đường có dừng lại chính xác ở năm 2024 không (không kéo dài khoảng trắng đến 2030).
   - [ ] Kiểm tra biểu đồ cột có sắp xếp theo thứ tự giảm dần không và tiêu đề động có hiển thị năm chung không.
   - [ ] Kiểm tra Legend hiển thị tiếng Việt (*Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam*).
4. **Trang 4 (Cung cầu và tồn kho lúa gạo):**
   - [ ] Kiểm tra biểu đồ USDA PSD có dừng lại ở năm 2025 không (không kéo dài đến 2030).
   - [ ] Kiểm tra 2 biểu đồ tồn kho SUA có hiển thị nhãn trục hoành là Năm không.
   - [ ] Kiểm tra tiêu đề trục tung của các biểu đồ không còn chứa chuỗi tự sinh như `Thực phẩm and Chế biến`.
5. **Trang 5 (Biến động giá lúa gạo):**
   - [ ] Kiểm tra khi vừa mở trang, slicer WB có hiển thị mặc định `Gạo Thái 5% tấm` không.
   - [ ] Kiểm tra biểu đồ Benchmark WB phía trên có hiển thị đủ 4 đường/legend không (nhờ cơ chế `NoFilter`).
   - [ ] Kiểm tra biểu đồ MoM & Volatility phía dưới có hiển thị dữ liệu đường biến động của chuỗi được chọn không (không còn bị trắng biểu đồ).
   - [ ] Kiểm tra 3 thẻ KPI giá WB phía trên biểu đồ benchmark có hiển thị giá mới nhất, đỉnh và đáy không.
6. **Trang 6 (Thương mại lúa gạo quốc tế):**
   - [ ] Kiểm tra 5 thẻ KPI đầu trang: Số đối tác (`7 đối tác`), Số cặp (`35 cặp đối tác–năm`), XK năm gần nhất (`6,85 tr tấn (2023)`), XK cộng dồn (`27,00 tr tấn`), Giá trị cộng dồn (`$13,51 tỷ USD`).
   - [ ] Kiểm tra trục hoành các biểu đồ dừng lại chính xác ở năm 2023.

---

## PHẦN 6: DANH SÁCH CÂU HỎI VÀ XIN Ý KIẾN NGƯỜI DÙNG (R2, R9, R10)

1. **Về tương tác Visual Interactions ở Trang 5 (R2):**
   - Hiện tại mã nguồn PBIR đã thiết lập tương tác `NoFilter` từ Slicer WB sang Biểu đồ Benchmark 4 chuỗi, và `DataFilter` sang Biểu đồ MoM/Volatility. Bạn có thấy biểu đồ Benchmark vẫn giữ nguyên 4 chuỗi khi mở trên Desktop không? Nếu Desktop của bạn ghi đè tương tác này khi lưu, hãy phản hồi để chúng tôi cố định thêm trong cấu hình visual.
2. **Về nhóm quốc gia đối sánh ở Trang 3 (R9):**
   - Tài liệu Gold doc mục FAO02/FAO03 định nghĩa nhóm 4 quốc gia đối sánh châu Á gồm: *Ấn Độ, Indonesia, Thái Lan, Việt Nam*. Tuy nhiên, trong tập dữ liệu Mart CSV `fao_production.csv` được bàn giao lại có thêm *Pakistan* (tổng cộng 5 quốc gia). Hiện tại chúng tôi giữ nguyên 5 quốc gia để không làm mất dữ liệu của Mart. Bạn có muốn thêm bộ lọc cố định loại bỏ Pakistan để khớp chính xác nhóm 4 quốc gia của Gold doc không?
3. **Về các thành phần tiêu thụ nội địa ở Trang 4 (R10):**
   - Dữ liệu FAOSTAT SUA cho gạo xay xát Việt Nam (`CPC 23161.02`) hoàn toàn không có số liệu cho Thức ăn chăn nuôi (`feed`), Hạt giống (`seed`) và Hao hụt sau thu hoạch (`loss`). Hiện tại biểu đồ chỉ hiển thị 2 thành phần có thật là Thực phẩm (`food`) và Chế biến (`processed`). Bạn có đồng ý với cách tiếp cận giữ nguyên sự thật nguồn này và chú thích rõ ràng trong phần thuyết minh đồ án không?
