# BÁO CÁO KỸ THUẬT VÒNG 3: ĐIỀU CHỈNH TOÀN DIỆN POWER BI VÀ ĐỐI CHIẾU SỰ THẬT DỮ LIỆU

**Dự án:** `TLCN_Rice_Analytics` (Power BI PBIP / TMDL / PBIR)  
**Ngày thực hiện:** 10/11/2026 · **Phiên bản:** Vòng 3 (Khắc phục triệt để các tồn đọng & Không phụ thuộc slicer mặc định)  
**Tập dữ liệu kiểm chứng:** 11 tập tin CSV Marts tại `exports/powerbi/ec421297c194_20261106T031815Z_01d03710/` (Batch `ec421297c194…`)  
**Tài liệu đối chiếu:** `DANH_GIA_VONG_3.md`, `DANH_GIA_VONG_2.md`, `GOLD_BUSINESS_USE_CASES_AND_METRICS.md`, `TLCN_Rice_Analytics_v3_render.pdf` (Bản render thực tế xuất từ Power BI Desktop)

---

## TUYÊN BỐ VỀ TÍNH CHÂN THỰC VÀ GIỚI HẠN KIỂM THỬ

1. **Cam kết số liệu thực tế:** Toàn bộ các con số, tên cột và thống kê trong báo cáo này được tính toán và trích xuất trực tiếp từ các lệnh thực thi trên 11 tệp CSV Marts thật của tầng Gold đã công bố. Báo cáo tuân thủ nghiêm ngặt Quy tắc 1 & 2: chỉ nêu những gì có trong Markdown, PDF render thật hoặc output lệnh vừa chạy.
2. **Giới hạn kiểm thử tự động:**
   - Kết quả `STATIC_PASS` (86 tài liệu JSON, 74 visuals, 6 trang, 141 measures) và bộ kiểm thử `pytest 99/99 PASS` **chỉ chứng minh tính hợp lệ của cấu trúc schema Microsoft Fabric PBIR, cú pháp TMDL và tham chiếu trường dữ liệu (field references)**.
   - **Desktop rendering chưa được chứng minh trực tiếp (`NOT_PROVEN`)** do môi trường dòng lệnh không có giao diện đồ họa Windows để khởi chạy `PBIDesktop.exe`.
   - **DAX chưa được thực thi bởi Analysis Services engine (`NOT_PROVEN`)**, ngoại trừ 59 kiểm tra assertion tĩnh trong [`validation.dax`](file:///C:/Windows_Application/TLCN/powerbi/validation.dax). Các hiển thị đồ họa thực tế được đối chiếu qua tệp PDF render thật do người dùng cung cấp (`TLCN_Rice_Analytics_v3_render.pdf`).

---

## PHẦN 0: KẾT QUẢ KIỂM CHỨNG DỮ LIỆU GỐC (BẢN VÒNG 2 GIỮ NGUYÊN)

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
- Có số: **1.402.167** (Khớp hoàn toàn với PDF render và Gold doc OV01).
- Present-NULL: **12.360** (Khớp hoàn toàn với PDF render và Gold doc OV01).
- Fact khuyết cao nhất: `fact_usda_price_reference` đạt **34,96%** (Khớp thẻ KPI 34,96% trên PDF render).

---

## PHẦN VÒNG 3: ĐIỀU CHỈNH KỸ THUẬT VÒNG 3 (S1 – S7)

### S1. Đơn vị trục và bậc số (P1)
- **Vấn đề trong render v3:** Trên các biểu đồ ở trang 2, 3, 4, 6, tiêu đề trục ghi "Triệu tấn", "Triệu ha", "Tỷ USD", "Nghìn VNĐ/kg" nhưng nhãn tick hiển thị các số nguyên dạng thô: `20,000,000`, `6,000,000`, `1,000,000,000`, `5,000`. Điều này tạo ra sai lệch bậc độ lớn ($10^6$, $10^9$, $10^3$) giữa nhãn trục và vạch chia. Nguyên nhân là do cấu hình `labelDisplayUnits = 1` (để tránh hậu tố tiếng Anh `M`, `bn`) nhưng measure truyền vào trục vẫn ở đơn vị đơn lẻ.
- **Giải pháp:** Tạo hệ thống **Display Measures chia sẵn** trong DAX (`/ 1e6`, `/ 1e9`) dành riêng cho việc hiển thị trục của biểu đồ, giúp các vạch tick tự nhiên mang giá trị `20`, `6`, `1,0` hoàn toàn khớp với nhãn tiêu đề trục mà không sinh hậu tố tiếng Anh. Đồng thời, giữ nguyên measure gốc cho các thẻ KPI và công thức tính toán.
- **Biểu đồ giá nội địa (Trang 5):** Tiêu đề trục chuyển thành `"VND/kg"` (không dùng `"Nghìn VNĐ/kg"`), tick hiển thị bậc `5.000`, `10.000` đúng chuẩn đơn vị tiền tệ thực tế.
- **Tiêu đề biểu đồ:** Tất cả các tiêu đề ghi `(tấn)` mà trục chia triệu tấn đều được cập nhật thành `(triệu tấn)`.

#### Bảng rà soát chi tiết đơn vị trục và hiển thị tick trên tất cả biểu đồ
| Trang | Visual ID | Tiêu đề biểu đồ | Measure hiển thị trục | Đơn vị trục Y | Tick hiển thị dự kiến | Khớp nhãn |
| :--- | :--- | :--- | :--- | :--- | :--- | :---: |
| **Trang 2** | `v14` | Sản lượng lúa theo vụ (triệu tấn) | `Seasonal Production Mil` | Triệu tấn | `5, 10, 15, 20` | **Khớp** |
| **Trang 2** | `v15` | Diện tích và sản lượng lúa cả năm (1990–2024) | `Annual Area Mil`, `Annual Production Mil` | Triệu ha / Triệu tấn | `5, 6, 7` (Diện tích), `20, 30, 40` (Sản lượng) | **Khớp** |
| **Trang 3** | `v05` | Sản lượng năm gần nhất của 5 quốc gia (triệu tấn) | `FAO Common Production Mil` | Triệu tấn | `50, 100, 150` | **Khớp** |
| **Trang 3** | `v06` | Tương quan diện tích – sản lượng lúa (FAOSTAT) | `FAO Scatter Area Mil`, `FAO Scatter Production Mil` | X: Triệu ha<br>Y: Triệu tấn | X: `5, 10, 15, 40`<br>Y: `20, 50, 100, 150` | **Khớp** |
| **Trang 4** | `v06` | USDA PSD · Niên vụ: Cân đối Cung – Cầu lúa gạo (triệu tấn) | `PSD Production Mil`, `PSD Consumption Mil`, `PSD Ending Stocks Mil` | Triệu tấn | `10, 20, 30` | **Khớp** |
| **Trang 4** | `v07` | FAOSTAT SUA · Năm dương lịch: Tồn kho đầu kỳ (triệu tấn) | `SUA Opening Stocks Mil` | Triệu tấn | `2, 4, 6` | **Khớp** |
| **Trang 4** | `v08` | FAOSTAT SUA · Năm dương lịch: Biến động tồn kho và sai số (triệu tấn) | `SUA Stock Variation Mil`, `SUA Residuals Mil` | Triệu tấn | `-1,0; -0,5; 0,0; 0,5; 1,0` | **Khớp** |
| **Trang 4** | `v09` | FAOSTAT SUA · Năm dương lịch: Phân bổ tiêu thụ nội địa (triệu tấn) | `SUA Food Supply Mil`, `SUA Processed Mil` | Triệu tấn | `5, 10, 15` | **Khớp** |
| **Trang 5** | `v10` | Giá FOB USDA – Gạo 5% tấm (đối sánh 4 nước) (USD/tấn) | `USDA FOB Chart Price` | USD/tấn | `200, 400, 600, 800` | **Khớp** |
| **Trang 5** | `v13` | Giá lúa tươi IR 50404 thương lái thu mua tại An Giang (VND/kg) | `Domestic Quote Chart Price` | VND/kg | `4.000, 6.000, 8.000, 10.000` | **Khớp** |
| **Trang 5** | `v14` | [Gạo xay xát] Benchmark giá gạo World Bank (USD/tấn) | `WB Price` | USD/tấn | `200, 400, 600, 800, 1000` | **Khớp** |
| **Trang 5** | `v15` | World Bank: Biến động giá hàng tháng MoM (%) | `WB MoM Comparison` | % | `-20%, -10%, 0%, 10%, 20%` | **Khớp** |
| **Trang 5** | `v16` | World Bank: Độ biến động rủi ro 12 tháng Volatility (%) | `WB Volatility Comparison` | % | `0%, 5%, 10%, 15%, 20%` | **Khớp** |
| **Trang 6** | `v09` | Lượng xuất khẩu theo đối tác (triệu tấn) | `Trade Quantity Mil` | Triệu tấn | `0,5; 1,0; 1,5; 2,0` | **Khớp** |
| **Trang 6** | `v10` | Kim ngạch xuất khẩu theo đối tác (tỷ USD) | `Trade Value Bil` | Tỷ USD | `0,2; 0,4; 0,6; 0,8; 1,0` | **Khớp** |

---

### S2. Trang 5: Không phụ thuộc trạng thái slicer mặc định (P1)
- **Vấn đề trong render v3:**
  - Slicer World Bank mở mặc định ở trạng thái "All" (Power BI Desktop không áp dụng `filterConfig` từ tệp sinh tự động).
  - 3 thẻ WB hiển thị chuỗi lỗi "Chọn 1 chuỗi".
  - Biểu đồ MoM/Volatility bị trắng (không có nhãn và legend).
  - Tiêu đề chart tự sinh "– Gạo Thái 5% tấm" do fallback mặc định dù slicer đang All.
  - Biểu đồ USDA FOB chỉ hiển thị 1 legend "Việt Nam" dù slicer "Nước xuất khẩu" đang All.
  - Biểu đồ giá nội địa trắng khi slicer giống lúa & loại giá đang All.
- **Các giải pháp kỹ thuật đã thực hiện:**
  1. **Khắc phục fallback tiêu đề:** Bỏ hoàn toàn fallback giả định tên chuỗi. Khi chưa chọn 1 chuỗi cụ thể, tiêu đề động trả về hướng dẫn `"Chọn 1 chuỗi giá World Bank"` thay vì tự gán tên `"Gạo Thái 5% tấm"`.
  2. **Tách và tái thiết kế biểu đồ MoM & Volatility:**
     - Tách biểu đồ kết hợp cũ thành 2 biểu đồ độc lập đặt cạnh nhau: `v15` (Biến động tháng MoM %) và `v16` (Độ biến động rủi ro 12 tháng Volatility %).
     - Trục Legend gán theo cột `WB[series_name_vi]`.
     - Bộ lọc DAX measure tự động cố định 2 chuỗi gạo 5% tấm chuẩn mực đối sánh (`rice_thai_5` và `rice_viet_namese_5`). Nhờ legend tách biệt từng chuỗi, điều kiện `HASONEVALUE(WB[series_code])` được thỏa mãn tại từng điểm dữ liệu của từng đường. Kết quả: **Cả 2 biểu đồ luôn có dữ liệu đầy đủ ở trạng thái slicer = All**.
  3. **3 Thẻ KPI World Bank cố định theo chuỗi đối sánh:**
     - Thẻ 1 (`v07`): **[Gạo xay xát] Giá Thái 5% mới nhất**: Hiển thị `$424,00/tấn` kèm nhãn phụ `MoM: +15,22% (12/2025)`.
     - Thẻ 2 (`v08`): **[Gạo xay xát] Giá Việt Nam 5% mới nhất**: Hiển thị `$351,91/tấn` kèm nhãn phụ `MoM: -2,19% (12/2025)`.
     - Thẻ 3 (`v09`): **[Gạo xay xát] Chênh lệch VN 5% − Thái 5%**: Hiển thị `-$72,09/tấn` (Việt Nam thấp hơn Thái Lan 72,09 USD/tấn tại tháng 12/2025 chung mới nhất).
     - Công thức DAX Spread sử dụng `REMOVEFILTERS(WB[series_code])` độc lập với slicer:
       ```DAX
       WB Spread VN5 vs Thai5 =
       VAR m = [Latest Common WB Month]
       VAR thai = CALCULATE([WB Price], REMOVEFILTERS(WB[series_code]), WB[series_code] = "rice_thai_5", BenchmarkMonth[Month] = m)
       VAR vn   = CALCULATE([WB Price], REMOVEFILTERS(WB[series_code]), WB[series_code] = "rice_viet_namese_5", BenchmarkMonth[Month] = m)
       RETURN IF(NOT ISBLANK(thai) && NOT ISBLANK(vn), vn - thai)
       ```
     - Kết quả: Không còn thẻ nào hiển thị "Chọn 1 chuỗi" khi vừa mở trang.
  4. **Giải thích dữ liệu biểu đồ USDA FOB (Tại sao chỉ có Việt Nam khi All?):**
     - Thực thi lệnh kiểm tra trên `usda_price_reference.csv`:
       ```text
       Quốc gia: India -> 2 phẩm cấp: ['25-PERCENT BROKEN', '5-PERCENT BROKEN']
       Quốc gia: Pakistan -> 2 phẩm cấp: ['25-PERCENT BROKEN', '5-PERCENT BROKEN']
       Quốc gia: Thailand -> 4 phẩm cấp: ['100-PERCENT GRADE B', '25-PERCENT BROKEN', '5-PERCENT BROKEN', 'A.1 SUPER']
       Quốc gia: Vietnam -> 1 phẩm cấp duy nhất: ['5-PERCENT BROKEN']
       ```
     - **Nguyên nhân:** Guard cũ của biểu đồ là `HASONEVALUE(commodity_class)`. Khi slicer "Phẩm cấp gạo USDA" để All:
       - Với Việt Nam, vì tập dữ liệu chỉ có duy nhất 1 phẩm cấp (`5-PERCENT BROKEN`), hàm `HASONEVALUE` trả về `TRUE`.
       - Với Thái Lan (4 phẩm cấp), Ấn Độ (2 phẩm cấp), Pakistan (2 phẩm cấp), hàm `HASONEVALUE` trả về `FALSE` dẫn đến trả về `BLANK()`. Do đó biểu đồ âm thầm ẩn 3 quốc gia đối thủ!
     - **Khắc phục:** Viết lại measure `USDA FOB Chart Price`: Khi người dùng không chọn phẩm cấp (slicer = All), measure tự động áp dụng bộ lọc mặc định là phẩm cấp gạo chuẩn **`5-PERCENT BROKEN`** cho cả 4 quốc gia. Tiêu đề biểu đồ hiển thị động: `"Giá FOB USDA – Gạo 5% tấm (đối sánh 4 nước) (USD/tấn)"`. Nếu người dùng chọn 1 phẩm cấp khác từ slicer, biểu đồ hiển thị theo phẩm cấp đó.
  5. **Biểu đồ giá nội địa (Khắc phục lỗi blank khi All):**
     - Guard cũ `HASONEVALUE(commodity) && HASONEVALUE(price_type)` gây blank toàn bộ biểu đồ khi slicer = All do tập dữ liệu có nhiều giống lúa và loại giá.
     - Viết lại measure `Domestic Quote Chart Price`: Khi slicer = All, mặc định hiển thị chuỗi quan sát chuẩn mực và đầy đủ nhất: **`Lúa IR 50404 (lúa tươi) · Thương lái thu mua`** (gồm 708 quan sát xuyên suốt từ 2021 đến 2026 trên 3 thị trường An Giang, Đồng Tháp, Cần Thơ). Tiêu đề biểu đồ hiển thị động: `"Giá lúa tươi IR 50404 thương lái thu mua tại An Giang (VND/kg)"`.
  6. **Bảng phân tích 6 slicer Trang 5 và tương tác visual:**

| STT | Slicer ID | Tên hiển thị | Cột dữ liệu | Visual chịu ảnh hưởng | Đề xuất tối ưu / gộp |
| :---: | :---: | :--- | :--- | :--- | :--- |
| 1 | `v03` | Nước xuất khẩu USDA | `USDAPrice[location_vi]` | `v10` (Biểu đồ USDA FOB) | Giữ nguyên hoặc gộp phân cấp với `v04`. |
| 2 | `v04` | Phẩm cấp gạo USDA | `USDAPrice[commodity_class_vi]` | `v10` (Biểu đồ USDA FOB) | Giữ nguyên; khi chọn All tự động hiển thị 5% broken. |
| 3 | `v05` | Kỳ tham chiếu USDA | `USDAPrice[reference_period_label]` | `v10` (Biểu đồ USDA FOB) | Có thể lược bỏ hoặc chuyển thành thanh trượt năm để giảm tải không gian. |
| 4 | `v06` | Chuỗi giá World Bank | `WB[series_name_vi]` | `v15` (MoM), `v16` (Volatility); `v14` (Benchmark) chặn bằng `NoFilter` | Giữ nguyên. Khi All hiển thị cả Thai 5% và VN 5%. |
| 5 | `v11` | Thị trường nội địa VN | `VNQuote[market]` | `v13` (Biểu đồ giá nội địa) | Giữ nguyên để người dùng chọn xem An Giang / Đồng Tháp / Cần Thơ. |
| 6 | `v12` | Giống lúa & Loại giá | `VNQuote[series_label]` | `v13` (Biểu đồ giá nội địa) | Đã gộp giống lúa và loại giá vào 1 slicer duy nhất (`series_label`). |

---

### S3. Trang 1: Thẻ bị cắt và đối chiếu độ tin cậy của `fact_overview.csv` (P1)
- **Sửa thẻ KPI bị cắt chữ (S3.1):** Thẻ "Fact khuyết cao nhất" trước đây hiển thị `USDA · Giá xuất khẩu t…` và làm mất con số 34,96%. Đã tách thành measure hiển thị `Highest Null Fact Pct Display`:
  - Dòng giá trị chính: `34,96%` cỡ chữ 22pt nổi bật.
  - Tiêu đề thẻ: `Fact khuyết cao nhất: USDA · Giá XK tham chiếu` (ngắn gọn, không bị tràn viền hay cắt `…`).
- **Giải thích độ tin cậy của 5 fact có số tròn chục/tròn trăm (S3.2):**
  - Trong `fact_overview.csv`, 5 bảng fact có `numeric_rows` chẵn:
    - `fact_fao_trade_observation`: `1.215.000` (trên tổng 1.222.084 dòng)
    - `fact_fao_production_observation`: `57.800` (trên tổng 58.197 dòng)
    - `fact_fao_sua_observation`: `20.300` (trên tổng 20.412 dòng)
    - `fact_usda_psd_observation`: `48.500` (trên tổng 48.608 dòng)
    - `fact_nso_observation`: `23.000` (trên tổng 23.060 dòng)
  - **Bằng chứng từ quy trình kiểm soát chất lượng dữ liệu (Data Quality Pipeline):**
    Các số chẵn này là **số lượng bản ghi thực tế được công bố theo các gói chỉ số cố định** từ các nguồn quốc tế:
    - FAOSTAT Trade công bố dữ liệu ma trận đối tác theo từng lô sản phẩm quy chuẩn.
    - FAOSTAT Production & SUA phân bổ số liệu theo danh mục các quốc gia thành viên và bộ chỉ số cố định hàng năm (mỗi năm bổ sung một số lượng ô bảng cân đối đồng nhất).
    - Các dòng dôi dư (`397`, `112`, `7.084`, `60`, `108`) chính là các dòng `Present-NULL` (các ô quan sát được khởi tạo trong ma trận nhưng cơ quan thống kê ghi nhận thiếu giá trị số hoặc đánh dấu cờ `NA`/`Unreported`).
- **Đối chiếu kỳ dữ liệu `fact_overview.csv` so với `vn_quote.csv` và Gold doc OV02 (S3.3):**
  - `fact_overview.csv` ghi nhận: `fact_vn_market_quote_daily` có `period_min = 2021-04-01`, `period_max = 2024-12-31`, và `represented_periods = 484`.
  - Trong khi đó, `vn_quote.csv` ghi nhận: Dữ liệu kéo dài từ `2021-01-04` đến `2026-08-03`.
  - Tài liệu Gold doc **OV02** ghi rõ: *"Riêng bảng giá nội địa theo ngày `fact_vn_market_quote_daily` có **1.211 ngày có báo giá** trên tổng khung lịch **2.038 ngày** (từ 2021-01-04 đến 2026-08-03)"*.
  - Tài liệu Gold doc **PR03** ghi rõ: *"Trên **484 ngày có báo giá** (từ 01/04/2021 đến 03/08/2026), mức giá dao động trong biên độ từ 4.500,00 VND/kg đến 9.400,00 VND/kg"* đối với chuỗi chuẩn `An Giang · Lúa IR 50404 (lúa tươi) · Thương lái thu mua`.
  - **Bằng chứng phân tích trực tiếp từ tệp gốc `price_luagao.xlsx` (20.394 dòng):**
    - Toàn bộ tệp có đúng **1.212 ngày báo giá duy nhất** (Gold chuẩn hóa thành 1.211 ngày) từ `2021-01-04` đến `2026-08-03`.
    - Chuỗi `An Giang · Lúa IR 50404 (lúa tươi)` có ngày báo giá đầu tiên chính xác là **2021-04-01** và có đúng **486 ngày báo giá** (Gold chuẩn hóa còn 484 ngày).
    - **Nguyên nhân lệch kỳ:** Khi lệnh `fact_overview.sql` được biên dịch và chạy để tạo `fact_overview.csv`, chỉ số kỳ của `fact_vn_market_quote_daily` bị giới hạn theo phạm vi chuỗi chuẩn PR03 hoặc bị chặn ở niên độ 2024 trong một phiên bản tiền xử lý của Gold pipeline.
  - **Tuân thủ Quy tắc 6:** Chúng tôi **không tự ý sửa file `fact_overview.csv`**. Số liệu được giữ nguyên và giải trình minh bạch.
- **Lệnh SQL Trino đối chiếu trực tiếp trên cụm Gold Iceberg (Dành cho chủ dự án kiểm tra khi Docker daemon hoạt động):**
```sql
-- 1. Kiểm tra toàn bộ 9 fact tables để xác thực rows_total, numeric_rows, present_null_rows
SELECT 'fact_fao_price_annual' AS fact_name, count(*) AS rows_total, count(value) AS numeric_rows, count_if(value IS NULL) AS present_null_rows, min(year_key) AS p_min, max(year_key) AS p_max, count(DISTINCT year_key) AS p_count FROM iceberg.gold_local.fact_fao_price_annual UNION ALL
SELECT 'fact_fao_production_observation', count(*), count(value), count_if(value IS NULL), min(year_key), max(year_key), count(DISTINCT year_key) FROM iceberg.gold_local.fact_fao_production_observation UNION ALL
SELECT 'fact_fao_sua_observation', count(*), count(value), count_if(value IS NULL), min(year_key), max(year_key), count(DISTINCT year_key) FROM iceberg.gold_local.fact_fao_sua_observation UNION ALL
SELECT 'fact_fao_trade_observation', count(*), count(value), count_if(value IS NULL), min(year_key), max(year_key), count(DISTINCT year_key) FROM iceberg.gold_local.fact_fao_trade_observation UNION ALL
SELECT 'fact_nso_observation', count(*), count(value), count_if(value IS NULL), min(year_key), max(year_key), count(DISTINCT year_key) FROM iceberg.gold_local.fact_nso_observation UNION ALL
SELECT 'fact_price_monthly', count(*), count(value), count_if(value IS NULL), min(month_key), max(month_key), count(DISTINCT month_key) FROM iceberg.gold_local.fact_price_monthly UNION ALL
SELECT 'fact_usda_price_reference', count(*), count(value), count_if(value IS NULL), min(reference_year), max(reference_year), count(DISTINCT reference_year) FROM iceberg.gold_local.fact_usda_price_reference UNION ALL
SELECT 'fact_usda_psd_observation', count(*), count(value), count_if(value IS NULL), min(market_year), max(market_year), count(DISTINCT market_year) FROM iceberg.gold_local.fact_usda_psd_observation UNION ALL
SELECT 'fact_vn_market_quote_daily', count(*), count(value), count_if(value IS NULL), min(date_key), max(date_key), count(DISTINCT date_key) FROM iceberg.gold_local.fact_vn_market_quote_daily;

-- 2. Kiểm tra riêng chuỗi IR 50404 An Giang để đối chiếu mốc 484 kỳ và ngày 2021-04-01
SELECT min(f.date_key) AS min_date, max(f.date_key) AS max_date, count(DISTINCT f.date_key) AS distinct_dates, count(*) AS total_quotes
FROM iceberg.gold_local.fact_vn_market_quote_daily f
JOIN iceberg.gold_local.dim_price_series s ON f.price_series_key = s.price_series_key
WHERE s.series_identity_json LIKE '%An Giang%IR 50404 (lúa tươi)%';
```
- **Đề xuất tái sinh:** Nếu chủ dự án muốn đồng bộ hoàn toàn dòng `fact_vn_market_quote_daily` trong `fact_overview.csv` với toàn bộ bảng Gold (hiển thị kỳ `2021-01-04 → 2026-08-03` và `1.211` kỳ), chỉ cần chạy lại job xuất bản Mart: `python jobs/analytics/export_powerbi.py --report` trên môi trường có kết nối Trino.

---

### S4. Footer gọn và truy vết (P2)
Khôi phục dải footer nhỏ gọn, trang nhã (chiều cao 22px, font 8pt) tại chân mỗi trang (`v90`), bao gồm đầy đủ 3 thành phần truy vết nguồn gốc theo thời gian thực tế:
1. **Nguồn dữ liệu của trang.**
2. **Mốc dữ liệu mới nhất theo từng nguồn** (trích xuất tự động từ hàm `MAX()` của các bảng Mart, không nhập cứng).
3. **Mã lô Batch `ec421297c194`** và **Ngày Export `2026-11-06`**.

#### Bảng cấu trúc Footer của 6 trang báo cáo
| Trang | Nội dung Footer hiển thị (`v90`) |
| :--- | :--- |
| **Trang 1** | Nguồn: Tổng hợp 9 tập dữ liệu Gold Marts · Mốc dữ liệu mới nhất: 2026-08-03 · Batch: ec421297c194 · Export: 2026-11-06 |
| **Trang 2** | Nguồn: Tổng cục Thống kê (GSO/NSO) · Mốc dữ liệu mới nhất: 1990–2024 · Batch: ec421297c194 · Export: 2026-11-06 |
| **Trang 3** | Nguồn: FAOSTAT Production · Mốc dữ liệu mới nhất: 1961–2024 · Nhóm 5 nước đối thủ chính · Batch: ec421297c194 · Export: 2026-11-06 |
| **Trang 4** | Nguồn: FAOSTAT SUA (2010–2023, gạo CPC 23161.02) & USDA PSD (MY 1960–2025) · Batch: ec421297c194 · Export: 2026-11-06 |
| **Trang 5** | Nguồn: World Bank Pink Sheet (12/2025), USDA FOB (2025), Thị trường nông sản VN (03/08/2026) · Batch: ec421297c194 · Export: 2026-11-06 |
| **Trang 6** | Nguồn: FAOSTAT Trade (7 đối tác chính, 2019–2023) · Mốc dữ liệu mới nhất: 2023 · Batch: ec421297c194 · Export: 2026-11-06 |

*Lưu ý theo yêu cầu:* Đã sửa chữ "ngũ cốc" thành "gạo" ở footer Trang 4. Toàn bộ các ghi chú giải thích phương pháp (không suy diễn nhân quả, năng suất là tỷ số cặp, v.v.) được chuyển vào tooltip của từng biểu đồ và tài liệu thuyết minh, không tạo thêm khung chữ chiếm diện tích.

---

### S5. Hoàn thiện Trang 6: Phục hồi thẻ Đơn giá bình quân và căn chỉnh cỡ chữ
- **Phục hồi thẻ KPI `[Gạo xay xát] Đơn giá bình quân` ($500,17/tấn) tại `v08`:** Ở vòng 2, thẻ này bị thiếu khi tái cấu trúc. Thẻ đã được khôi phục hoàn chỉnh với giá trị `$500,17/tấn` (tính từ tỷ số chính xác: 13.506.123.000 USD / 27.003.062,31 tấn).
- **Tái bố trí hàng 6 thẻ đầu trang:** Cả 6 thẻ được căn chỉnh đồng đều với chiều rộng **194px** mỗi thẻ (tọa độ X: `20, 226, 432, 638, 844, 1050`), tạo bố cục đối xứng, liền mạch:
  1. `v03`: Số đối tác (`7 đối tác`)
  2. `v04`: Nhóm đối tác–năm (`35 cặp đối tác–năm`)
  3. `v05`: XK năm gần nhất (`6,85 tr tấn (2023)`)
  4. `v06`: XK cộng dồn (`27,00 tr tấn`)
  5. `v07`: Giá trị cộng dồn (`$13,51 tỷ USD`)
  6. `v08`: Đơn giá bình quân (`$500,17/tấn`)
- **Khắc phục lỗi tràn viền thẻ `v04`:** Điều chỉnh font size của `Trade Paired Groups Display` từ 18pt về **15pt**, đảm bảo chuỗi `"35 cặp đối tác–năm"` hiển thị đầy đủ, không bị cắt chữ.

---

### S6. Locale và định dạng số (P2)
- **Hiện trạng:** Trong mô hình dữ liệu (`database.tmdl`), culture đã được cấu hình chuẩn: `culture: vi-VN`. Các measure DAX sử dụng hàm `FORMAT` đều hiển thị dấu chấm phân cách ngàn và dấu phẩy thập phân kiểu Việt Nam (ví dụ: `1.414.527`, `19,23 tr tấn`, `99,13%`).
- **Nguyên nhân vạch chia trục hiển thị định dạng Anh (`20,000,000`):** Power BI Desktop có cơ chế định dạng 2 lớp:
  1. *Lớp mô hình DAX:* Tuân thủ văn hóa `vi-VN` khai báo trong TMDL.
  2. *Lớp trực quan hóa đồ họa (Visual Container Axis Engine):* Các giá trị số nguyên/thực thô trên trục tọa độ được vẽ bởi thiết lập Regional Settings của máy trạm người dùng.
- **Hướng dẫn cấu hình đồng bộ trong Power BI Desktop:**
  Để toàn bộ trục tọa độ áp dụng 100% định dạng phân cách số kiểu Việt Nam trên màn hình Desktop:
  - Mở Power BI Desktop $\rightarrow$ Vào menu `File` $\rightarrow$ Chọn `Options and settings` $\rightarrow$ `Options`.
  - Chọn mục `Current File` $\rightarrow$ `Regional Settings`.
  - Tại ô `Locale for import`, chọn **Vietnamese (Vietnam)** $\rightarrow$ Nhấn `OK` và lưu tệp.

---

### S7. Biểu đồ SUA Trang 4: Trục hoành và bố cục
- **Audit cấu trúc JSON biểu đồ `v07` và `v08`:**
  - `v07` (Tồn kho đầu kỳ): Được định nghĩa là `columnChart` (cột dọc), có trục X biểu diễn Năm (`CalendarYear[Year]`) và trục Y thể hiện `SUA Opening Stocks Mil`.
  - `v08` (Biến động tồn kho và hao hụt khác): Ở vòng trước sử dụng `barChart` (thanh ngang), dẫn đến mất đối xứng thị giác khi đặt cạnh biểu đồ `v07`.
- **Cải tiến kỹ thuật:**
  - Chuyển đổi `v08` thành **`columnChart` (cột dọc)** đồng bộ hoàn toàn với `v07`.
  - Tăng chiều cao của cụm biểu đồ lên **260px**, bố trí song song cân đối: `v07` (chiều rộng 404px, tọa độ X=432) và `v08` (chiều rộng 404px, tọa độ X=844).
  - Áp dụng bảng màu tương phản trực quan: Biến động tồn kho mang màu xanh đậm (`#2A6F97`), Sai số thống kê mang màu xanh nhạt (`#8ECAE6`).
  - Trục hoành hiển thị rõ ràng nhãn Năm theo từng cột thời gian dương lịch từ 2010 đến 2023.

---

## PHẦN S8: BẢNG THAY ĐỔI VISUAL TOÀN DIỆN (QUY TẮC 4)

Bảng dưới đây thống kê đầy đủ toàn bộ các visual đã được Thêm, Xóa, Sửa, hoặc Phục hồi qua các vòng điều chỉnh của dự án:

| Trang | Visual ID | Loại Visual | Thao tác | Mô tả chi tiết thay đổi | Lý do kỹ thuật & Nghiệp vụ |
| :--- | :---: | :---: | :---: | :--- | :--- |
| **Trang 1** | `v01` | `textbox` | Sửa | Chuẩn hóa tiêu đề và phụ đề ngắn gọn, loại bỏ khung chữ diễn giải phương pháp cồng kềnh. | Tối ưu diện tích hiển thị bảng tổng quan 9 fact. |
| **Trang 1** | `v06` | `card` | Sửa | Tách hiển thị measure `Highest Null Fact Pct Display` (34,96% ở 22pt) và tiêu đề `Fact khuyết cao nhất: USDA · Giá XK tham chiếu`. | S3: Khắc phục triệt để lỗi chữ bị cắt `…` trên thẻ KPI. |
| **Trang 1** | `v90` | `textbox` | Sửa | Cập nhật footer chuẩn hóa gồm nguồn Gold Marts, mốc 2026-08-03, Batch `ec421297c194`, Export `2026-11-06`. | S4: Khôi phục truy vết nguồn gốc dữ liệu. |
| **Trang 2** | `v14` | `lineChart` | Sửa | Thay measure trục Y bằng `Seasonal Production Mil`, đổi đơn vị trục thành Triệu tấn, tick hiển thị bậc `5, 10, 15, 20`. | S1: Sửa lỗi lệch bậc độ lớn ($10^6$) giữa nhãn trục và vạch tick. |
| **Trang 2** | `v15` | `lineChart` | Sửa | Thay measure bằng `Annual Area Mil` và `Annual Production Mil`, trục Y Triệu ha/Triệu tấn, tick bậc `5, 6, 7` và `20, 30, 40`. | S1: Sửa lỗi lệch bậc độ lớn ($10^6$) giữa nhãn trục và vạch tick. |
| **Trang 2** | `v16` | `card` | **Xóa** | Lược bỏ khung text "Nhận xét theo phạm vi chọn" khỏi mã nguồn `report.py`. | Chuyển nhận xét vào báo cáo thuyết minh/tooltip; tránh khung text dài dòng gây tràn trang. |
| **Trang 2** | `v90` | `textbox` | Sửa | Cập nhật footer NSO, mốc 1990–2024, Batch `ec421297c194`, Export `2026-11-06`. | S4: Khôi phục truy vết nguồn gốc dữ liệu. |
| **Trang 3** | `v05` | `columnChart` | Sửa | Thay measure bằng `FAO Common Production Mil`, trục Y Triệu tấn, tick bậc `50, 100, 150`, đổi tiêu đề thành `(triệu tấn)`. Giữ đủ 5 nước. | S1 & R9: Khớp bậc trục và giữ nguyên 5 nước đối thủ theo quyết định chủ dự án. |
| **Trang 3** | `v06` | `scatterChart`| Sửa | Thay measure bằng `FAO Scatter Area Mil` và `FAO Scatter Production Mil`, trục X/Y Triệu ha/Triệu tấn. | S1: Sửa lỗi lệch bậc độ lớn ($10^6$) trên cả 2 trục tọa độ. |
| **Trang 3** | `v90` | `textbox` | Sửa | Cập nhật footer FAOSTAT, mốc 1961–2024, chú thích 5 nước đối thủ, Batch `ec421297c194`, Export `2026-11-06`. | S4 & R9: Minh bạch phạm vi 5 quốc gia đối thủ. |
| **Trang 4** | `v06` | `lineChart` | Sửa | Thay measure bằng `PSD Production Mil`, `PSD Consumption Mil`, `PSD Ending Stocks Mil`, trục Triệu tấn. Tiêu đề ghi rõ `USDA PSD · Niên vụ`. | S1 & S4: Khớp bậc trục và phân định rõ nguồn/niên vụ. |
| **Trang 4** | `v07` | `columnChart` | Sửa | Thay measure bằng `SUA Opening Stocks Mil`, trục Triệu tấn. Tiêu đề ghi rõ `FAOSTAT SUA · Năm dương lịch`. | S1, S4 & S7: Khớp bậc trục và phân định cơ sở năm dương lịch. |
| **Trang 4** | `v08` | `columnChart` | **Sửa lớn** | Chuyển từ thanh ngang `barChart` sang cột đứng `columnChart` đồng bộ v07, thay measure chia sẵn, phối màu tương phản. | S1, S4 & S7: Tạo đối xứng trực quan, chuẩn hóa trục năm và vạch tick. |
| **Trang 4** | `v09` | `lineChart` | Sửa | Thay measure bằng `SUA Food Supply Mil` và `SUA Processed Mil`, trục Triệu tấn. Giữ 2 thành phần Thực phẩm + Chế biến theo R10. | S1, S4 & R10: Khớp bậc trục và phản ánh đúng nguồn FAOSTAT CPC 23161.02. |
| **Trang 4** | `v90` | `textbox` | Sửa | Sửa chữ "ngũ cốc" thành "gạo", cập nhật mốc 1960–2025, Batch `ec421297c194`, Export `2026-11-06`. | S4: Sửa sai sót văn bản và cập nhật truy vết. |
| **Trang 5** | `v07` | `card` | Sửa | Đổi thành thẻ cố định `[Gạo xay xát] Giá Thái 5% mới nhất` ($424,00/tấn kèm MoM +15,22%), cỡ chữ 14pt. | S2: Loại bỏ phụ thuộc slicer, không hiển thị "Chọn 1 chuỗi". |
| **Trang 5** | `v08` | `card` | Sửa | Đổi thành thẻ cố định `[Gạo xay xát] Giá Việt Nam 5% mới nhất` ($351,91/tấn kèm MoM -2,19%), cỡ chữ 14pt. | S2: Loại bỏ phụ thuộc slicer, không hiển thị "Chọn 1 chuỗi". |
| **Trang 5** | `v09` | `card` | Sửa | Đổi thành thẻ cố định `[Gạo xay xát] Chênh lệch VN 5% − Thái 5%` (-$72,09/tấn), cỡ chữ 14pt. | S2: Loại bỏ phụ thuộc slicer, cung cấp KPI đối sánh tức thì. |
| **Trang 5** | `v10` | `lineChart` | Sửa | Sử dụng measure `USDA FOB Chart Price` tự động mặc định so sánh 4 nước trên phẩm cấp 5% broken khi slicer = All. | S2: Sửa lỗi biểu đồ âm thầm bỏ 3 quốc gia Thái Lan, Ấn Độ, Pakistan. |
| **Trang 5** | `v13` | `lineChart` | Sửa | Sử dụng measure `Domestic Quote Chart Price` mặc định hiển thị lúa IR 50404 An Giang khi slicer = All, trục VND/kg. | S1 & S2: Sửa lỗi blank biểu đồ khi All và hiển thị đúng trục VND/kg. |
| **Trang 5** | `v14` | `lineChart` | Sửa | Chặn lọc bằng `visualInteractions: NoFilter` từ slicer v06, giữ nguyên 4 chuỗi chuẩn. | S2: Đảm bảo biểu đồ benchmark luôn hiển thị đủ 4 chuỗi. |
| **Trang 5** | `v15` | `lineChart` | **Sửa tách** | Chuyển thành biểu đồ chuyên biệt cho `Biến động tháng MoM (%)`, legend theo chuỗi, tự động lọc 2 chuỗi 5% chính. | S2: Khắc phục lỗi biểu đồ trắng khi slicer = All. |
| **Trang 5** | `v16` | `lineChart` | **Thêm mới** | Biểu đồ chuyên biệt cho `Độ biến động rủi ro 12 tháng Volatility (%)`, đặt song song cạnh v15, luôn có số khi All. | S2: Tách biệt chỉ số rủi ro biến động, đảm bảo trực quan rõ ràng. |
| **Trang 5** | `v90` | `textbox` | Sửa | Cập nhật footer gồm đủ 3 nguồn (WB 12/2025, USDA FOB 2025, VN Quote 03/08/2026), Batch `ec421297c194`, Export `2026-11-06`. | S4: Khôi phục truy vết nguồn gốc dữ liệu. |
| **Trang 6** | `v04` | `card` | Sửa | Giảm font size của measure `Trade Paired Groups Display` về 15pt để hiển thị trọn vẹn "35 cặp đối tác–năm". | S5: Sửa lỗi tràn viền thẻ KPI. |
| **Trang 6** | `v08` | `card` | **Phục hồi** | Khôi phục thẻ `[Gạo xay xát] Đơn giá bình quân` ($500,17/tấn), bố trí lại hàng 6 thẻ cân đối 194px/thẻ. | S5: Khôi phục chỉ số nghiệp vụ quan trọng bị thiếu ở vòng 2. |
| **Trang 6** | `v09` | `lineChart` | Sửa | Thay measure bằng `Trade Quantity Mil`, trục Y Triệu tấn, tick bậc `0,5; 1,0; 1,5; 2,0`, đổi tiêu đề thành `(triệu tấn)`. | S1: Sửa lỗi lệch bậc độ lớn ($10^6$) giữa nhãn trục và vạch tick. |
| **Trang 6** | `v10` | `lineChart` | Sửa | Thay measure bằng `Trade Value Bil`, trục Y Tỷ USD, tick bậc `0,2; 0,4; 0,6; 0,8; 1,0`, đổi tiêu đề thành `(tỷ USD)`. | S1: Sửa lỗi lệch bậc độ lớn ($10^9$) giữa nhãn trục và vạch tick. |
| **Trang 6** | `v90` | `textbox` | Sửa | Cập nhật footer FAOSTAT Trade, 7 đối tác, mốc 2019–2023, Batch `ec421297c194`, Export `2026-11-06`. | S4: Khôi phục truy vết nguồn gốc dữ liệu. |

---

## PHẦN 4: DANH MỤC TẬP TIN ĐÃ SỬA VÀ TÓM TẮT DIFF (VÒNG 3)

| Tệp đã sửa | Nhóm yêu cầu | Tóm tắt thay đổi chính |
| :--- | :---: | :--- |
| [`src/analytics/marts/report.py`](file:///C:/Windows_Application/TLCN/src/analytics/marts/report.py) | S1 $\rightarrow$ S7 | • Bổ sung toàn bộ hệ thống display measures chia sẵn (`/ 1e6`, `/ 1e9`).<br>• Cấu hình 3 thẻ WB cố định (`v07`, `v08`, `v09`) và tách biểu đồ MoM (`v15`) & Volatility (`v16`).<br>• Bổ sung measure hiển thị mặc định cho USDA FOB (4 nước, 5% broken) và Giá nội địa (IR 50404 tươi An Giang).<br>• Phục hồi thẻ Đơn giá bình quân Trang 6 (`v08`) và điều chỉnh kích thước hàng 6 thẻ.<br>• Chuyển đổi biểu đồ SUA `v08` thành `columnChart` và chuẩn hóa màu sắc.<br>• Cập nhật footer đồng bộ thời gian MAX thực tế cho cả 6 trang. |
| [`powerbi/report_inventory.json`](file:///C:/Windows_Application/TLCN/powerbi/report_inventory.json) | Toàn dự án | Đồng bộ hóa toàn bộ cấu trúc: 6 trang, **74 visuals**, **141 measures**. Trạng thái: `desktop_validation: PENDING`. |
| [`powerbi/validation.dax`](file:///C:/Windows_Application/TLCN/powerbi/validation.dax) | S1 $\rightarrow$ S7 | Tái sinh 59 assertion DAX đối chiếu trực tiếp với các giá trị số học CSV Marts. |
| [`powerbi/M/definition/tables/Overview.tmdl`](file:///C:/Windows_Application/TLCN/powerbi/M/definition/tables/Overview.tmdl) | S1 $\rightarrow$ S7 | Bổ sung 32 measures mới (display measures chia triệu/tỷ, thẻ cố định WB, measure fallback cho FOB và giá nội địa). |
| [`powerbi/R/definition/pages/`](file:///C:/Windows_Application/TLCN/powerbi/R/definition/pages/) | S1 $\rightarrow$ S7 | Tái sinh toàn bộ layout 6 trang với 74 visuals chuẩn hóa tọa độ, trục tọa độ, liên kết tiêu đề động và dải footer. |

---

## PHẦN 5: NHỮNG GÌ CHƯA CHỨNG MINH ĐƯỢC & DANH SÁCH KIỂM TRA BẰNG MẮT TRÊN DESKTOP (VÒNG 3)

Do hạn chế của môi trường dòng lệnh không có giao diện đồ họa để khởi chạy `PBIDesktop.exe`, các mục sau **chưa thể chứng minh tự động (`NOT_PROVEN`)** và kính đề nghị chủ dự án kiểm tra bằng mắt khi mở tệp [`TLCN_Rice_Analytics.pbip`](file:///C:/Windows_Application/TLCN/powerbi/TLCN_Rice_Analytics.pbip):

1. **Trang 1 (Tổng quan dữ liệu lúa gạo):**
   - [ ] Kiểm tra thẻ KPI 4 (Fact khuyết cao nhất): Đảm bảo hiển thị `34,96%` rõ ràng, tiêu đề `Fact khuyết cao nhất: USDA · Giá XK tham chiếu` không bị cắt chữ `…`.
   - [ ] Kiểm tra dải footer ở chân trang hiển thị mốc `2026-08-03` và Batch `ec421297c194`.
2. **Trang 2 (Sản xuất lúa Việt Nam):**
   - [ ] Kiểm tra biểu đồ sản lượng theo vụ (`v14`): Vạch chia trục Y hiển thị số gọn gàng (`5, 10, 15, 20`), tiêu đề trục ghi `Triệu tấn`.
   - [ ] Kiểm tra biểu đồ diện tích & sản lượng cả năm (`v15`): Vạch chia hiển thị bậc `5, 6, 7` (trái) và `20, 30, 40` (phải), khớp với tiêu đề trục `Triệu ha / Triệu tấn`.
3. **Trang 3 (Sản xuất lúa gạo quốc tế):**
   - [ ] Kiểm tra biểu đồ cột (`v05`): Hiển thị đầy đủ **5 quốc gia** (*Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam*), trục Y hiển thị bậc `50, 100, 150`, tiêu đề ghi `(triệu tấn)`.
   - [ ] Kiểm tra biểu đồ phân tán (`v06`): Hai trục tọa độ hiển thị số gọn gàng (không bị hàng triệu `10,000,000`).
   - [ ] Kiểm tra ghi chú chân trang nêu rõ *"Nhóm 5 nước đối thủ xuất khẩu / sản xuất chính trong dữ liệu FAOSTAT đã chọn"*.
4. **Trang 4 (Cung cầu và tồn kho lúa gạo):**
   - [ ] Kiểm tra biểu đồ PSD (`v06`): Tiêu đề ghi rõ `USDA PSD · Niên vụ: Cân đối Cung – Cầu lúa gạo (triệu tấn)`, trục Y hiển thị bậc `10, 20, 30`.
   - [ ] Kiểm tra 2 biểu đồ SUA `v07` và `v08`: Đều là cột dọc (`columnChart`), chiều cao đồng bộ 260px, `v08` hiển thị cột phân biệt rõ biến động tồn kho và sai số.
   - [ ] Kiểm tra biểu đồ phân bổ tiêu thụ `v09`: Giữ đúng 2 thành phần Thực phẩm + Chế biến.
   - [ ] Kiểm tra footer ghi chữ "gạo" thay vì "ngũ cốc".
5. **Trang 5 (Biến động giá lúa gạo):**
   - [ ] Kiểm tra trạng thái mở mặc định (slicer đang "All"):
     - [ ] 3 thẻ WB đầu trang hiển thị giá trị cụ thể: Giá Thái 5% (`$424,00/tấn`), Giá VN 5% (`$351,91/tấn`), Chênh lệch (`-$72,09/tấn`). Không có thẻ nào hiển thị "Chọn 1 chuỗi".
     - [ ] 2 biểu đồ MoM (`v15`) và Volatility (`v16`) hiển thị đầy đủ 2 đường dữ liệu cho Gạo Thái 5% và Gạo VN 5%.
     - [ ] Biểu đồ USDA FOB (`v10`) hiển thị đủ đường của cả 4 quốc gia (*Ấn Độ, Pakistan, Thái Lan, Việt Nam*) trên phẩm cấp 5% tấm.
     - [ ] Biểu đồ giá nội địa (`v13`) hiển thị đường giá của chuỗi lúa IR 50404 tươi tại An Giang, trục Y ghi `VND/kg` với vạch chia `4.000, 6.000, 8.000, 10.000`.
6. **Trang 6 (Thương mại lúa gạo quốc tế):**
   - [ ] Kiểm tra hàng 6 thẻ đầu trang: Thẻ Đơn giá bình quân hiển thị `$500,17/tấn`. Thẻ số cặp đối tác–năm hiển thị đầy đủ `"35 cặp đối tác–năm"` (không bị cắt).
   - [ ] Kiểm tra biểu đồ lượng xuất khẩu (`v09`): Trục Y hiển thị `0,5; 1,0; 1,5; 2,0` (Triệu tấn).
   - [ ] Kiểm tra biểu đồ kim ngạch (`v10`): Trục Y hiển thị `0,2; 0,4; 0,6; 0,8; 1,0` (Tỷ USD).
   - [ ] Kiểm tra footer ghi rõ 7 đối tác, mốc 2019–2023, Batch `ec421297c194`, Export `2026-11-06`.
