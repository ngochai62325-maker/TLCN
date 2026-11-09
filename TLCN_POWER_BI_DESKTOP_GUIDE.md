# TLCN Rice Lakehouse — Hướng dẫn dựng báo cáo Power BI Desktop miễn phí

> Phiên bản hướng dẫn: 27/10/2026. Mục tiêu: **tự kéo thả, tạo biểu đồ và lưu `.pbix` trên Windows**, không dùng Streamlit hoặc Power BI Pro. Đây là hướng dẫn dựng báo cáo, **không phải file `.pbix` đã tạo**. Các tên CSV/cột bên dưới là **hợp đồng export đề xuất**; Codex cần tạo và kiểm tra các file thực tế từ published Gold trước khi bạn bấm Import.

## 1. Nguồn dữ liệu và phạm vi đã nghiệm thu

- Gold: 8 dimensions, 9 facts, **1.414.527 observations**; truy vấn qua `iceberg.gold_local` theo published snapshots.
- 6 notebooks tại `notebooks/analytics/`, 24 figures và 22 findings tại `docs/analytics/EDA_FINDINGS.md`.
- Analytic access hiện có: `src/analytics/`, `jobs/analytics/query_eda.py`, `queries/analytics/` với allowlist và Gold publication/read gate.
- **Không dùng** consumer Gold trả 20 sample rows để tính KPI; không xuất cả 1.226.470 dòng Trade sang Power BI khi chưa có lý do.
- Published snapshot và source-scoped identities phải được bảo toàn. CSV là bản trích xuất tại **một thời điểm**, không phải live subscription.

### Tại sao chọn CSV thay vì kết nối trực tiếp?

Power BI Desktop có hỗ trợ **Get Data > Text/CSV** trên máy Windows. CSV nhỏ đã được SQL nhóm/lọc bằng Trino nên nhanh và nhẹ hơn so với chạy Power BI qua nhiều lớp JDBC/ODBC và truy vấn lại toàn bộ Iceberg. Chỉ làm mới dữ liệu khi cần: chạy lại Python export theo published batch, sau đó bấm **Home > Refresh** trong Power BI Desktop.

## 2. Cài Power BI Desktop miễn phí

1. Mở trang chính thức [Download Power BI Desktop](https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-get-the-desktop), chọn Microsoft Store hoặc bản cài Windows chính thức.
2. Cài rồi mở **Power BI Desktop**, chọn **Blank report** nếu được hỏi.
3. Không cần Pro để tự xây report và lưu file `.pbix` trên máy. Các tính năng chia sẻ/cộng tác trên Power BI Service chịu giới hạn license riêng; báo cáo local `.pbix` là đủ để trình diễn cho giảng viên.
4. Vào **File > Options and settings > Options** để chọn locale phù hợp khi cần; nếu số thập phân trong CSV dùng dấu chấm thì đặt kiểu số bằng **Change Type > Using Locale... > Decimal Number > English (United States)** trong Power Query.

## 3. Chuẩn bị dữ liệu — bước Codex phải làm trong repository

Tạo một exporter **chỉ đọc published Gold**; nên đặt tại `jobs/analytics/export_powerbi.py` và dùng lại `src/analytics/`. Đầu ra là các file CSV nhỏ, chẳng hạn trong `exports/powerbi/` (đưa thư mục này vào `.gitignore`).

**Các CSV đề xuất cho hai trang đầu:**

| File | Grain bắt buộc | Cột yêu cầu (ví dụ contract xuất) | Dùng cho |
|---|---|---|---|
| `fact_overview.csv` | Một dòng trên mỗi Gold fact (9 dòng) | `fact_name,rows_total,numeric_rows,present_null_rows,numeric_coverage_pct,period_min,period_max,represented_periods,period_grain` | Tổng quan / coverage |
| `nso_national_annual.csv` | Một năm thuộc đúng `V06.12.csv`, national/annual/quantity; 1 dòng/năm | `year,production_t,area_ha,publication,geography_scope` | Sản lượng và diện tích VN |
| `nso_national_seasonal.csv` | Một năm × một mùa trong publication và national scope được chấp thuận | `year,season,production_t,publication,geography_scope` | Phân tích cơ cấu mùa vụ, không cộng với annual |
| `nso_reported_yield.csv` | Một năm của đúng publication V06.14, national/annual/quantity | `year,yield_kg_ha,publication` | Năng suất được công bố (riêng biệt) |
| `export_manifest.json` | Một batch | Batch ID, Gold/SQL definition hashes, tables/snapshots, scope, schema, row counts, SHA-256 từng CSV và thời gian export | Provenance / refresh integrity |

**Nếu một cột hoặc tên measure thực tế khác:** exporter phải map tường minh từ SQL output đã kiểm chứng sang contract CSV; không giả định Gold đã có các tên này. Mọi phép pivot phải kiểm tra uniqueness + present-NULL; thiếu dữ liệu ghi CSV trống cho value, **không zero-fill**. Tuyệt đối không chọn MAX/FIRST để che hai publication hoặc source-grain khác nhau.

**Các CSV mở rộng sau khi hai trang đầu hoàn chỉnh:**

- `fao_selected_production.csv`: `(year, country_code, product_code)` → `production_t`, `area_ha`, `computed_yield_kg_ha` chỉ khi paired observations hợp lệ; scope bốn M49 areas đã xét trong EDA, CPC 0113.
- `wb_rice_thai_5_monthly.csv`: `(period_date, exact_series_code)` → `price_usd_t`; preserve missing months, không interpolate.
- `vn_quote_series.csv`: `(quote_date, exact_price_series_key)` → `price_vnd_kg`; không gộp market/source khác nhau.
- `trade_vnm_export_selected.csv`: `(year, reporter_code, partner_code, product_code, direction)` → `quantity_t`, `value_usd`, `reported_unit_value_usd_t`, bounded 2019–2023, exact CPC 23161.02; **không dựng partner shares khi mẫu số chưa verified**.

### Nguyên tắc bảo vệ export

- Mỗi file phải đọc đúng Gold published snapshots qua `gold.consumer.read_window` hoặc cơ chế read gate hiện hành; kiểm trạng thái publication và batch trước/sau.
- Chỉ fixed allowlisted SQL và typed filters, không nhận SQL tự do từ tham số.
- Không vượt budget query hiện hành: 5.000 output rows, 5 MiB response, 15 s SQL, 64 MiB query memory, tuần tự. Nếu một export hợp lệ cần nhiều hơn: tách work units an toàn và rà soát budget, **không bỏ giới hạn**.
- Đảm bảo toàn bộ files có **cùng batch**; write vào thư mục tạm rồi atomic finalize; lỗi giữa chừng không để lại bộ CSV nửa vời.
- CSV có header, UTF-8 BOM (`utf-8-sig`) để Power BI đọc tiếng Việt; số xuất dạng nguyên/thập phân chưa format hàng nghìn, dấu chấm `.` cho thập phân; date ISO `YYYY-MM-DD`. Ghi checksum và row-count manifest.
- Không commit CSV/batch outputs (chỉ commit exporter/tests/docs). Không đưa credentials vào CSV hoặc PBIX.

**Sau khi Codex hiện thực exporter**, chạy đúng CLI mà Codex kiểm chứng trong README của exporter. Hướng dẫn này cố ý **không bịa một command line chưa được triển khai**.

## 4. Import dữ liệu vào Power BI Desktop

1. Vào **Home > Get data > Text/CSV**.
2. Chọn `fact_overview.csv`, xem preview, chọn **Transform Data**.
3. Trong Power Query, kiểm tra: `rows_total`, `numeric_rows`, `present_null_rows` là **Whole Number**; `numeric_coverage_pct` là **Decimal Number** (để thang **0–100**, chưa phải 0–1). Nếu cần dùng Power BI Percentage formatting, nên tính bằng DAX thay vì format trực tiếp cột 0–100.
4. Chọn **Close & Apply**. Đổi tên bảng hiển thị thành `FactOverview` (hoặc giữ tên khác và cập nhật công thức DAX).
5. Lặp lại với `nso_national_annual.csv` → bảng `NSO_Annual`; `nso_national_seasonal.csv` → `NSO_Seasonal`; `nso_reported_yield.csv` → `NSO_Yield`.
6. Kiểm kiểu `year` = Whole Number; `production_t`, `area_ha`, `yield_kg_ha` = Decimal Number/Fixed Decimal phù hợp precision thực; `season`, `publication` = Text.
7. Ở **Model view**, tạm **không tự nối các bảng chỉ vì chúng cùng có `year`**. NSO V06.12 và V06.14 là publication riêng, annual khác seasonal. Tạo date dimension/relationships sau khi quyết định ngữ nghĩa liên kết.
8. Lưu report qua **File > Save As > `TLCN_Rice_Analytics.pbix`**.

Nguồn thao tác chính thức: [Power Query Text/CSV](https://learn.microsoft.com/en-us/power-query/connectors/text-csv).

## 5. Trang 1 — Tổng quan dữ liệu (kéo thả)

Tạo một Report page mới và đặt tên **01 · Tổng quan**. Sử dụng duy nhất bảng `FactOverview` cho các KPI dưới đây.

### A. Tạo measures

Trong **Modeling > New measure**, tạo lần lượt (nếu Power BI locale yêu cầu, thay dấu phẩy phân tách đối số DAX bằng `;`):

```DAX
Total Observations = SUM ( FactOverview[rows_total] )

Numeric Observations = SUM ( FactOverview[numeric_rows] )

Present NULL = SUM ( FactOverview[present_null_rows] )

Numeric Coverage =
    DIVIDE ( [Numeric Observations], [Total Observations] )
```

Chọn measure `Numeric Coverage` → **Format = Percentage, 2 decimal places**.

### B. Kéo thả 4 KPI Cards

- Visual **Card**, Field = `[Total Observations]`.
- Card thứ 2 = `[Numeric Observations]`.
- Card thứ 3 = `[Present NULL]`.
- Card thứ 4 = `[Numeric Coverage]`.

**Đối chiếu published batch đã nghiệm thu trước đây (chỉ làm reference, không hardcode):** 1.414.527 tổng, 1.402.167 có số, 12.360 present-NULL, coverage 99,13%. Nếu import từ batch khác, số có thể thay đổi hợp lệ và cần đối chiếu manifest tương ứng.

### C. Biểu đồ và bảng

| Visual | Kéo vào X-axis / Category | Kéo vào Y-axis / Values | Lưu ý |
|---|---|---|---|
| Clustered bar chart | `FactOverview[fact_name]` | `[Total Observations]` | Trade vượt trội; có thể dùng log scale nếu Power BI/visual cho phép |
| 100% stacked bar chart | `FactOverview[fact_name]` | `numeric_rows`, `present_null_rows` (Sum) | Đặt tên series rõ ràng; tổng số dòng không phải sản lượng |
| Table | `fact_name`, `rows_total`, `numeric_rows`, `present_null_rows`, `represented_periods`, `period_min`, `period_max` | — | Min/max toàn fact **không chứng minh** mỗi series liên tục |

Tạo Text box nhỏ ghi: "Coverage = số quan sát có giá trị số / tổng số quan sát; missing khác zero; số dòng fact không phải sản lượng hay số giao dịch."

## 6. Trang 2 — Sản xuất lúa Việt Nam

Tạo trang **02 · Sản xuất lúa Việt Nam**.

### A. Tạo bộ lọc (Slicer)

- Slicer: `NSO_Annual[year]` với kiểu **Between** (nếu cần, dùng dạng List để chọn nhiều năm).
- Slicer publication có thể dùng nếu exporter tạo nhiều publications **và** giữ grain riêng đúng. Ở bản dễ nhất, file `NSO_Annual` chỉ chứa `V06.12.csv` national/annual/quantity, nên **ghi rõ scope cố định**, không thêm slicer ảo.

### B. Sản lượng và diện tích

- **Line chart**: X-axis `NSO_Annual[year]` → Y-axis `NSO_Annual[production_t]`; aggregation `Max` hoặc `Don't summarize` nếu có lựa chọn, **chỉ sau khi xác minh đúng một dòng/năm**. Không `SUM` qua nhiều năm để biến thành một KPI của "tổng sản lượng".
- **Line chart** thứ 2: X-axis `year`, Y-axis `area_ha`, đơn vị `ha`.
- **Table**: `year`, `production_t`, `area_ha`, có định dạng dấu ngăn cách hàng nghìn.
- **Clustered column chart** dùng `NSO_Seasonal[year]` / `season` / `production_t`: chỉ dùng ở phạm vi publication đã kiểm chứng; không kết hợp với annual series.
- **Line chart** riêng (hoặc trang phụ) cho `NSO_Yield[year]`, `yield_kg_ha` từ V06.14 — **không diễn giải như yield tính từ V06.12**.

### C. DAX measures cho đầu/cuối phạm vi đã lọc

Trong bảng `NSO_Annual`, khi dữ liệu thực bảo đảm đúng **một record cho mỗi year**:

```DAX
First Production (t) =
VAR Y0 =
    MINX (
        FILTER ( ALLSELECTED ( NSO_Annual ), NOT ISBLANK ( NSO_Annual[production_t] ) ),
        NSO_Annual[year]
    )
RETURN
    CALCULATE (
        MAX ( NSO_Annual[production_t] ),
        FILTER ( ALLSELECTED ( NSO_Annual ), NSO_Annual[year] = Y0 )
    )

Last Production (t) =
VAR Y1 =
    MAXX (
        FILTER ( ALLSELECTED ( NSO_Annual ), NOT ISBLANK ( NSO_Annual[production_t] ) ),
        NSO_Annual[year]
    )
RETURN
    CALCULATE (
        MAX ( NSO_Annual[production_t] ),
        FILTER ( ALLSELECTED ( NSO_Annual ), NSO_Annual[year] = Y1 )
    )

Production Change % =
VAR A = [First Production (t)]
VAR B = [Last Production (t)]
VAR DistinctYears =
    COUNTROWS ( FILTER ( ALLSELECTED ( NSO_Annual ), NOT ISBLANK ( NSO_Annual[production_t] ) ) )
RETURN
    IF ( DistinctYears < 2 || ISBLANK ( A ) || A <= 0, BLANK (), DIVIDE ( B - A, A ) )
```

Format `[Production Change %]` là Percentage. Đưa cả ba measures vào Cards. Với scope đầy đủ 1990–2024, đối chiếu kỳ vọng historical: **19.225.100 → 43.451.600 tấn, +126,01%**. Đây chỉ là giá trị reference để so, không ghi cứng vào report.

Nếu đổi sang một year duy nhất, change % phải trống (`BLANK`), không hiện 0% như thể không đổi. YoY bắt buộc tìm chính xác năm Y−1; không tính trên "hàng trước" khi data thiếu năm.

### D. Findings và chú thích

- Text box: "NSO V06.12.csv, national, annual, quantity; 1990–2024 nếu dùng scope mặc định."
- Text box: "Observed peak 45.091.000 tấn năm 2015 trong scope đã phân tích; đến 2024 giảm 3,64%. Đây là mô tả, không chứng minh quan hệ nhân quả."
- Khi cho người xem tùy chỉnh year filter, **không dùng Text box số tĩnh** như insight đang tính lại. Chọn KPI/measure động hoặc ghi rõ "EDA baseline 1990–2024" và không gán nó cho phạm vi lọc mới.

## 7. Trang mở rộng sau khi hai trang đầu ổn định

| Trang | Data export | Visual nên làm | Cảnh báo nghiệp vụ |
|---|---|---|---|
| FAO Production | `fao_selected_production.csv` | Line production/area/yield, selected-country bars | CPC + M49 exact; paired yield; không coi 4 areas là global ranking |
| Prices | `wb_rice_thai_5_monthly.csv`, `vn_quote_series.csv` | Monthly benchmark line, domestic quote line | Không trộn USD/tấn với VND/kg; giai đoạn thiếu phải hiển thị gaps |
| Trade | `trade_vnm_export_selected.csv` | Export quantity by partner, unit value trend | 2019–2023, CPC 23161.02, reporter VN; unit value ≠ market price; chưa chứng minh partner shares |
| Supply & Demand | Export từng scope SUA, PSD | Component bars, stock/flow panels | Calendar năm khác MY; stock không SUM qua các năm; signed residual giữ nguyên |

## 8. Làm mới dữ liệu và trình diễn offline

1. Khi cần update, chạy lại **exporter đã được kiểm tra**, bảo đảm published Gold batch đúng và không có writer.
2. Kiểm `export_manifest.json`: batch, row counts, SHA-256 và expected columns.
3. Không sửa đường dẫn file CSV tùy tiện; giữ nguyên path hoặc cập nhật trong **Transform data > Data source settings**.
4. Mở `.pbix` → **Home > Refresh**. So sánh KPI với export manifest/bounded SQL và kiểm Year/scope.
5. **Save** file `.pbix`. Trong chế độ Import, PBIX chứa dữ liệu đã nạp, có thể xem biểu đồ offline; Refresh cần lại CSV ở đúng đường dẫn. **Power BI offline không tự chạy live Gold publication gate khi bạn chỉ xem báo cáo**: gate được kiểm lúc Python export; hãy hiện batch ID và thời điểm export trong báo cáo để người xem biết dữ liệu thuộc snapshot nào.
6. Khi chia file PBIX, lưu ý **dữ liệu đã import có thể nằm bên trong tệp**, không đưa thông tin nhạy cảm vào export.

## 9. Checklist nghiệm thu báo cáo

- [ ] Power BI Desktop mở và lưu được `TLCN_Rice_Analytics.pbix`.
- [ ] `fact_overview.csv` chính xác 9 facts; `rows_total = numeric_rows + present_null_rows` từng fact.
- [ ] Overview Card đối chiếu đúng Gold published batch và CSV manifest.
- [ ] NSO annual đúng 1 record/năm, không SUM publication/national/region/season.
- [ ] Production & area hiển thị khác đơn vị và trên biểu đồ riêng.
- [ ] Reported yield V06.14 tách khỏi V06.12.
- [ ] Slicer năm làm đổi card và charts đúng; một năm đơn lẻ không sinh % change giả.
- [ ] NULL không đổi thành zero; series/calendar gaps không tự nối nếu thiếu.
- [ ] Cả hai trang có source, grain, unit, period và giới hạn diễn giải.
- [ ] Export dùng read gate, fixed SQL, same published batch, manifest SHA-256.
- [ ] Không có Gold/Silver/Bronze snapshot hay publication bị ghi sửa do exporter.

## 10. Giới hạn và nguồn tham khảo

- Power BI Desktop miễn phí cho dựng report local, nhưng chức năng chia sẻ/cộng tác trên Service thường cần giấy phép trả phí hoặc capacity phù hợp; không nhầm local `.pbix` với dashboard được share trên cloud.
- Repo hiện được báo đã có Gold+EDA, **không có CSV hoặc PBIX mới được tạo bởi hướng dẫn này**. Codex cần xuất CSV thật; Power BI Desktop/Windows phải thực hiện thao tác kéo thả để sinh `.pbix`.
- Microsoft Learn: [Download Power BI Desktop](https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-get-the-desktop), [Text/CSV connector](https://learn.microsoft.com/en-us/power-query/connectors/text-csv), [License types](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-features-license-type).
