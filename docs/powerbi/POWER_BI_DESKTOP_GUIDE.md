# Hướng dẫn Power BI Desktop Free — TLCN

Ngày 27/10/2026. Đây là hướng dẫn để **bạn tự kéo thả và lưu PBIX**, không phải `.pbix` đã được tạo/kiểm thử. CSV đến từ exporter thật trong repository, không phải bộ số liệu mock.

Bản `TLCN_POWER_BI_DESKTOP_GUIDE.md` ở root là proposal của owner và được giữ nguyên. Bản hiện hành này đối chiếu proposal với exporter: giữ bốn filenames/required fields, bổ sung presence flags và `season_label_vi`, dùng immutable bundle + parameter `ExportFolder` thay vì ghi đè bốn CSV riêng lẻ. [Schemas và mapping đầy đủ](README.md).

## 1. Cài và mở Desktop

Tải **Power BI Desktop 64-bit** từ [Microsoft Store/Download chính thức](https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-get-the-desktop), mở **Blank report**. Desktop là ứng dụng Windows miễn phí để tạo models/reports; báo cáo local và lưu `.pbix` không cần Power BI Pro hoặc cloud publication. Giới hạn chia sẻ trên Power BI Service là vấn đề license riêng. [Microsoft: Desktop getting started](https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-getting-started).

Không cài thêm Docker service hoặc database. Không dùng Power BI Report Server thay bản Desktop thông thường. Menu/field wells có thể đổi nhẹ theo bản cập nhật tháng; các bảng/cột dưới đây phải giữ đúng tên.

## 2. Xuất và kiểm tra CSV

Trong terminal PowerShell của repo:

```powershell
python -m jobs.analytics.export_powerbi
```

Chỉ tiếp tục khi kết quả là `POWERBI_EXPORT_COMPLETE`. Copy đường dẫn `bundle` từ JSON, rồi kiểm tra:

```powershell
python -m jobs.analytics.export_powerbi --verify 'C:\duong-dan-thuc\exports\powerbi\<bundle>'
```

Kết quả phải là `VERIFIED_OFFLINE`. Trong bundle có đúng bốn CSV và `export_manifest.json`. Mở JSON bằng VS Code: xem `published_batch`, `gold_model`, `export_timestamp`, `outputs`, `typed_scope` và `reconciliation`. Checksum/row counts/schema được verify bằng Python, không chỉ đếm file bằng mắt. Ghi lại batch và export timestamp cho report; đó **không phải** ngày NSO công bố hay observation mới nhất.

Export cũ không tự biến thành live khi Gold có batch mới. Không Import staging `.powerbi-*`, không trộn file từ hai folders và không dùng consumer 20 sample rows để tính totals.

## 3. Tắt tự nối quan hệ, rồi Import

1. **File → Options and settings → Options → Current File → Data Load → Relationships**: bỏ **Autodetect new relationships after data is loaded**; kiểm tra và bỏ các quan hệ tự tạo. Không nối fact với fact chỉ vì đều có `year`/`publication`. [Microsoft relationships](https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-create-and-manage-relationships).
2. **Home → Get Data → Text/CSV** → chọn `fact_overview.csv` trong bundle đã verify.
3. Preview: File Origin **65001: Unicode (UTF-8)**, Delimiter **Comma**. Chọn **Transform Data**. BOM giúp Power Query nhận UTF-8; kiểm `Đông xuân` sau khi nạp seasonal. [Text/CSV connector](https://learn.microsoft.com/en-us/power-query/connectors/text-csv).
4. Đổi query name thành **FactOverview**. Lặp lại ba file còn lại theo bảng:

| File | Query/table name dùng trong DAX |
|---|---|
| fact_overview.csv | FactOverview |
| nso_national_annual.csv | NSO_Annual |
| nso_national_seasonal.csv | NSO_Seasonal |
| nso_reported_yield.csv | NSO_Yield |

Không dùng Folder → Combine toàn bộ CSV vì schemas/grain khác nhau.

### Types trong Power Query

Xóa automatic `Changed Type` nếu đoán sai. Chọn column → **Data Type / Change Type → Using Locale**, số thập phân dùng **Decimal Number, English (United States)** vì CSV có dấu chấm.

| Fields | Type |
|---|---|
| `rows_total, numeric_rows, present_null_rows, represented_periods`, mọi `year` | Whole Number |
| `numeric_coverage_pct, production_t, area_ha, yield_kg_ha` | Decimal Number, giữ null |
| `production_present, area_present, yield_present` | True/False |
| `fact_name, period_min, period_max, period_grain, publication, geography_scope, season, season_label_vi` | Text |

`period_min/max` cố ý Text: chứa ISO year/month/day hoặc marketing/reference years, không phải một cột calendar date đồng nhất. `numeric_coverage_pct` là số 0–100; không format nó trực tiếp thành Percentage 0–1. Decimal Number của Power BI có giới hạn floating-point precision; không chọn Fixed Decimal bốn chữ số lẻ để làm tròn nguồn tám chữ số mà không cân nhắc. CSV/manifest là bản audit chính xác; các NSO reference values hiện tại là số nguyên trong các units đã nêu.

Không Replace null bằng 0. Ví dụ annual: `production_present=true` + `production_t=null` là source cell present-NULL; `area_present=false` + `area_ha=null` là absent cell. Zero numeric là quan sát thật. Seasonal/yield vắng một year/season thì không có row, không tự thêm giá trị 0.

### Một parameter cho toàn bộ bundle

1. Power Query **Home → Manage Parameters → New Parameter**: Name **ExportFolder**, Type **Text**, Suggested Values **Any value**, Current Value = đường dẫn bundle tuyệt đối, **không có dấu `\` cuối**.
2. Trong mỗi query, bấm step **Source**, bật Formula bar. Giữ nguyên `Csv.Document(...)`, chỉ đổi `File.Contents("đường dẫn cũ")` sang:

```powerquery
File.Contents(ExportFolder & "\fact_overview.csv")
```

Ba queries kia dùng chính xác `\nso_national_annual.csv`, `\nso_national_seasonal.csv`, `\nso_reported_yield.csv`. Đây là đường dẫn Windows trong ngôn ngữ M, không phải Python escape sequences. Nếu preview yêu cầu, đặt `Encoding=65001`, delimiter comma; số columns tương ứng **9 / 7 / 7 / 4**. **Close & Apply**.

## 4. Trang 01 — Tổng quan

Đổi tên page thành **01 · Tổng quan dữ liệu**. Trong **Modeling → New measure**, tạo **từng measure riêng** trong bảng FactOverview:

```DAX
Total Observations = SUM ( FactOverview[rows_total] )
```
```DAX
Numeric Observations = SUM ( FactOverview[numeric_rows] )
```
```DAX
Present NULL Observations = SUM ( FactOverview[present_null_rows] )
```
```DAX
Numeric Coverage = DIVIDE ( [Numeric Observations], [Total Observations] )
```

Format measure cuối **Percentage, 2 decimal places**. Không `AVERAGE(numeric_coverage_pct)`: facts có số dòng khác nhau nên coverage chung phải là ratio của tổng counts. SUM **row counts** hợp lệ; đây không phải SUM sản lượng/giá/stock.

Kéo thả:

| Visual | Fields |
|---|---|
| 4 Cards | Lần lượt bốn measures trên vào Value/Callout value |
| Clustered bar | Y-axis `FactOverview[fact_name]`, X-axis `[Total Observations]`; sort descending |
| 100% stacked bar | Y-axis `fact_name`, Values `[Numeric Observations]`, `[Present NULL Observations]` |
| Table | `fact_name, rows_total, numeric_rows, present_null_rows, period_min, period_max, represented_periods, period_grain` |

Card → Format visual → Display units **None**, decimal counts **0**. Coverage **2 decimal places**. Title tiếng Việt, nền trắng, chữ đậm tối; dùng xanh `#136F63` và cam `#D18B26` cho có số/NULL. Căn lề bằng Format → Align, không thêm visual nặng.

Text box: **“Coverage tính trên các dòng đã có, khác missing periods; NULL khác zero. Min–max toàn fact không chứng minh mỗi series liên tục. Snapshot/batch theo export manifest.”** Không nối FactOverview với DimYear ở bước sau: totals này là toàn bộ published fact coverage, không phân rã theo năm.

## 5. Model thời gian cho trang Việt Nam

Tạo page **02 · Sản xuất lúa Việt Nam**. **Modeling → New table**:

```DAX
DimYear =
VAR YearsInExports =
    UNION (
        SELECTCOLUMNS ( NSO_Annual, "Y", NSO_Annual[year] ),
        SELECTCOLUMNS ( NSO_Seasonal, "Y", NSO_Seasonal[year] ),
        SELECTCOLUMNS ( NSO_Yield, "Y", NSO_Yield[year] )
    )
RETURN
    SELECTCOLUMNS (
        GENERATESERIES ( MINX ( YearsInExports, [Y] ), MAXX ( YearsInExports, [Y] ), 1 ),
        "year", [Value]
    )
```

Trong **Model view**, tạo thủ công ba relationships:

```text
DimYear[year] 1 → * NSO_Annual[year]
DimYear[year] 1 → * NSO_Seasonal[year]
DimYear[year] 1 → * NSO_Yield[year]
```

Chọn **Single** cross-filter direction từ DimYear xuống facts; không Both/many-to-many/fact-to-fact. `DimYear[year]` là Whole Number, unique. Đây chỉ là shared calendar-year filtering, không khẳng định geography hoặc measurements hai publication đồng nhất. Nếu Power BI tự đề xuất 1:1 vì annual hiện unique/year, chọn đúng 1:* và Single theo model này.

Year slicer: chọn visual **Slicer**, kéo `DimYear[year]`, style **Between**. Cột year đặt **Don't summarize**. Source scope cố định: annual/seasonal **V06.12.csv / national / quantity**; reported yield **V06.14.csv** riêng. Không tạo slicer địa lý/phân loại giả khi CSV chỉ có một scope.

## 6. DAX sản lượng: giá trị theo năm và endpoints

Các measures dưới đây đặt trong NSO_Annual. Mỗi code block là một **New measure**; locale dùng `;` thì đổi dấu phân cách đối số phù hợp, không đổi column names.

```DAX
Annual Production (t) =
IF ( HASONEVALUE ( DimYear[year] ), MAX ( NSO_Annual[production_t] ), BLANK () )
```
```DAX
Annual Area (ha) =
IF ( HASONEVALUE ( DimYear[year] ), MAX ( NSO_Annual[area_ha] ), BLANK () )
```

MAX chỉ đọc một cell/year vì exporter đã reject duplicate year; không phải cách gộp nhiều geography/publications. Khi nhiều năm, hai measures theo-năm trả BLANK thay vì bịa một annual KPI bằng tổng các năm.

```DAX
First Production Year =
MINX (
    FILTER (
        ALLSELECTED ( DimYear[year] ),
        NOT ISBLANK ( CALCULATE ( MAX ( NSO_Annual[production_t] ) ) )
    ),
    DimYear[year]
)
```
```DAX
Latest Production Year =
MAXX (
    FILTER (
        ALLSELECTED ( DimYear[year] ),
        NOT ISBLANK ( CALCULATE ( MAX ( NSO_Annual[production_t] ) ) )
    ),
    DimYear[year]
)
```
```DAX
First Production (t) =
VAR Y = [First Production Year]
RETURN
    IF ( ISBLANK ( Y ), BLANK (),
        CALCULATE ( MAX ( NSO_Annual[production_t] ), REMOVEFILTERS ( DimYear ), DimYear[year] = Y ) )
```
```DAX
Latest Production (t) =
VAR Y = [Latest Production Year]
RETURN
    IF ( ISBLANK ( Y ), BLANK (),
        CALCULATE ( MAX ( NSO_Annual[production_t] ), REMOVEFILTERS ( DimYear ), DimYear[year] = Y ) )
```
```DAX
Production Absolute Change (t) =
VAR A = [First Production Year]
VAR B = [Latest Production Year]
RETURN
    IF ( ISBLANK ( A ) || ISBLANK ( B ) || A = B, BLANK (), [Latest Production (t)] - [First Production (t)] )
```
```DAX
Production Change (%) =
VAR BaseValue = [First Production (t)]
VAR ChangeValue = [Production Absolute Change (t)]
RETURN
    IF ( ISBLANK ( ChangeValue ) || ISBLANK ( BaseValue ) || BaseValue <= 0,
        BLANK (), DIVIDE ( ChangeValue, BaseValue ) )
```

Format Change (%) Percentage. Hai numeric years cùng giá trị → change 0 hợp lệ; một numeric year → temporal change BLANK. NULL endpoints bỏ qua khi chọn năm có số, **zero không bị bỏ qua**, mẫu số không dương không sinh tỷ lệ giả. Muốn area Cards, tạo measures tương tự thay `production_t` bằng `area_ha` và đổi tên/unit; không ghép publication khác.

## 7. Calendar-aware YoY

```DAX
Latest Production YoY (%) =
VAR Y = [Latest Production Year]
VAR CurrentValue = [Latest Production (t)]
VAR PriorYearInSelection =
    NOT ISBLANK ( Y ) &&
    CONTAINS ( ALLSELECTED ( DimYear[year] ), DimYear[year], Y - 1 )
VAR PreviousValue =
    IF ( PriorYearInSelection,
        CALCULATE ( MAX ( NSO_Annual[production_t] ), REMOVEFILTERS ( DimYear ), DimYear[year] = Y - 1 ) )
RETURN
    IF ( ISBLANK ( CurrentValue ) || ISBLANK ( PreviousValue ) || PreviousValue <= 0,
        BLANK (), DIVIDE ( CurrentValue - PreviousValue, PreviousValue ) )
```

Policy này chỉ tính khi **đúng Y−1 nằm trong lựa chọn và có số**. Chọn 2015 và 2024, không có 2023 trong selection → BLANK, không lấy 2015 làm “previous year”. Chọn một năm duy nhất → BLANK. Chọn range 1990–2024 → dùng 2023 cho YoY 2024. Không `COALESCE(...,0)` cho missing. [CALCULATE](https://learn.microsoft.com/en-us/dax/calculate-function-dax), [ALLSELECTED](https://learn.microsoft.com/en-us/dax/allselected-function-dax), [REMOVEFILTERS](https://learn.microsoft.com/en-us/dax/removefilters-function-dax) là cơ sở filter-context; DAX này vẫn cần bạn chạy/kiểm tra trong Desktop, chưa có PBIX acceptance tự động.

## 8. Kéo thả visuals trang Việt Nam

| Visual | Cách cấu hình |
|---|---|
| Cards | `[First Production (t)]`, `[Latest Production (t)]`, `[Production Absolute Change (t)]`, `[Production Change (%)]`, `[Latest Production YoY (%)]` |
| Line production | X-axis `DimYear[year]`, Y-axis `[Annual Production (t)]`; title “Sản lượng lúa — tấn” |
| Line area | X-axis `DimYear[year]`, Y-axis `[Annual Area (ha)]`; title “Diện tích — ha” |
| Table diagnostics | Các fields `year, production_t, area_ha, production_present, area_present, publication, geography_scope` trong NSO_Annual |

Không kéo trực tiếp Sum(production_t) vào một Card trên toàn bộ nhiều năm như thể đó là “sản lượng năm”. Hai physical units phải trên charts riêng. DimYear liên tục giúp thấy kỳ vắng; giữ BLANK, bật Show items with no data khi cần và kiểm tra cách visual hiển thị gaps, không nội suy hoặc zero-fill.

Tạo measure seasonal riêng trong NSO_Seasonal:

```DAX
Season Production (t) =
IF (
    HASONEVALUE ( DimYear[year] ) && HASONEVALUE ( NSO_Seasonal[season] ),
    MAX ( NSO_Seasonal[production_t] ), BLANK ()
)
```

**Clustered column chart**: X-axis `DimYear[year]`, Legend `NSO_Seasonal[season_label_vi]`, Y-axis `[Season Production (t)]`. Bấm một năm/range để bớt dày. Không thêm annual vào seasonal stack hoặc khẳng định tổng các vụ luôn bằng annual publication total.

Reported yield panel giữ riêng:

```DAX
Reported Yield (kg/ha) =
IF ( HASONEVALUE ( DimYear[year] ), MAX ( NSO_Yield[yield_kg_ha] ), BLANK () )
```

Line chart X `DimYear[year]`, Y measure trên; title **“Năng suất báo cáo V06.14 — kg/ha”**. Không diễn giải là production_t/area_ha từ V06.12, không average yield theo geography hay SUM qua năm.

Text boxes nguồn: “V06.12.csv, source-scoped national, annual/seasonal quantity tách riêng; year theo slicer; không official geography conversion”. Yield có chú thích riêng V06.14. Thêm short batch ID và export timestamp từ manifest, không ghi đó là publication date. Findings là mô tả; không kết luận diện tích/năng suất gây ra biến động sản lượng từ tương quan.

Gold `PUBLISHED` là trạng thái kỹ thuật của batch đã kiểm chứng. NSO vẫn có source release statuses `unconfirmed/provisional` (xem counts trong manifest); không diễn giải mọi measurement là số liệu công bố chính thức cuối cùng. Release-status counts của V06.12 trong manifest bao gồm quantity area seasonal được đọc rồi loại có chủ đích khỏi bộ CSV production-seasonal.

## 9. Đối chiếu với EDA và nghiệm thu thủ công

Các số sau là **reference của batch đã nghiệm thu trong EDA**, không constants trong DAX và không insight động cho một filter khác:

- Overview: tổng 1.414.527, có số 1.402.167, present-NULL 12.360, coverage 99,13%; USDA reference NULL 4.523/12.936, không phải quality score.
- V06.12 annual 1990–2024: production 19.225.100 → 43.451.600 tấn (+126,01%); area 6.042.800 → 7.127.100 ha (+17,94%).
- Observed production peak trong scope EDA: 45.091.000 tấn năm 2015; tới 2024 −3,64%, area cùng interval −8,95%; không causal claim.
- V06.14 yield riêng 1995–2024: 3.690 → 6.100 kg/ha.

Đối chiếu actual CSV/manifest trước; nếu published batch/scope thay đổi, reference có thể khác hợp lệ. Không sửa CSV để ép giống số cũ. Test slicer một năm: change/YoY theo policy trên BLANK; nhiều năm đủ Y−1: YoY mới có nghĩa. Test annual/seasonal units và flags; không zero-fill. Các tests Python bảo vệ CSV không đồng nghĩa measures/visuals đã được nghiệm thu trong PBIX.

## 10. Lưu và Refresh

1. **File → Save As → `TLCN_Rice_Analytics.pbix`**, chọn thư mục local của bạn. `.pbix` Git-ignored vì chứa dữ liệu import và có thể lớn.
2. Lần cập nhật: chạy lại exporter khi đủ RAM và Gold đã published → nhận **bundle mới** → `--verify` bundle đó.
3. Desktop **Transform data → Edit Parameters → ExportFolder** = bundle mới cho cả bốn queries. **Close & Apply**, rồi **Home → Refresh** nếu cần. Kiểm KPI/batch/scope, cập nhật source text boxes và Save.
4. Không Refresh giữa lúc chọn các đường dẫn không đồng bộ. Các bundle bất biến giúp không đọc nửa file hoặc trộn nhiều batches. Không chọn một bản cũ chỉ vì exporter mới vừa thất bại; timestamp cũ vẫn là cũ.
5. CSV Import/PBIX không tự chạy Gold gate khi xem offline; gate kiểm trong Python export. PBIX có dữ liệu trích xuất, không tự đồng bộ với Gold. Không cần Pro, cloud publication, DirectQuery/ODBC hoặc gateway cho local authoring. [Refresh Import models](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-data).

## 11. Checklist hai trang đầu

- [ ] Bốn bảng dùng một ExportFolder, verify manifest PASS, headers/types/NULL flags đúng.
- [ ] Chín fact counts reconcile; Numeric Coverage lấy ratio counts.
- [ ] Chỉ DimYear → NSO relationships Single; không fact-to-fact hoặc auto geography joins.
- [ ] First/latest/absolute/% change và Y−1 policy đúng cả khi filter năm thay đổi.
- [ ] Annual và seasonal không cộng chung; V06.14 reported yield riêng.
- [ ] Nguồn, grain, units, selected period và export batch/timestamp rõ ràng.
- [ ] Mở/lưu PBIX và Refresh thực tế PASS trong Desktop của owner.

Expansion sau đó xem [roadmap sáu subject areas](README.md#roadmap-sau-hai-trang-đầu); chưa xuất/import toàn bộ Trade, không thêm services hoặc ML.
