# Power BI report đã triển khai

Ngày 06/11/2026. Report dùng **CSV Import**, không DirectQuery, không web Dashboard và không thêm Docker service. [Nghiệm thu](REPORT_VALIDATION.md), [insight matrix](REPORT_INSIGHTS.md), [mart design](../analytics/DATA_MART_DESIGN.md).

## Artifacts

- [TLCN_Rice_Analytics.pbip](../../powerbi/TLCN_Rice_Analytics.pbip): editable project; report ở `powerbi/R`, semantic model ở `powerbi/M`. Tên folder ngắn tránh MAX_PATH trên Windows.
- `powerbi/TLCN_Rice_Analytics.pbix`: file thật được Power BI Desktop Save As và mở lại. File binary và imported data Git-ignored; không được tạo bằng rename ZIP.
- `powerbi/theme.json`: theme teal `#136F63`, palette phân biệt series.
- [report_inventory.json](../../powerbi/report_inventory.json): pages, measures, scopes. Trạng thái nghiệm thu runtime nằm trong `report_acceptance.json`, không suy từ generator inventory.
- [validation.dax](../../powerbi/validation.dax): assertions với reference tính độc lập từ bundle CSV.

Không ghi dữ liệu vào 17 Gold tables. Đường đọc: published Gold → Analytics allowlist/admission/read_window → immutable mart bundle → Power Query M → VertiPaq Import → native visuals.

## Semantic model

14 tables: `NSOAnnual`, `NSOSeasonal`, `NSOYield`, `FAO`, `SUA`, `PSD`, `WB`, `USDAPrice`, `VNQuote`, `Trade`, `CalendarYear`, `BenchmarkMonth`, `QuoteDate` và bảng thuần `KeyMeasures` (ẩn cột `_placeholder`, chỉ chứa DAX measures). (`fact_overview.csv` vẫn được giữ trong immutable bundle để `verify_bundle()` đối soát tổng quan sát với `export_manifest.json`, nhưng không nạp vào giao diện Power BI sau khi bỏ Trang 1 Tổng quan.)

Tám relationships, many-to-one, single direction: `CalendarYear` tới sáu annual facts (`NSOAnnual`/`NSOSeasonal`/`NSOYield`/`FAO`/`SUA`/`Trade`), `BenchmarkMonth` tới `WB`, `QuoteDate` tới `VNQuote`. `PSD` giữ marketing year độc lập; `USDAPrice` giữ reference year độc lập. Country/partner/source keys là attributes trong scoped marts; không tạo fact-to-fact joins hoặc universal country crosswalk.

CalendarYear là spine, không chứng minh observation tồn tại mỗi năm. Bộ lọc trang giới hạn `p01` (NSO) 1990–2024, `p02` (FAO) 1961–2024, `p03` (SUA) 2010–2023, `p05` (Trade) 2019–2023. Date domains theo imported datasets. Auto date/time tắt; refresh parallelism 1. Không tự phát hiện thêm relationships khi vận hành report.

Power Query đọc đúng 10 CSV nghiệp vụ từ **một parameter `ExportFolder`** (và kiểm tra `export_manifest.json` của bundle 11 CSV). Header/order/type, format version/status và row counts phải khớp manifest. Empty numeric cells giữ NULL; presence flags riêng. Python verifier phải chạy trước refresh để kiểm SHA-256 và semantics; Power Query không tự kiểm SHA-256 tất cả files hoặc vào live Gold gate. Mô hình Import là bản trích xuất đã lưu, không tự cập nhật khi publication thay đổi.

169 measures (trong bảng thuần `KeyMeasures`): gồm các analytical/insight measures, display measures chia đơn vị triệu/tỷ cho biểu đồ, và bộ card presentation wrappers / 3-tier reference measures. Wrappers chỉ đổi cách hiển thị BLANK thành “Không áp dụng”, không thay numerical measures bằng zero. Code/DAX identifiers tiếng Anh; captions, titles và nhận xét tiếng Việt. Raw source labels/codes vẫn được giữ để đối chiếu.

| Quy tắc | Measure / công thức |
|---|---|
| Annual levels | Chỉ một calendar year; endpoint cards tính riêng trong ALLSELECTED |
| YoY | Giá trị Y và exact Y−1, denominator dương; missing Y−1 → BLANK |
| Weighted yield | 1000×SUM(paired_production_t)/SUM(paired_area_ha), một năm |
| Common-year rank | Năm chung có paired data cho toàn selected area universe; không world rank |
| SUA/PSD stocks | Một period/metric; không SUM stock qua nhiều năm |
| WB/quote prices | Một tháng/ngày cho level; chart đánh giá từng period, không SUM giá |
| Volatility | Sample std của 12 consecutive log returns /13 positive monthly prices; không annualize |
| Trade unit value | SUM(paired_value_usd)/SUM(paired_quantity_t), cùng eligible contributor population |

## Năm trang và thao tác

1. **Việt Nam (`p01` · 01 · SX Việt Nam):** V06.12 production/area, endpoint change, calendar YoY, peak trong cards/insight; seasonal riêng và reported yield V06.14 riêng.
2. **Quốc tế (`p02` · 02 · SX Quốc tế):** 5 source areas (Ấn Độ, Indonesia, Pakistan, Thái Lan, Việt Nam), production index (1961=100) / weighted yield trends, common-year ranking và scatter theo năm chọn hoặc năm chung.
3. **Cung cầu (`p03` · 03 · Cung & Cầu):** SUA calendar food/processed/opening stocks/signed stock variation & residuals; USDA MY production/consumption/ending stocks riêng. Hai slicers tác động độc lập.
4. **Giá (`p04` · 04 · Giá lúa gạo):** USDA FOB 4 nước/4 phẩm cấp, WB 4 chuỗi USD/t (mặc định đối sánh Thái 5% & VN 5% ở MoM/Volatility), và giá lúa nội địa ĐBSCL theo thị trường/giống. Hai date slicers không join lẫn nhau.
5. **Thương mại (`p05` · 05 · Thương mại):** VN exports sang 7 đối tác trọng điểm (2019–2023), quantity/value/unit-value trends, year/partner selection và 6 cards đối chiếu paired coverage.

Trong Desktop edit mode: **Ctrl+click** thanh navigation; click tabs bên dưới cũng được. Mở slicer dropdown, chọn một/nhiều values. Hover slicer và bấm **eraser** để clear, hoặc bỏ checkbox đã chọn; clear không xóa page scope constraints. Chọn data point để cross-filter trong trang; click lại hoặc click canvas để clear. Tooltip trên common-year bar có year/rank; price tooltip có extrema/MoM. Không có slicer country/source giả cho một mart chỉ chứa một source identity.

## Commands PowerShell

Từ root repo, dùng environment Python Analytics hiện có. Khi export, Docker `tlcn-local` phải healthy, Gold published, không pipeline/writer, Windows available≥2GiB và WSL≥3GiB. Nên đóng Desktop trước export để giảm RAM.

```powershell
$env:PYTHONIOENCODING = 'utf-8'
$result = python -m jobs.analytics.build_powerbi_marts | ConvertFrom-Json
$bundle = $result.bundle
python -m jobs.analytics.export_powerbi --verify "$bundle"
python scripts/validate_powerbi_marts.py --bundle "$bundle"
```

Lệnh reconciliation cuối là acceptance cho published batch EDA hiện tại; nếu batch khác, nó từ chối reuse historical findings để yêu cầu establish EDA reference mới.

Mở PBIX đã có:

```powershell
Invoke-Item .\powerbi\TLCN_Rice_Analytics.pbix
```

Để refresh sang bundle mới: Home → Transform data → Edit parameters → `ExportFolder` = đường dẫn **cùng một immutable bundle** cho mọi bảng → Apply → Refresh → kiểm cards/DAX → Save. Không đổi path từng CSV. Xem report đã Import không cần chạy Docker hoặc Refresh; Docker chỉ cần lúc tạo export mới.

Để dựng lại toàn bộ project từ code (đóng Desktop trước; thao tác này ghi lại generated report, không giữ sửa tay trong project):

```powershell
python -m jobs.analytics.build_powerbi_report --bundle "$bundle"
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate_powerbi_model.ps1
python scripts/validate_powerbi_project.py --fetch-schemas
Invoke-Item .\powerbi\TLCN_Rice_Analytics.pbip
```

Desktop → Refresh/Apply changes → F12 → **Power BI file (*.pbix)** → Save As. Đóng/mở lại để xác nhận dữ liệu, theme và filters. Đây là đường chuyển đổi được Microsoft hỗ trợ; không có CLI converter giả. [PBIP documentation](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview).

## Phạm vi local

Power BI Desktop 2.159.1231.0 đã dùng để author/verify, không cần Pro/cloud Publish. CSV/PBIX chứa bản extract; giữ local theo quy định nhóm. Không có credentials trong M/DAX/CSV. File `.pbi/cache.abf`, user settings và PBIX không vào Git. Khi bàn giao máy khác cần export hoặc chép bundle được xác minh và cập nhật parameter; không thuộc nghiệm thu portability của teammate.
