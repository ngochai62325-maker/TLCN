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

13 tables: `Overview`, `NSOAnnual`, `NSOSeasonal`, `NSOYield`, `FAO`, `SUA`, `PSD`, `WB`, `VNQuote`, `Trade`, `CalendarYear`, `BenchmarkMonth`, `QuoteDate`.

Tám relationships, many-to-one, single direction: CalendarYear tới sáu annual facts (NSOAnnual/NSOSeasonal/NSOYield/FAO/SUA/Trade), BenchmarkMonth tới WB, QuoteDate tới VNQuote. PSD giữ marketing year độc lập. Country/partner/source keys là attributes trong scoped marts; không tạo fact-to-fact joins hoặc universal country crosswalk. Overview không bị year slicer làm mất full-data totals.

CalendarYear là spine, không chứng minh observation tồn tại mỗi năm. Page NSO giới hạn 1990–2024, SUA calendar2010–2023, Trade2019–2023. Date domains theo imported datasets. Auto date/time tắt; refresh parallelism1. Không tự phát hiện thêm relationships khi vận hành report.

Power Query đọc đúng mười CSV từ **một parameter `ExportFolder`**. Header/order/type, format version/status và row counts phải khớp manifest. Empty numeric cells giữ NULL; presence flags riêng. Python verifier phải chạy trước refresh để kiểm SHA-256 và semantics; Power Query không tự kiểm SHA-256 tất cả files hoặc vào live Gold gate. Mô hình Import là bản trích xuất đã lưu, không tự cập nhật khi publication thay đổi.

65 measures: 53 analytical/insight measures và 12 card presentation wrappers. Wrappers chỉ đổi cách hiển thị BLANK thành “Không áp dụng”, không thay numerical measures bằng zero. Code/DAX identifiers tiếng Anh; captions, titles và nhận xét tiếng Việt. Raw source labels/codes vẫn được giữ để đối chiếu.

| Quy tắc | Measure / công thức |
|---|---|
| Numeric coverage | SUM(numeric_rows)/SUM(rows_total); không AVG percentages |
| Annual levels | Chỉ một calendar year; endpoint cards tính riêng trong ALLSELECTED |
| YoY | Giá trị Y và exact Y−1, denominator dương; missing Y−1 → BLANK |
| Weighted yield | 1000×SUM(paired_production_t)/SUM(paired_area_ha), một năm |
| Common-year rank | Năm chung có paired data cho toàn selected area universe; không world rank |
| SUA/PSD stocks | Một period/metric; không SUM stock qua nhiều năm |
| WB/quote prices | Một tháng/ngày cho level; chart đánh giá từng period, không SUM giá |
| Volatility | Sample std của 12 consecutive log returns /13 positive monthly prices; không annualize |
| Trade unit value | SUM(paired_value_usd)/SUM(paired_quantity_t), cùng eligible contributor population |

## Sáu trang và thao tác

1. **Tổng quan:** counts/coverage cards, per-fact bars, inventory và min/max/represented-period diagnostics. Dùng scrollbar/tooltip để đọc đủ tên và chín facts.
2. **Việt Nam:** V06.12 production/area, endpoint change, calendar YoY, peak trong insight; seasonal riêng và reported yield V06.14 riêng.
3. **Quốc tế:** bốn source areas, production/yield trends, common-year ranking. Chọn **một năm** để scatter area/production có nghĩa; All years cố ý không vẽ scatter sai grain.
4. **Cung cầu:** SUA calendar food/opening/signed stock variation; USDA MY production/consumption/ending stock riêng. Hai slicers tác động độc lập.
5. **Giá:** WB rice_thai_5 USD/t và An Giang IR50404 VND/kg riêng; WB extrema trong tooltip, MoM/volatility và coverage. Hai date slicers không join lẫn nhau.
6. **Trade:** VN exports sang đúng ba partners, quantity/value/unit-value trends, year/partner selection, paired coverage và descriptive note.

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
