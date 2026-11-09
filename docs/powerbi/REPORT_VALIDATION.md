# Power BI physical acceptance — 06/11/2026

Baseline: `feat(gold)@cd2f1a77b27d5bcba6f1d925e3d9305952551bae`. Báo cáo này tiếp nối, không ghi đè [acceptance.json](acceptance.json) ngày27/10. Kết quả cuối và SHA-256 artifacts: [report_acceptance.json](report_acceptance.json). [Cách chạy](REPORT_IMPLEMENTATION.md).

## Phân biệt các loại bằng chứng

| Gate | Kiểm tra thực tế |
|---|---|
| Marts | Ten-CSV export thật qua existing admission + published snapshot read gate; exact typed checks/roundtrip, immutable promotion fence PASS |
| EDA | 22 finding IDs đối chiếu cùng batch; OV03 explicitly reuses identical-batch inventory |
| Source PBIR | 75 source JSON artifacts validated against official Microsoft schemas;63 visuals,six pages; field refs/canvas/IDs PASS |
| TMDL | Microsoft installed TOM TmdlSerializer parses13tables/8relationships/65measures |
| Desktop DAX | 58 assertions chạy thật; failures0; tất cả65 measures Ready; Import mode |
| Desktop UI | Six pages render dữ liệu thật; native navigation/year/partner slicers/eraser, cross-filter và source-specific empty states đã quan sát |
| PBIX | Native Desktop Save As *.pbix, đóng/mở lại, cached Import data và DAX assertions PASS; không rename ZIP |
| Preservation | 36 table before/after proofs bằng nhau; six historical DagRuns, publication và protected files không đổi |
| Regression | **589 passed**, 35,99s; zero failed/skipped trong full suite cuối |

Desktop dùng bản Store **2.159.1231.0**. Không có authoring MCP session kết nối để gọi; đã dùng Windows UI Automation cho report được tạo trong task và Microsoft TOM/ADOMD assemblies đi kèm Desktop. Không dùng private PBIX writer, không tạo screenshots giả.

## Visual và interaction evidence

Screenshots được chụp trực tiếp vùng canvas của owned Desktop, bỏ account/title chrome ngay lúc capture:
[Overview](report_evidence/page1.png), [NSO](report_evidence/page2.png), [FAO](report_evidence/page3.png), [SUA/PSD](report_evidence/page4.png), [Prices](report_evidence/page5.png), [Trade](report_evidence/page6.png), [single-year scatter](report_evidence/fao_single_year.png), [PBIX reopened](report_evidence/reopened.png).

- NSO default1990–2024:19.225.100→43.451.600t,+126,01%; latest YoY≈−0,11%; peak2015. Chọn1990 làm endpoints còn cùng năm, change/YoY không áp dụng; V06.14 yield không có điểm thay vì zero. Presentation wrappers diễn đạt “Không áp dụng”; numerical measures vẫn BLANK.
- Clear year khôi phục default scope. Navigation Ctrl+click hoạt động; scopes không vượt các page year limits.
- Philippines-only: five groups và unit value≈489,38; clear partner khôi phục15groups/≈491,14. DAX cũng kiểm ratio-of-sums nhiều năm và partners độc lập từ CSV.
- FAO chọn1961 vẽ bốn scatter points, common-year bar thay đổi; All years cố ý không vẽ single-year scatter.
- SUA signed values không bị ABS/clamp; USDA MY được lọc riêng, không theo CalendarYear. Price units/axes tách biệt, WB absent count0 không phải missing price được điền0.
- Tooltips giữ nguồn/metric phù hợp. Long raw fact names và chín inventory rows đọc bằng tooltip/scroll; không giấu hoặc loại facts để vừa canvas.

## DAX correctness

Assertions độc lập gồm counts/coverage, selected endpoints/peak, exact-calendar YoY, missing2025/Trade2024, single/multiple-period rules, paired weighted yield, common-year rank, signed SUA, PSD stocks, WB price/MoM/volatility, quote counts và Trade ratio-of-sums. Sau refresh/reopen,58/58 PASS. JSON lưu exact runtime/rows; số reference có tolerance cho VertiPaq double. Decimal text trong CSV không bị format theo số display của EDA.

Unit fixtures kiểm NULL/zero/missing-year/denominator/mixed-units; actual imported data không được thay bằng fixtures để nghiệm thu. Không claim đã inject mọi synthetic edge case vào Desktop thật.

## Performance và tài nguyên

Export:89,99s/28operations/26Trino queries,11,26s summed SQL transport,475.357CSVbytes/2.348rows. Max query-reported peak6,09MiB,spill0; không so ngang con số này với JVM RSS. Xem [DATA_MART_VALIDATION.md](../analytics/DATA_MART_VALIDATION.md).

Import đầu đã sẵn dữ liệu và chạy assertion thành công **trong ≤36s** từ lúc bấm Refresh (bao gồm khoảng chờ quan sát; không phải isolated engine timing). Một controlled full **local CSV refresh** qua TOM: **5.740ms**, maxParallelism1; Windows available lúc admission2.538.897.408bytes. Không query Trino/Spark trong local refresh.

DAX suite quan sát250–427ms cho58 assertions trên model Import; lần reopened cuối287ms. Page/slicer renders hoàn tất trong các vòng UI Automation capture vài giây, không có render error được quan sát; đây không phải browser profiler/p95 benchmark. Working sets được sample riêng cho Desktop/engine: khoảng678MiB/606MiB; Windows counters peak kể từ đầu sampled reopen session khoảng681MiB/658MiB, không đại diện mọi lần author/refresh trước đó. Sau Desktop mở, Windows có lúc dưới2GiB; không phát thêm Gold queries trong cửa sổ đó. Không tăng heap, không restart services.

## Các lỗi authoring đã sửa

M headers dùng M list `{}`, không JSON array. Calendar tables dùng small typed M queries; tắt auto-date, bỏ relationships tự phát. DAX variable `current` đổi sang `currentValue` để tránh reserved name. Card numbers dùng full values, blank presentation tiếng Việt. USDA slicer dùng marketing year có hiệu lực lên charts. Theme import/save/reopen được kiểm thật; resource names/paths được giữ đồng nhất. Trade có quantity/value/unit-value trends theo năm. Đây là lỗi trên project mới, không phải thay đổi dữ liệu Gold.

Source generator dùng official public PBIR schema versions đã validate. Desktop2.159 có thể ghi metadata schema mới (ví dụ visualContainer2.14) mà public endpoint hiện trả404; không claim external JSON-schema validation cho version chưa tải được. Với source canonical do generator tạo, official schema validation PASS; native PBIX/Desktop parse/render là bằng chứng riêng. Regenerate chỉ khi muốn ghi lại generated project; không ghi đè sửa tay trong Desktop đang mở.

## Reproduce

```powershell
python -m jobs.analytics.export_powerbi --verify 'exports/powerbi/ec421297c194_20261106T031815Z_01d03710'
python scripts/validate_powerbi_marts.py --bundle 'exports/powerbi/ec421297c194_20261106T031815Z_01d03710'
python scripts/validate_powerbi_project.py --fetch-schemas
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate_powerbi_model.ps1
python -m pytest tests/unit -q -o addopts= --basetemp=.test-tmp/powerbi-unit-check
Invoke-Item .\powerbi\TLCN_Rice_Analytics.pbix
```

Test environment:Python3.13; approved Spark3.5.5/Py4J archives dùng cho imports unit, không khởi JVM/Spark. Nếu chưa cài dev dependencies, dùng workflow trong foundation acceptance; không bỏ các test lỗi import. Managed sandbox có ACL hạn chế temp folders; full suite cuối chạy trong host context hợp lệ với unique basetemp, không sửa global ACL.

`scripts/query_powerbi_model.ps1 -Port <owned-report-port> -QueryFile powerbi/validation.dax` chạy read-only localhost ADOMD; port đổi mỗi Desktop session, không hardcode port trong report. `scripts/powerbi_desktop.ps1` chỉ thao tác PID/foreground của report được chỉ định; không phải launcher service hay runtime dependency.

## Giới hạn vận hành

Report là extract, không automatic refresh, không Pro/cloud Publish. Đã mở lại PBIX và query cached Import model không Refresh; không ngắt mạng hệ thống/reset Docker để thử offline. Model M chỉ File.Contents local, không Trino/HTTP/credentials. Không thực hiện second-laptop portability acceptance. Stock/yield/price/global-share limitations vẫn giữ nguyên. Không rerun Gold DAG hoặc notebooks chỉ để tăng số tests.
