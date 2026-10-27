# TLCN · Power BI Desktop Free

**Kiến trúc hiện hành:** published Gold → Analytics allowlist/read gate → Python CSV export → Power BI Desktop Import. Streamlit đã ngừng; không tạo web Dashboard, Docker service hoặc database mới. Gold và sáu notebooks EDA giữ nguyên. Report `.pbix` do owner tự tạo trong Desktop, chưa được tác giả hướng dẫn này dựng hoặc nghiệm thu.

Hướng dẫn thao tác: [POWER_BI_DESKTOP_GUIDE.md](POWER_BI_DESKTOP_GUIDE.md). [EDA findings](../analytics/EDA_FINDINGS.md) là evidence lịch sử, không dùng thay cho lần export hiện tại.

## Nghiệm thu 27/10/2026

**POWERBI_EXPORT_VERIFIED · REPORT_AUTHORING_PENDING.** Export thật và offline verifier PASS; **557 unit tests PASS** (522 baseline giữ lại +35 exporter/guard mới; 51 tests Streamlit-only đã gỡ). Bốn Gold consumers bounded readback PASS; 36 tables và publication/DagRuns/protected code không đổi. Six notebooks, EDA findings/acceptance và root proposal của owner giữ nguyên byte. Chi tiết: [acceptance.json](acceptance.json). Chưa tạo hoặc kiểm thử report/DAX trong Power BI Desktop.

Bundle cuối đã kiểm tra:

```text
exports/powerbi/ec421297c194_20261027T090736Z_ffc830b8/
```

| File | Rows thực tế | Scope |
|---|---:|---|
| fact_overview.csv | 9 | 1.414.527 total / 1.402.167 numeric / 12.360 present-NULL observations |
| nso_national_annual.csv | 35 | V06.12, 1990–2024, production + area, national annual quantity |
| nso_national_seasonal.csv | 105 | V06.12, 1990–2024, 3 source seasons riêng, national quantity |
| nso_reported_yield.csv | 30 | V06.14, 1995–2024, reported yield kg/ha riêng |

Runtime lần cuối **43,35 giây**, 15 Analytics requests + final metadata fence; không collect full Trade. Nso reconciliation so exact 70 annual cells, 105 seasonal production cells, 30 yield cells, giữ source observation keys. 105 seasonal **area** cells bị loại có chủ đích vì file seasonal trong scope này chỉ dành cho production.

Lượt đầu failed final guard do Windows text pipe CRLF: candidate đã cleanup, không để manifest success. Đã sửa protocol LF/CRLF, thêm regression, guard metadata-only và full export sau đó PASS. Staging cuối dùng inherited workspace ACL thay `mkdtemp` mode700; verifier đọc được trong managed context mà không sửa ACL hệ thống. Bundle thành công trước đó được giữ, bốn CSV có cùng checksums khi cùng published inputs. Các giới hạn foundation chưa được coi là đã khắc phục chỉ nhờ CSV export.

## Export trên PowerShell

Từ root repository, dùng Python environment Analytics hiện hữu (Python 3.10–3.13, pandas; không cần Streamlit/Plotly):

```powershell
python -m jobs.analytics.export_powerbi
```

Docker project `tlcn-local` cần hoạt động và Gold đã published. Exporter không tự start/restart Docker, tăng heap, chạy Spark hoặc trigger DAG. Windows available ≥2 GiB; WSL guard ≥3 GiB, không có writer/pipeline đang chạy. Nên đóng Power BI khi export nếu laptop thiếu RAM.

Thành công in JSON `POWERBI_EXPORT_COMPLETE` với `bundle` là đường dẫn tuyệt đối. Mỗi lần tạo một bộ bất biến:

```text
exports/powerbi/<batch-prefix>_<UTC-time>_<export-id>/
    fact_overview.csv
    nso_national_annual.csv
    nso_national_seasonal.csv
    nso_reported_yield.csv
    export_manifest.json
```

Đây là các CSV thực dưới `exports/powerbi/`, không có bốn bản sao rời ở root. Thư mục ẩn `.powerbi-*` chỉ là staging; không Import staging. Không có `latest` pointer tự động vì Power BI có thể đọc nhiều file vào các thời điểm khác nhau. Chọn **cùng một bundle** cho cả bốn bảng; dùng parameter `ExportFolder` trong hướng dẫn để refresh sang bundle mới.

Kiểm tra lại bộ đã xuất (thay đường dẫn bằng giá trị `bundle` vừa nhận):

```powershell
python -m jobs.analytics.export_powerbi --verify 'C:\duong-dan\exports\powerbi\<bundle>'
```

`VERIFIED_OFFLINE` xác nhận BOM/header/type/checksum/rows/grain/count reconciliation của CSV và manifest. Đây **không phải** kiểm tra live publication hiện tại. Exporter thực mới thực hiện publication/resource/writer gates. CSV và `.pbix` đều Git-ignored; không có credentials trong các file này.

## Export schemas v1

| CSV | Fields theo đúng thứ tự | Grain / unit |
|---|---|---|
| fact_overview.csv | `fact_name, rows_total, numeric_rows, present_null_rows, numeric_coverage_pct, period_min, period_max, represented_periods, period_grain` | Một dòng / Gold fact; đủ chín facts |
| nso_national_annual.csv | `year, production_t, area_ha, publication, geography_scope, production_present, area_present` | Một năm / V06.12.csv / một source-scoped national identity / annual / quantity; tấn và ha riêng |
| nso_national_seasonal.csv | `year, season, production_t, publication, geography_scope, production_present, season_label_vi` | Một năm × source season / cùng V06.12 national scope; tấn; không có annual rows |
| nso_reported_yield.csv | `year, yield_kg_ha, publication, yield_present` | Một năm / V06.14.csv / national annual quantity; kg/ha báo cáo |

Số nguyên và decimals không format hàng nghìn, decimal separator `.`; UTF-8 BOM, header, delimiter comma. Flags là `true/false`. Numeric NULL là ô trống; **present=true + trống** khác **present=false + trống**; genuine zero vẫn là `0`. Không tạo calendar rows khi cả hai annual measures đều vắng. Seasonal/yield chỉ chứa source observations thực sự tồn tại; vắng row không thành zero.

`period_min/max` là Text ISO partial/full: `YYYY`, `YYYY-MM`, `YYYY-MM-DD`; `period_grain` phân biệt `calendar_year`, `calendar_month`, `calendar_date`, `marketing_year`, `reference_year`. Không biến marketing/reference year thành ngày quan sát 01/01. `numeric_coverage_pct` ở thang 0–100, làm tròn tám số lẻ; Power BI measure Percentage phải dùng tỷ lệ counts 0–1.

## Mapping từ API hiện hành

| Export field | Verified Analytics source / expression |
|---|---|
| rows_total / numeric_rows / present_null_rows | `fact_overview.rows / numeric_observations / missing_observations` |
| numeric_coverage_pct | `numeric_rows / rows_total * 100`; NULL nếu mẫu số 0; không average các tỷ lệ fact |
| period_min / period_max / represented_periods | `first_period / last_period / represented_periods`; format theo `analytics.queries.PERIODS` |
| year / publication / geography_scope | `nso_observations.year_key / source_table / source_level` (`national`) |
| production_t / area_ha | Pivot **không aggregate** `measure=production, unit=tonne` / `measure=area, unit=hectare`; cùng V06.12/geo/year/annual/quantity |
| season / season_label_vi | Source season code giữ nguyên; Vietnamese display lookup, không đổi identity |
| yield_kg_ha | V06.14 `measure=yield`, `unit=kg/hectare`, annual/quantity; không derive bằng join V06.12 |
| *_present | Có source cell hay không; không suy từ numeric value |

NSO scope được tìm từ `nso_scope_catalogue` actual fact data, phải có đúng một eligible national identity và đủ measures; ambiguous/missing scope là blocker. Manifest giữ cả `geography_key`, `source_geography_key`, source observation keys; `national` không phải một official country-code conversion.

## Integrity và hoàn tất an toàn

- `EDAClient` gọi CLI cũ bằng argument list; mỗi request đi qua Analytics admission và `gold.consumer.read_window`. Chỉ dùng `publication`, `fact_overview`, `nso_scope_catalogue`, `nso_observations`; không SQL tự do, không 20-row samples, không collect Trade fact.
- Giữ budgets 5.000 rows / 5 MiB / 15 s SQL / 64 MiB query memory; queries tuần tự, không cache.
- Kiểm tra same batch/manifest fingerprint/version mỗi response, count reconciliation của chín facts với published manifest; exact NSO keyed cell/value/NULL/presence/multiplicity và CSV roundtrip, không chỉ checksum.
- Viết bốn CSV vào staging sibling; source/schema/checksum/row-count validation hoàn tất trước promotion. Final runner guard tái dùng source/Gold shared locks và Analytics serial lock, kiểm tra 17 UUID/snapshot bindings và SQL hashes, giữ read window khi host rename cả thư mục bằng một filesystem operation, rồi re-read publication trước ACK/release.
- Exception/timeout/batch-change không báo success, xóa candidate/manifest của **lần hiện tại** bằng known flat filenames; không recursive delete dữ liệu tùy ý và không sửa các bundle cũ. Retry sinh bundle mới. Power failure có thể để staging; không import `.powerbi-*`. CSV là static extract có publication identity và thời điểm, không live subscription sau export.
- Manifest `powerbi-csv-1` lưu model, batch, publication fingerprint, 17 Gold table UUID/snapshots, query names/versions/definition & compiled hashes, typed scope, encoding/schema/rows/SHA-256, lineage và reconciliation. Nó không ghi publication timestamp không có trong Gold.

## Kiểm thử

```powershell
python -m pytest tests/unit/analytics -q
```

Unit fixtures kiểm tra NULL/absent/zero/precision, schema/units/grain/source identity, partial write, changed publication/definition/snapshot, resource/timeout và guard readback. Physical export/preservation evidence được ghi sau lần nghiệm thu thực tế; không coi fixtures là physical acceptance. Runtime CSV và PBIX không được commit.

## Roadmap sau hai trang đầu

| Subject | Future export / scope dựa EDA | Visual và giới hạn |
|---|---|---|
| FAOSTAT Global Rice Production | `fao_production`, CPC 0113, selected M49 areas 356/360/704/764, source calendar years | Production/area lines, paired yield =1000×production_t/area_ha khi area>0; weighted yield dùng paired sums; bốn areas không phải world ranking/total |
| Vietnam Domestic Prices | `price_coverage` + `price_observations`, một exact market/commodity/type/unit/currency/source series, bounded dates | Daily quote line và missing diagnostics; không AVG khác series hoặc tự daily→monthly |
| World Bank benchmark | Exact `rice_thai_5`, monthly USD/tonne; EDA 2000–2025 | Monthly line/rolling calendar features; giữ gaps, không ghép annual/indices/giá VND |
| International Trade | Một reporter 704, CPC 23161.02, export 2019–2023 và partners source-scoped | Bilateral lines, paired SUM(value)/SUM(quantity) unit value khi hợp lệ; không market-price claim, mirrored totals hoặc partner shares thiếu verified denominator; không full-history pivot vào Power BI |
| FAOSTAT SUA | Vietnam/CPC 23161.02, các element thực có; EDA 2010–2023 | Component lines/bars, signed 5071/5166 giữ nguyên; stocks khác flows, không SUM stock qua years hoặc coi supported subset là full balance identity |
| USDA PSD | Exact Vietnam / Rice, Milled / attribute / marketing year | Production/demand/stocks panels riêng MY/TY, rough/milled, milling fraction; không ghép calendar year hoặc convert theo tên địa lý |

Chưa implement các exports mở rộng này. Chưa có official province conformance, FX hay ML/PIT forecasting. Teammate tiếp tục second-laptop portability; nhiệm vụ này không takeover.
