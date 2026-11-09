# Analytics Readiness — 27/10/2026

> **Owner decision mới: Power BI Desktop Free.** Streamlit đã ngừng; dùng [CSV exporter](../powerbi/README.md) và [hướng dẫn dựng hai report pages](../powerbi/POWER_BI_DESKTOP_GUIDE.md). EDA Phase 01 đã COMPLETE, six notebooks và historical evidence giữ nguyên. Không tạo web Dashboard mới.

> **Analytics Phase 01 đã có executed EDA.** Đọc [EDA_FINDINGS](EDA_FINDINGS.md),
> [execution evidence](EDA_ACCEPTANCE.json) và [reproduction/read protocol](README.md).
> Query foundation và sáu notebooks đã được triển khai; chưa có Dashboard. Phần
> readiness dưới đây là baseline planning trước EDA, không thay các findings mới.

**READY_FOR_ANALYTICS_QUERY_DESIGN**, với giới hạn source-scoped bên dưới.
Foundation acceptance: [H01–H05](../foundation/ACCEPTANCE.md). Chưa có Dashboard hay
dependency/service mới. Runtime work tiếp theo phải qua resource admission:
Windows available >=2 GiB và các checks của local demo; sample cuối chỉ 0,62 GiB.

## Dữ liệu được phép dùng

Published Gold `gold-20261025.1`: **8 dimensions + 9 facts**, 1.414.527 fact rows,
4 version-controlled Semantic SQL Query Definitions. Input `iceberg.silver_local`,
output `iceberg.gold_local`. [Manifest/consumer verification](../foundation/baseline_final.json),
[actual pinned coverage/34 metrics](../foundation/analytics_coverage.json).
Min/max dưới đây chỉ là khoảng có dữ liệu toàn fact, **không chứng minh continuity
hay coverage của từng series/geography**. Query coverage chạy trên published snapshots.

Dimensions: `dim_source_system`, `dim_year`, `dim_month`, `dim_date`,
`dim_source_geography`, `dim_source_product`, `dim_observation_type`, `dim_price_series`.
Geography/product/series keys là source-scoped; geography labels không phải official
crosswalk. Time dimensions conformed cho đúng calendar grain; PSD marketing year và
USDA reference year/period có semantics riêng.

| Fact | Rows / numeric observations | Coverage thực tế |
|---|---:|---|
| fact_nso_observation | 23.060 / 22.114 | 1990–2024 |
| fact_fao_production_observation | 22.738 / 22.509 | 1961–2024 |
| fact_fao_sua_observation | 36.380 / 36.123 | 2010–2023 |
| fact_fao_trade_observation | 1.226.470 / 1.226.470 | 1986–2024 |
| fact_usda_psd_observation | 990 / 990 | marketing year 1960–2025 |
| fact_price_monthly | 61.573 / 55.168 | month keys 196001–202512; FAO + WB, namespaces riêng |
| fact_fao_price_annual | 9.993 / 9.993 | 1991–2025 |
| fact_usda_price_reference | 12.936 / 8.413 | reference year 1985–2025 |
| fact_vn_market_quote_daily | 20.387 / 20.387 | date keys 20210104–20260803 |

## Business questions và metric rules

| Câu hỏi / nguồn | Filters bắt buộc và công thức | Grain/unit/coverage restriction | Visualization và chi phí dự kiến |
|---|---|---|---|
| Sản lượng/diện tích/năng suất VN theo nguồn NSO — `vw_nso_source_observations` | `source_table`, `measure`, `season`, `statistic_kind`, source geography key và year range. `quantity`: giá trị Gold trực tiếp | Annual và seasonal riêng; quantity và development_index riêng; tonne/hectare/kg-per-hectare/percent theo unit. Không cộng national+region+province hoặc cộng annual+seasons | Line + bar theo candidate geography; không official map. Nhỏ (~23k rows), filter sớm |
| Xu hướng sản lượng và harvested area — `vw_fao_production_yield` | Chọn source geography/product, years; metric 5510 production_t, 5312 area_ha | Chỉ cộng geography/product đã chứng minh không chồng lấn. Không gọi phép cộng cả aggregate areas là world total | Line, ranked bar; nhỏ (~23k input rows) |
| So sánh năng suất — cùng production semantic query | `computed_yield_kg_ha=1000*production_t/area_ha`, chỉ khi area>0 và production có numeric value; giữ `present_*`, contributor keys | Paired source/year/geography/product. Yield tổng = 1000*SUM(paired production)/SUM(paired area), không AVG(country yields). Reported 5412 hiển thị riêng | Paired lines/scatter; không cross-source grain conversion |
| Các thành phần SUA — `vw_sua_supported_balance` | Chọn một geography/product/year; 12 supported codes từ metric inventory | Tonne; 5071 stock variation và 5166 residual giữ signed. 5113 opening stocks là stock; không SUM qua years. Không gọi 12 metrics là một identity cân đối hoàn chỉnh nếu chưa chứng minh | Component bars/lines; nhỏ (~36k input). Không dùng pie có negative values |
| Thương mại song phương — fact trade, bốn metric codes 5610/5622/5910/5922 | Một reporter, một product/flow, bounded year range; partner list từ đúng source identity. Tonne hoặc USD riêng | Reporter/partner roles không đổi. Không cộng mirrored reporter flows thành global totals; unspecified/aggregate partners hiển thị riêng, không silently discard | Partner ranking/line; fact ~1,2m rows, bắt buộc pushdown filters và bounded output |
| Reported trade unit values — `vw_trade_reported_unit_values` | One year + reporter (+ product/partner). Import USD/import tonnes hoặc export USD/export tonnes, denominator>0, value present | USD/tonne; Gold value đã đổi từ 1000 USD, không nhân lần nữa. Group pairing + `present_*` phân biệt absent/NULL. Aggregated ratio chỉ trên cùng paired population | Scatter/line; chi phí cao hơn. Không full-history pivot làm interactive endpoint |
| Giá quote trong nước — daily fact + price_series/date dimensions | Một series identity gồm market, commodity, price_type, unit, currency, information source; date range | Một quote/day theo đầy đủ identity. Không AVG các market/unit/currency/source khác nhau; chưa phê duyệt daily→monthly aggregation | Daily line + gaps; ~20k rows, date filter |
| Giá quốc tế — monthly fact + price_series/month/source dimensions | Một source benchmark/series, price_kind, unit/currency, month range | FAO và WB namespaces riêng; indices không là monetary prices. Không merge annual FAO hoặc USDA reference rows vào monthly series | Multi-line chỉ khi cùng unit/currency/kind; ~62k rows, filter series trước |
| USDA PSD supply/demand — PSD fact + observation type | Country/commodity/attribute chính xác theo nguồn; market_year range | 15 attributes thực có, gồm rough/milled production, MY/TY, stocks/flows, yield và milling fraction. Không calendar-year join ngầm, không stock SUM qua years, không rough↔milled factor mới | Attribute lines, stock/flow panels tách biệt; 990 rows |
| Annual/reference prices — annual FAO và USDA reference facts | Source series + year; USDA thêm reference_period/statistic/source class trong series identity | USDA reference_year không tự suy calendar month; annual FAO không trộn monthly. Không biến NULL price thành 0 | Lines/table; ~10k/~13k rows |
| Coverage/missing/quality — facts + published manifest | Numeric coverage=count(value)/count(*), missing count, `dq_warnings_json`, source/snapshot and source period | Row coverage không phải uptime/SLA hay complete publication vintage. NSO warnings không bị đổi thành dữ liệu invalid | Coverage cards/table; chỉ small grouped results, không đọc full fact vào pandas |

## Semantic access và performance

Current `jobs/gold/query_gold.py --view <allowlisted-name>` kiểm tra publication,
definition/version hashes, 17 schema/UUID/snapshot identities và shared read lock;
query đúng published snapshot set. API này trả **tối đa 20 sample rows**. Nó không
phải Analytics aggregate API và không được dùng sample để tính dashboard totals.

Analytics hiện có **allowlist riêng** cho fixed analytical SELECTs, dùng
`gold.consumer.read_window()` và manifest snapshot bindings. Exporter tái sử dụng
client/CLI này; không nhận SQL/free-form table names, không sửa bốn approved Gold definitions hoặc
Gold code fingerprint để thêm reporting. Reporter/product/year/date/series
parameters phải typed, bounded và lọc trước pivot/join; Trino DDL/DML bị từ chối.

Mỗi response có batch, query-definition hash/version, normalized parameters,
snapshot identity và coverage. Không result cache. CSV là immutable extract được
verify và finalize dưới publication read fence, không phải live UI session; mở
PBIX offline không tự gọi Gold gate hoặc tự cập nhật dữ liệu.

Actual bounded samples sau foundation: NSO ~5,41 s, production ~2,10 s,
SUA ~1,58 s, Trade ~2,31 s (20 rows/latest validated year). Không suy thành SLA
cho chart aggregates. Trade interactive query phải profile một reporter/product/year
trước khi mở rộng; full-history semantic validation vẫn là offline acceptance.

**Budget Analytics/export hiện hành:** một query tại một thời điểm; <=5.000 output rows,
<=5 MiB result; 64 MiB Trino query memory; 15 s interactive timeout và cancel-on-failure.
Nếu query chưa đạt budget, thu hẹp filters hoặc defer chart, không thêm aggregate tables
để vượt gate. Không dùng cache để bỏ qua publication/resource verification.

## Reporting MVP — Power BI Desktop

Python exporter trên host gọi fixed Analytics CLI trong runner bằng argument list.
Credentials và Gold publication gate ở runtime hiện hữu. Xuất một bundle CSV đầy đủ
vào `exports/powerbi/`, rồi owner Import và kéo thả trong Desktop Free. Không
Spark/DAG trigger, không Raw/Silver reads, không Docker service/API/database mới.

Hai report pages đầu: **Tổng quan chín facts** và **Sản xuất lúa Việt Nam** (V06.12
annual/seasonal, V06.14 reported yield riêng). Source-scoped NSO identity, units,
NULL/zero/presence và calendar-aware metrics phải giữ nguyên. Các exports FAO,
prices, Trade, SUA, PSD chỉ là [roadmap](../powerbi/README.md#roadmap-sau-hai-trang-đầu).

Chưa hỗ trợ: official provincial choropleth/63→34 transitions, universal geographic
hoặc product conformance, USDA-name→FAO-M49 joins, automatic paddy/milled conversion,
FX conversion/price-level comparison khác tiền, global trade totals từ mirrored flows,
monthly interpolation, causal conclusions, release-vintage/PIT forecasting.

**Next task cụ thể:** owner dựng/lưu/Refresh hai trang PBIX từ cùng verified bundle,
đối chiếu DAX/KPI với manifest/EDA reference; không khôi phục Streamlit. Python CSV
acceptance không thay thế Desktop report acceptance. Portability/second-laptop
acceptance vẫn do teammate phụ trách.
