# Analytics Readiness — 27/10/2026

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

Task Analytics đầu tiên phải thêm **allowlist riêng đã review** cho fixed analytical
SELECTs, dùng `gold.consumer.read_window()` và manifest snapshot bindings. Không
nhận SQL/free-form table names từ UI, không sửa bốn approved Gold definitions hoặc
Gold code fingerprint chỉ để thêm Dashboard. Reporter/product/year/date/series
parameters phải typed, bounded và lọc trước pivot/join; Trino DDL/DML bị từ chối.

Mỗi response phải có batch, query-definition hash/version, normalized parameters,
snapshot identity và coverage. Cache key phải gồm chúng; mỗi request/cache hit vẫn
qua live publication/read gate. TTL không thay publication validation. Không giữ
DB lock/resource trong UI session; chỉ giữ trong read/query operation.

Actual bounded samples sau foundation: NSO ~5,41 s, production ~2,10 s,
SUA ~1,58 s, Trade ~2,31 s (20 rows/latest validated year). Không suy thành SLA
cho chart aggregates. Trade interactive query phải profile một reporter/product/year
trước khi mở rộng; full-history semantic validation vẫn là offline acceptance.

**Đề xuất budget, chưa benchmark:** một query tại một thời điểm; <=5.000 output rows,
<=5 MiB result; 64 MiB Trino query memory; 15 s interactive timeout và cancel-on-failure.
Nếu query chưa đạt budget, thu hẹp filters hoặc defer chart, không thêm aggregate tables
để vượt gate. Cache giới hạn ~32 entries/300 s, chỉ dữ liệu nhỏ; đo RSS trên máy 16 GiB.

## Dashboard MVP được đề xuất

Một local Python **Streamlit process trên host**, gọi fixed Analytics CLI trong
runner hiện có bằng argument list (`shell=False`), nhận JSON nhỏ. CLI giữ credentials
và publication gate trong runtime hiện hữu, query qua Trino; Dashboard không khởi
động Spark, trigger Airflow hoặc đọc Raw/Silver. Không thêm Docker service/API/database.
Chưa cài Streamlit trong nhiệm vụ foundation.

Streamlit phù hợp đề xuất này vì có data cache với TTL/entry limits; Dash dùng
callbacks rõ ràng và phù hợp nếu cần tương tác phức tạp hơn. Chọn Streamlit cho MVP
là **quyết định thiết kế đề xuất**, không phải kết quả benchmark. Tham chiếu chính thức:
[Streamlit caching](https://docs.streamlit.io/develop/concepts/architecture/caching),
[Dash callbacks](https://dash.plotly.com/basic-callbacks).

MVP đầu: (1) publication/coverage, (2) NSO quantity trends theo source table/season,
(3) FAO production/area/computed yield, (4) một domestic quote hoặc WB benchmark
price series. SUA/PSD là trang tiếp theo; Trade chỉ mở sau bounded-query benchmark.

Chưa hỗ trợ: official provincial choropleth/63→34 transitions, universal geographic
hoặc product conformance, USDA-name→FAO-M49 joins, automatic paddy/milled conversion,
FX conversion/price-level comparison khác tiền, global trade totals từ mirrored flows,
monthly interpolation, causal conclusions, release-vintage/PIT forecasting.

**Next task cụ thể:** implement 3–4 allowlisted Analytics queries và CLI read gate,
tests unpublished/writer-active/tampered hash/invalid parameters/snapshot mismatch,
full-data aggregate correctness và bounded-cost profile. Chỉ sau acceptance đó mới
thêm Streamlit UI. Portability/second-laptop acceptance vẫn do teammate phụ trách.
