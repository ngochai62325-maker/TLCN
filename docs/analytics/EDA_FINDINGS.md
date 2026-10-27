# EDA Findings — 27/10/2026

**EDA_PHASE_01_COMPLETE.** Sáu notebooks đã execute độc lập theo từng cell; không có unexecuted/error code cells. Không Dashboard/ML, không lấy KPI từ consumer 20 rows. Xếp thứ tự findings dưới đây theo business relevance và evidence clarity, không phải statistical causality.

Published batch: `ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235`. Gold model `gold-20261025.1`. Scope/typed filters, SQL hashes, query IDs và timings ở từng [evidence JSON](evidence/). Tất cả findings là descriptive observations trên immutable snapshots, không xác nhận official geography conformance, causality hoặc publication vintage.

## VERIFIED FINDINGS

### VN01 — Thay đổi annual production_t

**Business question / scope:** NSO V06.12.csv, national, annual, quantity; unit in metric

**OBSERVATION — exact measured result:** V06.12: 19,225,100.00 năm 1990 → 43,451,600.00 năm 2024 (+126.01%).

**INTERPRETATION / business implication / limitation:** So sánh endpoints trong đúng publication; không suy causal attribution hoặc release vintage.

**Evidence:** [01_vietnam_rice_production.ipynb](../../notebooks/analytics/01_vietnam_rice_production.ipynb), Câu hỏi 1; query `nso_observations`; retrieval cell IDs `cell-003`, `cell-023`. [Query parameters/calculated values](evidence/01_vietnam_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### VN05 — Xu hướng dài hạn che giai đoạn sau observed peak

**Business question / scope:** NSO V06.12.csv, national annual quantity; peak selected from observed 1990–2024 series

**OBSERVATION — exact measured result:** Annual production đạt observed peak 45,091,000.00 tonnes năm 2015; tới 2024 còn 43,451,600.00 tonnes (-3.64%). Diện tích cùng interval thay đổi -8.95%.

**INTERPRETATION / business implication / limitation:** Descriptive peak-to-latest comparison; không phải formal structural-break test hoặc chứng minh area gây thay đổi production.

**Evidence:** [01_vietnam_rice_production.ipynb](../../notebooks/analytics/01_vietnam_rice_production.ipynb), Câu hỏi 1; query `nso_observations`; retrieval cell IDs `cell-003`, `cell-023`. [Query parameters/calculated values](evidence/01_vietnam_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### VN02 — Thay đổi annual area_ha

**Business question / scope:** NSO V06.12.csv, national, annual, quantity; unit in metric

**OBSERVATION — exact measured result:** V06.12: 6,042,800.00 năm 1990 → 7,127,100.00 năm 2024 (+17.94%).

**INTERPRETATION / business implication / limitation:** So sánh endpoints trong đúng publication; không suy causal attribution hoặc release vintage.

**Evidence:** [01_vietnam_rice_production.ipynb](../../notebooks/analytics/01_vietnam_rice_production.ipynb), Câu hỏi 1; query `nso_observations`; retrieval cell IDs `cell-003`, `cell-023`. [Query parameters/calculated values](evidence/01_vietnam_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### FAO01 — Production, area và computed yield của Viet Nam

**Business question / scope:** FAOSTAT M49 704 / CPC 0113; source quantity metrics

**OBSERVATION — exact measured result:** 1961–2024: production +382.92%, harvested area +48.89%, computed paired yield +224.35%.

**INTERPRETATION / business implication / limitation:** Accounting association của production/area; derived yield không là independent causal explanation.

**Evidence:** [02_global_rice_production.ipynb](../../notebooks/analytics/02_global_rice_production.ipynb), Câu hỏi 1; query `fao_production`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/02_global_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### PR01 — High/low của WB benchmark

**Business question / scope:** World Bank rice_thai_5, USD/tonne, monthly

**OBSERVATION — exact measured result:** rice_thai_5, 2000–2025: high 907.00 USD/t tại 2008-04; low 163.75 USD/t tại 2001-04.

**INTERPRETATION / business implication / limitation:** Observed benchmark extrema; không đại diện mọi giống/grade hay giá nội địa Việt Nam.

**Evidence:** [04_rice_price_analysis.ipynb](../../notebooks/analytics/04_rice_price_analysis.ipynb), Câu hỏi 1; query `price_observations`; retrieval cell IDs `cell-003`, `cell-012`, `cell-021`. [Query parameters/calculated values](evidence/04_rice_price_analysis.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### TR03 — Bilateral reported quantity — Philippines

**Business question / scope:** Viet Nam reporter; CPC 23161.02; export; exact selected partner identity

**OBSERVATION — exact measured result:** 2019–2023: 2,129,613.10 → 3,015,899.86 tonnes (+41.62%).

**INTERPRETATION / business implication / limitation:** Bilateral reported change; không suy global exporter rank, mirror reconciliation hoặc price causality.

**Evidence:** [05_international_rice_trade.ipynb](../../notebooks/analytics/05_international_rice_trade.ipynb), Câu hỏi 2; query `trade`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/05_international_rice_trade.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### TR04 — Reported unit value sang Philippines

**Business question / scope:** Viet Nam reporter / partner M49 608 / CPC 23161.02 / export; value-quantity pairing

**OBSERVATION — exact measured result:** 2019–2023: 414.96 → 560.88 USD/tonne (+35.16%).

**INTERPRETATION / business implication / limitation:** Có thể phản ánh composition/contract/reporting effects; không phải independent market price hoặc causal inflation estimate.

**Evidence:** [05_international_rice_trade.ipynb](../../notebooks/analytics/05_international_rice_trade.ipynb), Câu hỏi 2; query `trade`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/05_international_rice_trade.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### PSD02 — USDA MY Ending Stocks

**Business question / scope:** USDA PSD Vietnam / Rice, Milled; exact attribute

**OBSERVATION — exact measured result:** MY 2010–2025: 1,941,000.00 → 2,297,000.00 tonnes (+18.34%).

**INTERPRETATION / business implication / limitation:** Descriptive marketing-year trend; không reconcile với FAOSTAT calendar observations.

**Evidence:** [03_rice_supply_demand.ipynb](../../notebooks/analytics/03_rice_supply_demand.ipynb), Câu hỏi 2; query `psd`; retrieval cell IDs `cell-012`. [Query parameters/calculated values](evidence/03_rice_supply_demand.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### PR02 — Window volatility cao nhất

**Business question / scope:** Same WB benchmark; complete-calendar 12 returns; ddof=1

**OBSERVATION — exact measured result:** Highest trailing 12-month log-return sample std: 17.47% tại 2009-01.

**INTERPRETATION / business implication / limitation:** Historical variability, không là forecast hoặc causal event attribution.

**Evidence:** [04_rice_price_analysis.ipynb](../../notebooks/analytics/04_rice_price_analysis.ipynb), Câu hỏi 1; query `price_observations`; retrieval cell IDs `cell-003`, `cell-012`, `cell-021`. [Query parameters/calculated values](evidence/04_rice_price_analysis.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### VN03 — Reported yield trong V06.14

**Business question / scope:** V06.14.csv; national annual quantity

**OBSERVATION — exact measured result:** 3,690.00 → 6,100.00 kg/ha, 1995–2024 (+65.31%).

**INTERPRETATION / business implication / limitation:** Yield tăng/giảm là descriptive source observation; không đồng nhất với derived yield của V06.12.

**Evidence:** [01_vietnam_rice_production.ipynb](../../notebooks/analytics/01_vietnam_rice_production.ipynb), Câu hỏi 3; query `nso_observations`; retrieval cell IDs `cell-003`, `cell-023`. [Query parameters/calculated values](evidence/01_vietnam_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### VN04 — Season có reported production lớn nhất trong selected table

**Business question / scope:** V06.12.csv; national seasonal quantity, no annual summation

**OBSERVATION — exact measured result:** Năm 2024, winter_spring: 20,333,900.00 tonnes, trong 3 seasonal observations của V06.12.

**INTERPRETATION / business implication / limitation:** Đây là so sánh các season do nguồn định nghĩa, không xác nhận coverage của mọi lịch mùa vụ.

**Evidence:** [01_vietnam_rice_production.ipynb](../../notebooks/analytics/01_vietnam_rice_production.ipynb), Câu hỏi 3; query `nso_observations`; retrieval cell IDs `cell-003`, `cell-023`. [Query parameters/calculated values](evidence/01_vietnam_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### FAO02 — Thay đổi computed yield trong selected universe

**Business question / scope:** Four explicitly selected FAOSTAT source areas; CPC 0113, paired measures

**OBSERVATION — exact measured result:** Viet Nam: 1,896.59 → 6,151.66 kg/ha (+224.35%, 1961–2024).

**INTERPRETATION / business implication / limitation:** Largest proportional change among selected endpoints, not global best performance.

**Evidence:** [02_global_rice_production.ipynb](../../notebooks/analytics/02_global_rice_production.ipynb), Câu hỏi 1; query `fao_production`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/02_global_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### FAO03 — Common-year ranking trong bốn source areas

**Business question / scope:** Exact four-area comparator universe, common numeric year, CPC 0113

**OBSERVATION — exact measured result:** Năm 2024, India có 217,867,864 tonnes, cao nhất trong bốn source areas được chọn.

**INTERPRETATION / business implication / limitation:** Không phải world ranking và không chứng minh verified global total.

**Evidence:** [02_global_rice_production.ipynb](../../notebooks/analytics/02_global_rice_production.ipynb), Câu hỏi 2; query `fao_production`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/02_global_rice_production.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### SUA01 — SUA food_supply_quantity_t

**Business question / scope:** FAOSTAT Viet Nam, 23161.02; calendar years

**OBSERVATION — exact measured result:** 2010–2023: 14,050,000.00 → 13,837,660.13 tonnes (-1.51%).

**INTERPRETATION / business implication / limitation:** Reported component trend; opening stock is not annual flow and no closing stock is invented.

**Evidence:** [03_rice_supply_demand.ipynb](../../notebooks/analytics/03_rice_supply_demand.ipynb), Câu hỏi 1; query `sua`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/03_rice_supply_demand.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### SUA02 — SUA opening_stocks_t

**Business question / scope:** FAOSTAT Viet Nam, 23161.02; calendar years

**OBSERVATION — exact measured result:** 2010–2023: 5,917,024.29 → 6,113,011.31 tonnes (+3.31%).

**INTERPRETATION / business implication / limitation:** Reported component trend; opening stock is not annual flow and no closing stock is invented.

**Evidence:** [03_rice_supply_demand.ipynb](../../notebooks/analytics/03_rice_supply_demand.ipynb), Câu hỏi 1; query `sua`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/03_rice_supply_demand.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### PSD01 — USDA MY Production

**Business question / scope:** USDA PSD Vietnam / Rice, Milled; exact attribute

**OBSERVATION — exact measured result:** MY 2010–2025: 26,371,000.00 → 26,200,000.00 tonnes (-0.65%).

**INTERPRETATION / business implication / limitation:** Descriptive marketing-year trend; không reconcile với FAOSTAT calendar observations.

**Evidence:** [03_rice_supply_demand.ipynb](../../notebooks/analytics/03_rice_supply_demand.ipynb), Câu hỏi 2; query `psd`; retrieval cell IDs `cell-012`. [Query parameters/calculated values](evidence/03_rice_supply_demand.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### PR03 — Range của một domestic quote

**Business question / scope:** ["An Giang","Lúa IR 50404 (lúa tươi)","Thương lái thu mua","VND/kg","VND","CTV địa phương"]

**OBSERVATION — exact measured result:** Selected exact identity có 484 quoted dates; numeric range 4,500.00–9,400.00 VND/kg trong 2021-04-01–2026-08-03.

**INTERPRETATION / business implication / limitation:** Chỉ đại diện quote identity được in; không phải mức giá trung bình thị trường hay toàn Việt Nam.

**Evidence:** [04_rice_price_analysis.ipynb](../../notebooks/analytics/04_rice_price_analysis.ipynb), Câu hỏi 2; query `price_observations`; retrieval cell IDs `cell-003`, `cell-012`, `cell-021`. [Query parameters/calculated values](evidence/04_rice_price_analysis.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### TR01 — Bilateral reported quantity — China, mainland

**Business question / scope:** Viet Nam reporter; CPC 23161.02; export; exact selected partner identity

**OBSERVATION — exact measured result:** 2019–2023: 347,012.45 → 753,672.54 tonnes (+117.19%).

**INTERPRETATION / business implication / limitation:** Bilateral reported change; không suy global exporter rank, mirror reconciliation hoặc price causality.

**Evidence:** [05_international_rice_trade.ipynb](../../notebooks/analytics/05_international_rice_trade.ipynb), Câu hỏi 2; query `trade`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/05_international_rice_trade.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### TR02 — Bilateral reported quantity — Malaysia

**Business question / scope:** Viet Nam reporter; CPC 23161.02; export; exact selected partner identity

**OBSERVATION — exact measured result:** 2019–2023: 546,516.60 → 400,504.18 tonnes (-26.72%).

**INTERPRETATION / business implication / limitation:** Bilateral reported change; không suy global exporter rank, mirror reconciliation hoặc price causality.

**Evidence:** [05_international_rice_trade.ipynb](../../notebooks/analytics/05_international_rice_trade.ipynb), Câu hỏi 2; query `trade`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/05_international_rice_trade.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** source-filtered trend, paired-measure or comparison chart; see MVP table below

### OV01 — Numeric coverage khác nhau giữa các subject areas

**Business question / scope:** All nine published facts; count(value)/count(*)

**OBSERVATION — exact measured result:** 9 facts có 1,414,527 observations. fact_usda_price_reference có 4,523/12,936 present-NULL (34.96%).

**INTERPRETATION / business implication / limitation:** NULL cần được giữ trong diagnostics; không thay bằng zero hoặc so missing rates như source quality scores.

**Evidence:** [00_gold_data_overview.ipynb](../../notebooks/analytics/00_gold_data_overview.ipynb), Câu hỏi 1; query `fact_overview`; retrieval cell IDs `cell-003`. [Query parameters/calculated values](evidence/00_gold_data_overview.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** coverage/missingness table

### OV02 — Coverage được đếm bằng represented periods

**Business question / scope:** Global fact period domain, not individual series

**OBSERVATION — exact measured result:** Các khoảng trống calendar đã được tính trực tiếp từ distinct periods; xem bảng số dưới đây.

**INTERPRETATION / business implication / limitation:** Trend metrics cần kiểm tra calendar gaps ở đúng series; không suy continuity từ min/max.

**Evidence:** [00_gold_data_overview.ipynb](../../notebooks/analytics/00_gold_data_overview.ipynb), Câu hỏi 2; query `period_coverage`; retrieval cell IDs `cell-012`. [Query parameters/calculated values](evidence/00_gold_data_overview.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** coverage/missingness table

| Fact | Represented periods | Calendar span | Absent periods |
|---|---:|---:|---:|
| fact_nso_observation | 35 | 35 | 0 |
| fact_fao_production_observation | 64 | 64 | 0 |
| fact_fao_sua_observation | 14 | 14 | 0 |
| fact_fao_trade_observation | 39 | 39 | 0 |
| fact_usda_psd_observation | 66 | 66 | 0 |
| fact_price_monthly | 792 | 792 | 0 |
| fact_fao_price_annual | 35 | 35 | 0 |
| fact_usda_price_reference | 41 | 41 | 0 |
| fact_vn_market_quote_daily | 1211 | 2038 | 827 |

Daily absences can include non-reporting days. These are whole-fact domains, not proof of continuity for every individual series.

### OV03 — Units và identities chưa universally conformed

**Business question / scope:** Published dimensions

**OBSERVATION — exact measured result:** Có 34 metric definitions, 1042 price series và 6 geography namespaces trong inventory.

**INTERPRETATION / business implication / limitation:** NSO/FAO/USDA/price benchmarks cần filters riêng; ưu tiên production và từng price series có đủ numeric/calendar coverage.

**Evidence:** [00_gold_data_overview.ipynb](../../notebooks/analytics/00_gold_data_overview.ipynb), Câu hỏi 3; query `catalogue`; retrieval cell IDs `cell-019`. [Query parameters/calculated values](evidence/00_gold_data_overview.json). Confidence: **VERIFIED within stated scope**, no causal claim.

**Dashboard:** coverage/missingness table

## Data quality và scope discoveries

- Present-NULL, genuine zero và absent calendar rows được đếm riêng. Structural exclusions không thể suy ra từ retained Gold rows. Source flags/release statuses được giữ; unconfirmed không thành FINAL.

- V06.12 national annual quantity và V06.14 reported yield là publications riêng. Không resolve bằng MAX/FIRST/latest ingestion; không SUM national/regional/provincial hoặc annual với seasons.

- Global dimension membership không chứng minh fact availability. SUA Viet Nam: `F0030` trả empty; final analysis chọn actual CPC `23161.02` (Rice, milled). Có 8 metric codes/112 observations trong 2010–2023, không phải đủ 12 supported codes toàn model. Empty-scope attempt được giữ tại [attempt evidence](evidence/attempt_03_empty_F0030.json). Không thay absent cells bằng zero.

- Trade whole-fact max year 2024 không có nghĩa Viet Nam reporter có data năm đó. [2024 smoke](evidence/trade_smoke_2024.json) trả 0 groups; actual selected product coverage kết thúc 2023. [2023 smoke](evidence/trade_smoke_2023.json) trả 85 groups. Notebook 05 dùng 2019–2023 và trả 397 bilateral-year groups, không collect 1,226,470 fact rows.

- NSO area, FAO harvested area, SUA Rice milled và USDA Rice, Milled không tự trở thành các measures tương đương giữa providers. World Bank table chứa cả non-rice commodity benchmarks; notebook chọn exact `rice_thai_5`.

## EXPLORATORY HYPOTHESES — chưa được kiểm định

1. Production growth có association với thay đổi area và computed yield. Derived yield là accounting ratio, không phải independent causal explanation.

2. WB benchmark volatility và trade unit-value changes có thể liên quan composition/contracts/reporting; không dùng coincident movements để suy cause.

3. Monthly seasonality hoặc khác biệt giữa NSO publications cần investigation riêng, coverage checks và statistical tests; chưa có kết luận seasonality.

## UNSUPPORTED / DEFERRED

Official provincial maps/63→34 transitions; universal product/geography crosswalks; mirrored global trade totals; partner concentration/shares khi denominator chưa verified; FX conversions; paddy↔milled conversions; closing stocks suy ra cho SUA; cross-provider balance reconciliation; vintage/PIT forecasts; unrestricted 39-year Trade pivot.

## Notebook execution và performance

| Notebook | Queries | Returned rows per request | Charts | Wall seconds |
|---|---:|---|---:|---:|
| 00_gold_data_overview.ipynb | 26 | 17, 1, 1, 1, 1, 1, 1, 1, 1, 1, 35, 64, 14, 39, 66, 792, 35, 41, 1211, 987, 295, 34, 1042, 52, 5, 2 | 3 | 77.39 |
| 01_vietnam_rice_production.ipynb | 4 | 17, 560, 30, 180 | 7 | 15.75 |
| 02_global_rice_production.ipynb | 4 | 17, 987, 295, 256 | 3 | 15.59 |
| 03_rice_supply_demand.ipynb | 6 | 17, 987, 295, 2, 14, 990 | 4 | 19.45 |
| 04_rice_price_analysis.ipynb | 8 | 17, 1042, 312, 542, 484, 396, 35, 372 | 3 | 26.26 |
| 05_international_rice_trade.ipynb | 5 | 17, 987, 295, 6, 397 | 4 | 18.20 |

53 requests gồm publication metadata calls; SQL HTTP elapsed tối đa **1.035 s**; response tối đa **440,912 bytes**. Sampled Windows available minimum **2.66 GiB**; notebook child-process RSS peak **184.31 MiB**. Đây không phải toàn bộ WSL/Trino heap peak. Trino per-query peak memory/spill không được statement API trả trong profile này (NULL), không giả định 0. Giữ session memory cap 64 MB và query timeout 15 s; không đổi Trino 448/512 MiB heap. No result cache. No query exceeded its row/byte/time budget.

## Gold preservation và tests

Xem [final preservation](evidence/preservation_EDA-05.json): 36 existing tables giữ UUID/snapshot/metadata/data files; publication/context/manifest và Gold DagRun states không đổi. Không trigger pipeline hoặc ghi Iceberg. Unit/Gold regression và 4 Trino VALUES fixtures được phân biệt với actual notebook analytical execution. Tests không tạo catalog objects.

## Dashboard MVP — đề xuất dựa trên findings

| Priority | KPI/chart | Query / required filters | Cost & limits |
|---|---|---|---|
| 1 | Numeric coverage và present-NULL/absent-period diagnostics | fact_overview + period_coverage, exact fact | Small aggregates; không source-quality ranking |
| 2 | NSO national production/area trends | nso_observations; V06.12, national, quantity, annual, 1990–2024 | Giữ publication/measure/unit riêng |
| 3 | NSO seasonal production | same query; winter_spring/summer_autumn/main_season | Không cộng annual vào seasons; no official-map claim |
| 4 | FAO production và paired yield comparisons | fao_production; CPC 0113, 4 exact M49 keys, common year | Selected-universe ranking, no global total/AVG yields |
| 5 | WB monthly rice benchmark + 12M volatility | price_observations; exact rice_thai_5 series | USD/tonne, calendar windows, no filling |
| 6 | Domestic quote series trend + gaps | price_coverage then price_observations; exact selected quote identity | VND/kg; do not blend markets/sources |
| 7 | Reported bilateral quantity / unit-value comparison | trade; reporter 704, CPC 23161.02, export, 2019–2023, selected partners | 397 groups in tested scope; not market price or global share |

SUA/PSD stocks/flows là trang mở rộng, tách source/time semantics. Chưa code Streamlit/Dash. Next smallest task: một local read-only Dashboard với publication/coverage + NSO annual page, dùng chính allowlisted CLI và source-filter contracts đã kiểm chứng; sau đó thêm FAO/price pages. Không đổi Gold semantic hashes/publication; serialize requests và benchmark lại sau khi thêm UI.
