# Cleaning specification — cleaning_monthly_price

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Producer prices / indices

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| faostat/price.csv | Producer prices / indices | 15334 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv |  | observations | csv | utf-8-sig | 2509272 | 15334 | 16 | 15335 | 1e2e1032e9a48100 | c6d49e6a1430866c358a84cc960448f5c6ec5be7ef0c8faaac2277eaa74ad2af |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: area × item × element × reporting year × month-or-annual × unit
- **time**: Monthly observations + Annual value, explicitly separate frequencies
- **geography**: M49 lexical code and raw area label; LCU/SLC currency identities need source dictionary
- **measure**: price (LCU/tonne, SLC/tonne, USD/tonne) vs development index 2014–2016=100
- **units**: price basis is encoded in Element, not only Unit; index unit candidate index_points
- **additivity**: prices and indices nonadditive; annual value not summed with monthly values



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | Value | 15334 | 15334 | 0 | 0 | 0 | 0 | 0 | 2 | 0.0 | 731137000.0 | 1911 | Element, Unit, Item | {np.int64(1): 4369, np.int64(2): 2818} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | Year | year candidate | 1991 | 2025 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| faostat/price.csv | raw table | 15334 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | Value | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15334 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | 245344 | 0 | 3136 | 0 | 0 | 2 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | 3 | Area Code (M49) | object | numeric | 1617 | 0 | False |
| faostat/price.csv | 7 | Item Code (CPC) | object | numeric | 15334 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-001 | faostat/price.csv |  | Area Code (M49), Item Code (CPC) | identifier_numeric_inference | 16951 | cells | [['Area Code (M49)', "['004', '008', '012', '024']"], ['Item Code (CPC)', "['0113']"]] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-041 | faostat/price.csv |  | Value | iqr_outlier_candidate | 1911 | observations | row 193, column Value: 3525300; row 319, column Value: 211.1; row 321, column Value: 254.63 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within Element, Unit, Item | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-042 | faostat/price.csv |  | Flag | source_estimate_provenance | 3160 | records | row 44, column Flag: E; row 45, column Flag: E; row 46, column Flag: E | Keep flags and descriptions as provenance; do not promote estimates to official/final | raw flag + source status | KEEP_RAW | Flag labels/dictionary accompany raw observations | VERIFIED | Accept source status retention | No loss of flags or notes through canonical mapping | RAW_PROFILE |
| DQ-043 | faostat/price.csv |  | Months / Element | mixed_frequency_and_statistic | 9993 | records | row 2, column Months: Annual value; row 3, column Months: Annual value; row 4, column Months: Annual value | Separate annual from monthly; price index from monetary price; interpret Unit together with Element | frequency + statistic_kind + period_raw + nullable month | NORMALIZE | Annual value and month labels coexist; Element explicitly says price index base 2014–2016 | VERIFIED | Approve separate canonical statistic/frequency fields | No fabricated month for annual or currency for index | RAW_PROFILE |
| DQ-044 | faostat/price.csv |  | Unit | index_blank_unit_is_structural | 3136 | records | row 44, column Unit: ; row 45, column Unit: ; row 46, column Unit:  | Preserve raw empty unit; set canonical index_points/base only for identified index Element | index_points; base_period 2014–2016 | NORMALIZE | Element explicitly identifies a base-100 index, not physical/currency measure | VERIFIED | Accept index representation | Index rows never receive currency/tonne conversion | RAW_PROFILE |
| DQ-045 | faostat/price.csv |  | release / geography / units | business_scope_metadata | 15334 | records | Source code/category distributions in STEP 6 | Approve geography aggregation membership, release identity and per-measure period/unit mapping | versioned dimensions + period_basis + source_vintage | PENDING_REVIEW | LCU/SLC currency histories, month coverage expectations, aggregation weights and index treatment | UNKNOWN | Approve source-specific contracts | Aggregation restricted to disjoint approved coverage | RAW_PROFILE |

## 7. Canonical Silver schema recommendations

| canonical_field | proposed_type | meaning/evidence_limit |
| --- | --- | --- |
| source_file | string | Raw relative path |
| source_sheet_or_table | string nullable | Workbook sheet/HTML table when present |
| source_content_sha256 | string | Snapshot identity, not business release |
| raw_row | long | CSV record ordinal or Excel physical row |
| raw_column | string | Original column/ordinal for wide projections |
| raw_value | string nullable | Keep source lexical value/native-cell provenance |
| raw_unit | string nullable | Keep exact Raw unit, even anomalous/blank |
| canonical_value | decimal, precision pending review | Never silently coerce invalid numeric |
| canonical_unit | string nullable | Only approved measure-scoped mapping |
| missing_reason | string nullable | NULL/blank/token/structural semantics kept distinct |
| dq_status | string | Retained/flagged/quarantined, proposed statuses |
| source_vintage | string nullable | Business release identity still pending when absent |
| area_code_m49 | string | retain leading zeros |
| item_code_cpc | string | retain decimal/alpha codes |
| element_code | string | measure semantics |
| reporting_year | integer | year basis explicit |
| month | integer nullable | NULL for annual |
| frequency | string | monthly/annual |
| statistic_kind | string | quantity/yield/price/index |
| currency | string nullable | LCU/SLC identity pending |
| price_basis | string nullable | per tonne from Element |
| index_base_period | string nullable | from Element |
| flag | string | source status |
| raw_note | string nullable | retain source notes |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/price.csv | 1 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year + Months Code | 15334 | 0 | 0 | 0 | 0 | 0 |
| faostat/price.csv | 2 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year + Months Code + Unit | 15334 | 3136 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-041 | iqr_outlier_candidate | 1911 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

LCU/SLC currency histories, month coverage expectations, aggregation weights and index treatment

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-041 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-045 | business_scope_metadata | UNKNOWN | PENDING_REVIEW | Approve source-specific contracts |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-001: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-041: Canonical retention count unchanged for IQR signals
- [ ] DQ-042: No loss of flags or notes through canonical mapping
- [ ] DQ-043: No fabricated month for annual or currency for index
- [ ] DQ-044: Index rows never receive currency/tonne conversion
- [ ] DQ-045: Aggregation restricted to disjoint approved coverage
- [ ] Raw input hashes unchanged; inventory/file/sheet coverage reconciled.
- [ ] Every retained observation has file/sheet/raw row/raw column/value/unit lineage.
- [ ] Missing candidates and parser failures reconcile; zero not silently NULL.
- [ ] Candidate keys checked again after approved normalization; conflicts handled explicitly.
- [ ] Canonical output keeps source period basis, statistic kind and raw quality flags.

Notebook execution invariants for this snapshot:

| check | status | detail |
| --- | --- | --- |
| Full input coverage | PASS | Every selected data file loaded, including reference CSVs and all workbook sheets |
| No sampling | PASS | All reported counts are full-data |
| NSO complete grouping | PASS | Every V06.* read even with NSO file filter |
| CSV header/body width | PASS | Positional headers preserved |
| Numeric accounting | PASS | Valid + missing candidates + actual parse failures = observation count |
| Missing classes do not overlap | PASS | NULL/empty/whitespace/token classes accounted without double-counting |
| Decision evidence | PASS | Every decision has evidence and proposed validation |
| Decision enums | PASS | Only allowed handling/confidence |
| Raw and old artifacts unchanged | PASS | 34 original files hash-checked |
| Milling factor identity | PASS | Milled ≈ rough×encoded/10000 within source integer reporting precision |

## 12. Mapping tới file Raw, observed rows/columns và cross-source evidence

CSV row = header-inclusive record ordinal (multiline record có thể khác physical line). Excel row = worksheet physical row. HTML row = table row ordinal (header row 1), không phải HTML text line. raw_column giữ literal field/year hoặc 1-based ordinal. Issue table cung cấp representative exact coordinates; toàn bộ masks/profiles ở notebook để drill down.

### faostat/price.csv

Raw records: 15334; columns: 16; sheet/CSV physical rows: 15335; header rows: [1].

Literal headers/positional grid labels:

```text
["Domain Code", "Domain", "Area Code (M49)", "Area", "Element Code", "Element", "Item Code (CPC)", "Item", "Year Code", "Year", "Months Code", "Months", "Unit", "Value", "Flag", "Flag Description"]
```

Relevant cross-source checks (diagnostic; incompatibilities retained):

| relation | status | matched_observations | finding | compatibility_limits |
| --- | --- | --- | --- | --- |
| FAOSTAT production ↔ area × yield | DIAGNOSTIC | 7311 | 0 outside lexical rounding band; max abs delta=2147.1 t | Same area/item/year and explicit ha, kg/ha, t; discrepancies flagged for methodology/rounding review |
| FAOSTAT Production ↔ SUA Production | DIAGNOSTIC | 1553 | 343 different matched values | Exact raw M49/CPC/year match, both t; release/calendar and processing definitions still require source confirmation |
| USDA PSD ↔ FAOSTAT production | INCOMPATIBLE | 0 | USDA has rough and milled production; FAOSTAT has Rice and t/ha conventions | USDA marketing/crop-year spans vs FAOSTAT reporting-year labels; no approved period/milling/geography mapping |
| Trade value/quantity ↔ rice price quotes | DIAGNOSTIC | 283454 | Import quantity: 31483 zero-quantity pairs; median unit value=903.022 USD/t | Trade mix, partner, time basis and quantity grade differ from producer/FOB quotes; no automatic equality, no USD conversion for LCU/SLC |
| Trade value/quantity ↔ rice price quotes | DIAGNOSTIC | 276329 | Export quantity: 21968 zero-quantity pairs; median unit value=904.762 USD/t | Trade mix, partner, time basis and quantity grade differ from producer/FOB quotes; no automatic equality, no USD conversion for LCU/SLC |

Không kế thừa quy tắc từ tài liệu/Silver cũ; mọi proposal có origin trong decision table.

## 13. Ghi chú các vấn đề và đặc thù dữ liệu FAOSTAT (Data Anomalies & Issues Profile)

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu price.csv, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Trộn lẫn tần suất thời gian và chỉ số tổng hợp (Mixed Monthly & Annual Frequency)
- Cột `Months` chứa đồng thời cả 12 tháng (`January` đến `December`) và bản ghi tổng kết năm mang nhãn `"Annual value"`.
- Cột `Element` chứa đồng thời cả Giá danh nghĩa tại cổng trại (Producer Price) và Chỉ số giá (Producer Price Index cơ sở 2014–2016=100).
- **Vấn đề**: Tồn tại cả dữ liệu chi tiết theo tháng và dữ liệu gộp cả năm trên cùng một cấu trúc cột, tiềm ẩn rủi ro nhân đôi giá trị nếu thực hiện tổng hợp (aggregation) theo năm mà không lọc tách tần suất.

### 13.2. Cột đơn vị khuyết thiếu có chủ đích (Intentional Blank Unit ở dòng Index)
- Có 3.136 quan sát có trường `Unit` để trống hoàn toàn.
- **Bản chất**: Đây là các dòng đo lường Chỉ số giá sản xuất (Producer Price Index - PPI). Vì chỉ số là đại lượng không thứ nguyên (index points tính trên mốc cơ sở 100), FAOSTAT quy ước không gán đơn vị tiền tệ/khối lượng.

### 13.3. Đa dạng đơn vị tiền tệ và tính phi cộng gộp (Currency Heterogeneity & Non-additivity)
- Giá sản xuất được biểu diễn dưới 3 hệ quy chiếu tiền tệ khác nhau: `LCU/tonne` (đồng nội tệ quốc gia), `SLC/tonne` (nội tệ chuẩn hóa), và `USD/tonne`.
- **Vấn đề**: Không thể thực hiện phép cộng gộp trên trường `Value` giữa các quốc gia hoặc giữa các đơn vị tiền tệ khác nhau do sai lệch tỷ giá và quy mô giá trị.

### 13.4. Dị biệt thang đo và giá trị ngoại lai thống kê (Scale Disparity & IQR Outlier Signals)
- Giá tính theo đồng nội tệ LCU của các quốc gia có tỷ giá thấp (ví dụ VND) đạt mức hàng trăm triệu đơn vị (`max = 731,137,000.0`), trong khi giá tính theo USD hoặc chỉ số PPI chỉ ở mức hàng trăm (`~100 – 900`).
- **Hiện tượng**: Sự phân hóa thang đo quy mô lớn này tạo ra 1.911 tín hiệu cảnh báo IQR outlier.

### 13.5. Sự hiện diện của số 0 đầu trong mã định danh (Leading Zeros in M49 & CPC Codes)
- Trường `Area Code (M49)` chứa mã quốc gia có số 0 ở đầu (`'004'`, `'008'`, `'012'`, `'024'`).
- Trường `Item Code (CPC)` chứa mã hàng hóa phân loại có số 0 ở đầu (`'0113'`).
- **Vấn đề**: Nguy cơ mất thông tin định danh và gãy liên kết quan hệ (JOIN) nếu trường mã bị tự động ép kiểu sang dạng số (Integer/Numeric).

### 13.6. Tỷ trọng dữ liệu ước tính (FAO Estimation Flags)
- Có 3.160 bản ghi mang cờ `Flag = E` (FAO data estimation), phản ánh số liệu do chuyên gia FAO ước tính mô hình thay vì số liệu thống kê gốc từ quốc gia thành viên.
