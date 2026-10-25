# Cleaning specification — cleaning_production


| file | measure/season | observation_rows |
| --- | --- | --- |
| faostat/production_world.csv | Crops/livestock production | 22738 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv |  | observations | csv | utf-8-sig | 3292244 | 22738 | 15 | 22739 | d1e64ff0245aedbe | 6a23d820d61e990d1c108ffe3468e7f39a5beb57e376d5174e332dfc9e1200cc |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: area × item × element × year × unit
- **time**: annual reporting year; calendar/crop alignment pending
- **geography**: M49 code + raw area, including any aggregates; no blind sum over all areas
- **measure**: Area harvested, Yield, Production
- **units**: ha → ha; kg/ha → kg/ha (or t/ha ×0.001); t → tonne, dimension-specific
- **additivity**: area/production only across disjoint geography; yield nonadditive, weighted by matching harvested area



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv | Value | 22738 | 22509 | 229 | 0 | 0 | 0 | 0 | 572 | 0.0 | 217867864.16 | 2524 | Element, Unit, Item | {np.int64(1): 6278, np.int64(2): 467} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv | Year | year candidate | 1961 | 2024 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| faostat/production_world.csv | raw table | 22738 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv | Value | 0 | 229 | 0 | 0 | 572 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 229 | 229 | 22738 | 1.0071246371712552 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv | 341070 | 0 | 22578 | 0 | 0 | 572 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/production_world.csv | 3 | Area Code (M49) | object | numeric | 2529 | 0 | False |
| faostat/production_world.csv | 7 | Item Code (CPC) | object | numeric | 22738 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-002 | faostat/production_world.csv |  | Area Code (M49), Item Code (CPC) | identifier_numeric_inference | 25267 | cells | [['Area Code (M49)', "['004', '008', '012', '024']"], ['Item Code (CPC)', "['0113']"]] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-046 | faostat/production_world.csv |  | Value | blank_measure | 229 | observations | row 297, column Value: ; row 298, column Value: ; row 299, column Value:  | Preserve NULL measure with raw value/flag; never impute 0 | nullable decimal + missing_reason | NORMALIZE | Blank raw measure; Flag M description confirms missing | VERIFIED | Approve retention/nullable schema | Missing count reconciles; no introduced numeric zero | RAW_PROFILE |
| DQ-047 | faostat/production_world.csv |  | Value | iqr_outlier_candidate | 2524 | observations | row 970, column Value: 8299; row 973, column Value: 8800; row 976, column Value: 8842.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within Element, Unit, Item | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-048 | faostat/production_world.csv |  | Flag | source_estimate_provenance | 3943 | records | row 59, column Flag: E; row 60, column Flag: E; row 61, column Flag: E | Keep flags and descriptions as provenance; do not promote estimates to official/final | raw flag + source status | KEEP_RAW | Flag labels/dictionary accompany raw observations | VERIFIED | Accept source status retention | No loss of flags or notes through canonical mapping | RAW_PROFILE |
| DQ-049 | faostat/production_world.csv |  | release / geography / units | business_scope_metadata | 22738 | records | Source code/category distributions in STEP 6 | Approve geography aggregation membership, release identity and per-measure period/unit mapping | versioned dimensions + period_basis + source_vintage | PENDING_REVIEW | harvested vs sown area compatibility, geographic aggregates, vintage and calendar basis | UNKNOWN | Approve source-specific contracts | Aggregation restricted to disjoint approved coverage | RAW_PROFILE |

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
| faostat/production_world.csv | 1 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year | 22738 | 0 | 0 | 0 | 0 | 0 |
| faostat/production_world.csv | 2 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year + Unit | 22738 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-047 | iqr_outlier_candidate | 2524 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

harvested vs sown area compatibility, geographic aggregates, vintage and calendar basis

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-047 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-049 | business_scope_metadata | UNKNOWN | PENDING_REVIEW | Approve source-specific contracts |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-002: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-046: Missing count reconciles; no introduced numeric zero
- [ ] DQ-047: Canonical retention count unchanged for IQR signals
- [ ] DQ-048: No loss of flags or notes through canonical mapping
- [ ] DQ-049: Aggregation restricted to disjoint approved coverage
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

### faostat/production_world.csv

Raw records: 22738; columns: 15; sheet/CSV physical rows: 22739; header rows: [1].

Literal headers/positional grid labels:

```text
["Domain Code", "Domain", "Area Code (M49)", "Area", "Element Code", "Element", "Item Code (CPC)", "Item", "Year Code", "Year", "Unit", "Value", "Flag", "Flag Description", "Note"]
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

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu production_world.csv, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Phân biệt giữa Dữ liệu khuyết thiếu (Blank Measure) và Giá trị 0 thực tế (Zero Observations)
- Có 229 quan sát có trường `Value` để trống hoàn toàn (rỗng), tất cả đều đi kèm `Flag = M` (`Data not available`).
- Đồng thời có 572 quan sát mang giá trị số `0.0` thực tế (chủ yếu ở diện tích và sản lượng của các quốc gia không phát sinh hoạt động trồng lúa trong năm tương ứng).
- **Vấn đề**: Thể hiện hai trạng thái nghiệp vụ hoàn toàn khác nhau: Không thu thập được dữ liệu (Missing) đối lập với Không phát sinh hoạt động sản xuất (Zero production).

### 13.2. Tính phi cộng gộp của chỉ tiêu Năng suất (Non-additivity of Yield)
- Cột `Element` chứa 3 chỉ tiêu: Diện tích thu hoạch (`ha`), Sản lượng (`t`), và Năng suất (`kg/ha`).
- **Vấn đề**: Năng suất là đại lượng tỷ suất bình quân, không thể cộng gộp (non-additive) theo chiều không gian (giữa các quốc gia) hoặc theo chiều thời gian (qua các năm). Việc tính toán tổng hợp cho nhóm quốc gia bắt buộc phải dùng bình quân gia quyền theo diện tích.

### 13.3. Đồng tồn tại giữa Quốc gia đơn lẻ và Thực thể tổng hợp khu vực (Country vs. Regional Aggregates)
- Cột `Area` và `Area Code (M49)` chứa đồng thời cả các quốc gia độc lập (Vietnam, India, China...) và các vùng tổng hợp địa lý/chính trị (World, Asia, South-eastern Asia, Africa...).
- **Vấn đề**: Rủi ro tính trùng số liệu quy mô lớn (double-counting) nếu áp dụng phép tính tổng trên toàn bộ cột `Area`.

### 13.4. Dữ liệu ước tính từ mô hình tính toán của FAO (Estimation Provenance)
- Có 3.943 quan sát mang cờ `Flag = E` (FAO data estimation) và 624 quan sát mang cờ `Flag = I` (Imputed value). Số liệu thực tế do quốc gia công bố chính thức chỉ chiếm một phần trong tổng thể dữ liệu.

### 13.5. Sự hiện diện của số 0 đầu trong mã định danh (Leading Zeros in M49 & CPC Codes)
- Có 2.529 bản ghi có `Area Code (M49)` chứa số 0 ở đầu (`'004'`, `'008'`, `'012'`, `'024'`).
- Toàn bộ 22.738 bản ghi của `Item Code (CPC)` đều mang mã dạng chuỗi có số 0 ở đầu (`'0113'`).
- **Vấn đề**: Nguy cơ biến dạng mã định danh nếu bị parser tự động chuyển đổi sang kiểu số nguyên.

### 13.6. Sai lệch đối chiếu chéo nội bộ (Internal Production vs. SUA Discrepancy)
- Kết quả đối chiếu chéo cho thấy tồn tại 343 trường hợp giá trị sản lượng giữa `production_world.csv` và `supply_utilization.csv` bị lệch nhau (do khác biệt giữa niên vụ nông nghiệp và năm dương lịch, hoặc giữa quy ước thóc/gạo thô và gạo quy đổi).
