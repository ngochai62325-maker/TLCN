# Cleaning specification — cleaning_supply_utilization

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Supply Utilization Accounts

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| faostat/supply_utilization.csv | Supply Utilization Accounts | 36380 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv |  | observations | csv | utf-8-sig | 6096569 | 36380 | 15 | 36381 | d1e64ff0245aedbe | 3a0fa590b9fd8c3a408b3583e66fdec55db98d353993a009821688f8428e5b3c |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: area × CPC item × element × year × unit
- **time**: annual labels; source release calendar basis needs confirmation
- **geography**: source M49 strings; aggregate area membership pending
- **measure**: production, opening stocks, imports/exports, stock variation, residuals and uses
- **units**: t → tonne with raw unit retained; rice vs rice milled remain separate items
- **additivity**: flows only over disjoint coverage; stocks not additive over time; residuals/sign conventions require equation policy



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | Value | 36380 | 36123 | 257 | 0 | 0 | 0 | 1516 | 8660 | -6425000.0 | 214429949.05 | 6364 | Element, Unit, Item | {np.int64(1): 2569, np.int64(2): 16691} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | Year | year candidate | 2010 | 2023 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | raw table | 36380 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | Value | 0 | 257 | 0 | 0 | 8660 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 257 | 257 | 36380 | 0.7064321055525014 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | 545700 | 0 | 34246 | 0 | 0 | 8660 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/supply_utilization.csv | 3 | Area Code (M49) | object | numeric | 4294 | 0 | False |
| faostat/supply_utilization.csv | 7 | Item Code (CPC) | object | numeric | 19197 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-003 | faostat/supply_utilization.csv |  | Area Code (M49), Item Code (CPC) | identifier_numeric_inference | 23491 | cells | [['Area Code (M49)', "['004', '008', '012', '024']"], ['Item Code (CPC)', "['0113', '23161.02']"]] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-050 | faostat/supply_utilization.csv |  | Value | blank_measure | 257 | observations | row 217, column Value: ; row 224, column Value: ; row 231, column Value:  | Preserve NULL measure with raw value/flag; never impute 0 | nullable decimal + missing_reason | NORMALIZE | Blank raw measure; Flag M description confirms missing | VERIFIED | Approve retention/nullable schema | Missing count reconciles; no introduced numeric zero | RAW_PROFILE |
| DQ-051 | faostat/supply_utilization.csv |  | Value | negative_measure | 1516 | observations | row 20, column Value: -10521.01; row 28, column Value: -11214.4; row 37, column Value: -15000 | Keep negative values; flag by measure; residuals and stock variation sign policy pending | signed decimal, source unit | PENDING_REVIEW | Negative values observed; no nonnegative business rule assumed | UNKNOWN | Approve per-measure negative policy | Negative residual/stock values not automatically quarantined | RAW_PROFILE |
| DQ-052 | faostat/supply_utilization.csv |  | Value | iqr_outlier_candidate | 6364 | observations | row 4, column Value: 3475.33; row 12, column Value: 2111.61; row 20, column Value: -10521.01 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within Element, Unit, Item | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-053 | faostat/supply_utilization.csv |  | Flag | source_estimate_provenance | 28180 | records | row 2, column Flag: E; row 4, column Flag: E; row 5, column Flag: I | Keep flags and descriptions as provenance; do not promote estimates to official/final | raw flag + source status | KEEP_RAW | Flag labels/dictionary accompany raw observations | VERIFIED | Accept source status retention | No loss of flags or notes through canonical mapping | RAW_PROFILE |
| DQ-054 | faostat/supply_utilization.csv |  | release / geography / units | business_scope_metadata | 36380 | records | Source code/category distributions in STEP 6 | Approve geography aggregation membership, release identity and per-measure period/unit mapping | versioned dimensions + period_basis + source_vintage | PENDING_REVIEW | negative residual/stock policy, exact balance equation, processing basis and release identity | UNKNOWN | Approve source-specific contracts | Aggregation restricted to disjoint approved coverage | RAW_PROFILE |

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
| faostat/supply_utilization.csv | 1 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year | 36380 | 0 | 0 | 0 | 0 | 0 |
| faostat/supply_utilization.csv | 2 | Domain Code + Area Code (M49) + Item Code (CPC) + Element Code + Year + Unit | 36380 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-052 | iqr_outlier_candidate | 6364 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

negative residual/stock policy, exact balance equation, processing basis and release identity

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-051 | negative_measure | UNKNOWN | PENDING_REVIEW | Approve per-measure negative policy |
| DQ-052 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-054 | business_scope_metadata | UNKNOWN | PENDING_REVIEW | Approve source-specific contracts |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-003: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-050: Missing count reconciles; no introduced numeric zero
- [ ] DQ-051: Negative residual/stock values not automatically quarantined
- [ ] DQ-052: Canonical retention count unchanged for IQR signals
- [ ] DQ-053: No loss of flags or notes through canonical mapping
- [ ] DQ-054: Aggregation restricted to disjoint approved coverage
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

### faostat/supply_utilization.csv

Raw records: 36380; columns: 15; sheet/CSV physical rows: 36381; header rows: [1].

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

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu supply_utilization.csv, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Sự hiện diện hợp lệ của các Giá trị Âm trong Bảng cân đối (Valid Negative Measures)
- Có tới 1.516 quan sát mang giá trị âm (ví dụ: `-10,521.01`, `-15,000.0`, giá trị nhỏ nhất đạt `-6,425,000.0 t`).
- **Bản chất nghiệp vụ**: Dữ liệu thuộc bảng Cân đối Cung – Cầu (Supply Utilization Accounts - SUA). Các đại lượng như `Stock Variation` (Biến động tồn kho: cuối kỳ trừ đầu kỳ) hoặc `Residuals` (Sai số cân đối/điều chỉnh) mang giá trị âm khi lượng xuất kho lớn hơn nhập kho hoặc khi có hao hụt cân đối.
- **Vấn đề**: Giá trị âm mang ngữ nghĩa kế toán cân đối, không phải lỗi nhập liệu hay nhiễu kỹ thuật.

### 13.2. Mật độ giá trị 0 cao ở các kênh sử dụng đặc thù (High Frequency of Zero Observations)
- Có 8.660 quan sát mang giá trị `0.0`.
- **Bản chất**: Phản ánh các luồng sử dụng nông sản không phát sinh tại nhiều quốc gia (ví dụ lúa gạo dùng cho chăn nuôi `Feed`, sản xuất công nghiệp phi thực phẩm `Industrial use`, hoặc tiêu thụ du lịch `Tourist consumption` bằng 0).

### 13.3. Dữ liệu khuyết thiếu mang cờ xác thực (Blank Observations with Flag M)
- Có 257 quan sát có trường `Value` để trống hoàn toàn (rỗng), tất cả đều đi kèm `Flag = M` (`Data not available`). Không đồng nhất với giá trị `0.0`.

### 13.4. Hệ mã hóa hàng hóa CPC phức tạp có phần thập phân (Complex Decimal CPC Item Codes)
- Cột `Item Code (CPC)` chứa các mã phân loại chi tiết có phân tầng thập phân (ví dụ: `23161.02` - Milled paddy equivalents / broken rice) bên cạnh các mã có số 0 đầu (`0113` - Rice, paddy).
- **Vấn đề**: Nguy cơ sai lệch định danh nếu parser chuyển đổi trường mã thành kiểu số thực (float) hoặc làm tròn số.

### 13.5. Tỷ trọng dữ liệu ước tính và tính toán nội suy rất lớn (Dominance of Estimated & Imputed Flags)
- Có tới 28.180 trên tổng số 36.380 bản ghi (chiếm hơn 77% dữ liệu) mang cờ ước tính: `Flag = E` (FAO estimate) hoặc `Flag = I` (Imputed value). Chỉ một tỷ lệ nhỏ là số liệu thống kê chính thức của cơ quan hải quan/thống kê quốc gia.

### 13.6. Mất cân đối diện tích và sản lượng đối chiếu (Discrepancy with Production Domain)
- Xuất hiện 343 điểm bất tương thích về sản lượng khi đối chiếu với tệp `production_world.csv` cho cùng cặp quốc gia, hàng hóa và năm.
