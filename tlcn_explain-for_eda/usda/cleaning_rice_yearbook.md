# Cleaning specification — cleaning_rice_yearbook

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Rice Yearbook export price quotes

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | Rice Yearbook export price quotes | 13131 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv |  | observations | csv | utf-8-sig | 2122534 | 13131 | 10 | 13132 | e02baadd6db39821 | 7d3a0d77cf5fe5e6ad243cd9ed58983bff10f672c8b92aa9bd0b8e0c9ab261ba |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: table × commodity × class × YEAR × reference period × location × statistic × unit
- **time**: month labels and MARKETING aggregate; YEAR in August-start tables is not automatically calendar year
- **geography**: country quote origin/location, not necessarily production geography
- **measure**: FREIGHT ON BOARD PRICE, separate grades/classes and reporting periods
- **units**: DOLLARS PER METRIC TON; USD/tonne candidate supported by ERS series context, retain raw label
- **additivity**: prices nonadditive; MARKETING average not summed with monthly values; average weights unknown



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | VALUE | 13131 | 8537 | 4594 | 0 | 0 | 0 | 0 | 0 | 96.75 | 2000.0 | 141 | CLASS_DESCRIPTION, LOCATION_DESCRIPTION, UNIT_DESCRIPTION | {np.int64(1): 558, np.int64(2): 1026} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | YEAR | year candidate | 1985 | 2025 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | raw table | 13131 | 195 | 390 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | VALUE | 0 | 0 | 0 | 4594 | 0 | 0 | 0 | 0 | 4594 | 0 | 0 | 0 | 0 | 0 | 4594 | 13131 | 34.9859112024979 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | 131310 | 0 | 0 | 0 | 4594 | 0 |

_Không có bản ghi trong phạm vi phân tích._

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-036 | usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv |  | all fields | exact_duplicate_rows | 195 | excess records | row 7631, column record: DOLLARS PER METRIC TON; row 7632, column record: DOLLARS PER METRIC TON; row 7633, column record: DOLLARS PER METRIC TON | Propose lineage-preserving dedup only after repeat/release policy; retain all source references | one observation + duplicate lineage | PENDING_REVIEW | Exact full-row equality, no hash-only test | PROPOSED | Approve exact duplicate retention policy | Before/after difference equals approved duplicate excess | RAW_PROFILE |
| DQ-119 | usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv |  | VALUE | missing_token_candidate | 4594 | observations | row 2, column VALUE: NA; row 5, column VALUE: NA; row 6, column VALUE: NA | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-120 | usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv |  | VALUE | iqr_outlier_candidate | 141 | observations | row 2620, column VALUE: 329; row 2629, column VALUE: 346; row 2649, column VALUE: 929 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within CLASS_DESCRIPTION, LOCATION_DESCRIPTION, UNIT_DESCRIPTION | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-121 | usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv |  | YEAR / REFERENCE_PERIOD_DESCRIPTION | marketing_average_vs_month | 1022 | records | row 110, column REFERENCE_PERIOD_DESCRIPTION: MARKETING; row 111, column REFERENCE_PERIOD_DESCRIPTION: MARKETING; row 112, column REFERENCE_PERIOD_DESCRIPTION: MARKETING | Preserve MARKETING as annual marketing aggregate; other month-to-calendar conversions remain review-only | reporting_year + reference_period + frequency + period_basis | PENDING_REVIEW | Raw tables show 1985/86 or 1997/98 titles and MARKETING rows; ERS lists export quotes https://www.ers.usda.gov/data-products/rice-yearbook | PROPOSED | Approve per-table year/month convention and average weights | MARKETING never parsed as month; no aggregate/month double counting | RAW_PROFILE |

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
| table_number | string | retain table identity |
| commodity_raw | string | RICE |
| class_raw | string | grade/product specification |
| location_raw | string | quote origin |
| reporting_year | integer | Raw YEAR |
| reference_period_raw | string | month/MARKETING |
| frequency | string | month/marketing average |
| calendar_month | date nullable | requires approved YEAR convention |
| statistic_kind | string | FOB quote |
| currency | string | USD candidate |
| price_basis | string | metric tonne |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv | 1 | TABLE_NUMBER + COMMODITY_DESCRIPTION + CLASS_DESCRIPTION + YEAR + REFERENCE_PERIOD_DESCRIPTION + LOCATION_DESCRIPTION + STATISTIC_DESCRIPTION + UNIT_DESCRIPTION | 13131 | 0 | 390 | 195 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-120 | iqr_outlier_candidate | 141 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

NA semantics, per-table YEAR-to-calendar conversion and partial year expected coverage

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-036 | exact_duplicate_rows | PROPOSED | PENDING_REVIEW | Approve exact duplicate retention policy |
| DQ-119 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-120 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-121 | marketing_average_vs_month | PROPOSED | PENDING_REVIEW | Approve per-table year/month convention and average weights |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-036: Before/after difference equals approved duplicate excess
- [ ] DQ-119: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-120: Canonical retention count unchanged for IQR signals
- [ ] DQ-121: MARKETING never parsed as month; no aggregate/month double counting
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

### usda/Export-prices-Thailand-Vietnam-India-and-Pakistan.csv

Raw records: 13131; columns: 10; sheet/CSV physical rows: 13132; header rows: [1].

Literal headers/positional grid labels:

```text
["TABLE_NAME", "TABLE_NUMBER", "COMMODITY_DESCRIPTION", "CLASS_DESCRIPTION", "YEAR", "REFERENCE_PERIOD_DESCRIPTION", "LOCATION_DESCRIPTION", "STATISTIC_DESCRIPTION", "VALUE", "UNIT_DESCRIPTION"]
```

Relevant cross-source checks (diagnostic; incompatibilities retained):

| relation | status | matched_observations | finding | compatibility_limits |
| --- | --- | --- | --- | --- |
| USDA Milling Rate (.9999) ↔ milled / rough | VERIFIED_ENCODING | 66 | ratio range 0.6250..0.6600; max milled reconstruction delta 0.32 thousand tonnes | Official source evidence + same Raw period; 1/10000 applies only to Milling Rate, raw (1000 MT) label retained |
| USDA Yield ↔ rough production / harvested area | DIAGNOSTIC | 66 | max abs delta 0.00542614 t/ha | Observed rough-basis yield identity; never infer milled-basis yield from Commodity label alone |
| USDA PSD ↔ FAOSTAT production | INCOMPATIBLE | 0 | USDA has rough and milled production; FAOSTAT has Rice and t/ha conventions | USDA marketing/crop-year spans vs FAOSTAT reporting-year labels; no approved period/milling/geography mapping |
| World Bank rice quotes ↔ USDA Yearbook export quotes | PENDING_PERIOD_REVIEW | 0 | Both quote prices can have USD/tonne basis, but grade/FOB and quote sample differ | World Bank calendar month vs USDA reporting YEAR/month/MARKETING; approved per-table calendar mapping missing |

Official evidence for Milling Rate: [Vietnam annual 2010](https://apps.fas.usda.gov/newgainapi/api/report/downloadreportbyfilename?filename=Grain+and+Feed+Annual_Hanoi_Vietnam_4-5-2010.pdf), [TU1012 rice formula, printed page 17](https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Grain+and+Feed+Annual_Ankara_Turkey_03-01-2001.pdf). Cross-source inference uses these historical documents and same-Raw rough/milled ratios. The 1/10000 encoding is supported; per-year period starts and release status remain pending.

Không kế thừa quy tắc từ tài liệu/Silver cũ; mọi proposal có origin trong decision table.

## 13. Ghi chú các vấn đề và đặc thù dữ liệu (Data Anomalies & Issues Profile)


### 13.1. Trộn lẫn dữ liệu Tháng và Bình quân cả niên vụ (MARKETING Aggregate)
- Cột `REFERENCE_PERIOD_DESCRIPTION` chứa 12 tháng từ `August` (tháng 8) đến `July` (tháng 7 năm sau), và xen lẫn dòng tổng kết mang nhãn `MARKETING` (1.022 dòng).
- **Vấn đề**: Sự đồng tồn tại của mức chi tiết tháng và mức tổng hợp niên vụ trên cùng một cột dẫn đến rủi ro nhân đôi giá trị nếu thực hiện phép tính tổng hợp (AVG/SUM) theo năm mà không lọc tách trường này.

### 13.2. Chu kỳ niên vụ tiếp thị bắt đầu từ Tháng 8 (August-start Marketing Year)
- Bảng giá của USDA ERS tổ chức theo niên vụ lúa gạo quốc tế (bắt đầu từ tháng 8 năm trước đến tháng 7 năm sau).
- **Vấn đề**: Cột `YEAR` phản ánh năm niên vụ bắt đầu, không trùng khớp hoàn toàn với năm dương lịch (Calendar year).

### 13.3. Tỷ lệ khuyết thiếu dữ liệu rất cao (Token NA chiếm ~35% quan sát)
- Có **4.594 trên tổng số 13.131 dòng mang giá trị chuỗi `"NA"`** (tương đương 34,98%).
- **Bản chất**: Phản ánh các phẩm cấp gạo chưa phát sinh giao dịch trong các giai đoạn lịch sử trước đây (ví dụ các giống gạo thơm chất lượng cao mới lai tạo) hoặc giai đoạn một số quốc gia chưa mở cửa xuất khẩu gạo thương mại chính thức. Token `"NA"` mang ý nghĩa không phát sinh báo giá, không tương đương với mức giá bằng 0.

### 13.4. Trùng lặp dòng dữ liệu toàn phần (Exact Duplicate Rows)
- Có **195 dòng bị trùng lặp hoàn toàn** trên cả 10 cột (tổng cộng 390 bản ghi tham gia nhóm trùng lặp, quan sát tại các dòng như 7631, 7632, 7633...).

### 13.5. Phân hóa phẩm cấp gạo chi tiết (Class Specification & Price Dispersion)
- Cột `CLASS_DESCRIPTION` phân hóa nhiều phẩm cấp gạo khác nhau theo tỷ lệ tấm và đặc tính giống (Thái Lan 100% Grade B, 5% tấm, 25% tấm, Parboiled; Gạo Basmati vs Non-Basmati của Ấn Độ/Pakistan...).
- Mức giá FOB dao động từ 96.75 USD/tấn (giai đoạn thập niên 1980) đến 2.000 USD/tấn (gạo Basmati/gạo thơm đặc sản), phát sinh 141 tín hiệu IQR outlier.
