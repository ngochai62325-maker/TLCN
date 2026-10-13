# Cleaning specification — cleaning_schema_group_01

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Diện tích gieo trồng và sản lượng lúa cả năm

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| nso/V06.12.csv | Diện tích gieo trồng và sản lượng lúa cả năm | 560 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv |  | observations | csv | cp1252 | 6281 | 71 | 10 | 72 | d075850e303d8e13 | 22f59d3870d02142d655610894ee83d507a53723861ffee407425d69487ff5bb |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: statistic kind × reporting year × measure × season, national
- **time**: Reporting/crop year; So b? is provisional; no final status inferred for other rows
- **geography**: national table scope; source label retained
- **measure**: physical quantities and previous-year=100 development indices in separate rows
- **units**: physical headers contain Nghìn ha / Nghìn t?n; index rows use % previous-year=100 despite physical column titles
- **additivity**: physical quantities only over disjoint geographies/seasons; region totals overlap provinces; indices/yields nonadditive

### NSO schema membership and coverage

| id | schema_fingerprint | organization | raw_rows | nonstructural_rows | year_fields | year_span | coverage_fingerprint | coverage_labels | group |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | d075850e303d8e13 | statistic_year_measure_table | 71 | 70 | 0 | Nam values | 571d84f4533c6759 | 2 | schema_group_01 |

## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | raw_value | 560 | 560 | 0 | 0 | 0 | 0 | 0 | 0 | 86.3 | 45091.0 | 16 | raw_column, statistic_kind | {np.int64(1): 560} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | year_candidate | year candidate | 1990 | 2024 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| nso/V06.12.csv | raw table | 71 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | raw_value | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 560 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | 710 | 0 | 10 | 0 | 0 | 0 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | 1 | Giá tr? và Ch? s? phát tri?n | object | text/empty | 0 | 70 | True |
| nso/V06.12.csv | 2 | Nam | object | mixed numeric/text | 0 | 2 | False |
| nso/V06.12.csv | 3 | T?ng di?n tích (Nghìn ha) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 4 | Di?n tích Lúa  ?ông xuân (Nghìn ha) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 5 | Di?n tích Lúa  hè thu (Nghìn ha) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 6 | Di?n tích Lúa mùa (Nghìn ha) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 7 | T?ng s?n lu?ng (Nghìn t?n) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 8 | S?n lu?ng Lúa  ?ông xuân (Nghìn t?n) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 9 | S?n lu?ng Lúa  hè thu (Nghìn t?n) | object | numeric | 0 | 0 | True |
| nso/V06.12.csv | 10 | S?n lu?ng Lúa mùa (Nghìn t?n) | object | numeric | 0 | 0 | True |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-009 | nso/V06.12.csv |  | all fields | structural_blank_row | 1 | records | row 72, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-010 | nso/V06.12.csv |  | Giá tr? và Ch? s? phát tri?n | lossy_encoding_labels | 70 | records | row 2, column Giá tr? và Ch? s? phát tri?n: Giá tr?; row 3, column Giá tr? và Ch? s? phát tri?n: Giá tr?; row 4, column Giá tr? và Ch? s? phát tri?n: Giá tr? | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 9 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-061 | nso/V06.12.csv |  | raw_value | iqr_outlier_candidate | 16 | observations | row 2, column T?ng di?n tích (Nghìn ha): 6042.8; row 3, column T?ng di?n tích (Nghìn ha): 6302.8; row 2, column Di?n tích Lúa  ?ông xuân (Nghìn ha): 2073.6 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-062 | nso/V06.12.csv |  | period_raw | provisional_period | 16 | observations | row 36, column T?ng di?n tích (Nghìn ha): So b? 2024; row 71, column T?ng di?n tích (Nghìn ha): So b? 2024; row 36, column Di?n tích Lúa  ?ông xuân (Nghìn ha): So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-063 | nso/V06.12.csv |  | statistic_raw / raw_column | index_vs_physical_units | 280 | observations | row 37, column T?ng di?n tích (Nghìn ha): Ch? s? phát tri?n (Nam tru?c =100) - %; row 38, column T?ng di?n tích (Nghìn ha): Ch? s? phát tri?n (Nam tru?c =100) - %; row 39, column T?ng di?n tích (Nghìn ha): Ch? s? phát tri?n (Nam tru?c =100) - % | Interpret unit by row statistic AND column measure; index rows keep %/previous-year=100, never thousand-unit scaling | statistic_kind + base_period + percent_index | NORMALIZE | Raw statistic label explicitly contains previous-year =100 and % | VERIFIED | Approve row-aware unit mapping | Index observations unaffected by physical scaling factors | RAW_PROFILE |

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
| geography_raw | string | exact lossy/historical label |
| geography_code | string nullable | approved effective-dated map only |
| geography_level | string nullable | national/region/province awaiting mapping |
| boundary_version | string nullable | historical coverage |
| reporting_year | integer | parsed raw year label |
| period_raw | string | retain provisional label |
| release_status | string | provisional where explicit; otherwise unconfirmed |
| measure | string | source metadata/header |
| season | string | all-year/đông xuân/hè thu+thu đông/mùa, exact scope |
| statistic_kind | string | physical quantity vs development index |
| index_base | string nullable | previous year=100 |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.12.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 560 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-009 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-061 | iqr_outlier_candidate | 16 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

irreversible ? labels, historical boundaries and effective dates, matrix units, release status, hè thu + thu đông season coverage

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-010 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-061 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-009: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-010: No automatic '?' substitution or modern province assignment
- [ ] DQ-061: Canonical retention count unchanged for IQR signals
- [ ] DQ-062: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-063: Index observations unaffected by physical scaling factors
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
| Lineage: nso/V06.12.csv | PASS | Every projected observation points to original row and column |
| Milling factor identity | PASS | Milled ≈ rough×encoded/10000 within source integer reporting precision |

## 12. Mapping tới file Raw, observed rows/columns và cross-source evidence

CSV row = header-inclusive record ordinal (multiline record có thể khác physical line). Excel row = worksheet physical row. HTML row = table row ordinal (header row 1), không phải HTML text line. raw_column giữ literal field/year hoặc 1-based ordinal. Issue table cung cấp representative exact coordinates; toàn bộ masks/profiles ở notebook để drill down.

### nso/V06.12.csv

Raw records: 71; columns: 10; sheet/CSV physical rows: 72; header rows: [1].

Literal headers/positional grid labels:

```text
["Giá tr? và Ch? s? phát tri?n", "Nam", "T?ng di?n tích (Nghìn ha)", "Di?n tích Lúa  ?ông xuân (Nghìn ha)", "Di?n tích Lúa  hè thu (Nghìn ha)", "Di?n tích Lúa mùa (Nghìn ha)", "T?ng s?n lu?ng (Nghìn t?n)", "S?n lu?ng Lúa  ?ông xuân (Nghìn t?n)", "S?n lu?ng Lúa  hè thu (Nghìn t?n)", "S?n lu?ng Lúa mùa (Nghìn t?n)"]
```

Relevant cross-source checks (diagnostic; incompatibilities retained):

| relation | status | matched_observations | finding | compatibility_limits |
| --- | --- | --- | --- | --- |
| NSO national ↔ V06.13.csv | DIAGNOSTIC | 30 | 3 different values in overlapping annual years | Source metadata says annual area/production; exact first-row national label; equal values support but do not independently prove missing matrix unit |
| NSO national ↔ V06.15.csv | DIAGNOSTIC | 30 | 3 different values in overlapping annual years | Source metadata says annual area/production; exact first-row national label; equal values support but do not independently prove missing matrix unit |
| NSO area/yield/production: lúa cả năm phân theo địa phương | PENDING_UNIT_REVIEW | 2077 | hypothesis yield tạ/ha → production ≈ area × yield /10: median abs delta=0.2 raw production units | Matrix unit metadata absent; factor /10 is a diagnostic hypothesis, not approved conversion; exact label joins only |
| NSO area/yield/production: lúa đông xuân phân theo địa phương | PENDING_UNIT_REVIEW | 2073 | hypothesis yield tạ/ha → production ≈ area × yield /10: median abs delta=0.084 raw production units | Matrix unit metadata absent; factor /10 is a diagnostic hypothesis, not approved conversion; exact label joins only |
| NSO area/yield/production: lúa hè thu và thu đông phân theo địa phương | PENDING_UNIT_REVIEW | 1056 | hypothesis yield tạ/ha → production ≈ area × yield /10: median abs delta=0.104 raw production units | Matrix unit metadata absent; factor /10 is a diagnostic hypothesis, not approved conversion; exact label joins only |
| NSO area/yield/production: lúa mùa phân theo địa phương | PENDING_UNIT_REVIEW | 1968 | hypothesis yield tạ/ha → production ≈ area × yield /10: median abs delta=0.074 raw production units | Matrix unit metadata absent; factor /10 is a diagnostic hypothesis, not approved conversion; exact label joins only |

Không kế thừa quy tắc từ tài liệu/Silver cũ; mọi proposal có origin trong decision table.

## 13. Ghi chú các vấn đề và đặc thù dữ liệu NSO (Data Anomalies & Issues Profile)

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên dữ liệu gốc V06.12.csv, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Xung đột ngữ nghĩa Đơn vị và Tiêu đề cột (Index vs. Physical Measure Conflict)
- Bảng dữ liệu chứa 2 khối thống kê với bản chất đo lường hoàn toàn khác nhau nhưng dùng chung tiêu đề cột:
  - **Khối 1 (Dòng 2 – 36, Năm 1990 – 2024)**: Đo lường giá trị vật lý tuyệt đối (Diện tích tính bằng `Nghìn ha`, Sản lượng tính bằng `Nghìn tấn`).
  - **Khối 2 (Dòng 37 – 71, Năm 1990 – 2024)**: Đo lường tốc độ phát triển liên hoàn với nhãn `Ch? s? phát tri?n (Nam tru?c =100) - %`. Bản chất đây là tỷ lệ phần trăm so với năm liền kề trước đó.
- **Vấn đề**: Toàn bộ tiêu đề cột vật lý (`T?ng di?n tích (Nghìn ha)`, `T?ng s?n lu?ng (Nghìn t?n)`) vẫn hiển thị nguyên vẹn ở nửa dưới bảng, tạo ra mâu thuẫn ngữ nghĩa trực tiếp giữa nhãn cột và dữ liệu dòng.

### 13.2. Mất mát mã hóa ký tự gốc (Irreversible Lossy Encoding / Ký hiệu `?`)
- Dữ liệu bị lỗi bảng mã nghiêm trọng: Các nguyên âm tiếng Việt có dấu và phụ âm đặc thù bị thay thế trực tiếp bằng ký tự dấu hỏi ASCII `?` ngay trong byte của file (ví dụ: `Giá tr?`, `Ch? s?`, `Nam`, `T?ng di?n tích`, `?ông xuân`, `s?n lu?ng`).
- **Vấn đề**: Thông tin ký tự tiếng Việt nguyên bản đã bị phá hủy một chiều ở tầng xuất file nguồn, không thể phục hồi bằng cách đổi thuật toán decode (UTF-8, CP1252, Latin-1).

### 13.3. Dị biệt định dạng chu kỳ thời gian và Trạng thái phát hành (Non-standard Period Token)
- Hầu hết các dòng tại cột `Nam` chứa năm định dạng số nguyên 4 chữ số (`1990` đến `2023`).
- Riêng bản ghi năm 2024 xuất hiện với giá trị chuỗi kèm tiền tố: `So b? 2024` (Sơ bộ 2024) ở cả khối Giá trị (dòng 36) và khối Chỉ số (dòng 71).
- **Vấn đề**: Cột mang kiểu dữ liệu hỗn hợp (mixed numeric/text) và lồng ghép giữa giá trị thời gian (`2024`) với trạng thái vòng đời dữ liệu (`Sơ bộ`).

### 13.4. Dòng trắng cấu trúc cuối tệp (Structural Blank Row)
- Dòng thứ 72 (dòng cuối cùng của file CSV) là một dòng trống hoàn toàn (`empty/null` trên toàn bộ 10 cột).

### 13.5. Phân hóa thang đo giá trị (Scale Disparity & IQR Outlier Signals)
- Do khối Giá trị (dao động từ hàng nghìn đến chục nghìn: `6,042.8` đến `45,091.0`) và khối Chỉ số (dao động quanh ngưỡng 100: `86.3` đến `114.8`) cùng nằm trên một cột, các giá trị của từng khối tạo ra độ lệch phân phối rất lớn (phát sinh 16 cảnh báo IQR outlier).
