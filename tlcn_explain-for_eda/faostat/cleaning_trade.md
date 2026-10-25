# Cleaning specification — cleaning_trade

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Detailed trade matrix

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | Detailed trade matrix | 1226470 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv | lookup/metadata | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv | lookup/metadata | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv | lookup/metadata | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv | lookup/metadata | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv | lookup/metadata | 0 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | observations | csv | utf-8-sig | 167990400 | 1226470 | 16 | 1226471 | 8fb828a43896c0df | 697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv |  | reference_data | csv | utf-8-sig | 236 | 10 | 2 | 11 | 6c6ca62af7a54514 | 21ea21944193371c33feaff5e9bddda4a0308517b47307178d1316c0c29bf4f6 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv |  | reference_data | csv | utf-8-sig | 133 | 4 | 2 | 5 | 96a47dfd514352f9 | 85e924f38861df798974e5f04d51997c4d0dc00a7ed09a7824af74a5af1d7c60 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv |  | reference_data | csv | utf-8-sig | 21739 | 572 | 3 | 573 | f060f55b85439aec | 1f6a7675ff0eb0fbd394ac498e6d4cdf4472cf0348549a9209bea62498986355 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv |  | reference_data | csv | utf-8-sig | 5769 | 255 | 3 | 256 | c4469d2206bfa481 | 1d40434b1bfbdfce056887190474cbad517ecf5bd3fd969e064fae0025c74301 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv |  | reference_data | csv | utf-8-sig | 5161 | 232 | 3 | 233 | 3d8be750c5f75f7a | 51371f9bab34b21286bb56bd16156166f34c8bd8691fb12220cd4f3bd79f2497 |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: reporter × partner × commodity/form × element × year × unit
- **time**: Year labels; calendar/trade coverage requires release documentation
- **geography**: reporter and partner source codes; country + region/world aggregates can coexist
- **measure**: import/export quantity (t) and value (1000 USD); mirror flows are distinct
- **units**: t → tonne; 1000 USD → USD ×1000 only for value elements; CPC prefix/decimal kept
- **additivity**: only disjoint partner, product and geographic coverage; never add import/export mirrors or paddy-milled equivalent and components



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | Value | 1226470 | 1226470 | 0 | 0 | 0 | 0 | 0 | 133948 | 0.0 | 5004090.0 | 214684 | Element, Unit, Item | {np.int64(1): 1005067, np.int64(2): 221403} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | Year | year candidate | 1986 | 2024 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | raw table | 1226470 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv | raw table | 10 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv | raw table | 4 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv | raw table | 572 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv | raw table | 255 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv | raw table | 232 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | Value | 0 | 0 | 0 | 0 | 133948 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1226470 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv | 20 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv | 8 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv | 1716 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv | 765 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv | 696 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 19623520 | 0 | 0 | 0 | 0 | 133948 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 2 | Reporter Country Code (M49) | category | text/empty | 122258 | 0 | False |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 5 | Partner Country Code (M49) | category | text/empty | 120486 | 0 | False |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 8 | Item Code (CPC) | category | text/empty | 74774 | 0 | False |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv | 2 | CPC Code | object | text/empty | 246 | 0 | False |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv | 2 | M49 Code | object | text/empty | 47 | 0 | False |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv | 2 | M49 Code | object | text/empty | 46 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-004 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Reporter Country Code (M49), Partner Country Code (M49), Item Code (CPC) | identifier_numeric_inference | 317518 | cells | [['Reporter Country Code (M49)', '["\'024", "\'072", "\'140", "\'204"]'], ['Partner Country Code (M49)', '["\'008", "\'004", "\'012", "\'024"]'], ['Item Code (CPC)', '["\'23162", "\'F0030", "\'23490.01", "\'23161.03"]']] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-005 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv |  | header | header_whitespace | 1 | headers | [' Description'] | Trim only canonical header alias; preserve raw header | canonical string field name | NORMALIZE | Literal leading/trailing spaces in source header | VERIFIED | Accept alias mapping | Canonical alias unique and raw name retained | RAW_PROFILE |
| DQ-006 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv |  | CPC Code | identifier_numeric_inference | 246 | cells | [['CPC Code', '["\'01929.07", "\'01371", "\'21422", "\'21529.03"]']] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-007 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv |  | M49 Code | identifier_numeric_inference | 47 | cells | [['M49 Code', '["\'004", "\'002", "\'008", "\'012"]']] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-008 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv |  | M49 Code | identifier_numeric_inference | 46 | cells | [['M49 Code', '["\'004", "\'002", "\'008", "\'012"]']] | Retain lexical codes as string; never numeric cast before dictionary validation | string codes | KEEP_RAW | Leading zeros present in raw lexical values | VERIFIED | Accept lexical-code schema | Round-trip code strings preserves leading zeros and CPC fractions/prefixes | RAW_PROFILE |
| DQ-055 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Value | iqr_outlier_candidate | 214684 | observations | row 34, column Value: 219.89; row 78, column Value: 5458.0; row 90, column Value: 5458.0 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within Element, Unit, Item | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-056 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Reporter Country Code (M49) | code_apostrophe_prefix | 1226470 | cells | row 2, column Reporter Country Code (M49): '024; row 3, column Reporter Country Code (M49): '024; row 4, column Reporter Country Code (M49): '024 | Propose removing exactly one leading apostrophe in canonical code after local dictionary match; raw code unchanged | string canonical_code + raw_code | NORMALIZE | Same protective prefix occurs in local country/item lookup CSV | PROPOSED | Approve source-code parser including alpha/decimal CPC | Join success and lexical leading zeros preserved | RAW_PROFILE |
| DQ-057 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Partner Country Code (M49) | code_apostrophe_prefix | 1226470 | cells | row 2, column Partner Country Code (M49): '008; row 3, column Partner Country Code (M49): '008; row 4, column Partner Country Code (M49): '008 | Propose removing exactly one leading apostrophe in canonical code after local dictionary match; raw code unchanged | string canonical_code + raw_code | NORMALIZE | Same protective prefix occurs in local country/item lookup CSV | PROPOSED | Approve source-code parser including alpha/decimal CPC | Join success and lexical leading zeros preserved | RAW_PROFILE |
| DQ-058 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Item Code (CPC) | code_apostrophe_prefix | 1226470 | cells | row 2, column Item Code (CPC): '23162; row 3, column Item Code (CPC): '23162; row 4, column Item Code (CPC): 'F0030 | Propose removing exactly one leading apostrophe in canonical code after local dictionary match; raw code unchanged | string canonical_code + raw_code | NORMALIZE | Same protective prefix occurs in local country/item lookup CSV | PROPOSED | Approve source-code parser including alpha/decimal CPC | Join success and lexical leading zeros preserved | RAW_PROFILE |
| DQ-059 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | Flag | source_estimate_provenance | 338879 | records | row 4, column Flag: E; row 5, column Flag: E; row 11, column Flag: X | Keep flags and descriptions as provenance; do not promote estimates to official/final | raw flag + source status | KEEP_RAW | Flag labels/dictionary accompany raw observations | VERIFIED | Accept source status retention | No loss of flags or notes through canonical mapping | RAW_PROFILE |
| DQ-060 | faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv |  | release / geography / units | business_scope_metadata | 1226470 | records | Source code/category distributions in STEP 6 | Approve geography aggregation membership, release identity and per-measure period/unit mapping | versioned dimensions + period_basis + source_vintage | PENDING_REVIEW | release identity, aggregate membership, calendar convention, bilateral mirror policy | UNKNOWN | Approve source-specific contracts | Aggregation restricted to disjoint approved coverage | RAW_PROFILE |

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
| reporter_code | string | source code system + raw M49 |
| partner_code | string | country or aggregate |
| item_code | string | preserve raw CPC/form |
| element_code | string | quantity/value and direction |
| reporting_year | integer | period basis pending |
| statistic_kind | string | quantity or trade value |
| flag | string | raw source estimate/status |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 1 | Reporter Country Code + Partner Country Code + Item Code + Element Code + Year | 1226470 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv | 2 | Reporter Country Code + Partner Country Code + Item Code + Element Code + Year + Unit | 1226470 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv | 1 | Element Code | 10 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv | 1 | Flag | 4 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv | 1 | Item Code | 572 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv | 1 | Partner Country Code | 255 | 0 | 0 | 0 | 0 | 0 |
| faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv | 1 | Reporter Country Code | 232 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-055 | iqr_outlier_candidate | 214684 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

release identity, aggregate membership, calendar convention, bilateral mirror policy

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-055 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-056 | code_apostrophe_prefix | PROPOSED | NORMALIZE | Approve source-code parser including alpha/decimal CPC |
| DQ-057 | code_apostrophe_prefix | PROPOSED | NORMALIZE | Approve source-code parser including alpha/decimal CPC |
| DQ-058 | code_apostrophe_prefix | PROPOSED | NORMALIZE | Approve source-code parser including alpha/decimal CPC |
| DQ-060 | business_scope_metadata | UNKNOWN | PENDING_REVIEW | Approve source-specific contracts |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-004: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-005: Canonical alias unique and raw name retained
- [ ] DQ-006: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-007: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-008: Round-trip code strings preserves leading zeros and CPC fractions/prefixes
- [ ] DQ-055: Canonical retention count unchanged for IQR signals
- [ ] DQ-056: Join success and lexical leading zeros preserved
- [ ] DQ-057: Join success and lexical leading zeros preserved
- [ ] DQ-058: Join success and lexical leading zeros preserved
- [ ] DQ-059: No loss of flags or notes through canonical mapping
- [ ] DQ-060: Aggregation restricted to disjoint approved coverage
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

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv

Raw records: 1226470; columns: 16; sheet/CSV physical rows: 1226471; header rows: [1].

Literal headers/positional grid labels:

```text
["Reporter Country Code", "Reporter Country Code (M49)", "Reporter Countries", "Partner Country Code", "Partner Country Code (M49)", "Partner Countries", "Item Code", "Item Code (CPC)", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]
```

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Elements.csv

Raw records: 10; columns: 2; sheet/CSV physical rows: 11; header rows: [1].

Literal headers/positional grid labels:

```text
["Element Code", "Element"]
```

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_Flags.csv

Raw records: 4; columns: 2; sheet/CSV physical rows: 5; header rows: [1].

Literal headers/positional grid labels:

```text
["Flag", " Description"]
```

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ItemCodes.csv

Raw records: 572; columns: 3; sheet/CSV physical rows: 573; header rows: [1].

Literal headers/positional grid labels:

```text
["Item Code", "CPC Code", "Item"]
```

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_PartnerCountries.csv

Raw records: 255; columns: 3; sheet/CSV physical rows: 256; header rows: [1].

Literal headers/positional grid labels:

```text
["Partner Country Code", "M49 Code", "Partner Countries"]
```

### faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/Trade_DetailedTradeMatrix_E_ReporterCountries.csv

Raw records: 232; columns: 3; sheet/CSV physical rows: 233; header rows: [1].

Literal headers/positional grid labels:

```text
["Reporter Country Code", "M49 Code", "Reporter Countries"]
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

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu rice_trade_data.csv và các bảng lookup, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Dị biệt ký tự tiền tố dấu nháy đơn `'` trong toàn bộ các cột mã định danh (Leading Apostrophe in Identifiers)
- Toàn bộ 1.226.470 bản ghi của 3 cột mã định danh:
  - `Reporter Country Code (M49)`: Ví dụ `'024`, `'072`, `'140`
  - `Partner Country Code (M49)`: Ví dụ `'008`, `'004`, `'012`
  - `Item Code (CPC)`: Ví dụ `'23162`, `'F0030`, `'23490.01`
  đều bị chèn một ký tự dấu nháy đơn `'` (ASCII 39) ngay đầu chuỗi byte.
- **Bản chất**: Đây là kỹ thuật định dạng xuất dữ liệu từ hệ thống nguồn nhằm ép trình đọc bảng tính hiểu là kiểu chuỗi (text protection), nhưng gây lỗi không khớp định danh (join failure) khi đối chiếu với các bảng danh mục chuẩn.

### 13.2. Khoảng trắng thừa trong tiêu đề bảng danh mục (Header Whitespace in Lookup Table)
- Trong tệp `Trade_DetailedTradeMatrix_E_Flags.csv`, tiêu đề cột thứ 2 bị dính khoảng trắng ở đầu chuỗi: `[' Description']` thay vì `['Description']`.

### 13.3. Hiện tượng luồng thương mại đối ứng không cân bằng (Asymmetry in Bilateral Mirror Trade Flows)
- Ma trận lưu trữ cả chiều Xuất khẩu (`Export Quantity`, `Export Value`) và chiều Nhập khẩu (`Import Quantity`, `Import Value`) giữa hai đối tác song phương.
- **Bản chất ngoại thương**: Xuất khẩu của nước A sang nước B thường không cân bằng với Nhập khẩu của nước B từ nước A (do chênh lệch thời gian vận tải biển, phương thức định giá CIF tại cảng đến vs FOB tại cảng đi, và độ trễ thủ tục hải quan).
- **Vấn đề**: Nguy cơ đúp số liệu nếu tổng hợp đồng thời cả luồng xuất và nhập mà không phân tách rõ chiều thương mại (flow direction).

### 13.4. Tỷ lệ bản ghi có giá trị bằng 0 rất lớn (High Zero-Value Volume)
- Có 133.948 quan sát mang giá trị `0.0`.
- **Bản chất**: Phản ánh các tuyến thương mại song phương đã từng thiết lập hạn ngạch hoặc danh mục mặt hàng nhưng không phát sinh giao dịch thực tế trong năm báo cáo.

### 13.5. Đa quy mô thang đo đơn vị giữa Khối lượng và Kim ngạch (Unit Scale Divergence)
- Khối lượng xuất nhập khẩu tính bằng tấn (`t`), trong khi Kim ngạch giá trị tính bằng nghìn đô la Mỹ (`1000 USD`).
- **Vấn đề**: Việc suy diễn đơn giá trung bình (Unit Value = Value / Quantity) đòi hỏi phải nhân hệ số quy đổi 1.000 để đưa về đơn vị chuẩn `USD/tấn`.
