# Cleaning specification — cleaning_schema_group_02

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Diện tích lúa cả năm phân theo địa phương

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| nso/V06.13.csv | Diện tích lúa cả năm phân theo địa phương | 2130 |
| nso/V06.14.csv | Năng suất gieo trồng lúa cả năm phân theo địa phương | 2130 |
| nso/V06.15.csv | Sản lượng lúa cả năm phân theo địa phương | 2130 |
| nso/V06.16.csv | Diện tích gieo trồng lúa đông xuân phân theo địa phương | 2130 |
| nso/V06.17.csv | Năng suất gieo trồng lúa đông xuân phân theo địa phương | 2130 |
| nso/V06.18.csv | Sản lượng lúa đông xuân phân theo địa phương | 2130 |
| nso/V06.19.csv | Diện tích gieo trồng lúa hè thu và thu đông phân theo địa phương | 1110 |
| nso/V06.20.csv | Năng suất gieo trồng lúa hè thu và thu đông phân theo địa phương | 1110 |
| nso/V06.21.csv | Sản lượng lúa hè thu và thu đông phân theo địa phương | 1110 |
| nso/V06.22.csv | Diện tích gieo trồng lúa mùa phân theo địa phương | 2130 |
| nso/V06.23.csv | Năng suất gieo trồng lúa mùa phân theo địa phương | 2130 |
| nso/V06.24.csv | Sản lượng lúa mùa phân theo địa phương | 2130 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv |  | observations | csv | cp1252 | 12726 | 72 | 31 | 73 | f845e3b32c566892 | 4aeba037c50f5ff2ff091f4b8804e3a39753ef008da9b83da52036c5e0ea7110 |
| nso/V06.14.csv |  | observations | csv | cp1252 | 11766 | 72 | 31 | 73 | f845e3b32c566892 | 8009fef207bec209d55e6cae1ef266916132d8055894a28bf792a8bafdaf52dc |
| nso/V06.15.csv |  | observations | csv | cp1252 | 14205 | 72 | 31 | 73 | f845e3b32c566892 | a3c8dc9ab522550ff21c3d9da551cfd549bd1b438ea40150c11573e4ca268af7 |
| nso/V06.16.csv |  | observations | csv | cp1252 | 11726 | 72 | 31 | 73 | f845e3b32c566892 | 57aeb150faa8b93b96db737c615001e76f14d62ed58553bb96ea69252b93f9a5 |
| nso/V06.17.csv |  | observations | csv | cp1252 | 11738 | 72 | 31 | 73 | f845e3b32c566892 | fbbb470e19ba52959bc81274d07f46d2517bf111cc5a979aa54e17e5e38585be |
| nso/V06.18.csv |  | observations | csv | cp1252 | 13393 | 72 | 31 | 73 | f845e3b32c566892 | 07fdf3300a30e648fc07a66afbafa3381558c9fe75c229388136c79394096d84 |
| nso/V06.19.csv |  | observations | csv | cp1252 | 6462 | 38 | 31 | 39 | f845e3b32c566892 | b22d8125db87de5934a723e5f23baf07d4f59358836368200dccfb2282a4f5e6 |
| nso/V06.20.csv |  | observations | csv | cp1252 | 6203 | 38 | 31 | 39 | f845e3b32c566892 | b88d9b821c7c894bbf822b09ca3682b670e3a31a82d39a1e9cd3be1e5f8014c8 |
| nso/V06.21.csv |  | observations | csv | cp1252 | 7127 | 38 | 31 | 39 | f845e3b32c566892 | 0eeb6628eb6cf986a34225b333ba53567b998fcd05e49bfb21c82eca6944fcab |
| nso/V06.22.csv |  | observations | csv | cp1252 | 11467 | 72 | 31 | 73 | f845e3b32c566892 | fc2d3eb16b634c3bf81872c42a883cbc6c5a6bc8dbd1e3d239c0586238ec34b7 |
| nso/V06.23.csv |  | observations | csv | cp1252 | 11505 | 72 | 31 | 73 | f845e3b32c566892 | 6340d6d096530b871117179cabf2e3acc6c9a102b03a83b2783ad14f8a385f76 |
| nso/V06.24.csv |  | observations | csv | cp1252 | 12702 | 72 | 31 | 73 | f845e3b32c566892 | 4d5cd0da34b5278a6452f577b4eb22fbf031f59a209a2f660a9db9a75d95de9b |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: raw geography label × year-column × file measure/season
- **time**: Reporting/crop year; So b? is provisional; no final status inferred for other rows
- **geography**: national/regions/provinces mixed, historical labels retained without modern code assignment
- **measure**: measure/season from metadata.txt; identical schema does not mean identical measure
- **units**: units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence
- **additivity**: physical quantities only over disjoint geographies/seasons; region totals overlap provinces; indices/yields nonadditive

### NSO schema membership and coverage

| id | schema_fingerprint | organization | raw_rows | nonstructural_rows | year_fields | year_span | coverage_fingerprint | coverage_labels | group |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.14.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.15.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.16.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.17.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.18.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.19.csv | f845e3b32c566892 | geography_year_matrix | 38 | 37 | 30 | 1995 → So b? 2024 | 1619f3df9cc6ea44 | 37 | schema_group_02 |
| nso/V06.20.csv | f845e3b32c566892 | geography_year_matrix | 38 | 37 | 30 | 1995 → So b? 2024 | 1619f3df9cc6ea44 | 37 | schema_group_02 |
| nso/V06.21.csv | f845e3b32c566892 | geography_year_matrix | 38 | 37 | 30 | 1995 → So b? 2024 | 1619f3df9cc6ea44 | 37 | schema_group_02 |
| nso/V06.22.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.23.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |
| nso/V06.24.csv | f845e3b32c566892 | geography_year_matrix | 72 | 71 | 30 | 1995 → So b? 2024 | 0c07a9a4e814b5e2 | 71 | schema_group_02 |

## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | raw_value | 2130 | 2086 | 44 | 0 | 0 | 0 | 0 | 0 | 4.6 | 7902.5 | 268 | raw_column, statistic_kind | {np.int64(1): 2086} |
| nso/V06.14.csv | raw_value | 2130 | 2086 | 44 | 0 | 0 | 0 | 0 | 0 | 15.3 | 71.9 | 48 | raw_column, statistic_kind | {np.int64(1): 2086} |
| nso/V06.15.csv | raw_value | 2130 | 2086 | 44 | 0 | 0 | 0 | 0 | 0 | 24.0 | 45091.0 | 256 | raw_column, statistic_kind | {np.int64(1): 2086} |
| nso/V06.16.csv | raw_value | 2130 | 2073 | 57 | 0 | 0 | 0 | 0 | 0 | 0.4 | 3168.0 | 277 | raw_column, statistic_kind | {np.int64(1): 2073} |
| nso/V06.17.csv | raw_value | 2130 | 2073 | 57 | 0 | 0 | 0 | 0 | 0 | 0.4 | 78.2 | 32 | raw_column, statistic_kind | {np.int64(1): 2073} |
| nso/V06.18.csv | raw_value | 2130 | 2073 | 57 | 0 | 0 | 0 | 0 | 0 | 0.2 | 21091.7 | 276 | raw_column, statistic_kind | {np.int64(1): 2073} |
| nso/V06.19.csv | raw_value | 1110 | 1059 | 51 | 0 | 0 | 0 | 0 | 3 | 0.0 | 2876.7 | 106 | raw_column, statistic_kind | {np.int64(1): 1059} |
| nso/V06.20.csv | raw_value | 1110 | 1056 | 54 | 0 | 0 | 0 | 0 | 0 | 16.1 | 73.1 | 37 | raw_column, statistic_kind | {np.int64(1): 1056} |
| nso/V06.21.csv | raw_value | 1110 | 1058 | 52 | 0 | 0 | 0 | 0 | 2 | 0.0 | 15461.2 | 128 | raw_column, statistic_kind | {np.int64(1): 1058} |
| nso/V06.22.csv | raw_value | 2130 | 1968 | 162 | 0 | 0 | 0 | 0 | 0 | 0.1 | 2601.9 | 222 | raw_column, statistic_kind | {np.int64(1): 1968} |
| nso/V06.23.csv | raw_value | 2130 | 1968 | 162 | 0 | 0 | 0 | 0 | 0 | 3.3 | 67.8 | 68 | raw_column, statistic_kind | {np.int64(1): 1968} |
| nso/V06.24.csv | raw_value | 2130 | 1968 | 162 | 0 | 0 | 0 | 0 | 0 | 0.2 | 9644.9 | 202 | raw_column, statistic_kind | {np.int64(1): 1968} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.14.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.15.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.16.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.17.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.18.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.19.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.20.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.21.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.22.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.23.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |
| nso/V06.24.csv | year_candidate | year candidate | 1995 | 2024 | 0 | not inferred without expected coverage |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| nso/V06.13.csv | raw table | 72 | 0 | 0 |
| nso/V06.14.csv | raw table | 72 | 0 | 0 |
| nso/V06.15.csv | raw table | 72 | 0 | 0 |
| nso/V06.16.csv | raw table | 72 | 0 | 0 |
| nso/V06.17.csv | raw table | 72 | 0 | 0 |
| nso/V06.18.csv | raw table | 72 | 0 | 0 |
| nso/V06.19.csv | raw table | 38 | 0 | 0 |
| nso/V06.20.csv | raw table | 38 | 0 | 0 |
| nso/V06.21.csv | raw table | 38 | 0 | 0 |
| nso/V06.22.csv | raw table | 72 | 0 | 0 |
| nso/V06.23.csv | raw table | 72 | 0 | 0 |
| nso/V06.24.csv | raw table | 72 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | raw_value | 0 | 0 | 0 | 44 | 0 | 44 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 44 | 2130 | 2.0657276995305165 |
| nso/V06.14.csv | raw_value | 0 | 0 | 0 | 44 | 0 | 44 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 44 | 2130 | 2.0657276995305165 |
| nso/V06.15.csv | raw_value | 0 | 0 | 0 | 44 | 0 | 44 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 44 | 2130 | 2.0657276995305165 |
| nso/V06.16.csv | raw_value | 0 | 0 | 0 | 57 | 0 | 57 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 57 | 2130 | 2.676056338028169 |
| nso/V06.17.csv | raw_value | 0 | 0 | 0 | 57 | 0 | 57 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 57 | 2130 | 2.676056338028169 |
| nso/V06.18.csv | raw_value | 0 | 0 | 0 | 57 | 0 | 57 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 57 | 2130 | 2.676056338028169 |
| nso/V06.19.csv | raw_value | 0 | 0 | 0 | 51 | 3 | 51 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 51 | 1110 | 4.594594594594595 |
| nso/V06.20.csv | raw_value | 0 | 0 | 0 | 54 | 0 | 54 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 54 | 1110 | 4.864864864864865 |
| nso/V06.21.csv | raw_value | 0 | 0 | 0 | 52 | 2 | 52 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 52 | 1110 | 4.684684684684685 |
| nso/V06.22.csv | raw_value | 0 | 0 | 0 | 162 | 0 | 162 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 162 | 2130 | 7.605633802816901 |
| nso/V06.23.csv | raw_value | 0 | 0 | 0 | 162 | 0 | 162 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 162 | 2130 | 7.605633802816901 |
| nso/V06.24.csv | raw_value | 0 | 0 | 0 | 162 | 0 | 162 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 162 | 2130 | 7.605633802816901 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | 2232 | 0 | 31 | 0 | 44 | 0 |
| nso/V06.14.csv | 2232 | 0 | 31 | 0 | 44 | 0 |
| nso/V06.15.csv | 2232 | 0 | 31 | 0 | 44 | 0 |
| nso/V06.16.csv | 2232 | 0 | 31 | 0 | 57 | 0 |
| nso/V06.17.csv | 2232 | 0 | 31 | 0 | 57 | 0 |
| nso/V06.18.csv | 2232 | 0 | 31 | 0 | 57 | 0 |
| nso/V06.19.csv | 1178 | 0 | 31 | 0 | 51 | 3 |
| nso/V06.20.csv | 1178 | 0 | 31 | 0 | 54 | 0 |
| nso/V06.21.csv | 1178 | 0 | 31 | 0 | 52 | 2 |
| nso/V06.22.csv | 2232 | 0 | 31 | 0 | 162 | 0 |
| nso/V06.23.csv | 2232 | 0 | 31 | 0 | 162 | 0 |
| nso/V06.24.csv | 2232 | 0 | 31 | 0 | 162 | 0 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nso/V06.13.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.13.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.14.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.14.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.15.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.15.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.16.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.16.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.17.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.17.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.18.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.18.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.19.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 24 | True |
| nso/V06.19.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.20.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 24 | True |
| nso/V06.20.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.21.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 24 | True |
| nso/V06.21.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.22.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.22.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.23.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.23.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |
| nso/V06.24.csv | 1 | T?nh, thành ph? | object | text/empty | 0 | 41 | True |
| nso/V06.24.csv | 31 | So b? 2024 | object | numeric | 0 | 0 | True |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-011 | nso/V06.13.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-012 | nso/V06.13.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-013 | nso/V06.14.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-014 | nso/V06.14.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-015 | nso/V06.15.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-016 | nso/V06.15.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-017 | nso/V06.16.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-018 | nso/V06.16.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-019 | nso/V06.17.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-020 | nso/V06.17.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-021 | nso/V06.18.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-022 | nso/V06.18.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-023 | nso/V06.19.csv |  | all fields | structural_blank_row | 1 | records | row 39, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-024 | nso/V06.19.csv |  | T?nh, thành ph? | lossy_encoding_labels | 24 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column T?nh, thành ph?: Ngh? An | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-025 | nso/V06.20.csv |  | all fields | structural_blank_row | 1 | records | row 39, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-026 | nso/V06.20.csv |  | T?nh, thành ph? | lossy_encoding_labels | 24 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column T?nh, thành ph?: Ngh? An | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-027 | nso/V06.21.csv |  | all fields | structural_blank_row | 1 | records | row 39, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-028 | nso/V06.21.csv |  | T?nh, thành ph? | lossy_encoding_labels | 24 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column T?nh, thành ph?: Ngh? An | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-029 | nso/V06.22.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-030 | nso/V06.22.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-031 | nso/V06.23.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-032 | nso/V06.23.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-033 | nso/V06.24.csv |  | all fields | structural_blank_row | 1 | records | row 73, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-034 | nso/V06.24.csv |  | T?nh, thành ph? | lossy_encoding_labels | 41 | records | row 2, column T?nh, thành ph?: C? NU?C; row 3, column T?nh, thành ph?: Ð?ng b?ng sông H?ng; row 4, column T?nh, thành ph?: Hà N?i | Keep exact raw labels; request original Unicode export/approved exact dictionary; never fuzzy-map | string raw_label + nullable reviewed code | PENDING_REVIEW | UTF-8 decode fails; '?' already in bytes; 2 suspect headers. Decoder cp1252 is candidate only | UNKNOWN | Approve original source recovery and historical geography dictionary | No automatic '?' substitution or modern province assignment | RAW_PROFILE |
| DQ-064 | nso/V06.13.csv |  | raw_value | missing_token_candidate | 44 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-065 | nso/V06.13.csv |  | raw_value | iqr_outlier_candidate | 268 | observations | row 2, column 1995: 6765.6; row 3, column 1995: 1238.1; row 16, column 1995: 611.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-066 | nso/V06.13.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-067 | nso/V06.13.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-068 | nso/V06.14.csv |  | raw_value | missing_token_candidate | 44 | observations | row 28, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-069 | nso/V06.14.csv |  | raw_value | iqr_outlier_candidate | 48 | observations | row 11, column 1999: 56.8; row 12, column 1999: 61.6; row 14, column 1999: 58.8 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-070 | nso/V06.14.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-071 | nso/V06.14.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-072 | nso/V06.15.csv |  | raw_value | missing_token_candidate | 44 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-073 | nso/V06.15.csv |  | raw_value | iqr_outlier_candidate | 256 | observations | row 2, column 1995: 24963.7; row 3, column 1995: 5207.1; row 16, column 1995: 1669.8 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-074 | nso/V06.15.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-075 | nso/V06.15.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-076 | nso/V06.16.csv |  | raw_value | missing_token_candidate | 57 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-077 | nso/V06.16.csv |  | raw_value | iqr_outlier_candidate | 277 | observations | row 2, column 1995: 2421.3; row 3, column 1995: 605.4; row 16, column 1995: 193.0 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-078 | nso/V06.16.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-079 | nso/V06.16.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-080 | nso/V06.17.csv |  | raw_value | missing_token_candidate | 57 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-081 | nso/V06.17.csv |  | raw_value | iqr_outlier_candidate | 32 | observations | row 12, column 1996: 68.9; row 72, column 1998: 13.2; row 53, column 2002: 23.3 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-082 | nso/V06.17.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-083 | nso/V06.17.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-084 | nso/V06.18.csv |  | raw_value | missing_token_candidate | 57 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-085 | nso/V06.18.csv |  | raw_value | iqr_outlier_candidate | 276 | observations | row 2, column 1995: 10736.6; row 3, column 1995: 2692.4; row 31, column 1995: 1809.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-086 | nso/V06.18.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-087 | nso/V06.18.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-088 | nso/V06.19.csv |  | raw_value | missing_token_candidate | 51 | observations | row 18, column 1995: ..; row 35, column 1995: ..; row 35, column 1996: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-089 | nso/V06.19.csv |  | raw_value | iqr_outlier_candidate | 106 | observations | row 2, column 1995: 1742.4; row 25, column 1995: 1397.6; row 2, column 1996: 1984.2 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-090 | nso/V06.19.csv |  | period_raw | provisional_period | 37 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-091 | nso/V06.19.csv |  | geography / units / season | historical_geography_and_missing_units | 1110 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column 1995: Ngh? An | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-092 | nso/V06.20.csv |  | raw_value | missing_token_candidate | 54 | observations | row 17, column 1995: ..; row 18, column 1995: ..; row 35, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-093 | nso/V06.20.csv |  | raw_value | iqr_outlier_candidate | 37 | observations | row 13, column 1995: 51.3; row 9, column 1996: 49.0; row 13, column 1996: 51.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-094 | nso/V06.20.csv |  | period_raw | provisional_period | 37 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-095 | nso/V06.20.csv |  | geography / units / season | historical_geography_and_missing_units | 1110 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column 1995: Ngh? An | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-096 | nso/V06.21.csv |  | raw_value | missing_token_candidate | 52 | observations | row 18, column 1995: ..; row 35, column 1995: ..; row 18, column 1996: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-097 | nso/V06.21.csv |  | raw_value | iqr_outlier_candidate | 128 | observations | row 2, column 1995: 6500.8; row 25, column 1995: 5296.4; row 2, column 1996: 6878.5 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-098 | nso/V06.21.csv |  | period_raw | provisional_period | 37 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-099 | nso/V06.21.csv |  | geography / units / season | historical_geography_and_missing_units | 1110 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: B?c Trung B? và Duyên h?i mi?n Trung; row 4, column 1995: Ngh? An | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-100 | nso/V06.22.csv |  | raw_value | missing_token_candidate | 162 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-101 | nso/V06.22.csv |  | raw_value | iqr_outlier_candidate | 222 | observations | row 2, column 1995: 2601.9; row 3, column 1995: 632.7; row 16, column 1995: 418.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-102 | nso/V06.22.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-103 | nso/V06.22.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-104 | nso/V06.23.csv |  | raw_value | missing_token_candidate | 162 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-105 | nso/V06.23.csv |  | raw_value | iqr_outlier_candidate | 68 | observations | row 12, column 1995: 50.8; row 36, column 1995: 7.3; row 63, column 1996: 49.6 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-106 | nso/V06.23.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-107 | nso/V06.23.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |
| DQ-108 | nso/V06.24.csv |  | raw_value | missing_token_candidate | 162 | observations | row 27, column 1995: ..; row 50, column 1995: ..; row 69, column 1995: .. | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-109 | nso/V06.24.csv |  | raw_value | iqr_outlier_candidate | 202 | observations | row 2, column 1995: 7726.3; row 3, column 1995: 2511.4; row 16, column 1995: 1094.7 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within raw_column, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-110 | nso/V06.24.csv |  | period_raw | provisional_period | 71 | observations | row 2, column So b? 2024: So b? 2024; row 3, column So b? 2024: So b? 2024; row 4, column So b? 2024: So b? 2024 | Retain reporting year candidate and explicit provisional status; no so_b → final | integer year + provisional status + raw period | NORMALIZE | Literal So b? marker in raw period label | VERIFIED | Accept provisional representation | All So b? observations tagged provisional; no unproved final status | RAW_PROFILE |
| DQ-111 | nso/V06.24.csv |  | geography / units / season | historical_geography_and_missing_units | 2130 | observations | row 2, column 1995: C? NU?C; row 3, column 1995: Ð?ng b?ng sông H?ng; row 4, column 1995: Hà N?i | Keep exact historical labels and mixed levels; require effective-dated exact mapping and official matrix unit metadata | nullable geography code/level + season + raw unit | PENDING_REVIEW | units absent in matrix CSV; candidates thousand ha/thousand t and yield tạ/ha require official unit evidence; measure/season titles from nso/metadata.txt | UNKNOWN | Approve historical dictionary, matrix units and seasonal scope | No fuzzy mapping, no current province code backfill, no region/province double counting | RAW_SOURCE_METADATA |

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
| nso/V06.13.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.14.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.15.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.16.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.17.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.18.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.19.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 1110 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.20.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 1110 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.21.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 1110 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.22.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.23.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |
| nso/V06.24.csv | 1 | geography_raw + period_raw + raw_column + statistic_kind | 2130 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-011 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-013 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-015 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-017 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-019 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-021 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-023 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-025 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-027 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-029 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-031 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-033 | structural_blank_row | 1 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-065 | iqr_outlier_candidate | 268 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-069 | iqr_outlier_candidate | 48 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-073 | iqr_outlier_candidate | 256 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-077 | iqr_outlier_candidate | 277 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-081 | iqr_outlier_candidate | 32 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-085 | iqr_outlier_candidate | 276 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-089 | iqr_outlier_candidate | 106 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-093 | iqr_outlier_candidate | 37 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-097 | iqr_outlier_candidate | 128 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-101 | iqr_outlier_candidate | 222 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-105 | iqr_outlier_candidate | 68 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-109 | iqr_outlier_candidate | 202 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

irreversible ? labels, historical boundaries and effective dates, matrix units, release status, hè thu + thu đông season coverage

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-012 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-014 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-016 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-018 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-020 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-022 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-024 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-026 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-028 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-030 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-032 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-034 | lossy_encoding_labels | UNKNOWN | PENDING_REVIEW | Approve original source recovery and historical geography dictionary |
| DQ-064 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-065 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-067 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-068 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-069 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-071 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-072 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-073 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-075 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-076 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-077 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-079 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-080 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-081 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-083 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-084 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-085 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-087 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-088 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-089 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-091 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-092 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-093 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-095 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-096 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-097 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-099 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-100 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-101 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-103 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-104 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-105 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-107 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |
| DQ-108 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-109 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-111 | historical_geography_and_missing_units | UNKNOWN | PENDING_REVIEW | Approve historical dictionary, matrix units and seasonal scope |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-011: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-012: No automatic '?' substitution or modern province assignment
- [ ] DQ-013: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-014: No automatic '?' substitution or modern province assignment
- [ ] DQ-015: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-016: No automatic '?' substitution or modern province assignment
- [ ] DQ-017: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-018: No automatic '?' substitution or modern province assignment
- [ ] DQ-019: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-020: No automatic '?' substitution or modern province assignment
- [ ] DQ-021: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-022: No automatic '?' substitution or modern province assignment
- [ ] DQ-023: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-024: No automatic '?' substitution or modern province assignment
- [ ] DQ-025: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-026: No automatic '?' substitution or modern province assignment
- [ ] DQ-027: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-028: No automatic '?' substitution or modern province assignment
- [ ] DQ-029: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-030: No automatic '?' substitution or modern province assignment
- [ ] DQ-031: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-032: No automatic '?' substitution or modern province assignment
- [ ] DQ-033: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-034: No automatic '?' substitution or modern province assignment
- [ ] DQ-064: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-065: Canonical retention count unchanged for IQR signals
- [ ] DQ-066: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-067: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-068: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-069: Canonical retention count unchanged for IQR signals
- [ ] DQ-070: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-071: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-072: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-073: Canonical retention count unchanged for IQR signals
- [ ] DQ-074: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-075: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-076: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-077: Canonical retention count unchanged for IQR signals
- [ ] DQ-078: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-079: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-080: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-081: Canonical retention count unchanged for IQR signals
- [ ] DQ-082: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-083: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-084: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-085: Canonical retention count unchanged for IQR signals
- [ ] DQ-086: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-087: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-088: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-089: Canonical retention count unchanged for IQR signals
- [ ] DQ-090: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-091: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-092: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-093: Canonical retention count unchanged for IQR signals
- [ ] DQ-094: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-095: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-096: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-097: Canonical retention count unchanged for IQR signals
- [ ] DQ-098: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-099: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-100: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-101: Canonical retention count unchanged for IQR signals
- [ ] DQ-102: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-103: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-104: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-105: Canonical retention count unchanged for IQR signals
- [ ] DQ-106: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-107: No fuzzy mapping, no current province code backfill, no region/province double counting
- [ ] DQ-108: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-109: Canonical retention count unchanged for IQR signals
- [ ] DQ-110: All So b? observations tagged provisional; no unproved final status
- [ ] DQ-111: No fuzzy mapping, no current province code backfill, no region/province double counting
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
| Lineage: nso/V06.13.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.14.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.15.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.16.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.17.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.18.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.19.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.20.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.21.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.22.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.23.csv | PASS | Every projected observation points to original row and column |
| Lineage: nso/V06.24.csv | PASS | Every projected observation points to original row and column |
| Milling factor identity | PASS | Milled ≈ rough×encoded/10000 within source integer reporting precision |

## 12. Mapping tới file Raw, observed rows/columns và cross-source evidence

CSV row = header-inclusive record ordinal (multiline record có thể khác physical line). Excel row = worksheet physical row. HTML row = table row ordinal (header row 1), không phải HTML text line. raw_column giữ literal field/year hoặc 1-based ordinal. Issue table cung cấp representative exact coordinates; toàn bộ masks/profiles ở notebook để drill down.

### nso/V06.13.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.14.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.15.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.16.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.17.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.18.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.19.csv

Raw records: 38; columns: 31; sheet/CSV physical rows: 39; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.20.csv

Raw records: 38; columns: 31; sheet/CSV physical rows: 39; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.21.csv

Raw records: 38; columns: 31; sheet/CSV physical rows: 39; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.22.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.23.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
```

### nso/V06.24.csv

Raw records: 72; columns: 31; sheet/CSV physical rows: 73; header rows: [1].

Literal headers/positional grid labels:

```text
["T?nh, thành ph?", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "So b? 2024"]
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

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu V06.13.csv đến V06.24.csv, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Pha trộn đa cấp bậc hành chính trong cùng một cột địa lý (Mixed Administrative Granularity)
- Cột `T?nh, thành ph?` chứa đồng thời 3 cấp bậc địa lý khác nhau trên cùng một trục dữ liệu mà không có cột phân loại cấp hành chính:
  - **Cấp toàn quốc (Cấp 0)**: Bản ghi `C? NU?C`.
  - **Cấp vùng kinh tế/sinh thái (Cấp 1)**: 6 vùng kinh tế (ví dụ: `Ð?ng b?ng sông H?ng`, `B?c Trung B? và Duyên h?i mi?n Trung`, `Ð?ng b?ng sông C?u Long`...).
  - **Cấp tỉnh / thành phố trực thuộc Trung ương (Cấp 2)**: Các đơn vị hành chính cấp tỉnh (`Hà N?i`, `B?c Ninh`, `H?i Phòng`...).
- **Vấn đề**: Cột dữ liệu chứa cả phần tử cha và các phần tử con, dẫn đến nguy cơ nhân ba (triple-counting) số liệu thực tế nếu áp dụng phép tổng hợp (aggregation/SUM) trên toàn bộ cột.

### 13.2. Biến động địa giới hành chính theo dòng lịch sử (Historical Boundary Dynamics)
- **Hiện tượng sáp nhập tỉnh**:
  - Tỉnh Hà Tây (`Hà Tây`) có số liệu thực từ năm 1995 đến năm 2007; từ năm 2008 trở đi toàn bộ chuỗi số liệu mang ký hiệu `..` (do sáp nhập vào Hà Nội từ ngày 01/08/2008 theo Nghị quyết số 15/2008/QH12).
  - Ngay tại mốc năm 2008, số liệu của Hà Nội tăng đột biến (ví dụ ở file diện tích `V06.13.csv`, Hà Nội từ `51.3` nghìn ha năm 2007 nhảy vọt lên `206.9` nghìn ha năm 2008).
- **Hiện tượng chia tách tỉnh (Mốc 2004)**:
  - Các sự kiện chia tách địa giới hành chính năm 2004 (Cần Thơ tách thành TP Cần Thơ và tỉnh Hậu Giang; Đắk Lắk tách ra Đắk Nông; Lai Châu tách ra Điện Biên) tạo ra sự gián đoạn cấu trúc hoặc phân mảnh chuỗi thống kê trước và sau năm 2004.

### 13.3. Đa ngữ nghĩa của ký hiệu dữ liệu thiếu `..` (Multi-semantic Missing Token)
- Ký hiệu chuỗi `..` xuất hiện 44 đến 162 lần trên mỗi file ma trận, mang nhiều bản chất nghiệp vụ khác biệt tùy theo thực thể và thời gian:
  - **Không áp dụng / Đơn vị giải thể**: Điển hình là chuỗi giá trị của tỉnh Hà Tây giai đoạn 2008 – 2024.
  - **Không phát sinh số liệu / Không thu thập**: Các địa phương có diện tích canh tác không đáng kể hoặc chưa thiết lập hệ thống thống kê chỉ tiêu tương ứng trong giai đoạn lịch sử.
- **Vấn đề**: Ký hiệu `..` hoàn toàn không tương đương với giá trị bằng không (`0.0`).

### 13.4. Khuyết thiếu hoàn toàn siêu dữ liệu đơn vị đo lường (Complete Absence of Measurement Units in Schema)
- Cả 12 file ma trận (V06.13 đến V06.24) hoàn toàn không có dòng hoặc cột mô tả đơn vị đo lường.
- Qua kiểm định chéo toán học giữa 3 bộ file (Diện tích, Năng suất, Sản lượng), công thức quan sát được là:
  $$\text{Sản lượng} \approx \frac{\text{Diện tích} \times \text{Năng suất}}{10}$$
  với sai số median delta rất nhỏ ($0.07$ – $0.2$). Điều này phản ánh ngầm định các đơn vị: Diện tích tính bằng `Nghìn ha`, Năng suất tính bằng `Tạ/ha`, và Sản lượng tính bằng `Nghìn tấn`.

### 13.5. Bất đối xứng về phạm vi địa lý giữa các mùa vụ (Asymmetric Seasonal Geographic Scope)
- Các file thuộc vụ lúa cả năm (V06.13 – 15), lúa đông xuân (V06.16 – 18), và lúa mùa (V06.22 – 24) có **72 dòng dữ liệu** (bao gồm cả nước, các vùng và đầy đủ các tỉnh).
- Các file lúa hè thu và thu đông (V06.19 – 21) chỉ có **38 dòng dữ liệu**.
- **Đặc thù**: Đây là hiện tượng bình thường về mặt sinh thái nông nghiệp (miền Bắc không canh tác vụ hè thu và thu đông do thời tiết rét mùa đông và mưa bão mùa thu), không phải lỗi thiếu hụt dòng của file.

### 13.6. Mất mát mã hóa ký tự gốc (Irreversible Lossy Encoding / Ký hiệu `?`)
- Dữ liệu bị lỗi bảng mã một chiều: Ký tự dấu hỏi ASCII `?` thay thế các nguyên âm tiếng Việt có dấu ngay trong chuỗi byte lưu trữ (`C? NU?C`, `Ð?ng b?ng sông H?ng`, `Hà N?i`, `Hà Tây`, `B?c Ninh`, `H?i Phòng`...).
- **Vấn đề**: Các nhãn địa danh bị mất tính định danh nguyên vẹn, cản trở việc join/khớp dữ liệu theo chuỗi văn bản thông thường nếu không có từ điển đối chiếu cố định.

### 13.7. Cấu trúc ma trận ngang (Wide Matrix Layout) và Tiền tố trạng thái công bố
- Dữ liệu lưu dưới dạng ma trận gồm 31 cột (cột 1 là địa phương, cột 2 đến 31 là các mốc năm từ `1995` đến `So b? 2024`).
- Cột năm cuối cùng chứa tiền tố trạng thái: `"So b? 2024"` (Sơ bộ 2024).

### 13.8. Dòng trắng cấu trúc cuối tệp (Structural Blank Row)
- Dòng cuối cùng của mỗi file (dòng 73 ở các file 72 bản ghi, dòng 39 ở các file 38 bản ghi) là dòng trống toàn bộ (`empty/null` trên toàn bộ 31 cột).
