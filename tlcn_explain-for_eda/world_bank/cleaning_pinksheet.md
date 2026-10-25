# Cleaning specification — cleaning_pinksheet

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Pink Sheet Monthly Prices

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx | lookup/metadata | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Pink Sheet Monthly Prices | 56232 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Pink Sheet Monthly Indices | 12672 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | lookup/metadata | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | lookup/metadata | 0 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx | AFOSHEET | metadata | xlsx | OOXML Unicode | 778415 | 0 | 0 | 1 | 93e9c2361c34788b | 83a320affafc83512d4af4c7198af6ce086326c8b6923d9f64dd905172f86290 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Prices | observations | xlsx | OOXML Unicode | 778415 | 798 | 89 | 798 | 865d4a9ce34b74ca | 83a320affafc83512d4af4c7198af6ce086326c8b6923d9f64dd905172f86290 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | observations | xlsx | OOXML Unicode | 778415 | 1362 | 81 | 1362 | 9ace360185409737 | 83a320affafc83512d4af4c7198af6ce086326c8b6923d9f64dd905172f86290 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Description | metadata | xlsx | OOXML Unicode | 778415 | 403 | 87 | 403 | f96adad8b96bb19a | 83a320affafc83512d4af4c7198af6ce086326c8b6923d9f64dd905172f86290 |
| world_bank/CMO-Historical-Data-Monthly.xlsx | Index Weights | metadata | xlsx | OOXML Unicode | 778415 | 92 | 251 | 92 | fa560e6be083e718 | 83a320affafc83512d4af4c7198af6ce086326c8b6923d9f64dd905172f86290 |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: series/commodity specification × calendar month × statistic kind
- **time**: YYYYMmm monthly; no annual dataset in this workbook
- **geography**: origin/FOB/CIF specification in Description; commodity name is not country code
- **measure**: nominal USD prices vs 2010=100 indices; every grade/series stays distinct
- **units**: source unit row for prices; USD/mt → USD/tonne; indices remain index_points with base year
- **additivity**: prices/indices nonadditive; annual aggregation needs weights/coverage policy



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | raw_value | 56232 | 49827 | 6405 | 0 | 0 | 0 | 0 | 0 | 0.0287 | 52179.048 | 1138 | commodity_raw, unit_raw, statistic_kind | {np.int64(1): 6047, np.int64(2): 14086, np.int64(3): 2427, np.int64(4): 6220, np.int64(5): 885, np.int64(6): 1456, np.int64(7): 1447, np.int64(8): 1046, np.int64(9): 2157, np.int64(10): 701, np.int64(11): 6544, np.int64(13): 1, np.int64(14): 1, np.int64(16): 15} |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | raw_value | 12672 | 12672 | 0 | 0 | 0 | 0 | 0 | 0 | 1.81259494779253 | 336.13744649421 | 149 | commodity_raw, unit_raw, statistic_kind | {np.int64(1): 43, np.int64(2): 419, np.int64(8): 11, np.int64(9): 112, np.int64(10): 1088, np.int64(11): 10785, np.int64(12): 3, np.int64(13): 39, np.int64(14): 162, np.int64(15): 4} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | period_raw | calendar month | 1960-01-01 00:00:00 | 2025-12-01 00:00:00 | 0 | [] |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | period_raw | calendar month | 1960-01-01 00:00:00 | 2025-12-01 00:00:00 | 0 | [] |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::AFOSHEET | raw table | 0 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | raw table | 798 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | raw table | 1362 | 561 | 562 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Description | raw table | 403 | 291 | 292 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights | raw table | 92 | 19 | 20 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | raw_value | 0 | 0 | 0 | 6405 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 219 | 6186 | 0 | 6405 | 56232 | 11.390311566367904 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | raw_value | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 12672 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Description | 35061 | 34819 | 0 | 0 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights | 23092 | 22887 | 0 | 0 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 110322 | 96832 | 0 | 6 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 71022 | 13852 | 0 | 0 | 6405 | 0 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 2 | 1 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 3 | 2 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 4 | 3 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 5 | 4 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 6 | 5 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 7 | 6 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 8 | 7 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 9 | 8 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 10 | 9 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 11 | 10 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 12 | 11 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 13 | 12 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 14 | 13 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 15 | 14 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 16 | 15 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 17 | 16 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 18 | 17 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 19 | 18 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 20 | 19 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 21 | 20 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 22 | 21 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 23 | 22 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 24 | 23 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 25 | 24 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 26 | 25 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 27 | 26 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 28 | 27 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 29 | 28 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 30 | 29 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 31 | 30 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 32 | 31 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 33 | 32 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 34 | 33 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 35 | 34 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 36 | 35 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 37 | 36 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 38 | 37 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 39 | 38 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 40 | 39 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 41 | 40 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 42 | 41 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 43 | 42 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 44 | 43 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 45 | 44 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 46 | 45 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 47 | 46 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 48 | 47 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 49 | 48 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 50 | 49 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 51 | 50 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 52 | 51 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 53 | 52 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 54 | 53 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 55 | 54 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 56 | 55 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 57 | 56 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 58 | 57 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 59 | 58 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 60 | 59 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 61 | 60 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 62 | 61 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 63 | 62 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 64 | 63 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 65 | 64 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 66 | 65 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 67 | 66 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 68 | 67 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 69 | 68 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 70 | 69 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 71 | 70 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 72 | 71 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 2 | 1 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 3 | 2 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 4 | 3 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 5 | 4 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 6 | 5 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 7 | 6 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 8 | 7 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 9 | 8 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 10 | 9 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 11 | 10 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 12 | 11 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 13 | 12 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 14 | 13 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 15 | 14 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 16 | 15 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 17 | 16 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights | 9 | 8 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights | 12 | 11 | object | mixed numeric/text | 0 | 0 | False |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights | 15 | 14 | object | mixed numeric/text | 0 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-037 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | all fields | structural_blank_row | 562 | records | row 5, column record: ; row 802, column record: ; row 803, column record:  | Exclude only fully blank records from observation projection; preserve raw record lineage/count | no observation | EXCLUDE_STRUCTURAL | All cells blank/null/whitespace in raw row | VERIFIED | Review this explicit structural predicate | raw_rows = retained_rows + structural_blank_rows | RAW_PROFILE |
| DQ-038 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | all fields | exact_duplicate_rows | 561 | excess records | row 5, column record: ; row 802, column record: ; row 803, column record:  | Propose lineage-preserving dedup only after repeat/release policy; retain all source references | one observation + duplicate lineage | PENDING_REVIEW | Exact full-row equality, no hash-only test | PROPOSED | Approve exact duplicate retention policy | Before/after difference equals approved duplicate excess | RAW_PROFILE |
| DQ-039 | world_bank/CMO-Historical-Data-Monthly.xlsx | Description | all fields | exact_duplicate_rows | 291 | excess records | row 15, column record: ; row 16, column record: ; row 25, column record:  | Propose lineage-preserving dedup only after repeat/release policy; retain all source references | one observation + duplicate lineage | PENDING_REVIEW | Exact full-row equality, no hash-only test | PROPOSED | Approve exact duplicate retention policy | Before/after difference equals approved duplicate excess | RAW_PROFILE |
| DQ-040 | world_bank/CMO-Historical-Data-Monthly.xlsx | Index Weights | all fields | exact_duplicate_rows | 19 | excess records | row 7, column record: ; row 20, column record: ; row 28, column record:  | Propose lineage-preserving dedup only after repeat/release policy; retain all source references | one observation + duplicate lineage | PENDING_REVIEW | Exact full-row equality, no hash-only test | PROPOSED | Approve exact duplicate retention policy | Before/after difference equals approved duplicate excess | RAW_PROFILE |
| DQ-126 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Prices | raw_value | missing_token_candidate | 6405 | observations | row 7, column 5: …; row 8, column 5: …; row 9, column 5: … | Define source-specific token map with missing reason; retain original token; no zero imputation | nullable decimal + raw token | PENDING_REVIEW | Literal token distribution from full observation view; historical closure vs not available can differ | PROPOSED | Approve source token semantics | Each approved token maps to NULL; unapproved tokens flagged | RAW_PROFILE |
| DQ-127 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Prices | raw_value | iqr_outlier_candidate | 1138 | observations | row 587, column 2: 122.63260894661; row 588, column 2: 131.52111111111; row 589, column 2: 132.82518445323 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within commodity_raw, unit_raw, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-128 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Prices | header / unit rows | multirow_header_and_units | 6 | sheet rows | Raw sheet rows [1, 2, 3, 4, 5, 6] | Separate metadata/header/unit rows by explicit YYYY Mmm observation predicate; keep hierarchical index names and price unit row | series_id + statistic_kind + month + raw unit + decimal | EXCLUDE_STRUCTURAL | Monthly data rows match YYYY Mmm; actual header rows retained in raw grid | VERIFIED | Approve sheet layout adapter | Observation + structural sheet rows reconcile; price/index separated | RAW_PROFILE |
| DQ-129 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Prices | sheet metadata | stale_update_header | 1 | metadata fields | World Bank Commodity Price Data (The Pink Sheet); monthly prices in nominal US dollars, 1960 to present; (monthly series are available only in nominal US dollars); Updated on January 06, 2025 | Flag source update header discrepancy; identify vintage by file hash, never treat header as reliable ingest timestamp | raw source note + content hash + reviewed vintage | FLAG | Raw updated date 2025-01-06 precedes last monthly observation 2025-12-01 | VERIFIED | Confirm workbook release identity | All source date discrepancies retained/reported | RAW_PROFILE |
| DQ-130 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | raw_value | iqr_outlier_candidate | 149 | observations | row 591, column 2: 155.68923913003; row 592, column 2: 156.22566755666; row 756, column 2: 157.10932339206 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within commodity_raw, unit_raw, statistic_kind | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-131 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | header / unit rows | multirow_header_and_units | 570 | sheet rows | Raw sheet rows [1, 2, 3, 4, 5, 6, 7, 8, 9, 802, 803, 804] | Separate metadata/header/unit rows by explicit YYYY Mmm observation predicate; keep hierarchical index names and price unit row | series_id + statistic_kind + month + raw unit + decimal | EXCLUDE_STRUCTURAL | Monthly data rows match YYYY Mmm; actual header rows retained in raw grid | VERIFIED | Approve sheet layout adapter | Observation + structural sheet rows reconcile; price/index separated | RAW_PROFILE |
| DQ-132 | world_bank/CMO-Historical-Data-Monthly.xlsx | Monthly Indices | sheet metadata | stale_update_header | 1 | metadata fields | World Bank Commodity Price Data (The Pink Sheet); monthly indices based on nominal US dollars, 2010=100, 1960 to present; (monthly series are available only in nominal US dollar terms); Updated on January 06, 2025 | Flag source update header discrepancy; identify vintage by file hash, never treat header as reliable ingest timestamp | raw source note + content hash + reviewed vintage | FLAG | Raw updated date 2025-01-06 precedes last monthly observation 2025-12-01 | VERIFIED | Confirm workbook release identity | All source date discrepancies retained/reported | RAW_PROFILE |

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
| series_raw | string | exact header/hierarchical index label |
| series_id | string | approved stable specification |
| month | date | calendar month from YYYYMmm |
| statistic_kind | string | price/index |
| currency | string nullable | nominal USD for monetary series |
| price_basis | string nullable | source unit row |
| index_base | string nullable | 2010=100 for indices |
| series_description | string nullable | Description sheet, dated definition changes retained |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | 1 | commodity_raw + period_raw + statistic_kind | 56232 | 0 | 0 | 0 | 0 | 0 |
| world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | 1 | commodity_raw + period_raw + statistic_kind | 12672 | 0 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-037 | structural_blank_row | 562 | records | EXCLUDE_STRUCTURAL | Exclude only fully blank records from observation projection; preserve raw record lineage/count | raw_rows = retained_rows + structural_blank_rows |
| DQ-127 | iqr_outlier_candidate | 1138 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-128 | multirow_header_and_units | 6 | sheet rows | EXCLUDE_STRUCTURAL | Separate metadata/header/unit rows by explicit YYYY Mmm observation predicate; keep hierarchical index names and price unit row | Observation + structural sheet rows reconcile; price/index separated |
| DQ-129 | stale_update_header | 1 | metadata fields | FLAG | Flag source update header discrepancy; identify vintage by file hash, never treat header as reliable ingest timestamp | All source date discrepancies retained/reported |
| DQ-130 | iqr_outlier_candidate | 149 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |
| DQ-131 | multirow_header_and_units | 570 | sheet rows | EXCLUDE_STRUCTURAL | Separate metadata/header/unit rows by explicit YYYY Mmm observation predicate; keep hierarchical index names and price unit row | Observation + structural sheet rows reconcile; price/index separated |
| DQ-132 | stale_update_header | 1 | metadata fields | FLAG | Flag source update header discrepancy; identify vintage by file hash, never treat header as reliable ingest timestamp | All source date discrepancies retained/reported |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

ellipsis missing semantics, header update date inconsistent with data tail, vintage and description changes over time

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-038 | exact_duplicate_rows | PROPOSED | PENDING_REVIEW | Approve exact duplicate retention policy |
| DQ-039 | exact_duplicate_rows | PROPOSED | PENDING_REVIEW | Approve exact duplicate retention policy |
| DQ-040 | exact_duplicate_rows | PROPOSED | PENDING_REVIEW | Approve exact duplicate retention policy |
| DQ-126 | missing_token_candidate | PROPOSED | PENDING_REVIEW | Approve source token semantics |
| DQ-127 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-130 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-037: raw_rows = retained_rows + structural_blank_rows
- [ ] DQ-038: Before/after difference equals approved duplicate excess
- [ ] DQ-039: Before/after difference equals approved duplicate excess
- [ ] DQ-040: Before/after difference equals approved duplicate excess
- [ ] DQ-126: Each approved token maps to NULL; unapproved tokens flagged
- [ ] DQ-127: Canonical retention count unchanged for IQR signals
- [ ] DQ-128: Observation + structural sheet rows reconcile; price/index separated
- [ ] DQ-129: All source date discrepancies retained/reported
- [ ] DQ-130: Canonical retention count unchanged for IQR signals
- [ ] DQ-131: Observation + structural sheet rows reconcile; price/index separated
- [ ] DQ-132: All source date discrepancies retained/reported
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
| Lineage: world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices | PASS | Every projected observation points to original row and column |
| Lineage: world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices | PASS | Every projected observation points to original row and column |
| Milling factor identity | PASS | Milled ≈ rough×encoded/10000 within source integer reporting precision |

## 12. Mapping tới file Raw, observed rows/columns và cross-source evidence

CSV row = header-inclusive record ordinal (multiline record có thể khác physical line). Excel row = worksheet physical row. HTML row = table row ordinal (header row 1), không phải HTML text line. raw_column giữ literal field/year hoặc 1-based ordinal. Issue table cung cấp representative exact coordinates; toàn bộ masks/profiles ở notebook để drill down.

### world_bank/CMO-Historical-Data-Monthly.xlsx::AFOSHEET

Raw records: 0; columns: 0; sheet/CSV physical rows: 1; header rows: [].

Literal headers/positional grid labels:

```text
[]
```

### world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Prices

Raw records: 798; columns: 89; sheet/CSV physical rows: 798; header rows: [].

Literal headers/positional grid labels:

```text
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88]
```

Derived header names + source unit row (projection only):

| raw_column_ordinal | series_raw | raw_unit |
| --- | --- | --- |
| 2 | Crude oil, average | ($/bbl) |
| 3 | Crude oil, Brent | ($/bbl) |
| 4 | Crude oil, Dubai | ($/bbl) |
| 5 | Crude oil, WTI | ($/bbl) |
| 6 | Coal, Australian | ($/mt) |
| 7 | Coal, South African ** | ($/mt) |
| 8 | Natural gas, US | ($/mmbtu) |
| 9 | Natural gas, Europe | ($/mmbtu) |
| 10 | Liquefied natural gas, Japan | ($/mmbtu) |
| 11 | Natural gas index | (2010=100) |
| 12 | Cocoa | ($/kg) |
| 13 | Coffee, Arabica | ($/kg) |
| 14 | Coffee, Robusta | ($/kg) |
| 15 | Tea, avg 3 auctions | ($/kg) |
| 16 | Tea, Colombo | ($/kg) |
| 17 | Tea, Kolkata | ($/kg) |
| 18 | Tea, Mombasa | ($/kg) |
| 19 | Coconut oil | ($/mt) |
| 20 | Groundnuts | ($/mt) |
| 21 | Fish meal | ($/mt) |
| 22 | Groundnut oil ** | ($/mt) |
| 23 | Palm oil | ($/mt) |
| 24 | Palm kernel oil | ($/mt) |
| 25 | Soybeans | ($/mt) |
| 26 | Soybean oil | ($/mt) |
| 27 | Soybean meal | ($/mt) |
| 28 | Rapeseed oil | ($/mt) |
| 29 | Sunflower oil | ($/mt) |
| 30 | Barley | ($/mt) |
| 31 | Maize | ($/mt) |
| 32 | Sorghum | ($/mt) |
| 33 | Rice, Thai 5%  | ($/mt) |
| 34 | Rice, Thai 25%  | ($/mt) |
| 35 | Rice, Thai A.1 | ($/mt) |
| 36 | Rice, Viet Namese 5% | ($/mt) |
| 37 | Wheat, US SRW | ($/mt) |
| 38 | Wheat, US HRW | ($/mt) |
| 39 | Banana, Europe | ($/kg) |
| 40 | Banana, US | ($/kg) |
| 41 | Orange | ($/kg) |
| 42 | Beef ** | ($/kg) |
| 43 | Chicken ** | ($/kg) |
| 44 | Lamb ** | ($/kg) |
| 45 | Shrimps, Mexican | ($/kg) |
| 46 | Sugar, EU | ($/kg) |
| 47 | Sugar, US | ($/kg) |
| 48 | Sugar, world | ($/kg) |
| 49 | Tobacco, US import u.v. | ($/mt) |
| 50 | Logs, Cameroon | ($/cubic meter) |
| 51 | Logs, Malaysian | ($/cubic meter) |
| 52 | Sawnwood, Cameroon | ($/cubic meter) |
| 53 | Sawnwood, Malaysian | ($/cubic meter) |
| 54 | Plywood | (cents/sheet) |
| 55 | Cotton, A Index | ($/kg) |
| 56 | Rubber, TSR20 ** | ($/kg) |
| 57 | Rubber, RSS3 | ($/kg) |
| 58 | Phosphate rock | ($/mt) |
| 59 | DAP | ($/mt) |
| 60 | TSP | ($/mt) |
| 61 | Urea  | ($/mt) |
| 62 | Potassium chloride ** | ($/mt) |
| 63 | Aluminum | ($/mt) |
| 64 | Iron ore, cfr spot | ($/dmtu) |
| 65 | Copper | ($/mt) |
| 66 | Lead | ($/mt) |
| 67 | Tin | ($/mt) |
| 68 | Nickel | ($/mt) |
| 69 | Zinc | ($/mt) |
| 70 | Gold | ($/troy oz) |
| 71 | Platinum | ($/troy oz) |
| 72 | Silver | ($/troy oz) |
| 73 |  |  |
| 74 |  |  |
| 75 |  |  |
| 76 |  |  |
| 77 |  |  |
| 78 |  |  |
| 79 |  |  |
| 80 |  |  |
| 81 |  |  |
| 82 |  |  |
| 83 |  |  |
| 84 |  |  |
| 85 |  |  |
| 86 |  |  |
| 87 |  |  |
| 88 |  |  |
| 89 |  |  |

### world_bank/CMO-Historical-Data-Monthly.xlsx::Monthly Indices

Raw records: 1362; columns: 81; sheet/CSV physical rows: 1362; header rows: [].

Literal headers/positional grid labels:

```text
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80]
```

Derived header names + source unit row (projection only):

| raw_column_ordinal | series_raw | raw_unit |
| --- | --- | --- |
| 2 | Total Index | index (2010=100) |
| 3 | Energy | index (2010=100) |
| 4 | Non-energy ** | index (2010=100) |
| 5 | Agriculture ** | index (2010=100) |
| 6 | Beverages | index (2010=100) |
| 7 | Food ** | index (2010=100) |
| 8 | Oils & Meals | index (2010=100) |
| 9 | Grains | index (2010=100) |
| 10 | Other Food ** | index (2010=100) |
| 11 | Raw Materials | index (2010=100) |
| 12 | Timber | index (2010=100) |
| 13 | Other Raw Mat. | index (2010=100) |
| 14 | Fertilizers ** | index (2010=100) |
| 15 | Metals  & Minerals | index (2010=100) |
| 16 | Base Metals (ex. iron ore) | index (2010=100) |
| 17 | Precious Metals | index (2010=100) |
| 18 |  | index (2010=100) |
| 19 |  | index (2010=100) |
| 20 |  | index (2010=100) |
| 21 |  | index (2010=100) |
| 22 |  | index (2010=100) |
| 23 |  | index (2010=100) |
| 24 |  | index (2010=100) |
| 25 |  | index (2010=100) |
| 26 |  | index (2010=100) |
| 27 |  | index (2010=100) |
| 28 |  | index (2010=100) |
| 29 |  | index (2010=100) |
| 30 |  | index (2010=100) |
| 31 |  | index (2010=100) |
| 32 |  | index (2010=100) |
| 33 |  | index (2010=100) |
| 34 |  | index (2010=100) |
| 35 |  | index (2010=100) |
| 36 |  | index (2010=100) |
| 37 |  | index (2010=100) |
| 38 |  | index (2010=100) |
| 39 |  | index (2010=100) |
| 40 |  | index (2010=100) |
| 41 |  | index (2010=100) |
| 42 |  | index (2010=100) |
| 43 |  | index (2010=100) |
| 44 |  | index (2010=100) |
| 45 |  | index (2010=100) |
| 46 |  | index (2010=100) |
| 47 |  | index (2010=100) |
| 48 |  | index (2010=100) |
| 49 |  | index (2010=100) |
| 50 |  | index (2010=100) |
| 51 |  | index (2010=100) |
| 52 |  | index (2010=100) |
| 53 |  | index (2010=100) |
| 54 |  | index (2010=100) |
| 55 |  | index (2010=100) |
| 56 |  | index (2010=100) |
| 57 |  | index (2010=100) |
| 58 |  | index (2010=100) |
| 59 |  | index (2010=100) |
| 60 |  | index (2010=100) |
| 61 |  | index (2010=100) |
| 62 |  | index (2010=100) |
| 63 |  | index (2010=100) |
| 64 |  | index (2010=100) |
| 65 |  | index (2010=100) |
| 66 |  | index (2010=100) |
| 67 |  | index (2010=100) |
| 68 |  | index (2010=100) |
| 69 |  | index (2010=100) |
| 70 |  | index (2010=100) |
| 71 |  | index (2010=100) |
| 72 |  | index (2010=100) |
| 73 |  | index (2010=100) |
| 74 |  | index (2010=100) |
| 75 |  | index (2010=100) |
| 76 |  | index (2010=100) |
| 77 |  | index (2010=100) |
| 78 |  | index (2010=100) |
| 79 |  | index (2010=100) |
| 80 |  | index (2010=100) |
| 81 |  | index (2010=100) |

### world_bank/CMO-Historical-Data-Monthly.xlsx::Description

Raw records: 403; columns: 87; sheet/CSV physical rows: 403; header rows: [].

Literal headers/positional grid labels:

```text
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86]
```

Rice definitions from source Description sheet:

| physical_row | definition |
| --- | --- |
| 42 | Rice (Thailand), 5% broken, white rice (WR), milled, indicative price based on  weekly surveys of export transactions, government standard, f.o.b. Bangkok |
| 43 | Rice (Thailand), 25% broken, WR, milled indicative survey price, government standard, f.o.b. Bangkok |
| 44 | Rice (Thailand), 100% broken, A.1 Super from 2006 onwards, government standard, f.o.b. Bangkok; prior to 2006, A1 Special, a slightly lower grade than A1 Super. |
| 45 | Rice (Vietnam), 5% broken, WR, milled, weekly indicative survey price, Minimum Export Price, f.o.b. Hanoi |

### world_bank/CMO-Historical-Data-Monthly.xlsx::Index Weights

Raw records: 92; columns: 251; sheet/CSV physical rows: 92; header rows: [].

Literal headers/positional grid labels:

```text
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250]
```

Relevant cross-source checks (diagnostic; incompatibilities retained):

| relation | status | matched_observations | finding | compatibility_limits |
| --- | --- | --- | --- | --- |
| World Bank rice quotes ↔ USDA Yearbook export quotes | PENDING_PERIOD_REVIEW | 0 | Both quote prices can have USD/tonne basis, but grade/FOB and quote sample differ | World Bank calendar month vs USDA reporting YEAR/month/MARKETING; approved per-table calendar mapping missing |

Không kế thừa quy tắc từ tài liệu/Silver cũ; mọi proposal có origin trong decision table.

## 13. Ghi chú các vấn đề và đặc thù dữ liệu (Data Anomalies & Issues Profile)

### 13.1. Cấu trúc Header đa tầng và Dòng siêu dữ liệu lồng ghép (Multi-row Header & Embedded Metadata)
- Dữ liệu trong sheet `Monthly Prices` không bắt đầu từ dòng 1 mà có 5 dòng tiêu đề và siêu dữ liệu:
  - **Dòng 1 – 3**: Tiêu đề báo cáo và ngày cập nhật tĩnh (`World Bank Commodity Price Data...`, `Updated on January 06, 2025`).
  - **Dòng 4**: Tên mặt hàng (`Commodity`).
  - **Dòng 5**: Đơn vị đo lường (`Unit Description` như `($/mt)`, `($/bbl)`).
  - **Dòng 6 trở đi**: Mới là chuỗi dữ liệu thực tế bắt đầu từ `1960M01`.
- Sheet `Monthly Indices` có tới 570 dòng bao gồm các dòng tiêu đề phân nhóm ngành hàng (`Agriculture`, `Beverages`, `Food`...) xen kẽ và các dòng trống ở cuối bảng.
- **Vấn đề**: Cấu trúc tệp mang định dạng bảng tính hiển thị (presentation layout) thay vì dạng bảng phẳng (flat tabular data), dễ gây lỗi nhận diện kiểu dữ liệu nếu parser đọc dữ liệu từ dòng đầu tiên.

### 13.2. Ký hiệu Dữ liệu khuyết thiếu Dấu ba chấm (Missing Tokens '…' và '...')
- Có tới **6.405 ô mang ký tự dấu ba chấm**:
  - 6.186 ô là ký tự Unicode Horizontal Ellipsis `…` (`\u2026`).
  - 219 ô là 3 dấu chấm ASCII `...`.
- **Bản chất**: Thể hiện quy ước của World Bank đối với các mốc thời gian lịch sử chưa bắt đầu theo dõi thu thập giá của mặt hàng đó (ví dụ chuỗi giá gạo Việt Nam `Rice, Viet Namese 5%` bắt đầu có từ năm 1990; toàn bộ giai đoạn 1960–1989 mang ký tự `…`).
- **Vấn đề**: Ký hiệu này không phải chuỗi rỗng và hoàn toàn không tương đương với giá trị bằng 0.

### 13.3. Định dạng Chu kỳ thời gian dạng chuỗi 'YYYYMmm'
- Trục thời gian ở cột đầu tiên được lưu dưới dạng chuỗi văn bản: `1960M01`, `1960M02` ... `2025M12` (kéo dài 792 tháng liên tục).
- **Vấn đề**: Cần phân tích cú pháp để ánh xạ về ngày đầu tháng của năm dương lịch tương ứng trước khi nạp vào mô hình thời gian chuẩn.

### 13.4. Mâu thuẫn giữa Ngày cập nhật trên Header và Chuỗi dữ liệu thực tế (Stale Update Header)
- Dòng metadata thứ 3 của sheet ghi nhãn văn bản: `"Updated on January 06, 2025"`.
- Tuy nhiên, chuỗi dữ liệu thực tế kéo dài đến tháng 12/2025 (`2025M12`).
- **Vấn đề**: Nhãn ngày cập nhật là chuỗi tĩnh trong mẫu bảng tính, không phản ánh chính xác thời điểm kết thúc dữ liệu hoặc mốc phát hành thực tế của dữ liệu.

### 13.5. Khoảng trắng thừa và Quy ước đặt tên cột Lúa gạo (Rice Columns Naming)
- Các cột theo dõi lúa gạo có chứa khoảng trắng thừa ở cuối tiêu đề hoặc viết tách từ:
  - `'Rice, Thai 5% '` (khoảng trắng ở cuối)
  - `'Rice, Thai 25% '` (khoảng trắng ở cuối)
  - `'Rice, Thai A.1'` (Gạo tấm Thái Lan 100%)
  - `'Rice, Viet Namese 5%'` (Viết tách từ `"Viet Namese"` thay vì `"Vietnamese"`)
- Toàn bộ 4 cột gạo đều có đơn vị danh nghĩa là `$/mt` (USD/tấn), khảo sát theo điều kiện giao hàng FOB Bangkok và FOB Hanoi.

### 13.6. Dòng rỗng cấu trúc quy mô lớn trong sheet Chỉ số (Structural Blank Rows)
- Sheet `Monthly Indices` chứa 562 dòng rỗng hoàn toàn trên tất cả các cột, nằm phân bố giữa các khối ngành hàng và ở phần cuối của bảng tính.
