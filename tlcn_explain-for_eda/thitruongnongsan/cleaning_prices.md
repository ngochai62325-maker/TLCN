# Cleaning specification — cleaning_prices

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

Domestic rice/paddy market quotes

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx | Domestic rice/paddy market quotes | 20394 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx | price_lua_gao | observations | xlsx | OOXML Unicode | 756440 | 20394 | 8 | 20395 | 890c4ca3e5b6bc21 | 8ce4fbbb188d65c2327b73d06f07265ae889c18421095eb157c7f5e46a5a9063 |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: raw product × market × price kind × unit × currency × date × source (candidate)
- **time**: month/day/year 12h timestamps, timezone absent; observed daily quote dates
- **geography**: market labels; never fuzzy-map to province codes without approved dictionary
- **measure**: price quotes; fresh/dry paddy, rice grades and local names remain distinct
- **units**: VNĐ/Kg, Vnđ/Kg, VNĐ/kg, Đồng/kg + currency VNĐ; candidate VND/kg without value rescaling
- **additivity**: prices nonadditive; average requires grain/product/market/price kind and weights



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | Giá | 20394 | 20394 | 0 | 0 | 0 | 0 | 0 | 29 | 0.0 | 80000.0 | 920 | Tên_mặt_hàng, Đơn_vị_tính, Loại_tiền | {np.int64(1): 206, np.int64(2): 418} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | Ngày | month/day/year timestamp; timezone absent | 2021-01-04 00:00:00 | 2026-08-03 00:00:00 | 0 | daily market coverage not assumed; no expected trading calendar |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | raw table | 20394 | 3 | 6 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | Giá | 0 | 0 | 0 | 0 | 29 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 20394 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | 163152 | 0 | 0 | 0 | 0 | 29 |

| id | column_position | raw_column | storage_dtype | inferred_dtype | leading_zero_count | encoding_suspect_count | header_encoding_suspect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | 1 | Tên_mặt_hàng | object | mixed numeric/text | 0 | 0 | False |

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-035 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | all fields | exact_duplicate_rows | 3 | excess records | row 3892, column record: 5600; row 3893, column record: 5600; row 3907, column record: 5600 | Propose lineage-preserving dedup only after repeat/release policy; retain all source references | one observation + duplicate lineage | PENDING_REVIEW | Exact full-row equality, no hash-only test | PROPOSED | Approve exact duplicate retention policy | Before/after difference equals approved duplicate excess | RAW_PROFILE |
| DQ-112 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Giá | iqr_outlier_candidate | 920 | observations | row 20, column Giá: 15500; row 28, column Giá: 8542.86; row 94, column Giá: 8542.86 | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | decimal + optional review flag | FLAG | IQR computed from all nonmissing values within Tên_mặt_hàng, Đơn_vị_tính, Loại_tiền | PROPOSED | Approve only business thresholds, if desired | Canonical retention count unchanged for IQR signals | RAW_PROFILE |
| DQ-113 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Tên_mặt_hàng + Thị_trường + Loại_giá + Đơn_vị_tính + Loại_tiền + Ngày | business_key_value_conflict | 8 | key groups | row 3892, column Giá: 5600; row 3893, column Giá: 5600; row 3907, column Giá: 5600 | Retain competing observations pending grain/release policy; quarantine only unresolved collisions of approved key; no last-wins | observation + raw quote/source lineage | PENDING_REVIEW | Same candidate key has distinct raw measure values | PROPOSED | Approve business key and conflict resolution | Approved-key conflicts either retained as distinct grain or explicitly quarantined | RAW_PROFILE |
| DQ-114 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Tên_mặt_hàng + Thị_trường + Loại_giá + Đơn_vị_tính + Loại_tiền + Ngày + Nguồn | business_key_value_conflict | 2 | key groups | row 3892, column Giá: 5600; row 3893, column Giá: 5600; row 3907, column Giá: 5600 | Retain competing observations pending grain/release policy; quarantine only unresolved collisions of approved key; no last-wins | observation + raw quote/source lineage | PENDING_REVIEW | Same candidate key has distinct raw measure values | PROPOSED | Approve business key and conflict resolution | Approved-key conflicts either retained as distinct grain or explicitly quarantined | RAW_PROFILE |
| DQ-115 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Giá | zero_price | 29 | records | row 12245, column Giá: 0; row 12277, column Giá: 0; row 12387, column Giá: 0 | Preserve/flag zero; ask whether no-quote or true zero before NULL/quarantine rule | decimal price + review flag | PENDING_REVIEW | Zero is a genuine numeric Raw value | UNKNOWN | Approve zero-price business policy | No zero converted to NULL without approved source semantics | RAW_PROFILE |
| DQ-116 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Đơn_vị_tính / Loại_tiền | unit_spelling_aliases | 20394 | records | {'VNĐ/Kg': 10064, 'Vnđ/Kg': 5072, 'VNĐ/kg': 3880, 'Đồng/kg': 1378} | Propose exact alias dictionary for observed VND/kg spellings, no price rescaling; keep raw unit/currency | VND/kg + raw_unit/currency | NORMALIZE | Raw currency VNĐ and per-kg labels agree; no tonne basis in observations | PROPOSED | Approve four explicit observed aliases | Value unchanged; unit dictionary covers only approved exact strings | RAW_PROFILE |
| DQ-117 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Ngày | explicit_us_date_format | 20394 | records | row 2, column Ngày: 8/3/2026 12:00:00 AM; row 3, column Ngày: 7/31/2026 12:00:00 AM; row 4, column Ngày: 7/31/2026 12:00:00 AM | Parse %m/%d/%Y %I:%M:%S %p; retain raw timestamp; timezone unresolved, derive quote_date only after grain review | local naive timestamp/date + raw date | NORMALIZE | Dates such as 7/31/2026 rule out day/month; full parse tested in STEP 5 | VERIFIED | Approve date grain and timezone policy | Full Raw date parse succeeds; no timezone invented | RAW_PROFILE |
| DQ-118 | thitruongnongsan/price_luagao.xlsx | price_lua_gao | Tên_mặt_hàng / Nguồn / Thị_trường | product_market_source_dictionary | 20394 | records | Category distributions in STEP 5; source names mix collaborators, districts and Bán lẻ | Retain product details fresh/dry/grade, raw markets and raw source; approve exact dictionaries, no fuzzy commodity merge | raw strings + nullable reviewed dimension IDs | PENDING_REVIEW | zero-price meaning, conflicting quote duplicates, unit alias approval, product and source dictionaries | UNKNOWN | Approve product/geography/provenance dictionaries | No merging different processing basis, grades or markets | RAW_PROFILE |

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
| product_raw | string | variety/freshness/grade retained |
| product_id | string nullable | approved exact dictionary |
| market_raw | string | source location |
| price_kind_raw | string | quote context |
| currency | string | VND candidate |
| price_basis | string | kg candidate, no rescaling |
| quote_datetime_raw | string | source timestamp |
| quote_date | date | date grain approval pending |
| quote_timezone | string nullable | absent in Raw |
| quote_source_raw | string | source/provenance context |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | 1 | Tên_mặt_hàng + Thị_trường + Loại_giá + Đơn_vị_tính + Loại_tiền + Ngày | 20394 | 0 | 22 | 11 | 8 | 8 |
| thitruongnongsan/price_luagao.xlsx::price_lua_gao | 2 | Tên_mặt_hàng + Thị_trường + Loại_giá + Đơn_vị_tính + Loại_tiền + Ngày + Nguồn | 20394 | 0 | 10 | 5 | 2 | 2 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

| issue_id | issue_type | affected_count | count_basis | handling | proposed_rule | validation_test |
| --- | --- | --- | --- | --- | --- | --- |
| DQ-112 | iqr_outlier_candidate | 920 | observations | FLAG | Flag descriptive within-group IQR signals; review trend, geography and units; never auto-remove | Canonical retention count unchanged for IQR signals |

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

zero-price meaning, conflicting quote duplicates, unit alias approval, product and source dictionaries

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-035 | exact_duplicate_rows | PROPOSED | PENDING_REVIEW | Approve exact duplicate retention policy |
| DQ-112 | iqr_outlier_candidate | PROPOSED | FLAG | Approve only business thresholds, if desired |
| DQ-113 | business_key_value_conflict | PROPOSED | PENDING_REVIEW | Approve business key and conflict resolution |
| DQ-114 | business_key_value_conflict | PROPOSED | PENDING_REVIEW | Approve business key and conflict resolution |
| DQ-115 | zero_price | UNKNOWN | PENDING_REVIEW | Approve zero-price business policy |
| DQ-116 | unit_spelling_aliases | PROPOSED | NORMALIZE | Approve four explicit observed aliases |
| DQ-118 | product_market_source_dictionary | UNKNOWN | PENDING_REVIEW | Approve product/geography/provenance dictionaries |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-035: Before/after difference equals approved duplicate excess
- [ ] DQ-112: Canonical retention count unchanged for IQR signals
- [ ] DQ-113: Approved-key conflicts either retained as distinct grain or explicitly quarantined
- [ ] DQ-114: Approved-key conflicts either retained as distinct grain or explicitly quarantined
- [ ] DQ-115: No zero converted to NULL without approved source semantics
- [ ] DQ-116: Value unchanged; unit dictionary covers only approved exact strings
- [ ] DQ-117: Full Raw date parse succeeds; no timezone invented
- [ ] DQ-118: No merging different processing basis, grades or markets
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

### thitruongnongsan/price_luagao.xlsx::price_lua_gao

Raw records: 20394; columns: 8; sheet/CSV physical rows: 20395; header rows: [1].

Literal headers/positional grid labels:

```text
["Tên_mặt_hàng", "Thị_trường", "Loại_giá", "Đơn_vị_tính", "Loại_tiền", "Nguồn", "Ngày", "Giá"]
```

Relevant cross-source checks (diagnostic; incompatibilities retained):

_Không có bản ghi trong phạm vi phân tích._

Không kế thừa quy tắc từ tài liệu/Silver cũ; mọi proposal có origin trong decision table.

## 13. Ghi chú các vấn đề và đặc thù dữ liệu (Data Anomalies & Issues Profile)

*Lưu ý: Phần này ghi nhận chi tiết hiện trạng, hành vi và các điểm bất thường quan sát được trên tập dữ liệu price_luagao.xlsx, phục vụ tra cứu kỹ thuật; không chứa đề xuất hoặc hướng dẫn xử lý code.*

### 13.1. Định dạng Ngày tháng chuỗi kiểu Mỹ (%m/%d/%Y)
- Cột `Ngày` lưu trữ dưới dạng chuỗi văn bản: `8/3/2026 12:00:00 AM`, `7/31/2026 12:00:00 AM`.
- Sự xuất hiện của ngày `7/31/2026` xác định cấu trúc thời gian là **Tháng / Ngày / Năm** (`%m/%d/%Y`).
- Phần giờ `12:00:00 AM` là giá trị mặc định của Excel, không mang thông tin thời gian thực hoặc múi giờ (timezone naive).
- **Vấn đề**: Nguy cơ đảo lộn giữa Ngày và Tháng đối với các ngày từ mùng 1 đến 12 nếu sử dụng cấu hình định dạng ngày Việt Nam thông thường (`%d/%m/%Y`), và văng lỗi đối với các ngày từ 13 trở đi.

### 13.2. Xung đột giá trị trong cùng một Khóa nghiệp vụ (Business Key Conflicts)
- Khi tạo khóa định danh kết hợp từ: `Tên_mặt_hàng + Thị_trường + Loại_giá + Đơn_vị_tính + Loại_tiền + Ngày`:
  - Có **8 nhóm khóa** xuất hiện nhiều mức giá khác nhau trong cùng một ngày giao dịch.
  - Khi bổ sung thêm trường `Nguồn` vào khóa, vẫn còn **2 nhóm khóa** có nhiều mức giá khác nhau.
- **Bản chất**: Hoạt động giao dịch thực tế của thương lái và thị trường có biến động giá theo buổi (sáng/chiều) hoặc báo theo dải giá (sàn/trần), nhưng cấu trúc bảng chỉ dừng lại ở mức ngày, không có timestamp chi tiết đến giờ/phút.

### 13.3. Dữ liệu Giá bằng 0 VNĐ phi thực tế (Zero Price Observations)
- Có **29 bản ghi có `Giá = 0.0`** (quan sát tại các dòng như 12245, 12277, 12387...).
- **Bản chất**: Lúa gạo là nông sản thương phẩm, giá giao dịch thực tế không bao giờ bằng 0 đồng. Đây là trường hợp không có giao dịch trong ngày (no-quote) hoặc hệ thống nguồn thu thập bị khuyết thiếu nhưng lại gán số 0 thay vì để NULL.

### 13.4. Bất nhất về chính tả Đơn vị tính (Unit Spelling Inconsistencies)
- Toàn bộ 20.394 dòng đều mang bản chất là "Đồng trên kilôgam", nhưng được ghi nhận dưới 4 biến thể chính tả khác nhau:
  - `VNĐ/Kg`: 10.064 dòng
  - `Vnđ/Kg`: 5.072 dòng
  - `VNĐ/kg`: 3.880 dòng
  - `Đồng/kg`: 1.378 dòng
- **Vấn đề**: Gây phân mảnh dữ liệu khi thực hiện tổng hợp (GROUP BY) hoặc tạo bảng danh mục nếu hệ thống phân biệt chữ hoa/chữ thường.

### 13.5. Phân mảnh cực lớn ở Danh mục Mặt hàng (195 Tên mặt hàng)
- Cột `Tên_mặt_hàng` có tới **195 biến thể khác nhau**, trộn lẫn nhiều tiêu chí:
  - **Độ ẩm canh tác**: Lúa tươi vs. Lúa khô (ví dụ: `Lúa IR 50404 (lúa tươi)` ~ 6.000 đ/kg vs. `Lúa IR 50404 (khô)` ~ 8.000 đ/kg).
  - **Mức độ chế biến**: Lúa (thóc) vs. Gạo nguyên liệu (`Gạo NL`) vs. Gạo thành phẩm xát trắng (`Gạo Jasmine`, `Đài Loan`...) vs. Phụ phẩm (tấm, cám).
  - **Giống lúa/gạo cụ thể**: IR 50404, OM 5451, OM 18, Đài thơm 8, ST24, ST25, Nàng Hoa...
- Thang đo giá phân hóa mạnh từ 5.000 đ/kg (lúa tươi) đến 80.000 đ/kg (gạo đặc sản), phát sinh 920 tín hiệu IQR outlier.

### 13.6. Phân tầng 8 Loại giá trong Chuỗi cung ứng (Supply Chain Price Tiers)
- Dữ liệu chứa 8 loại giá khác nhau đại diện cho các mắt xích trong chuỗi giá trị:
  - `Thương lái thu mua` (12.593 dòng - chiếm 62%, giá thu mua bờ ruộng).
  - `Thu mua` (4.530 dòng).
  - `Bán lẻ` (1.045 dòng - giá bán người tiêu dùng tại chợ/cửa hàng).
  - `Khác` (1.007 dòng).
  - `Bán ra` (728 dòng - giá kho/nhà máy bán sỉ).
  - `Xuất khẩu` (291 dòng - giá FOB/quy đổi).
  - `Tại chợ` (194 dòng).
  - `Đại lý thu mua` (6 dòng).
- **Vấn đề**: Mỗi loại giá phản ánh chi phí và biên lợi nhuận ở từng nấc phân phối khác nhau, không thể so sánh ngang hàng hoặc tính trung bình gộp.

### 13.7. Trùng lặp dòng toàn phần (Exact Duplicate Rows)
- Tồn tại 3 dòng bị trùng lặp hoàn toàn trên tất cả 8 cột (tổng cộng 6 bản ghi, ví dụ dòng 3892, 3893, 3907), giống nhau từ Tên mặt hàng, Thị trường, Loại giá, Đơn vị tính, Loại tiền, Nguồn, Ngày cho đến Giá.
