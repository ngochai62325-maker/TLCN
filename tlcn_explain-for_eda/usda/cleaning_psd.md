# Cleaning specification — cleaning_psd

Review snapshot: 2026-10-11. Generated from EDA_v2.ipynb; counts FULL_DATA for selected inputs. Source/file selection: SOURCE_FILTER=None, FILE_FILTER=None. Đây là thiết kế để review, chưa triển khai Silver. VERIFIED là đủ evidence cho đề xuất, không thay phê duyệt.

Counts records/cells/key groups/headers khác nhau và có overlap; không cộng affected_count để báo unique bad records.

## 1. Dataset/source overview

PSD Vietnam Rice, Milled (HTML export)

Nguồn kết luận: RAW_PROFILE là quan sát trực tiếp; RAW_SOURCE_METADATA là source metadata đi cùng Raw; OFFICIAL_SOURCE là tài liệu USDA được đối chiếu sau profiling. Không sử dụng Silver hoặc eda_*.md cũ để xác nhận quy tắc.

| file | measure/season | observation_rows |
| --- | --- | --- |
| usda/usda.xls | PSD Vietnam Rice, Milled (HTML export) | 990 |

## 2. Input files và schema fingerprint

| file | table | role | format | encoding | bytes | rows | columns | physical_rows | schema_fingerprint | content_sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls | html_table_1 | observations | html (.xls extension) | utf-8 | 45549 | 15 | 70 | 16 | 8cc6389658bb99c7 | 64a282114fcc6ddca3228a7af2a1db924200068de1e55d42c627043f6283f4cf |

rows là Raw parser records (Excel direct table đã tách header row); physical_rows là sheet grid size/CSV record count gồm header; schema fingerprint SHA256 rút gọn 16 ký tự của literal headers + format; full content hash cố định snapshot. CSV cp1252/latin-1 là decoder candidate, chưa chứng minh encoding gốc. Lookup CSV và workbook metadata được ghi rõ role.

## 3. Data grain và business semantics

- **grain**: commodity block × attribute × country × marketing/crop year
- **time**: 1960/1961 style periods retained; crop vs marketing/trade-year begin/end need evidence; TY not conflated with MY
- **geography**: Country Vietnam; blank Commodity cells appear inside block
- **measure**: 15 observed attributes including rough/milled production, Yield, MY and TY trade and Milling Rate
- **units**: 1000 HA ×1000 → ha; 1000 MT ×1000 → tonne, except Milling Rate (.9999) ×0.0001 → ratio; MT/HA → t/ha
- **additivity**: stocks not additive over years; yields/ratios nonadditive; Total Supply/Distribution overlap components; MY/TY trade distinct



## 4. Raw profiling findings

### Numeric values

| id | field | observations | numeric_valid | missing_candidates | parse_failures | comma_grouped | nonfinite | negative | zero | min | max | iqr_flagged | iqr_grouping | decimal_precision_counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | raw_value | 990 | 990 | 0 | 0 | 597 | 0 | 0 | 140 | 0.0 | 45066.0 | 0 | Attribute, Unit Description, raw_column | {np.int64(2): 66} |

### Date/year validation

| id | field | type | min | max | invalid | missing_periods |
| --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | period_raw | marketing/crop year; exact start month unknown | 1960/1961 | 2025/2026 | 0 | period labels only |

### Raw exact duplicates

| id | scope | rows | exact_duplicate_excess | exact_duplicate_participants |
| --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | raw table | 15 | 0 | 0 |

IQR flags là exploratory signals theo grouping ở bảng, không kết luận lỗi. Precision là số decimal digits quan sát, chưa phải canonical decimal scale. Negative values giữ nguyên chờ policy theo measure. Zero không mặc định missing.

## 5. Missing / duplicate / type / unit / geography issues

| id | field | python_null | empty_string | whitespace_only | token_candidates | zero_numeric | token_'..' | token_'NUL' | token_'NULL' | token_'NA' | token_'N/A' | token_'-' | token_'...' | token_'…' | blank_or_null | unresolved_missing_candidates | denominator_cells | candidate_rate_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | raw_value | 0 | 0 | 0 | 0 | 140 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 990 | 0.0 |

NULL, empty, whitespace, tokens và zero tách riêng. Full per-column Raw counts và native dtypes nằm trong STEP 2–3; bảng dưới ghi counts quan sát theo file và cột cần xử lý, gồm structural blank/header cells và leading-zero risk:

| id | raw_cells | null_cells | empty_cells | whitespace_cells | token_cells | zero_cells |
| --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | 1050 | 0 | 14 | 0 | 0 | 140 |

_Không có bản ghi trong phạm vi phân tích._

## 6. Cleaning decision table

| issue_id | file | table | field | issue_type | affected_count | count_basis | raw_examples | proposed_rule | canonical_type_unit | handling | evidence | confidence | required_approval | validation_test | origin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ-122 | usda/usda.xls | html_table_1 | raw_value | thousands_separator | 597 | cells | row 2, column 1960/1961: 4,602; row 4, column 1960/1961: 5,959; row 5, column 1960/1961: 9,168 | Parse only strict comma-grouped thousands grammar; retain raw lexical value | decimal | NORMALIZE | Values match grouped-thousands format; source HTML uses US number format | VERIFIED | Accept numeric parser grammar | 6,250 → 6250; reject ambiguous 1,25 and unrecognized tokens | RAW_PROFILE |
| DQ-123 | usda/usda.xls | html_table_1 | Commodity | grouped_commodity_blank | 924 | observations | row 3, column 1960/1961: ; row 4, column 1960/1961: ; row 5, column 1960/1961:  | Propose filling Commodity only inside proven HTML commodity block, with boundaries and raw blank retained | commodity candidate + raw Commodity | PENDING_REVIEW | First row Rice, Milled; subsequent attribute rows blank; HTML grouping | PROPOSED | Approve bounded commodity propagation | No propagation across country/table/block boundary | RAW_PROFILE |
| DQ-124 | usda/usda.xls | html_table_1 | Attribute / Unit Description / raw_value | milling_rate_encoding | 66 | cells | row 6, column 1960/1961: 6,500; row 6, column 1961/1962: 6,500; row 6, column 1962/1963: 6,500 | Only Milling Rate (.9999): canonical_value = raw_numeric / 10000; always retain raw_value and raw_unit; canonical unit ratio | decimal ratio (0..1) + raw unit/value | NORMALIZE | Raw encoded values 6250/6270/6500/6600; official FAS Vietnam 2010: 62.5–62.7%; TU1012 formula ×10000. STEP 8 verifies milled/rough identity. https://apps.fas.usda.gov/newgainapi/api/report/downloadreportbyfilename?filename=Grain+and+Feed+Annual_Hanoi_Vietnam_4-5-2010.pdf ; https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Grain+and+Feed+Annual_Ankara_Turkey_03-01-2001.pdf | VERIFIED | Approve measure-scoped conversion before Silver | 6250 → 0.6250; reconstruction of rough×ratio matches milled within reporting precision; other attributes unchanged | OFFICIAL_SOURCE |
| DQ-125 | usda/usda.xls | html_table_1 | period_raw / Attribute | marketing_crop_trade_year | 990 | observations | row 2, column 1960/1961: 1960/1961; row 3, column 1960/1961: 1960/1961; row 4, column 1960/1961: 1960/1961 | Keep year-span labels; distinguish MY and TY; approve period begin/end and vintage before calendar mapping | period_raw + period_basis + nullable start/end dates | PENDING_REVIEW | Year-span headers and TY attributes present; no begin-month/release fields in file | UNKNOWN | Approve period and forecast/release identity | No date/year conversion from first four digits alone | RAW_PROFILE |

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
| commodity_raw | string nullable | blank block labels retained |
| commodity | string nullable | bounded propagation pending |
| country_raw | string | Vietnam source label |
| attribute_raw | string | MY/TY/milling/rough/yield kept |
| period_raw | string | YYYY/YYYY |
| period_basis | string | marketing/crop/trade pending attribute policy |
| period_start | date nullable | no fabricated start date |
| period_end | date nullable | needs source evidence |
| statistic_kind | string | quantity/area/yield/ratio |
| processing_basis | string nullable | rough/milled |
| conversion_factor | decimal nullable | 0.0001 only Milling Rate; ×1000 only physical thousand unit |

Schema là đề xuất, chưa cast/convert/write Raw hay Silver. Preserve raw lexical identifiers, measure values/units, source flags/notes, period labels và lineage. Decimal precision/scale phải chọn theo source precision và downstream requirements; không tự làm tròn để PASS.

## 8. Business-key candidates

| id | candidate | key | rows | key_incomplete_rows | duplicate_participants | duplicate_excess | conflict_groups | value_conflict_groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| usda/usda.xls::html_table_1 | 1 | Commodity + Attribute + Country + period_raw | 990 | 924 | 0 | 0 | 0 | 0 |

Candidate ≠ approved unique constraint. duplicate_participants gồm cả bản gốc; duplicate_excess là phần dư; conflict_groups xét distinct full-row variants, value_conflict_groups chỉ xét distinct measure values. Không chọn winner theo file order/ingestion timestamp. Nếu release metadata thiếu, source_content_sha256 chỉ là snapshot lineage, không đại diện business vintage.

## 9. Proposed DQ / quarantine rules

_Không có bản ghi trong phạm vi phân tích._

Quarantine chỉ áp sau khi grain/parser/reference rules được review. Không quarantine tự động missing tokens, negatives, residuals, outliers hoặc nhãn tỉnh lịch sử. Key conflict pending cần xác định grain/release trước; giữ đầy đủ evidence, không last-wins.

## 10. Unknowns và required approvals

commodity block propagation authorization, period start dates, release/vintage and forecast status

| issue_id | issue_type | confidence | handling | required_approval |
| --- | --- | --- | --- | --- |
| DQ-123 | grouped_commodity_blank | PROPOSED | PENDING_REVIEW | Approve bounded commodity propagation |
| DQ-125 | marketing_crop_trade_year | UNKNOWN | PENDING_REVIEW | Approve period and forecast/release identity |

Review source-specific period basis, units, geography versions, missing tokens, exact/semantic duplicate policy and aggregation/additivity. Approve implementation only after this spec is reviewed; task hiện tại không viết Silver.

## 11. Validation checklist

- [ ] DQ-122: 6,250 → 6250; reject ambiguous 1,25 and unrecognized tokens
- [ ] DQ-123: No propagation across country/table/block boundary
- [ ] DQ-124: 6250 → 0.6250; reconstruction of rough×ratio matches milled within reporting precision; other attributes unchanged
- [ ] DQ-125: No date/year conversion from first four digits alone
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
| Lineage: usda/usda.xls::html_table_1 | PASS | Every projected observation points to original row and column |
| Milling factor identity | PASS | Milled ≈ rough×encoded/10000 within source integer reporting precision |

## 12. Mapping tới file Raw, observed rows/columns và cross-source evidence

CSV row = header-inclusive record ordinal (multiline record có thể khác physical line). Excel row = worksheet physical row. HTML row = table row ordinal (header row 1), không phải HTML text line. raw_column giữ literal field/year hoặc 1-based ordinal. Issue table cung cấp representative exact coordinates; toàn bộ masks/profiles ở notebook để drill down.

### usda/usda.xls::html_table_1

Raw records: 15; columns: 70; sheet/CSV physical rows: 16; header rows: [1].

Literal headers/positional grid labels:

```text
["Commodity", "Attribute", "Country", "1960/1961", "1961/1962", "1962/1963", "1963/1964", "1964/1965", "1965/1966", "1966/1967", "1967/1968", "1968/1969", "1969/1970", "1970/1971", "1971/1972", "1972/1973", "1973/1974", "1974/1975", "1975/1976", "1976/1977", "1977/1978", "1978/1979", "1979/1980", "1980/1981", "1981/1982", "1982/1983", "1983/1984", "1984/1985", "1985/1986", "1986/1987", "1987/1988", "1988/1989", "1989/1990", "1990/1991", "1991/1992", "1992/1993", "1993/1994", "1994/1995", "1995/1996", "1996/1997", "1997/1998", "1998/1999", "1999/2000", "2000/2001", "2001/2002", "2002/2003", "2003/2004", "2004/2005", "2005/2006", "2006/2007", "2007/2008", "2008/2009", "2009/2010", "2010/2011", "2011/2012", "2012/2013", "2013/2014", "2014/2015", "2015/2016", "2016/2017", "2017/2018", "2018/2019", "2019/2020", "2020/2021", "2021/2022", "2022/2023", "2023/2024", "2024/2025", "2025/2026", "Unit Description"]
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

### 13.1. Sai lệch định dạng tệp (HTML Table gắn đuôi mở rộng .xls)
- Tệp `usda.xls` mang phần mở rộng nhị phân của Excel (.xls) nhưng nội dung thực tế là mã nguồn bảng HTML (`<html><head>...<table class="...">`).
- **Bản chất**: Đây là tệp kết xuất từ giao diện web AngularJS của hệ thống USDA PSD Online.
- **Vấn đề**: Gây lỗi khi đọc bằng các engine phân tích bảng tính Excel tiêu chuẩn nếu không xác định đúng định dạng gốc.

### 13.2. Mã hóa nhân 10.000 của chỉ tiêu Tỷ lệ xát gạo (Milling Rate) và Sai lệch nhãn đơn vị
- Dòng đo lường `Milling Rate (.9999)` mang các giá trị nguyên như `6,500.00` hoặc `6,250.00`.
- Cột `Unit Description` ở cuối bảng hiển thị nhãn `(1000 MT)`.
- **Bản chất**: Đây là tỷ lệ thu hồi gạo xát quy ước hệ số nhân 10.000 ($6.500 / 10.000 = 0.6500 = 65\%$), hoàn toàn không phải khối lượng "6.500 nghìn tấn".
- **Bằng chứng toán học**: $\text{Rough Production} \times (\text{Milling Rate} / 10.000) \approx \text{Milled Production}$ (ví dụ năm 1960/1961: $9.168 \times 0.6500 = 5.959,2 \approx 5.959$).

### 13.3. Khuyết thiếu ô gộp danh mục hàng hóa (Grouped Commodity Blanks)
- Cột `Commodity` chỉ xuất hiện nhãn `"Rice, Milled"` ở dòng đầu tiên (`Area Harvested`). Toàn bộ 14 dòng chỉ tiêu thành phần phía dưới đều mang giá trị rỗng (`NaN/empty`).
- **Vấn đề**: Dữ liệu mang cấu trúc hiển thị trực quan (visual report layout) thay vì dạng bảng phẳng (flat tabular layout).

### 13.4. Cấu trúc Ma trận ngang đa niên vụ (Wide Matrix Layout với 66 niên vụ)
- Dữ liệu trải dài trên 70 cột, trong đó có 66 cột đại diện cho các niên vụ từ `1960/1961` đến `2025/2026`.
- Nhãn cột mang định dạng dải năm `YYYY/YYYY` thay vì năm dương lịch đơn lẻ.

### 13.5. Đồng tồn tại giữa Niên vụ tiếp thị (Marketing Year - MY) và Niên vụ thương mại (Trade Year - TY)
- Dữ liệu theo dõi song song 2 cặp chỉ tiêu ngoại thương: `Imports` vs. `TY Imports`, và `Exports` vs. `TY Exports`.
- **Bản chất**: Niên vụ tiếp thị (MY) tuân theo chu kỳ mùa vụ canh tác, trong khi Niên vụ thương mại (TY) tuân theo năm lịch chuẩn. Đây là 2 chỉ tiêu đo lường độc lập, không phải dữ liệu trùng lặp.

### 13.6. Dấu phẩy phân cách hàng nghìn kiểu Mỹ trong chuỗi số (Thousands Separators)
- Có 597 ô giá trị chứa dấu phẩy phân cách hàng nghìn (`4,602`, `5,959`, `9,168`).
- **Vấn đề**: Cần bảo đảm parser nhận diện dấu phẩy phân cách hàng nghìn của định dạng số chuẩn Mỹ để không nhầm lẫn với dấu thập phân.
