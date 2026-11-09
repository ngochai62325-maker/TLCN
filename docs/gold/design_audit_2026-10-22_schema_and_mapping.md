> **HISTORICAL AUDIT — không phải canonical design hiện tại.** Annotation ngày 25/10/2026: nội dung báo cáo bên dưới, findings, counts và kết quả kiểm tra thuộc thiết kế v3 27 bảng tại `1bb662c`, ngày 22/10. Các từ “hiện tại”, số dòng DBML và links tới filename DBML bên trong body phải được hiểu theo phiên bản đó. Xem lại DBML gốc bằng `git show 1bb662c:docs/gold/dim_fact_v3.dbml`.
>
> Canonical proposal mới là [README](README.md), [DBML cùng filename nhưng nội dung mới](dim_fact_v3.dbml) và [source_to_gold_mapping.csv](source_to_gold_mapping.csv), version `gold-20261025.1`: 17 bảng, chưa được owner phê duyệt và chưa triển khai ETL. Proposal 14-table MVP trong audit cũ đã được thay thế. [CSV ngày 22/10](design_audit_2026-10-22_field_mapping.csv) và [evidence JSON](audit_2026-10-22_evidence.json) là historical evidence bất biến; không phải mapping hoặc nghiệm thu của thiết kế mới. Không cập nhật lại số liệu lịch sử.

---

# Phụ lục Gold design audit — schema Silver và mapping từng cột

Ngày 22/10/2026. Đọc cùng [báo cáo A–I](design_audit_2026-10-22.md). Chỉ là đặc tả audit; không triển khai schema/ETL.

## Bổ sung B — toàn bộ logical Silver schema

`!` = nullable:false theo YAML; `?` = nullable:true/mặc định. Mọi business key được framework bắt buộc non-null dù field có `?`. Physical Iceberg nullability chưa được kiểm chứng. Mọi output bên dưới ở `iceberg.silver_local.<target>` khi bật local_nine.

### `faostat_monthly_price` → `iceberg.silver_local.faostat_monthly_price`

BK: `country_code, commodity_code, year, month_code, element_code`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `domain_code` | `string` | ! |
| `domain` | `string` | ! |
| `country_code` | `string` | ! |
| `country_name` | `string` | ! |
| `element_code` | `string` | ! |
| `element` | `string` | ! |
| `commodity_code` | `string` | ! |
| `commodity_name` | `string` | ! |
| `year` | `integer` | ! |
| `month_code` | `string` | ! |
| `month_name` | `string` | ! |
| `unit` | `string` | ? |
| `value` | `decimal(18,4)` | ? |
| `flag` | `string` | ? |
| `flag_description` | `string` | ? |
| `time_grain` | `string` | ! |
| `month` | `integer` | ? |
| `currency` | `string` | ? |
| `price_kind` | `string` | ! |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `faostat_production` → `iceberg.silver_local.faostat_production`

BK: `country_code, commodity_code, year, element_code`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `domain_code` | `string` | ! |
| `domain` | `string` | ! |
| `country_code` | `string` | ! |
| `country_name` | `string` | ! |
| `element_code` | `string` | ! |
| `element` | `string` | ! |
| `commodity_code` | `string` | ! |
| `commodity_name` | `string` | ! |
| `year` | `integer` | ! |
| `unit` | `string` | ? |
| `value` | `decimal(18,2)` | ? |
| `flag` | `string` | ? |
| `flag_description` | `string` | ? |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `faostat_supply_utilization` → `iceberg.silver_local.faostat_supply_utilization`

BK: `country_code, commodity_code, year, element_code`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `domain_code` | `string` | ! |
| `domain` | `string` | ! |
| `country_code` | `string` | ! |
| `country_name` | `string` | ! |
| `element_code` | `string` | ! |
| `element` | `string` | ! |
| `commodity_code` | `string` | ! |
| `commodity_name` | `string` | ! |
| `year` | `integer` | ! |
| `unit` | `string` | ? |
| `value` | `decimal(18,2)` | ? |
| `flag` | `string` | ? |
| `flag_description` | `string` | ? |
| `note` | `string` | ? |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `faostat_trade` → `iceberg.silver_local.faostat_trade`

BK: `reporter_country_code, partner_country_code, commodity_code, year, element_code`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `reporter_country_name` | `string` | ! |
| `reporter_country_code` | `string` | ! |
| `partner_country_name` | `string` | ! |
| `partner_country_code` | `string` | ! |
| `commodity_name` | `string` | ! |
| `commodity_code` | `string` | ! |
| `element_name` | `string` | ! |
| `element_code` | `string` | ! |
| `year` | `integer` | ! |
| `unit` | `string` | ? |
| `value` | `decimal(18,2)` | ? |
| `flag` | `string` | ? |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `nso` → `iceberg.silver_local.nso_rice_statistics`

BK: `source_table, source_geography_key, year, measure, season, statistic_kind`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `bronze_read_snapshot_id` | `long` | ! |
| `source_table` | `string` | ! |
| `source_column` | `string` | ! |
| `source_geography_key` | `string` | ! |
| `geography_raw` | `string` | ? |
| `geography_name_candidate` | `string` | ? |
| `geography_level` | `string` | ! |
| `geography_status` | `string` | ! |
| `geography_code` | `string` | ? |
| `boundary_version` | `string` | ? |
| `geography_mapping_version` | `string` | ! |
| `geography_evidence` | `string` | ! |
| `period_raw` | `string` | ! |
| `year` | `integer` | ! |
| `measure` | `string` | ! |
| `season` | `string` | ! |
| `statistic_kind` | `string` | ! |
| `release_status` | `string` | ! |
| `value_raw` | `string` | ? |
| `value_source` | `decimal(28,8)` | ? |
| `unit_raw` | `string` | ? |
| `unit_source_reference` | `string` | ! |
| `conversion_factor` | `decimal(8,0)` | ! |
| `value` | `decimal(28,8)` | ? |
| `unit` | `string` | ! |
| `unit_status` | `string` | ! |
| `unit_reference` | `string` | ! |
| `unit_evidence` | `string` | ! |
| `unit_mapping_version` | `string` | ! |
| `metadata_version` | `string` | ! |
| `quality_status` | `string` | ! |
| `missing_reason` | `string` | ? |
| `review_reason` | `string` | ? |
| `diagnostic_evidence` | `string` | ! |

### `thitruongnongsan` → `iceberg.silver_local.thitruongnongsan`

BK: `market, commodity, price_type, unit, currency, source, date`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `commodity` | `string` | ! |
| `market` | `string` | ! |
| `price_type` | `string` | ! |
| `unit` | `string` | ? |
| `currency` | `string` | ? |
| `source` | `string` | ? |
| `date` | `date` | ! |
| `price_value` | `decimal(18,2)` | ? |
| `year` | `integer` | ! |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `usda_psd` → `iceberg.silver_local.usda_rice_psd`

BK: `country, commodity, attribute, market_year`. Partition: `market_year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `country` | `string` | ! |
| `commodity` | `string` | ! |
| `attribute` | `string` | ! |
| `market_year` | `integer` | ! |
| `crop_year` | `string` | ! |
| `unit` | `string` | ? |
| `value` | `decimal(18,4)` | ? |
| `value_source` | `decimal(28,8)` | ? |
| `source_unit` | `string` | ? |
| `conversion_factor` | `decimal(18,6)` | ! |
| `value_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `usda_rice_yearbook` → `iceberg.silver_local.usda_rice_yearbook`

BK: `location, commodity_class, year, reference_period, statistic_type`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `table_name` | `string` | ? |
| `table_number` | `integer` | ? |
| `commodity` | `string` | ! |
| `commodity_class` | `string` | ! |
| `year` | `integer` | ! |
| `reference_period` | `string` | ! |
| `location` | `string` | ! |
| `statistic_type` | `string` | ! |
| `value` | `decimal(18,2)` | ? |
| `unit` | `string` | ? |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### `worldbank_pinksheet` → `iceberg.silver_local.worldbank_commodity_monthly`

BK: `commodity_code, period_date`. Partition: `year`.

| Cột | Spark type | Logical requiredness |
|---|---|---|
| `commodity_code` | `string` | ! |
| `commodity_name` | `string` | ! |
| `period_code` | `string` | ! |
| `period_date` | `date` | ! |
| `year` | `integer` | ! |
| `month` | `integer` | ! |
| `price` | `decimal(18,2)` | ? |
| `unit` | `string` | ? |
| `series_name` | `string` | ? |
| `value_raw` | `string` | ? |
| `unit_raw` | `string` | ? |
| `missing_reason` | `string` | ? |

### Cột kỹ thuật shared (ngoài schema.columns YAML)

| Cột | Kiểu theo Bronze/runtime code | Semantics |
|---|---|---|
| `_ingestion_run_id`, `_ingestion_batch_id`, `_source_id`, `_source_file`, `_source_checksum` | string | Provenance của Bronze; không publication timestamp. |
| `_ingestion_chunk_id`, `_source_snapshot_id` | long/bigint | `_source_snapshot_id` được FullLoader truyền0, không phải Iceberg read snapshot. |
| `_ingestion_timestamp` | timestamp, Bronze Trino TIMESTAMP(6) WITH TIME ZONE | Ingest time của chunk; Spark session UTC. |
| `_dq_warnings` | array<struct<rule_id:string,error_message:string,failed_column:string>> | Giữ warning của rule LOG/KEEP. |
| `_silver_processed_at` | timestamp | Processing timestamp; không so như business content để quyết định physical NOOP. |

Riêng NSO có `bronze_read_snapshot_id` trong YAML và bắt buộc đủ tám audit fields. Quarantine local: `iceberg.silver_local.dead_letters`, fields `quarantine_record_id STRING, source_dataset STRING, pipeline_run_id STRING, detected_at TIMESTAMP, business_key_hash STRING, errors ARRAY<STRUCT<rule_id:STRING,error_message:STRING,failed_column:STRING>>, payload STRING`; partition(source_dataset,days(detected_at)). Payload giữ observation gốc và lỗi; không phải Gold fact.

## Bổ sung E — quy ước đọc field matrix

DIRECT = field/giá trị có trực tiếp, có thể rename/select nhưng không phát minh. DERIVED = quy tắc xác định hoặc metadata ETL minh bạch; LOOKUP = cần dimension/reference/xref có scope; EXTERNAL = cần dữ liệu/reference chưa có; UNSUPPORTED = chưa có cơ sở tạo giá trị đúng; AMBIGUOUS = thiếu quyết định semantic/grain/availability. DIRECT không bảo đảm xong toàn bộ bảng: PK/pivot/xref vẫn có thể đang blocked. DERIVED có điều kiện phải đợi điều kiện được chứng minh; không coi công thức conditional là mapping đã READY.

Nguồn rút gọn (đều `iceberg.silver_local`): QCL=faostat_production; PP=faostat_monthly_price; SUA=faostat_supply_utilization; TM=faostat_trade; PSD=usda_rice_psd; YB=usda_rice_yearbook; WB=worldbank_commodity_monthly; VN=thitruongnongsan; NSO=nso_rice_statistics. Các `ref_*` có chữ planned là reference đề xuất, chưa tồn tại, không phải bảng Silver đã tìm thấy.

**L0 cho mọi LOOKUP:** source lookup table ghi ngay trong row; match phải đúng một member cho đúng source code-system/label/time/basis. >1 match → FAIL. 0 match → core publish FAIL hoặc QUARANTINE có payload/BK/reason/snapshot và reconciliation ledger; có thể giữ unresolved source member khi full source identity còn trong fact PK. Không silently drop, fuzzy auto-match hoặc gom nhiều unresolved values thành -1 làm collision. Optional descriptive attribute có thể NULL+WARN, không fabricate.

**PIVOT:** `MAX(value) FILTER(WHERE ...)` chỉ là chọn một giá trị sau khi kiểm tra COUNT(*)<=1 ở cell grain, không phải resolution cho conflicting observations. Không SUM/AVG/MAX qua publication, product, benchmark hay unit chưa tương đương. Missing observation/value giữ NULL; không COALESCE0.

### Grouping/filter definitions áp dụng cho các công thức

| Mã | Filter/grouping keys và điều kiện |
|---|---|
| G_NP | NSO geography_level='province',statistic_kind='quantity'; group year,source_geography_key,season,reviewed publication-family trước pivot area/production. Official geography lookup chưa có. Không xóa source_table khỏi identity khi chưa có publication policy. V3 season-only view loại annual có audit; corrected observation fact giữ annual. |
| G_NR | NSO geography_level IN('national','region'); group year,source_geography_key,season,source_series/publication,statistic_kind; source table phải được bảo toàn. Khi pivot quantity+index phải có explicit same-series alignment, không join national scopes từ nhiều publication rồi MAX. |
| G_Q | QCL CPC0113; group(year,country_code,commodity_code), mỗi element chỉ1row. Match keys sau xref vẫn unique; quantity unit checks trước casting. |
| G_P | PSD group(country,commodity,market_year,crop_year), pivot exact attribute names; verify unit1000HA/1000MT/fraction/MT/HA; TY khác MY. |
| G_S | SUA group(year,country_code,commodity_code), pivot 12 allowed elements, unittonne; signed5071/5166 giữ. Không giả metric absent bằng0. |
| G_T | TM group(year,reporter_country_code,partner_country_code,source commodity_code/basis), pivot5610/5622/5910/5922; scope core allowlist + excluded ledger. Product conformance không được gây cell collision. |
| G_C | PP monthly only; WB named source series, valid monthly period; YB chỉ 12 month labels và approved calendar mapping. Grain corrected(month,price_series_key), series giữ source benchmark/class/statistic/unit/currency. Source-provided annual summaries ở views riêng. |
| G_V | VN full BK market,commodity,price_type,unit,currency,source,date; corrected(date,quote_series_key), full quote tuple trong series. Không group chỉprovince/variety/type/date. |
| G_R | Aggregate curated non-overlapping provincial observations, cùng approved geography version, year/product và declared mutually-exclusive season set. Row-level area/prod pairing + coverage trước ratio; national selected source join1:1. |
| G_A | Calendar market-position view: annual QCL + trade aggregate trước join, cùng exact product basis/year; approved non-overlapping country population; PSD có viewmarketing-year riêng. Không gán crop-year start thành calendar equivalence. |
| G_B | Trade bilateral view tại(year,reporter,partner,exact product), cùng declared reporting-side/partner scope. Không cộng World total với details hoặc export với mirrored import. |
| G_M | Dense calendar month spine×country×product×chosen source price series×feature version; every feature requires available_at<=explicit cutoff and correct vintage. Targets dùng exact calendar +1/+3; training excludes missing labels and uses temporal split/embargo. Mart deferred. |

### Coverage

27/27 tables, 261/261 columns. Classification counts: AMBIGUOUS=23, DERIVED=121, DIRECT=30, EXTERNAL=14, LOOKUP=69, UNSUPPORTED=4. Các cột lineage lặp lại vẫn được liệt kê riêng để không bỏ sót.

### `Dim_Year` — DBML dòng 35

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 36 | DIRECT | QCL/PP/SUA/TM/NSO.year; PSD.market_year cho nhãn start-year | YYYY integer; calendar spine bổ sung years cần thiết. Không đồng nhất PSD event year với calendar period. |
| `decade` / 37 | DERIVED | Dim_Year.year_key | 10 * FLOOR(year_key / 10); không áp dụng công thức lên Unknown. |
| `is_leap_year` / 38 | DERIVED | Dim_Year.year_key | (year % 400 = 0) OR (year % 4 = 0 AND year % 100 <> 0). |

### `Dim_Month` — DBML dòng 43

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `month_key` / 44 | DERIVED | PP.year/month; WB.year/month; YB.year/reference_period | 100*calendar_year+month, month 1..12. PP time_grain=monthly; WB period hợp lệ; YB calendar mapping còn G07. Dense calendar tạo bằng date sequence. |
| `year_key` / 45 | DERIVED | Dim_Month.month_key | year=FLOOR(month_key/100), month=MOD(month_key,100), validate bằng calendar date; YB cần G07. |
| `month` / 46 | DERIVED | Dim_Month.month_key | year=FLOOR(month_key/100), month=MOD(month_key,100), validate bằng calendar date; YB cần G07. |
| `quarter` / 47 | DERIVED | Dim_Month.month | 1+FLOOR((month-1)/3). |
| `month_name` / 48 | DERIVED | Dim_Month.month | Calendar month-name với locale cố định, không lệ thuộc locale OS. |
| `harvest_season_code` / 49 | AMBIGUOUS | Không có monthly harvest calendar trong Silver | Rule tháng trong DBML là heuristic, chưa chứng minh đúng mọi vùng/năm. Bỏ khỏi conformed calendar hoặc dùng reference có geo/time scope; không coi source season annual là lịch thu hoạch tháng. |

### `Dim_Date` — DBML dòng 54

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `date_key` / 55 | DERIVED | VN.date / calendar spine | year(date)*10000+month(date)*100+day(date); chỉ valid date. |
| `full_date` / 56 | DIRECT | VN.date / generated calendar | Ngày observation, không publication date. Không tạo ngày giả cho Unknown để thỏa NOT NULL. |
| `month_key` / 57 | DERIVED | full_date | 100*year(full_date)+month(full_date). |
| `day_of_month` / 58 | DERIVED | full_date | day(full_date). |
| `day_of_week` / 59 | DERIVED | full_date | ISO weekday Monday=1..Sunday=7, không dùng Spark dayofweek trực tiếp nếu chưa đổi Sunday=1. |
| `day_name` / 60 | DERIVED | full_date | Tên weekday theo locale cố định. |
| `is_weekend` / 61 | DERIVED | day_of_week | day_of_week IN (6,7). |

### `Dim_Crop_Year` — DBML dòng 66

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `crop_year_key` / 67 | DERIVED | PSD.crop_year / approved key seed | Stable key cho nhãn YYYY/YYYY+1 (có thể start-year); không auto-increment Iceberg. Nhãn không chứa actual country marketing start/end dates. |
| `crop_year_label` / 68 | DIRECT | PSD.crop_year | Giữ nguyên nhãn source crop-year; không ép YYYY thành calendar event period. |
| `start_year` / 69 | DIRECT | PSD.market_year | Năm đầu của source label, không tự suy ngày bắt đầu niên vụ. |
| `end_year` / 70 | DERIVED | PSD.crop_year | Parse phần sau /, validate end=start+1; không suy country-specific month/day. |

### `Dim_Crop_Season` — DBML dòng 75

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `season_key` / 76 | DERIVED | NSO.season + fixed reference seed | Stable source-season key, giữ summer_autumn_autumn_winter nguyên khối; không sequence thay đổi khi rebuild. |
| `season_code` / 77 | DIRECT | NSO.season | Domain hiện tại: annual,winter_spring,summer_autumn,summer_autumn_autumn_winter,main_season; DBML domain đang drift. |
| `season_name_vi` / 78 | LOOKUP | Planned ref_crop_season, căn cứ NSO source_table/title metadata | Bảng tên hiển thị có version, mỗi code đúng một row; unmatched giữ raw code/cảnh báo, không tự đổi season identity. |
| `season_name_en` / 79 | LOOKUP | Planned ref_crop_season, căn cứ NSO source_table/title metadata | Bảng tên hiển thị có version, mỗi code đúng một row; unmatched giữ raw code/cảnh báo, không tự đổi season identity. |
| `is_annual_summary` / 80 | DERIVED | NSO.season | season='annual'; false không tự chứng minh các season còn lại mutually exclusive/đủ coverage. |

### `Dim_Source_System` — DBML dòng 89

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `source_system_key` / 90 | DERIVED | 9 canonical registry source IDs + stable seed | Seed ID ổn định, không dùng registry aliases như source mới; scope dataset, không chỉ provider FAO/USDA. |
| `source_code` / 91 | LOOKUP | Planned ref_source_system ↔ source_registry canonical IDs | FAOSTAT_QCL→faostat_production; TM→faostat_trade; SUA→faostat_supply_utilization; PP→faostat_monthly_price; USDA_PSD/YEARBOOK/WB/VN_NSO/VN_TTNS tương ứng 5 IDs còn lại. Missing mapping FAIL. |
| `source_name` / 92 | LOOKUP | source_registry.sources.<id>.source_name | Registry đã có sẵn, không phải Silver field: lookup đúng canonical ID, thiếu entry FAIL; không cần external source mới. |
| `publishing_org` / 93 | LOOKUP | source_registry.provider + reviewed publisher reference | USDA PSD=FAS, Yearbook=ERS; IPSARD publisher khác information-source field của quote. Unmatched publisher WARN, không đổi quote key. |
| `default_cadence` / 94 | AMBIGUOUS | source_registry.frequency; Silver reference periods | PSD frequency monthly là refresh, observation annual; Yearbook snapshot có monthly và MARKETING. Tách observation_frequency khỏi publication_cadence thay một default. |

### `Dim_Country` — DBML dòng 99

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `country_key` / 100 | LOOKUP | Planned ref_country_identity + Xref_Country_SourceAlias | Stable key theo area identity/code system; historical/aggregate giữ riêng; unmatched theo L0, không gom vào -1 gây collision. |
| `iso3_code` / 101 | EXTERNAL | Authoritative versioned country/area reference chưa có trong repository | FAO M49 và USDA name không tự cung cấp ISO3. Historical/aggregate có thể NULL; không ép China159 và mainland156 cùng ISO identity. |
| `m49_code` / 102 | DIRECT | QCL/PP/SUA.country_code; TM.reporter_country_code/partner_country_code | Giữ 3-digit source code; có FAO-specific/historical area codes, cần code-system metadata trước khi gọi là strict UN M49. |
| `country_name_en` / 103 | LOOKUP | Planned ref_country_identity; aliases từ Silver country_name/country/location | Tên canonical theo identity; chưa map thì giữ source label và audit, không fuzzy merge các thực thể lịch sử. |
| `region_un` / 104 | EXTERNAL | Authoritative UN area hierarchy version chưa có | Defer nếu không dùng dashboard; không suy region từ country string. |
| `sub_region_un` / 105 | EXTERNAL | Authoritative UN area hierarchy version chưa có | Defer nếu không dùng dashboard; không suy region từ country string. |
| `is_aggregate` / 106 | LOOKUP | Planned authoritative area membership/type reference | Không có aggregate flag trong active Silver. Code159 China so với mainland156 cần classification/coverage review; unmatched type không default false để tính global sums. |

### `Dim_Vietnam_Region` — DBML dòng 111

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `region_key` / 112 | DERIVED | Approved regional reference seed | Key ổn định sau phê duyệt region identity/boundary scope; không lấy row order. |
| `region_code` / 113 | EXTERNAL | Approved regional coding reference chưa có | DBML examples có thể là internal codes, phải ghi namespace/authority; không có region_code Silver. |
| `region_name_vi` / 114 | DIRECT | NSO.geography_name_candidate WHERE geography_level='region' | Chỉ candidate display name; không có parent_region_vi cho province. Không nâng candidate thành official mapping. |
| `region_name_en` / 115 | EXTERNAL | Reviewed regional labels/reference chưa có | Optional cho MVP; không tạo cột NOT NULL rồi điền guessed English labels. |

### `Dim_Vietnam_Province` — DBML dòng 120

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `province_key` / 121 | LOOKUP | Planned ref_vn_geography/Xref source geography + evidence | Stable source geography key có thể dùng MVP; official SCD2 key chỉ khi có source boundary and reference validity. Unmatched giữ source identity, không collapse. |
| `province_code` / 122 | EXTERNAL | Authoritative administrative geography reference chưa có | NSO.geography_code hiện NULL theo contract; không suy code từ sort order/tên tỉnh. |
| `province_name_vi` / 123 | DIRECT | NSO.geography_name_candidate WHERE geography_level='province' | Candidate label; giữ geography_raw/source_geography_key và review status; không phải official normalized identity. |
| `province_name_en` / 124 | EXTERNAL | Reviewed bilingual administrative reference chưa có | Defer/nullable, không fake để thỏa NOT NULL. |
| `region_key` / 125 | LOOKUP | Planned versioned ref_vn_geography_hierarchy | NSO không publish province→region hierarchy. Join theo source basis/time scope phải 0/1; >1 FAIL, 0 auditable unresolved. |
| `admin_boundary_version` / 126 | EXTERNAL | Authoritative historical boundary/series-basis reference chưa có | boundary_version Silver NULL. Không suy validity chỉ từ observation year; interval row-version khác actual statistical boundary của publication. |
| `valid_from` / 127 | EXTERNAL | Authoritative historical boundary/series-basis reference chưa có | boundary_version Silver NULL. Không suy validity chỉ từ observation year; interval row-version khác actual statistical boundary của publication. |
| `valid_to` / 128 | EXTERNAL | Authoritative historical boundary/series-basis reference chưa có | boundary_version Silver NULL. Không suy validity chỉ từ observation year; interval row-version khác actual statistical boundary của publication. |
| `is_current` / 129 | DERIVED | Approved geography reference intervals + fixed audit as-of date | valid_from <= reference_as_of < valid_to, half-open convention; chỉ có thể derive sau khi intervals có bằng chứng. Không default true cho mọi row. |

### `Xref_Vietnam_Province_Transition` — DBML dòng 134

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `transition_id` / 135 | DERIVED | Approved transition identity + stable seed/hash | Hash/seed của legacy,merged,effective_date,basis; natural-key uniqueness cần validate riêng. |
| `legacy_province_key` / 136 | LOOKUP | Dim_Vietnam_Province + authoritative transition reference | Không map bằng tên giống nhau; full merge/split/renaming phải có evidence. Missing province/reference FAIL/defer transition. |
| `merged_province_key` / 137 | LOOKUP | Dim_Vietnam_Province + authoritative transition reference | Không map bằng tên giống nhau; full merge/split/renaming phải có evidence. Missing province/reference FAIL/defer transition. |
| `reform_effective_date` / 138 | EXTERNAL | Authoritative administrative transition reference chưa có | 2025-07-01 trong DBML là design assertion; chưa xác minh nguồn pháp lý trong audit này, không coi là Silver field. |
| `area_allocation_ratio` / 139 | UNSUPPORTED | Không có allocation data theo boundary/measure | Không default1 để lấp dữ liệu. Full containment có thể1 khi authoritative crosswalk chứng minh; split cần allocation đúng measure/year; tỷ lệ diện tích đất không thay tỷ lệ sản lượng lúa. |

### `Dim_Rice_Product` — DBML dòng 144

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `product_key` / 145 | LOOKUP | Planned ref_product_identity + Xref_Product_SourceAlias | Stable keys giữ CPC/classification/basis riêng; L0. Không gộp F0030 với physical milled hay hai husked codes khi chưa chứng minh. |
| `product_code` / 146 | LOOKUP | Planned ref_product_identity từ observed source identity | Canonical code phải bao phủ core+scoped products; WB benchmarks/grades dùng thêm series key. Unmatched không map nhiều source products cùng Unknown PK. |
| `cpc_code` / 147 | DIRECT | QCL/PP/SUA/TM.commodity_code | Giữ string, apostrophe đã bỏ; 0113 không mất zero. WB/NSO/YB/VN không có CPC trực tiếp, NULL hoặc approved xref, không gán tùy ý. |
| `product_name_en` / 148 | LOOKUP | Planned ref_product_identity + source commodity labels | Tên canonical có reviewed equivalence; không coi cùng tên Rice là cùng basis. |
| `product_category` / 149 | LOOKUP | Planned product classification reference theo source code/grade | Giữ raw identity nếu chưa chắc stage; byproducts/F0030/parboiled phải classification riêng; unmatched status không tự nhận milled. |
| `processing_stage` / 150 | LOOKUP | Planned product classification reference theo source code/grade | Giữ raw identity nếu chưa chắc stage; byproducts/F0030/parboiled phải classification riêng; unmatched status không tự nhận milled. |
| `standard_paddy_to_milled_factor` / 151 | UNSUPPORTED | Không có empirical reference factor theo country/year/product | Bỏ/defer. Không hardcode0.65–0.67 để tạo sản lượng quan sát; PSD fraction chỉ là source-country-year reference, không universal factor. |

### `Dim_Rice_Variety` — DBML dòng 156

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `variety_key` / 157 | DERIVED | VN.commodity + stable source-label seed/hash | Định danh source label trước conformance; nếu gộp aliases phải collision test trên complete quote grain. Không làm mất fresh/dry/stage. |
| `variety_code` / 158 | DERIVED | VN.commodity + stable source-label seed/hash | Định danh source label trước conformance; nếu gộp aliases phải collision test trên complete quote grain. Không làm mất fresh/dry/stage. |
| `variety_name_vi` / 159 | DIRECT | VN.commodity | Giữ label thực; có195 labels và tên mơ hồ không suy giống chỉ từ code số. |
| `product_key` / 160 | LOOKUP | Planned ref_domestic_commodity_to_product / ref_product_identity | Mapping có stage/grade/moisture khi có bằng chứng; alias mơ hồ cần audit, không ép một product chỉ vì giống tên. |
| `grain_length_class` / 161 | EXTERNAL | Variety/grade reference chưa có | LONG/MEDIUM/SHORT khác trục FRAGRANT/GLUTINOUS; tách optional attributes hoặc bỏ khỏi MVP. |
| `broken_pct_standard` / 162 | LOOKUP | Planned reviewed domestic grade reference; source label có explicit % | Chỉ parse tỷ lệ khi label rõ và đúng grade; NULL cho không xác định/paddy; không suy 5% từ tên ST25 hay IR50404. |

### `Dim_Price_Type` — DBML dòng 167

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `price_type_key` / 168 | LOOKUP | Planned ref_price_statistic(dataset,element/statistic/price_type,unit,currency) | Giữ 8 domestic price-type labels; không tự gom hết vào wholesale/retail; L0. Mã nguồn và benchmark identity không được bỏ. |
| `price_type_code` / 169 | LOOKUP | Planned ref_price_statistic(dataset,element/statistic/price_type,unit,currency) | Giữ 8 domestic price-type labels; không tự gom hết vào wholesale/retail; L0. Mã nguồn và benchmark identity không được bỏ. |
| `element_code` / 170 | DIRECT | PP.element_code; VN.price_type/YB.statistic_type chỉ là labels | FAO element giữ nguyên; source khác chưa có statistic code thì NULL hoặc typed source-label key, không giả FAO code. |
| `price_stage` / 171 | LOOKUP | Planned source-specific price-stage reference | Producer/FOB/retail/purchase quote khác nhau. Urea/Brent/Wheat không mặc nhiên WB_FOB export rice. Unmatched stage giữ raw statistic và review. |
| `unit_standard` / 172 | LOOKUP | PP.element+unit; WB.unit; YB.unit; VN.unit | PP LCU/SLC/USD là currency token: element xác nhận per-tonne; index2014–2016 riêng. WB barrel/kg/MMBtu/index2010 không đổi sang tonne khi không có conversion. |
| `currency` / 173 | DERIVED | PP.currency; YB.unit=USD/tonne; VN.currency; WB.unit/header | PP direct; YB/WB USD với monetary units theo source; WB cents/sheet giữ scale cents hoặc convert/100 có rule; mọi index currency=NULL, không gán USD. |
| `is_index` / 174 | DERIVED | PP.price_kind/element_code; WB.unit | PP element5539 hoặc WB index(2010=100); giữ index base khác nhau, không dùng chung base2014–2016. |

### `Xref_Product_SourceAlias` — DBML dòng 183

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `xref_product_id` / 184 | DERIVED | Source alias natural key + approved mapping version | Stable seed/hash; source natural identity gồm code-system,code,label/stage khi cần; uniqueness thật theo scope đó. |
| `source_system_key` / 185 | LOOKUP | Dim_Source_System qua canonical dataset ID | Mỗi dataset đúng một source member; missing FAIL; source aliases không tạo dataset mới. |
| `source_commodity_code` / 186 | AMBIGUOUS | FAO.commodity_code; WB.commodity_code; PSD/YB/VN/NSO không cùng kiểu code | NOT NULL code không khả thi trực tiếp cho mọi nguồn. Đổi alias_type+nullable source_code+source_label hoặc documented label hash, không giả mã chính thức. |
| `source_commodity_name` / 187 | DIRECT | FAO.commodity_name; WB.commodity_name; PSD.commodity; YB.commodity_class; VN.commodity | NSO không có commodity column: scope rice lấy contract/source-table metadata, không pretend là cột Silver. Label+code join có scope, không global name join. |
| `product_key` / 188 | LOOKUP | Dim_Rice_Product + approved ref_product_identity | 0 match L0; >1 FAIL; many aliases→one product chỉ khi không mất fact grain hoặc còn source series discriminator. |

### `Xref_Country_SourceAlias` — DBML dòng 196

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `xref_country_id` / 197 | DERIVED | Source area alias identity + mapping version | Stable seed/hash, không sequence; name-only unique key chưa đủ cho historical/code-scope aliases. |
| `source_system_key` / 198 | LOOKUP | Dim_Source_System qua dataset ID | 0/1 mapping; missing FAIL. |
| `source_country_code` / 199 | DIRECT | FAO.country_code; TM reporter/partner codes | PSD/YB không có source country code trực tiếp: giữ NULL, dùng source name có scope; WB cần series→benchmark geography lookup. |
| `source_country_name` / 200 | DIRECT | FAO.country_name; TM reporter/partner names; PSD.country; YB.location | WB không có country column; country của benchmark từ approved series metadata, không suy toàn workbook là một quốc gia. |
| `country_key` / 201 | LOOKUP | Dim_Country + reviewed ref_country_identity | Unmatched giữ source ID trong fact grain/quarantine audit; known Unspecified khác unknown mapping; không silently drop. |

### `Fact_Vietnam_Provincial_Prod` — DBML dòng 213

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 214 | DIRECT | NSO.year | G_NP; calendar year. |
| `province_key` / 215 | LOOKUP | NSO.source_geography_key → planned ref_vn_geography → Dim_Vietnam_Province | G_NP, geography_level='province'; official SCD2 mapping chưa có; L0. |
| `season_key` / 216 | LOOKUP | NSO.season → Dim_Crop_Season | Domain theo executable contract; giữ season annual ở observation fact và chọn view scope; L0. |
| `product_key` / 217 | LOOKUP | Dim_Rice_Product + Dim_Source_System qua NSO dataset/table scope | Rice-paddy scope có metadata; không đọc NSO.commodity vì không có field đó; L0. |
| `source_system_key` / 218 | LOOKUP | Dim_Rice_Product + Dim_Source_System qua NSO dataset/table scope | Rice-paddy scope có metadata; không đọc NSO.commodity vì không có field đó; L0. |
| `area_planted_ha` / 221 | DERIVED | NSO.value WHERE measure='area',unit='hectare',statistic_kind='quantity' | G_NP: pivot duy nhất sau source-family mapping; value×1 ha, không ×1000 lần hai. NULL giữ NULL. |
| `production_harvested_t` / 222 | DERIVED | NSO.value WHERE measure='production',unit='tonne',statistic_kind='quantity' | G_NP: pivot duy nhất, value×1 tonne; không tự cộng các mùa gộp/chưa đầy đủ. |
| `is_preliminary` / 225 | DERIVED | NSO.release_status | release_status='provisional'; unconfirmed không đồng nghĩa FINAL. Nếu pivot flags khác nhau cần per-measure flags. |
| `_source_file` / 226 | AMBIGUOUS | NSO._source_file | Một contributor: giữ nguyên filename. Nhiều publication/file sau pivot: cần contributor refs theo measure, không MAX(filename). |
| `_ingest_ts` / 227 | DERIVED | NSO._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 228 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Vietnam_National_Prod_Reported` — DBML dòng 238

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 239 | DIRECT | NSO.year | G_NR. |
| `region_key` / 240 | LOOKUP | NSO source geography → planned ref_vn_geography/Dim_Vietnam_Region | region rows cần approved reference; national dùng typed NotApplicable hoặc source identity, không gom unknown regions vào national; L0. |
| `season_key` / 241 | LOOKUP | NSO.season → Dim_Crop_Season | G_NR; includes annual and seasons theo đúng source series; L0. |
| `product_key` / 242 | LOOKUP | NSO source-table scope → Dim_Rice_Product/Dim_Source_System | Reviewed rice scope và canonical source nso; L0. |
| `geography_level` / 243 | DERIVED | NSO.geography_level | Explicit mapping national→COUNTRY,region→REGION nếu giữ Gold enum; không filter uppercase trên Silver. |
| `source_system_key` / 244 | LOOKUP | NSO source-table scope → Dim_Rice_Product/Dim_Source_System | Reviewed rice scope và canonical source nso; L0. |
| `reported_area_planted_ha` / 247 | AMBIGUOUS | NSO.value,measure,unit,source_table | G_NR phải thêm source-series/publication vào PK trước pivot. Area/production/yield theo units hectare/tonne/kg/hectare đều×1; không MAX hay SUM across publications. Published yield có thể lệch derived ratio. |
| `reported_production_t` / 248 | AMBIGUOUS | NSO.value,measure,unit,source_table | G_NR phải thêm source-series/publication vào PK trước pivot. Area/production/yield theo units hectare/tonne/kg/hectare đều×1; không MAX hay SUM across publications. Published yield có thể lệch derived ratio. |
| `reported_yield_kg_ha` / 249 | AMBIGUOUS | NSO.value,measure,unit,source_table | G_NR phải thêm source-series/publication vào PK trước pivot. Area/production/yield theo units hectare/tonne/kg/hectare đều×1; không MAX hay SUM across publications. Published yield có thể lệch derived ratio. |
| `yoy_area_index_pct` / 250 | DERIVED | NSO.value WHERE statistic_kind='development_index' AND measure='area'/'production' | G_NR; source_table V06.12; unit percent giữ base100, value×1, không trừ100. Không có ở mọi geography/season; NULL là đúng. |
| `yoy_production_index_pct` / 251 | DERIVED | NSO.value WHERE statistic_kind='development_index' AND measure='area'/'production' | G_NR; source_table V06.12; unit percent giữ base100, value×1, không trừ100. Không có ở mọi geography/season; NULL là đúng. |
| `is_national_total` / 252 | DERIVED | NSO.geography_level | geography_level='national'; không dựa vào region_key=-1 vì -1 có thể unmatched. |
| `is_annual_total` / 253 | DERIVED | NSO.season | season='annual'. |
| `is_preliminary` / 254 | DERIVED | NSO.release_status | provisional→true, unconfirmed→false với release_status vẫn giữ; pivot flag conflicts không bị mất. |
| `_source_file` / 257 | AMBIGUOUS | NSO._source_file | Một contributor: giữ nguyên filename. Nhiều publication/file sau pivot: cần contributor refs theo measure, không MAX(filename). |
| `_ingest_ts` / 258 | DERIVED | NSO._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 259 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Global_Crop_Production` — DBML dòng 273

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 274 | DIRECT | QCL.year | G_Q. |
| `country_key` / 275 | LOOKUP | QCL.country_code/country_name → Xref_Country_SourceAlias/Dim_Country | G_Q; giữ historical/aggregate identities; L0. |
| `product_key` / 276 | LOOKUP | QCL.commodity_code → Xref_Product_SourceAlias/Dim_Rice_Product | CPC0113 source Rice, không ghép milled; L0. |
| `source_system_key` / 277 | LOOKUP | Dim_Source_System qua faostat_production | FAOSTAT_QCL; missing FAIL. |
| `area_harvested_ha` / 280 | DERIVED | QCL.value WHERE element_code='5312' AND unit='hectare' | G_Q pivot unique value×1 ha; không sum yield into area. |
| `production_paddy_t` / 281 | DERIVED | QCL.value WHERE element_code='5510' AND unit='tonne' | G_Q pivot unique value×1 tonne; NULL không0. |
| `producing_animals_or_seed_t` / 282 | UNSUPPORTED | QCL chỉ nhận elements5312,5412,5510 | Không có animal/seed element trong Silver hoặc Raw; bỏ cột. |
| `fao_flag` / 285 | AMBIGUOUS | QCL.flag per element | Area và production có thể flags khác nhau; giữ flags theo measure hoặc contributor map, không MAX(flag)/chọn Official cho cả row. |
| `_source_file` / 286 | DIRECT | QCL._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 287 | DERIVED | QCL._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 288 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Global_Rice_PSD` — DBML dòng 298

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `crop_year_key` / 299 | LOOKUP | PSD.crop_year/market_year → Dim_Crop_Year | G_P; label khác actual country-specific start/end; L0. |
| `country_key` / 300 | LOOKUP | PSD.country → Xref_Country_SourceAlias/Dim_Country | Current coverage chỉ Vietnam; L0. |
| `product_key` / 301 | LOOKUP | PSD.commodity → Xref_Product_SourceAlias/Dim_Rice_Product | Rice,Milled source scope; rough measure basis giữ riêng; L0. |
| `source_system_key` / 302 | LOOKUP | Dim_Source_System qua usda_psd | USDA_PSD; missing FAIL. |
| `area_harvested_ha` / 305 | DERIVED | PSD.value WHERE attribute='Area Harvested' AND unit='1000 HA' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `rough_production_t` / 306 | DERIVED | PSD.value WHERE attribute='Rough Production' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `milled_production_t` / 307 | DERIVED | PSD.value WHERE attribute='Production' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `beginning_stocks_t` / 308 | DERIVED | PSD.value WHERE attribute='Beginning Stocks' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `ty_imports_t` / 309 | DERIVED | PSD.value WHERE attribute='TY Imports' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `my_imports_t` / 310 | DERIVED | PSD.value WHERE attribute='Imports' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `total_supply_t` / 311 | DERIVED | PSD.value WHERE attribute='Total Supply' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `ty_exports_t` / 312 | DERIVED | PSD.value WHERE attribute='TY Exports' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `my_exports_t` / 313 | DERIVED | PSD.value WHERE attribute='Exports' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `domestic_consumption_t` / 314 | DERIVED | PSD.value WHERE attribute='Domestic Consumption' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `ending_stocks_t` / 315 | DERIVED | PSD.value WHERE attribute='Ending Stocks' AND unit='1000 MT' | G_P: unique attribute pivot rồi value*1000; area ra ha, quantities ra tonne. Stock là semi-additive theo time; MY/TY không cộng chung. Domestic Consumption là exact source name, khác note DBML. |
| `milling_rate_factor` / 318 | DIRECT | PSD.value WHERE attribute='Milling Rate (.9999)' AND unit='fraction' | G_P: value×1, đã chia10000 ở Silver; validate[0,1], không lấy conversion_factor0.0001 làm milling rate. |
| `_source_file` / 321 | DIRECT | PSD._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 322 | DERIVED | PSD._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 323 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Global_Supply_Utilization` — DBML dòng 333

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 334 | DIRECT | SUA.year | G_S; calendar year. |
| `country_key` / 335 | LOOKUP | SUA.country_code/country_name → country xref/dimension | G_S, aggregate/historical identities riêng; L0. |
| `product_key` / 336 | LOOKUP | SUA.commodity_code → product xref/dimension | G_S: giữ0113/23161.02 riêng; L0. |
| `source_system_key` / 337 | LOOKUP | Dim_Source_System qua faostat_supply_utilization | FAOSTAT_SUA; missing FAIL. |
| `opening_stocks_t` / 340 | DERIVED | SUA.value WHERE element_code='5113' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `production_t` / 341 | DERIVED | SUA.value WHERE element_code='5510' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `import_quantity_t` / 342 | DERIVED | SUA.value WHERE element_code='5610' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `export_quantity_t` / 343 | DERIVED | SUA.value WHERE element_code='5910' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `stock_variation_t` / 344 | DERIVED | SUA.value WHERE element_code='5071' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `domestic_supply_t` / 345 | AMBIGUOUS | SUA production/import/export/stock_variation | Không có element5301. Công thức candidate P+I-E-signed_stock_change chỉ được dùng khi xác minh convention dấu/các thành phần cùng basis và đủ non-null; hiện defer, không coi NULL component là0. |
| `feed_t` / 346 | DERIVED | SUA.value WHERE element_code='5520' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `seed_t` / 347 | DERIVED | SUA.value WHERE element_code='5525' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `waste_loss_t` / 348 | DERIVED | SUA.value WHERE element_code='5016' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `processing_t` / 349 | DERIVED | SUA.value WHERE element_code='5023' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `food_supply_quantity_t` / 350 | DERIVED | SUA.value WHERE element_code='5141' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `other_uses_t` / 351 | DERIVED | SUA.value WHERE element_code='5165' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `closing_stocks_t` / 352 | UNSUPPORTED | Không có element5114; opening và stock variation chưa chứng minh closing relation | Không fill từ next-year opening hoặc tự áp opening±variation khi chưa có source methodology/sign/coverage; defer. |
| `residuals_t` / 353 | DERIVED | SUA.value WHERE element_code='5166' AND unit='tonne' | G_S: unique element pivot value×1 tonne. Source code này thay code sai trong DBML nếu khác; giữ signed5071/5166. Thiếu observation/value giữ NULL, không0. |
| `food_supply_kg_per_capita_yr` / 356 | EXTERNAL | Không có element645; cần population đúng source/year/basis | Có thể derive food_supply_quantity_t*1000/population khi population reference được bổ sung và compatible; hiện không có denominator, không impute. |
| `calories_kcal_per_capita_day` / 357 | EXTERNAL | Không có element664; cần nutritional supply data và population | Không suy calories từ rice tonnes bằng default kcal/kg. Defer hoặc thêm nguồn nutrition được chứng minh. |
| `_source_file` / 360 | DIRECT | SUA._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 361 | DERIVED | SUA._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 362 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Global_Bilateral_Trade` — DBML dòng 372

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 373 | DIRECT | TM.year | G_T calendar year. |
| `reporter_country_key` / 374 | LOOKUP | TM.reporter_country_code/name → country xref/dimension | Reporter role; L0; giữ source identity. |
| `partner_country_key` / 375 | LOOKUP | TM.partner_country_code/name → country xref/dimension | Partner role khác reporter; Unspecified/World có type riêng nếu gặp; L0. |
| `product_key` / 376 | LOOKUP | TM.commodity_code/name → product xref/dimension | 12 source codes cần scope policy; F0030/basis giữ riêng; L0. |
| `source_system_key` / 377 | LOOKUP | Dim_Source_System qua faostat_trade | FAOSTAT_TM; missing FAIL. |
| `import_quantity_t` / 380 | DERIVED | TM.value WHERE element_code='5610' AND unit='tonne' | G_T: unique element pivot value*1; output tonne/USD tương ứng. Không cộng reporter export và mirrored partner import. Không coalesce missing thành0. |
| `import_value_usd` / 381 | DERIVED | TM.value WHERE element_code='5622' AND unit='1000 USD' | G_T: unique element pivot value*1000; output tonne/USD tương ứng. Không cộng reporter export và mirrored partner import. Không coalesce missing thành0. |
| `export_quantity_t` / 382 | DERIVED | TM.value WHERE element_code='5910' AND unit='tonne' | G_T: unique element pivot value*1; output tonne/USD tương ứng. Không cộng reporter export và mirrored partner import. Không coalesce missing thành0. |
| `export_value_usd` / 383 | DERIVED | TM.value WHERE element_code='5922' AND unit='1000 USD' | G_T: unique element pivot value*1000; output tonne/USD tương ứng. Không cộng reporter export và mirrored partner import. Không coalesce missing thành0. |
| `_source_file` / 386 | DIRECT | TM._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 387 | DERIVED | TM._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 388 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Country_Price` — DBML dòng 402

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `month_key` / 403 | AMBIGUOUS | PP(year,month), WB.period_date, YB(year,reference_period) | G_C: PP time_grain=monthly:100*year+month; WB:100*year+month; YB chưa xác minh calendar year rule G07. YEAR/MARKETING/7021 excluded có audit. |
| `country_key` / 404 | LOOKUP | FAO country xref; YB.location; planned WB series geography metadata | WB non-country benchmark dùng NotApplicable riêng, không gom unmatched country vào cùng meaning; L0. |
| `product_key` / 405 | LOOKUP | Source commodity/class → product xref + planned price-series dimension | Cần series discriminator để không merge SRW/HRW hoặc các5%-grades; L0. |
| `price_type_key` / 406 | LOOKUP | Dim_Price_Type qua source element/statistic/unit/currency | Không map Brent/Urea/Wheat thành rice FOB cùng một price type; index base giữ riêng; L0. |
| `source_system_key` / 407 | LOOKUP | Dim_Source_System qua 3 canonical price dataset IDs | Không gộp provider giá thành một SoR tự động; L0. |
| `price_value` / 410 | DIRECT | PP.value / YB.value / WB.price | G_C value×1 nếu unit được giữ trong series/type; không mix USD/barrel,USD/tonne,index. Source price NULL hợp lệ nhưng DBML NOT NULL: phải cho phép NULL hoặc explicit numeric-only view + exclusion/missing ledger. |
| `observation_flag` / 413 | DIRECT | PP.flag; YB/WB không có corresponding flag | PP giữ source flag; các nguồn khác NULL, không tạo Official giả; giữ missing_reason và warnings riêng. |
| `_source_file` / 414 | DIRECT | PP/YB/WB._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 415 | DERIVED | PP/YB/WB._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 416 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Fact_Vietnam_Daily_Market_Price` — DBML dòng 429

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `date_key` / 430 | DERIVED | VN.date | YYYYMMDD của quote observation date đã parse; G_V. |
| `province_key` / 431 | LOOKUP | VN.market → planned reviewed market-geography xref | Market không phải official province code; candidate alias chưa xác định boundary. Province FK không được thay source market/source identity trong PK; L0. |
| `variety_key` / 432 | LOOKUP | VN.commodity → domestic alias reference/Dim_Rice_Variety | Giữ fresh/dry/grade distinctions; aliases merged chỉ sau collision gate; L0. |
| `price_type_key` / 433 | LOOKUP | VN.price_type → reviewed source-specific price-type reference | 8 labels gồm purchase/export/market/other, không chỉ wholesale/retail; L0. |
| `source_system_key` / 434 | LOOKUP | Dim_Source_System qua thitruongnongsan | VN_TTNS khác VN.source information source; không thể thay trường source trong grain bằng source_system_key. |
| `market_label_raw` / 437 | DIRECT | VN.market | Giữ exact Silver label; nguồn location chi tiết còn có trong VN.source, phải giữ thêm trong corrected grain. |
| `price_vnd_kg` / 438 | DIRECT | VN.price_value WHERE unit='VND/kg' AND currency='VND' | G_V; zero giữ warning; không blanket reject0. Nullable trong Silver; numeric-only fact hiện có dữ liệu đầy đủ nhưng future NULL cần policy, không giả0. |
| `price_vnd_tonne` / 439 | DERIVED | VN.price_value WHERE unit='VND/kg' AND currency='VND' | price_vnd_kg*1000 VND/tonne; cùng quote/date/series, Decimal checked; không đổi sang USD nếu thiếu FX. |
| `_source_file` / 442 | DIRECT | VN._source_file | Giữ nguyên filename nguồn trong phạm vi dataset/snapshot; không coi filename là publication/vintage ID. |
| `_ingest_ts` / 443 | DERIVED | VN._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 444 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Agg_Vietnam_Regional_Performance_Yearly` — DBML dòng 457

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 458 | LOOKUP | Corrected Vietnam facts + approved historical region mapping | G_R; year/product từ đúng atomic source; geography reference chưa có nên chưa publish region totals. Mỗi observation match tối đa1 region; L0. |
| `region_key` / 459 | LOOKUP | Corrected Vietnam facts + approved historical region mapping | G_R; year/product từ đúng atomic source; geography reference chưa có nên chưa publish region totals. Mỗi observation match tối đa1 region; L0. |
| `product_key` / 460 | LOOKUP | Corrected Vietnam facts + approved historical region mapping | G_R; year/product từ đúng atomic source; geography reference chưa có nên chưa publish region totals. Mỗi observation match tối đa1 region; L0. |
| `total_area_planted_ha` / 463 | DERIVED | Fact_Vietnam_Provincial_Prod.area_planted_ha | G_R SUM trên approved atomic geography + mutually-exclusive complete season set. Giữ partial/null coverage, không SUM annual+seasonal. |
| `total_production_t` / 464 | DERIVED | Fact_Vietnam_Provincial_Prod.production_harvested_t | G_R SUM cùng coverage/basis với area nếu dùng yield; đơn vị tonne. |
| `weighted_yield_tonnes_ha` / 465 | DERIVED | Paired atomic production/area | G_R SUM(paired production_t)/NULLIF(SUM(paired area_ha),0); đếm cặp và missing. Tonne/ha trên planted area, không AVG(province yield). |
| `weighted_yield_quintals_ha` / 466 | DERIVED | weighted_yield_tonnes_ha | ×10 quintal/ha; không ×100 (đó là quintal→kg). |
| `national_sum_of_provinces_prod_t` / 469 | DERIVED | Corrected atomic Vietnam provincial production | Cùng year/product/season-set/boundary scope của G_R, SUM một lần trên provinces; group(year,product,basis), join1:1 vào từng region. |
| `regional_share_of_provincial_sum_pct` / 470 | DERIVED | G_R numerator + national provincial-sum denominator | 100*total_production_t/NULLIF(national_sum_of_provinces_prod_t,0); denominator repeated across regions không được SUM lại. |
| `reported_national_prod_t` / 471 | AMBIGUOUS | Fact_Vietnam_National_Prod_Reported nhiều publication/season | Trước join phải chọn duy nhất national+annual+preferred source series+product+year/basis; source conflicts G03 chưa giải quyết. Không join mọi seasonal publication. |
| `regional_share_of_reported_national_pct` / 472 | DERIVED | G_R + explicitly selected national reported row | 100*total_production_t/NULLIF(reported_national_prod_t,0); không ép các share cộng đúng100 khi official != provincial sum; cần coverage tương thích. |
| `_ingest_ts` / 475 | DERIVED | Vietnam fact contributors._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 476 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Agg_Country_Market_Position_Yearly` — DBML dòng 485

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 486 | LOOKUP | QCL calendar production + TM calendar trade + country/product dims | G_A; PSD crop-year không tự map sang calendar year; only approved non-overlapping country/product scope; L0. |
| `country_key` / 487 | LOOKUP | QCL calendar production + TM calendar trade + country/product dims | G_A; PSD crop-year không tự map sang calendar year; only approved non-overlapping country/product scope; L0. |
| `product_key` / 488 | LOOKUP | QCL calendar production + TM calendar trade + country/product dims | G_A; PSD crop-year không tự map sang calendar year; only approved non-overlapping country/product scope; L0. |
| `production_t` / 491 | AMBIGUOUS | Fact_Global_Crop_Production hoặc Fact_Global_Rice_PSD | QCL paddy calendar direct; PSD milled marketing-year và chỉ Vietnam. Không union vào cùng annual global denominator hoặc dùng generic milling factor. Đổi scope view. |
| `export_quantity_t` / 492 | DERIVED | Fact_Global_Bilateral_Trade measures | G_A: aggregate reporter-side theo(year,reporter,exact product), partner population mutually-exclusive; quantity tonne,valueUSD. Aggregate before joining production; không mirrored-side SUM. |
| `export_value_usd` / 493 | DERIVED | Fact_Global_Bilateral_Trade measures | G_A: aggregate reporter-side theo(year,reporter,exact product), partner population mutually-exclusive; quantity tonne,valueUSD. Aggregate before joining production; không mirrored-side SUM. |
| `import_quantity_t` / 494 | DERIVED | Fact_Global_Bilateral_Trade measures | G_A: aggregate reporter-side theo(year,reporter,exact product), partner population mutually-exclusive; quantity tonne,valueUSD. Aggregate before joining production; không mirrored-side SUM. |
| `import_value_usd` / 495 | DERIVED | Fact_Global_Bilateral_Trade measures | G_A: aggregate reporter-side theo(year,reporter,exact product), partner population mutually-exclusive; quantity tonne,valueUSD. Aggregate before joining production; không mirrored-side SUM. |
| `global_production_share_pct` / 498 | AMBIGUOUS | Comparable source/time/product production denominator | 100*production_t/NULLIF(SUM(production_t) trên approved complete/non-overlapping population,0). Chưa có global milled coverage từ PSD; chưa được publish như global share. |
| `global_export_qty_share_pct` / 499 | DERIVED | Reporter export_quantity_t trong G_A | 100*country_exports_t/NULLIF(SUM(country_exports_t) trên declared reporters cùng year/product,0); ghi covered-world nếu thiếu reporters. |
| `global_export_val_share_pct` / 500 | DERIVED | Reporter export_value_usd trong G_A | 100*country_export_usd/NULLIF(SUM(country_export_usd) cùng population/year/product,0); không dùng quantity denominator. |
| `global_export_rank` / 501 | DERIVED | G_A reporter export quantities | DENSE_RANK() theo year/exact product/coverage scope ORDER BY export_quantity_t DESC; định nghĩa tie và NULL ngoài ranking. |
| `global_production_rank` / 502 | AMBIGUOUS | Comparable production in G_A | DENSE_RANK chỉ sau chọn comparable source/time/product/country population; không rank Vietnam-only PSD rồi gọi global. |
| `_ingest_ts` / 505 | DERIVED | QCL/TM/conditional PSD contributors._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 506 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Agg_Bilateral_Trade_Share_Yearly` — DBML dòng 515

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `year_key` / 516 | LOOKUP | Corrected Fact_Global_Bilateral_Trade + dims | G_B; preserve report side and exact product; L0. |
| `reporter_country_key` / 517 | LOOKUP | Corrected Fact_Global_Bilateral_Trade + dims | G_B; preserve report side and exact product; L0. |
| `partner_country_key` / 518 | LOOKUP | Corrected Fact_Global_Bilateral_Trade + dims | G_B; preserve report side and exact product; L0. |
| `product_key` / 519 | LOOKUP | Corrected Fact_Global_Bilateral_Trade + dims | G_B; preserve report side and exact product; L0. |
| `export_quantity_t` / 521 | DERIVED | TM element5910/5922/5610/5622 → Fact_Global_Bilateral_Trade same-grain columns | G_T pivot đã kiểm tra unique: qty value×1tonne, monetary value×1000USD. G_B view chọn lại same-grain measures; không cần physical aggregate hay SUM một lần nữa. |
| `export_value_usd` / 522 | DERIVED | TM element5910/5922/5610/5622 → Fact_Global_Bilateral_Trade same-grain columns | G_T pivot đã kiểm tra unique: qty value×1tonne, monetary value×1000USD. G_B view chọn lại same-grain measures; không cần physical aggregate hay SUM một lần nữa. |
| `import_quantity_t` / 523 | DERIVED | TM element5910/5922/5610/5622 → Fact_Global_Bilateral_Trade same-grain columns | G_T pivot đã kiểm tra unique: qty value×1tonne, monetary value×1000USD. G_B view chọn lại same-grain measures; không cần physical aggregate hay SUM một lần nữa. |
| `import_value_usd` / 524 | DERIVED | TM element5910/5922/5610/5622 → Fact_Global_Bilateral_Trade same-grain columns | G_T pivot đã kiểm tra unique: qty value×1tonne, monetary value×1000USD. G_B view chọn lại same-grain measures; không cần physical aggregate hay SUM một lần nữa. |
| `weighted_export_unit_price_usd_t` / 525 | DERIVED | G_B export_value_usd/export_quantity_t | SUM(export_value_usd)/NULLIF(SUM(export_quantity_t),0) trên cùng covered pairs, USD/tonne; không AVG đơn giá; NULL pair có coverage flag. |
| `weighted_import_unit_price_usd_t` / 526 | DERIVED | G_B import_value_usd/import_quantity_t | SUM(import_value_usd)/NULLIF(SUM(import_quantity_t),0), USD/tonne; valuation source chưa xác nhận không tự assert CIF. |
| `partner_share_of_reporter_exports_pct` / 527 | DERIVED | G_B export_quantity_t | 100*partner_exports/NULLIF(SUM(exports) OVER(year,reporter,exact product,declared partner-scope),0); World/All không cộng với detail; Unspecified policy phải rõ. |
| `partner_share_of_reporter_imports_pct` / 528 | DERIVED | G_B import_quantity_t | 100*partner_imports/NULLIF(SUM(imports) OVER(year,reporter,exact product,declared partner-scope),0); giữ denominator scope/coverage giống numerator. |
| `_ingest_ts` / 531 | DERIVED | Trade fact contributors._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 532 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

### `Forecasting_Data_Mart_Monthly` — DBML dòng 541

| Cột / dòng DBML | Phân loại | Nguồn / lookup table | Transformation, units, filters, grouping, hạn chế |
|---|---|---|---|
| `as_of_month_key` / 542 | DERIVED | Dim_Month dense spine + explicit prediction cutoff policy | G_M: YYYYMM; phải có cutoff timestamp và feature/target series IDs/version bên cạnh, month key riêng không chứng minh availability. |
| `country_key` / 543 | LOOKUP | Dim_Country(VNM), Dim_Rice_Product(reviewed5%-milled) + target price-series reference | G_M: lookup theo target series; mapping chưa có thì defer/FAIL, không lấy country/product=-1 cho training; L0. |
| `product_key` / 544 | LOOKUP | Dim_Country(VNM), Dim_Rice_Product(reviewed5%-milled) + target price-series reference | G_M: lookup theo target series; mapping chưa có thì defer/FAIL, không lấy country/product=-1 cho training; L0. |
| `target_price_vnm_5pct_t_plus_1` / 549 | DERIVED | Chosen VNM5 price series in corrected price fact | G_M target=price tại calendar_month(t)+1, self-join country/product/series; chỉ LEAD1 trên dense spine còn NULL. Target thiếu giữ NULL, không train/impute nhãn. |
| `target_price_vnm_5pct_t_plus_3` / 550 | DERIVED | Chosen VNM5 price series | G_M target=price tại calendar_month(t)+3, cùng partition/series, giữ NULL. Không next3available rows. |
| `price_vnm_5pct_t` / 555 | AMBIGUOUS | WB.rice_viet_namese_5 hoặc YB VIETNAM5-PERCENT BROKEN | Có direct numeric observation; canonical provider/series và available_at<=cutoff chưa xác minh. WB254 numeric vs YB327 numeric không COALESCE tùy tiện. |
| `price_vnm_5pct_lag1` / 556 | DERIVED | Chosen VNM5 point-in-time series | G_M calendar t-1, chỉ phiên bản available_at<=cutoff; gap giữNULL. |
| `price_vnm_5pct_lag3` / 557 | DERIVED | Chosen VNM5 point-in-time series | G_M calendar t-3, không third-previous available observation; vintage condition bắt buộc. |
| `price_vnm_5pct_sma_3m_at_t` / 558 | DERIVED | Chosen VNM5 PIT series trên[t-2,t] | G_M AVG(price) khi COUNT(numeric)=3 trong đúng3months; unitUSD/t; nếu current-t price chưa available thì NULL/đổi feature lag có định nghĩa. |
| `price_vnm_5pct_volatility_3m_at_t` / 559 | DERIVED | Chosen VNM5 PIT series trên[t-2,t] | G_M STDDEV_SAMP(price) với3numeric tháng đầy đủ, USD/t; nêu sample/pop convention; không dùng missing-skipping rows window. |
| `price_thai_5pct_t` / 564 | AMBIGUOUS | WB price WHERE commodity_code='rice_thai_5' | Event-month numeric trực tiếp, USD/tonne; chưa chứng minh công bố trước end-of-t. Không mặc định available; dùng availability ledger. |
| `price_spread_vnm_thai_at_t` / 565 | DERIVED | Chosen compatible PIT VNM5 và Thai5 series | G_M VNM_price_t-Thai_price_t, cùng month/unit và availability<=cutoff; một operand NULL thì NULL; source comparability phải khai báo. |
| `urea_price_usd_t` / 566 | AMBIGUOUS | WB price WHERE commodity_code='urea' | USD/tonne trực tiếp theo event month, nhưng PIT availability/vintage chưa có; chỉ dùng khi available<=cutoff. |
| `brent_oil_price_usd_t` / 567 | AMBIGUOUS | WB price WHERE commodity_code='crude_oil_brent' | Đơn vị USD/barrel, không USD/tonne. Hậu tố _t ở đây nghĩa month t, cần đổi tên rõ; PIT availability chưa có. |
| `wheat_price_usd_t` / 568 | AMBIGUOUS | WB wheat_us_srw hoặc wheat_us_hrw | Chưa chọn benchmark; cả2USD/tonne nhưng không interchangeable. Chọn series rõ và availability<=cutoff, không AVG/gộp để lấp thiếu. |
| `harvest_season_code` / 573 | AMBIGUOUS | Dim_Month heuristic chưa có authority | Không có source monthly crop calendar country/region/year; thay calendar month feature deterministic hoặc defer heuristic. |
| `prior_year_vnm_paddy_prod_t` / 574 | AMBIGUOUS | QCL Vietnam5510 hoặc preferred NSO national quantity production | Candidate period year(t)-1 và tonne; source/definition/availability/vintage phải được chọn. year lag không bảo đảm public lúc t; không join cả QCL và NSO làm fanout. |
| `prior_crop_year_vnm_ending_stocks_t` / 575 | AMBIGUOUS | PSD Vietnam Ending Stocks | Crop-year prior hoàn tất cần actual country calendar + publication/vintage, không dùng MAX(market_year)<year(t) thay available_at. Khi có ref: value*1000tonne; hiện defer. |
| `_ingest_ts` / 578 | DERIVED | PIT feature contributors + target provenance (separate)._ingestion_timestamp | Đề xuất định nghĩa rõ: MAX(upstream ingestion timestamp) trên đúng contributor set tại grain của bảng; UTC, không phải publication/available_at hay current_timestamp(). |
| `etl_batch_id` / 579 | DERIVED | Gold input snapshot manifest + code/reference versions | Metadata ETL hợp lệ, không có trực tiếp trong Silver: SHA256(canonical input table UUID/snapshot IDs + Gold code/contract/xref versions). Attempt run ID tách ở log; không tạo UUID mới vào content khi replay. |

