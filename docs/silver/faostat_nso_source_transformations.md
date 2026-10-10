# Silver FAOSTAT + NSO — triển khai phần Hoàng

Ngày xác minh: 10/10/2026. Audit trước thay đổi và proposal tích hợp:
[`faostat_nso_repository_audit.md`](faostat_nso_repository_audit.md).

## Trạng thái và phạm vi

Ba transformer FAOSTAT đã triển khai và kiểm thử với Spark 3.5.5; NSO có
preprocessor đề xuất xử lý national wide/matrix và kiểm tra schema bị mất.
**Chưa nghiệm thu pipeline production end-to-end.** Không chỉnh sửa shared
framework, YAML contracts, runner của Phúc hay DAG vì yêu cầu người dùng cần
approval trước. Không ghi/xóa Bronze/Silver, bucket, volume hoặc secrets.

| Source | Source transformation | Production integration |
|---|---|---|
| Production | Implemented, typed long observations + source DQ | Partially implemented: dynamic preprocess được framework hiện hữu nhận diện; custom DQ/raw metadata chưa được framework giữ |
| Price | Implemented, annual/monthly/currency/index được phân biệt | Blocked: PRICE rules YAML chưa có sql_expr; schema derived chưa được approve |
| SUA | Implemented, long elements/product/notes/flags giữ nguyên | Blocked: SUA rules YAML chưa có sql_expr; negative Residuals cần xác nhận nghiệp vụ |
| NSO | Partially implemented: national/matrix proposal và schema guard | Blocked: Bronze thiếu matrix columns, contract NEEDS_PROFILING, unit/province mapping chưa được duyệt |

Không chạy framework hiện hữu để nghiệm thu source DQ đầy đủ: framework hiện
chỉ gọi preprocess, cast/project rồi dùng rules từ YAML. Nó không gọi
`transformer.validate/quality_rules`; các annotation `_value_parse_error`,
`_negative_value`, `_source_payload`, batch/chunk/source_id và price-derived
fields không được giữ qua projection. Vì thế tests source-local không chứng
minh DAG đã áp đầy đủ rules. Preview gọi đúng shared DQ engine với source rules
để chẩn đoán; nó không thay thế production writer/orchestrator.

## File đã tạo

| File | Mục đích |
|---|---|
| `src/silver/transformers/faostat_source.py` | Helpers riêng FAOSTAT, dùng contract loader/sanitizer/DQ engine hiện hữu; kiểm tra required input, typed projection, raw payload |
| `src/silver/transformers/faostat_production.py` | Production/area/yield và controlled unit conversion |
| `src/silver/transformers/faostat_monthly_price.py` | Annual/monthly, currency, producer price/index |
| `src/silver/transformers/faostat_supply_utilization.py` | SUA long, source element mapping, current negative-value policy |
| `src/silver/transformers/nso_vietnam.py` | Draft national/matrix unpivot, season/level/statistic distinction, fail rõ nếu mất schema |
| `jobs/silver/preview_faostat_nso.py` | CLI đọc Bronze hoặc local FAOSTAT CSV; counters/DQ/reconciliation, không writes |
| `scripts/preview_faostat_nso_bronze.ps1` | Preview live Bronze trong container tạm Spark 3.5.5, repo chỉ đọc |
| `scripts/test_faostat_nso_isolated.ps1` | Pytest offline trong container không mạng, warehouse Iceberg tạm riêng |
| `tests/unit/silver/faostat_nso_fixtures.py` | Fixtures nhỏ chứa schema/metadata nguồn |
| `tests/unit/silver/test_faostat_nso_transformers.py` | 29 source unit tests |
| `tests/integration/test_faostat_nso_silver.py` | Framework dispatch với external writes mocked; ba actual Iceberg MERGE/quarantine rerun tests trên catalog tạm |
| `docs/silver/faostat_nso_repository_audit.md` | Audit trước code và danh sách thay đổi cần approval |
| `docs/silver/faostat_nso_source_transformations.md` | Mapping, DQ, results và commands |

## Contract/API tái sử dụng

Các class tên đúng convention `<DatasetId>Transformer`, module
`silver.transformers.<dataset_id>`, public extension `preprocess(df)`.
`transform(df)` của FAOSTAT project đúng các cột/types của YAML hiện tại,
đồng thời giữ audit/diagnostic columns cho preview. `validate(df)` trả
`(valid_df, quarantine_df)` thông qua `SilverQualityEngine.apply_rules`.
Không có source writer hay mapping engine mới.

Preview dùng `IcebergMergeEngine.deduplicate` và chỉ collect các rule counters
nhỏ. Tests thực dùng `IcebergMergeEngine.merge` và
`SilverQuarantineManager.route_quarantine` trên Hadoop catalog `hoang_test`
trong `/tmp`, không dùng REST catalog, MinIO hay shared dead_letters.

## Source-to-target mapping FAOSTAT

Bronze writer đã sanitize headers và có thể infer source codes thành số.

| Bronze column | Output field | Rule |
|---|---|---|
| `domain_code`, `domain` | cùng tên | Trim string; không tự tạo domain |
| `area_code_m49_` | `country_code` | Bỏ apostrophe, bỏ `.0` của số nguyên, pad 1–3 chữ số thành 3; không chuyển sang ISO hay quốc gia hiện tại |
| `area` | `country_name` | Trim source label; giữ lịch sử/aggregate identities |
| `item_code_cpc_` | `commodity_code` | Bỏ apostrophe; source `113`/`113.0` khôi phục `0113`; giữ `23161.02` độc lập |
| `item` | `commodity_name` | Giữ `Rice` / `Rice, milled`, không tự gán Rice là paddy hay gộp dạng sản phẩm |
| `element_code`, `element` | cùng tên | Trim/code normalization; code/name phải khớp snapshot mappings |
| `year` | `year` int | Chỉ chuỗi 4 chữ số; invalid → NULL, DQ fail, raw payload giữ giá trị gốc |
| `unit`, `value` | cùng tên | Conversion theo source/element; try_cast decimal rộng trước conversion, round theo YAML ở output |
| `flag`, `flag_description` | cùng tên | Trim; giữ statistical flag, không fill missing |
| `note` | `note` của SUA | Giữ note. Production không có note trong YAML, nhưng raw payload giữ note nguồn |
| mọi input/technical columns | `_source_payload` | JSON deterministic theo thứ tự cột, trước normalization, giữ cả NULL |
| `_ingestion_*`, `_source_*` | cùng tên | Source `transform` giữ toàn bộ, kể cả batch/chunk/checksum/snapshot |

M49 canonical master/ISO mapping chưa có trong repository. Source mapping
status `source_m49_only` ghi rõ chỉ chuẩn hóa source code, chưa xác nhận code
với master quốc gia. Không ánh xạ FAO China/historical codes sang nước hiện tại.

Grain giữ đúng published YAML:

- Production/SUA: `country_code × commodity_code × year × element_code`.
- Price: thêm `month_code`. Annual giữ key `7021`; monthly `7001..7012`.
- Không pivot Production/SUA, không cộng elements, không aggregate prices,
  không balance correction/KPI/ML features.

### Production units

| Element | Allowed source unit | Canonical unit | Value multiplier |
|---|---|---|---:|
| `5312`, Area harvested | `ha`, `hectare` | `hectare` | 1 |
| `5510`, Production | `t`, `tonne` | `tonne` | 1 |
| `5510`, Production | `kg` | `tonne` | 0.001 |
| `5412`, Yield | `kg/ha`, `kg/hectare` | `kg/hectare` | 1 |
| `5412`, Yield | `hg/ha` | `kg/hectare` | 0.1 |
| `5412`, Yield | `t/ha` | `kg/hectare` | 1000 |

Snapshot/live Bronze thực tế dùng `ha`, `kg/ha`, `t`. Các conversion kg/hg/t
khác được test bằng fixtures theo định nghĩa metric units; không giả định chúng
hiện có trong live source. DQ chặn cặp element/unit không tương thích và overflow.
Output Production/SUA decimal(18,2); Price decimal(18,4). Âm rất nhỏ vẫn fail
DQ dù output rounding thành 0. Không đổi missing thành 0.

### Price time/currency semantics

Các trường sau là **diagnostic proposal**, chưa publish trong shared YAML:

| Source | Diagnostic representation |
|---|---|
| `7001..7012`, đúng tên January..December | `time_grain=monthly`, `month=1..12` |
| `7021`, `Annual value` | `time_grain=annual`, `month=NULL`; không tạo ngày/tháng giả hoặc tự tính annual average |
| `5530`, unit `LCU` | `price_kind=producer_price`, `currency=LCU` |
| `5531`, unit `SLC` | `price_kind=producer_price`, `currency=SLC` |
| `5532`, unit `USD` | `price_kind=producer_price`, `currency=USD` |
| `5539`, index 2014–2016=100, NULL unit | `price_kind=producer_price_index`, `currency=NULL`, `unit=NULL` |

Mâu thuẫn code/name/currency/unit đi quarantine partition trong preview.
LCU/SLC không được coi là USD. Không đổi index thành price hoặc producer price
thành export price. Annual value chỉ là nhãn source; không suy luận công thức
tổng hợp từ nhãn này.

### SUA semantics

Mapping code/name được xác minh ở CSV và Trino: Loss 5016, Processed 5023,
Stock Variation 5071, Opening stocks 5113, Food supply quantity (tonnes) 5141,
Other uses (non-food) 5165, Residuals 5166, Production 5510, Feed 5520,
Seed 5525, Import quantity 5610, Export quantity 5910. Chỉ normalize observed
`t/tonne` thành `tonne`, multiplier 1.

Giữ policy hiện tại: chỉ 5071 được âm. 236 Residuals âm không sửa dấu hay fill;
chúng vào quarantine preview, chờ xác nhận liệu 5166 âm có hợp lệ về nghiệp vụ.

## NSO proposal và giới hạn

- V06.12 national wide: tám measure/season columns, units từ header nghìn ha/
  nghìn tấn. Quantity nhân 1000; development-index rows giữ percent và giá trị
  nguồn, không nhân 1000. Năm `So b? 2024` ghi `is_provisional=true`.
- V06.13–24 matrix: unpivot các cột `col_YYYY` / `so_b_YYYY`, dùng metadata.txt
  xác định area/yield/production và season. V06.19–21 giữ
  `summer_autumn_autumn_winter`, khác V06.12 `summer_autumn`.
- National/region source labels được đánh dấu riêng; không gộp cùng province.
  Labels hỏng/unknown không fuzzy repair, province_code=NULL và needs_review.
- Units của matrix không xuất hiện trong headers/metadata.txt đã cung cấp:
  để NULL và quarantine chờ reference chính thức, không suy diễn từ values.
- NUL footer và missing cells không bị drop ngầm. Khi đủ schema, mỗi cell
  unpivot giữ observation kể cả NULL; footer/statistic không hợp lệ bị DQ bắt.
- Draft grain: `source_table × geography_name_raw × geography_level × year ×
  season × measure × statistic_kind`. Đây chưa phải approved production key.
  Không dùng output draft để tính national total; các national observations
  giữa V06.12 và matrix có thể chồng lặp, cần chọn source theo Gold contract.
- Actual Bronze thiếu toàn bộ province/year matrix columns; adapter fail rõ
  trước khi bịa observation. Nguyên nhân code:
  `BronzeIcebergWriter._dataframe_to_arrow` align theo table schema hiện hữu;
  source file khác schema có thể mất extra columns. Không sửa Bronze owner.
- Cần province mapping versioned (valid_from/to, source label, source-table
  version), ví dụ Hà Tây/Hà Nội; không map tên hỏng sang mã tỉnh hiện tại.

## DQ và reconciliation

Source rules dùng SQL `coalesce(condition,false)` để SQL NULL không lọt qua
engine shared. Common FAOSTAT checks: mandatory fields theo YAML, year range,
M49 syntax, known CPC/code-name pair, malformed/overflow numeric. Source rules
thêm unit/element/date/currency/sign validity. NULL value nguồn hợp lệ giữ NULL.

FAOSTAT không expansion/filter:

```text
Bronze input rows = valid rows before dedup + quarantined rows
valid rows before dedup = output rows + deduplicated rows
intentionally excluded rows = 0
```

NSO unpivot phải thêm expanded observation count; không so sánh raw wide rows
với Silver long rows một cách máy móc. Preview báo input, observation, expansion,
valid, quarantine, deduplicated, output và excluded riêng.

## Kết quả đã thực sự chạy

Host Windows: Python 3.13/PySpark 4.1.2/JDK21:

```powershell
python -m pytest tests/unit/silver/test_faostat_nso_transformers.py tests/integration/test_faostat_nso_silver.py tests/unit/test_silver_contract_loader.py tests/unit/test_source_registry.py tests/unit/test_incremental_loader.py -q
```

**52 passed, 3 skipped, 0 failed**. Ba skips là opt-in isolated Iceberg tests.
Lượt sandbox ban đầu bị lỗi quyền thư mục tạm; rerun ngoài sandbox đạt kết quả trên.

Target image Linux: Python 3.10.16/PySpark 3.5.5:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test_faostat_nso_isolated.ps1 -SkipDownload
```

**38 passed, 0 failed, 0 skipped**. Gồm 29 unit tests Hoàng, 1 framework-dispatch
integration, 3 actual Iceberg MERGE/quarantine rerun tests và 5 existing regression
tests (Trade/PSD/WorldBank transformations, shared DQ/dedup). Không cộng số test
hai môi trường vì có test trùng nhau. Script không dùng Docker services hiện hữu.
Các lỗi dependency/PYTHONPATH/hostname trong lần chuẩn bị container đã được xử lý
trong script chạy thành công; không ghi chúng là failure của transformer.

MERGE tests chứng minh rerun giữ một valid record và một dead-letter cho mỗi
fixture source. Chưa chứng minh không tạo thêm Iceberg snapshots: engine vẫn
UPDATE matched rows. Tie cùng timestamp/source_file chưa có deterministic
survivorship; freshness guard batch cũ/newer target và partition/v2/ZSTD vẫn
là shared-storage issues cần owner phối hợp.

### Preview live Bronze bằng Spark 3.5.5

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preview_faostat_nso_bronze.ps1
```

| Dataset | Input/observations | Valid | Quarantine candidates | Deduplicated | Output candidates |
|---|---:|---:|---:|---:|---:|
| Production | 22.738 | 22.738 | 0 | 0 | 22.738 |
| Price | 15.334 | 15.334 | 0 | 0 | 15.334 |
| SUA | 36.380 | 36.144 | 236 (`HOANG_SUA_VALUE`) | 0 | 36.144 |
| NSO | 833 (Trino count) | Not evaluated | Not evaluated | Not evaluated | Blocked: missing matrix columns |

Production có 229 NULL values; SUA có 257; Price có 0, xác minh bằng Trino.
Ba bảng FAOSTAT có 0 duplicate business keys qua Trino GROUP BY.
SUA có 1.280 Stock Variation âm được giữ và 236 Residuals âm bị DQ chặn.
Price có 9.993 annual và 5.341 monthly source observations, trong annual gồm
3.136 price indices. Đây là counters quan sát, không tính KPI ở Silver.

Preview exit code **1** do NSO blocked, đồng thời JSON chứa kết quả ba FAOSTAT;
không coi toàn bộ E2E passed. `writes_performed=false` cho mọi source. Chưa có
Silver tables trong REST catalog; Airflow không được trigger; Trino chỉ xác minh
Bronze. Quarantine counts là candidates, chưa được lưu shared dead_letters.

## Lệnh tái lập và bước tiếp theo

Từ root repository, chuẩn bị và chạy isolated tests:

```powershell
# Lần đầu tải wheel pytest thuần Python vào .pytest_cache rồi chạy offline.
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test_faostat_nso_isolated.ps1

# Các lần sau dùng wheel đã có, không tải lại.
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test_faostat_nso_isolated.ps1 -SkipDownload

# Chỉ đọc live Bronze, chọn từng nguồn để nhận exit 0 khi preview đạt.
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preview_faostat_nso_bronze.ps1 -Dataset faostat_production
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preview_faostat_nso_bronze.ps1 -Dataset faostat_monthly_price
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/preview_faostat_nso_bronze.ps1 -Dataset faostat_supply_utilization

# Preview local CSV trên host có Spark phù hợp, từ repository root.
python jobs/silver/preview_faostat_nso.py --dataset faostat_production --csv data/raw/faostat/production_world.csv
```

Spark runtime có Iceberg jars và network catalog có thể chạy incremental
**preview**; đây chưa phải incremental production integration:

```powershell
python jobs/silver/preview_faostat_nso.py --dataset faostat_production --mode incremental --watermark 2026-10-01T00:00:00+00:00
python jobs/silver/preview_faostat_nso.py --dataset faostat_production --mode incremental --run-id ACTUAL_BRONZE_RUN_ID
```

`ACTUAL_BRONZE_RUN_ID` cần thay bằng metadata run thực. Không ghi `.env` hoặc
credentials. Spark container Compose chưa chạy; shared DAG/API và mount contracts
cần phối hợp Phúc. Cổng 5005 chưa được publish cho host.

Để hoàn tất E2E cần approval của người dùng cho proposal shared hooks/contracts/
registry/DAG, phê duyệt riêng trước khi jobs ghi Iceberg đang dùng chung, xác nhận
policy Residuals âm, và phối hợp sửa schema Bronze NSO/mapping units/provinces.
Không đưa lệnh ghi production giả khi integration chưa được triển khai/duyệt.
