> Tài liệu quy tắc làm sạch và chuẩn hóa dữ liệu cho 9 nguồn Silver (`iceberg.silver_local.*`). Xem thêm [Tổng quan Silver](README.md), [Data Catalog](data_catalog.md) và [Đặc tả Schema](schemas.md).

# Data Cleaning & Transformation rules

## Quy tắc chung

| Vấn đề | Xử lý |
|---|---|
| Tên cột | Dùng cùng sanitizer Bronze: ký tự ngoài ASCII chữ/số/underscore → `_`, gộp underscore, lowercase, tiền tố `col_` nếu bắt đầu bằng số. |
| Chuỗi | Trim hai đầu; chuỗi rỗng → NULL; trường raw có `preserve_whitespace` giữ nguyên; giữ Unicode, country/product identity và nội dung có ý nghĩa. |
| Missing numeric/date | NULL, blank, `NA`, `N/A`, `NULL`, `..`, `...`, `…` → NULL trước cast ở projection chung; preprocessing từng nguồn có thể nghiêm ngặt hơn. Không thay bằng 0. |
| Kiểu dữ liệu | Cast theo `schema.columns[].data_type` bằng SQL `try_cast`, có hiệu lực cả khi ANSI mode bật. Overflow/malformed input tạo `SYS_CAST_ERR`; numeric fraction không được truncate thành integer. |
| Bắt buộc | Tất cả business keys và cột `nullable: false` phải non-null; kiểm tra row-level bằng `SYS_REQUIRED_<column>`. Cột bắt buộc vắng khỏi schema gây fail-fast. |
| Cột tùy chọn vắng | Tạo NULL có kiểu đúng; không tự tạo country/year/measure giả. |
| Decimal | Theo scale contract: Production/Trade/SUA/Yearbook/IPSARD 2, FAOSTAT Price/PSD 4, NSO 8. Chuyển đơn vị trước khi làm tròn. |
| Float | Nếu contract dùng float/double, NaN và infinity bị quarantine. Các contract hiện tại dùng decimal cho measurements. |
| DQ SQL NULL | UNKNOWN được xem là fail; tránh lọt dữ liệu lỗi do SQL three-valued logic. |
| DQ action | `QUARANTINE` chặn ghi Silver; `LOG` và `KEEP` giữ row, lưu `_dq_warnings` và log số row có warning. Severity dùng để mô tả; action quyết định routing. |
| Lỗi nguồn | Source-specific `quality_rules()` được chạy cùng YAML DQ sau projection. Cast error luôn chặn kể cả khi YAML không có DQ rule. |
| Lineage | Giữ tám audit fields Bronze nếu có; lưu payload nguồn trước biến đổi cho quarantine. Raw fields được publish khi contract yêu cầu (NSO/USDA); `_source_payload` và diagnostic flags nội bộ chỉ dùng xử lý/quarantine. |

Không tự thực thi các câu mô tả natural language trong `transformation` hoặc
`normalization`. Các phép biến đổi phải được code hóa trong source preprocess;
DQ phải có `sql_expr` cụ thể. Validator từ chối contract READY thiếu biểu thức.

## FAOSTAT Production, Monthly Price và SUA

- M49: bỏ leading apostrophe và `.0`; chỉ pad mã gồm 1–3 chữ số thành 3 chữ số.
  Giữ mã lịch sử và định danh nguồn, không suy ra ISO hiện đại.
- CPC: `113`/`113.0` khôi phục thành `0113`; giữ `23161.02` cho Rice, milled.
  Unknown mapping đi quarantine; không gộp rice với rice, milled.
- Year phải bốn chữ số, trong khoảng 1960 đến current year + 1.
- Giữ flag/flag description/note theo schema; kiểm tra code/name của element.
- Value được parse ở precision cao; ghi nhận parse error và dấu âm từ raw
  trước làm tròn, tránh `-0.001` biến thành zero hợp lệ.

| Nguồn / element | Đơn vị nguồn | Đầu ra / hệ số |
|---|---|---|
| Production `5312` Area harvested | `ha`, `hectare` | `hectare`, ×1 |
| Production `5510` Production | `t`, `tonne`, `kg` | `tonne`; kg ×0.001 |
| Production `5412` Yield | `kg/ha`, `kg/hectare`, `hg/ha`, `t/ha` | `kg/hectare`; hg ×0.1, t ×1000 |
| SUA | `t`, `tonne` | `tonne`, ×1 |

Unsupported element/unit pair hoặc overflow đi quarantine. Production và
price không cho giá trị âm. SUA cho **Stock Variation (`5071`) âm** theo
contract hiện tại; Residuals (`5166`) âm đi quarantine để review.

Monthly Price giữ `7001..7012` là tháng 1..12; `7021 / Annual value` có
`time_grain=annual`, `month=NULL`. Không gán tháng giả cho annual.
Element `5530/5531/5532` giữ currency `LCU/SLC/USD`,
`price_kind=producer_price`; `5539` là price index có currency và unit NULL.
Không FX conversion và không trộn index với monetary price.

## FAOSTAT Trade

M49 reporter/partner dùng đúng source field M49; legacy FAO country codes không
được dùng làm fallback vì thuộc hệ mã khác. CPC được giữ dưới dạng chuỗi.
Quantity elements `5610/5910` dùng `tonne`; monetary `5622/5922` dùng
`1000 USD`, không tự nhân 1000 hay đổi sang quantity. Mã M49/element, year,
unit không hợp lệ, giá trị âm hoặc malformed đi quarantine.
Aggregate countries như World vẫn giữ observation/identity trong dữ liệu nguồn.

## USDA PSD

Unpivot các cột đúng pattern `col_YYYY_YYYY` bằng Spark `array + explode`;
mọi value được chuyển sang raw string trước unpivot để tránh Spark yêu cầu
wide columns cùng kiểu. Header phải có năm cuối = năm đầu + 1.

Giữ `market_year` và `crop_year=YYYY/YYYY+1`, không ép thành calendar year.
Commodity blank/NULL được default `Rice, Milled` theo dataset rice PSD đã biết.
Không default country; country/attribute thiếu đi quarantine.
Unit quantity bỏ parentheses, giữ `1000 MT`, `1000 HA`, `MT/HA`.
Theo quyết định người dùng 11/10/2026, exact attribute `Milling Rate (.9999)`
được chia 10.000, dùng `fraction`: 6250 → 0,6250. Giữ `value_source`,
`source_unit` và `conversion_factor`; không cộng fraction vào khối lượng.
`value DECIMAL(18,4)` giữ đủ bốn số lẻ, nguồn `DECIMAL(28,8)`.
Missing giữ NULL; fraction ngoài [0,1], malformed và conversion mismatch
đi quarantine. Đây là quy tắc scale đã được người dùng xác nhận dựa trên
header nguồn; nhãn mass gốc không được diễn giải thành khối lượng.
Ví dụ nhãn/format trong [USDA PSD report](https://apps.fas.usda.gov/gainfiles/200103/65680145.pdf).
Value missing giữ NULL. Rule negative hiện là **LOG** trong YAML, vì vậy giữ
row với `_dq_warnings`; muốn chặn negative cần đổi action sang QUARANTINE.

## USDA Rice Yearbook

Giữ `location`, `commodity_class`, `reference_period`, `statistic_type` từ
source. Không suy đoán quốc gia từ table number hoặc chỉ giữ riêng tables
25–28; input registry hiện là snapshot export prices. `DOLLARS PER METRIC TON`
→ `USD/tonne`; missing values → NULL; malformed hoặc negative đi quarantine.

## World Bank Pink Sheet

Unpivot toàn bộ commodity columns, giữ mỗi observation kể cả period invalid
để quarantine. Parse đúng `YYYYMmm` → ngày đầu tháng; kiểm tra month 1..12,
year 1960..2050. Trim/uppercase period trước parse.

Bronze adapter tạo `Commodity_N` cho header Excel rỗng. Silver chỉ loại các
cột synthetic này khi aggregation xác nhận toàn bộ cột là NULL; vẫn giữ các
series có tên thật dù mọi price missing, và giữ synthetic có value để review.
Snapshot hiện có 17 cột trailing rỗng, không phải 17 commodity observations.

Unit lấy từ suffix tên cột do Bronze adapter ghép workbook name + unit row:
`mt→USD/tonne`, `kg→USD/kg`, `bbl→USD/barrel`, `mmbtu→USD/MMBtu`,
`troy_oz→USD/troy_oz`, `cubic_meter→USD/cubic_meter`, `dmtu→USD/dmtu`,
`cents_sheet→cents/sheet`, `2010_100→index(2010=100)`.
Unknown unit suffix đi quarantine. Commodity name được tạo từ tên nguồn,
không phải mapping giữa các tổ chức. `series_name` optional hiện NULL.

Workbook còn dùng replacement character `�` làm missing marker; source adapter
Silver xử lý marker này và `…`, `...`, `..`, `NA` thành NULL. Các chuỗi lỗi khác
giữ cho cast error. Price ≤ 0 hiện là warning LOG theo contract, không drop row.

## IPSARD / Thị trường nông sản

Định dạng ngày đã quan sát là `M/d/yyyy h:mm:ss a`, ví dụ
`8/3/2026 12:00:00 AM` là **2026-08-03**. Cho phép thêm ISO date/timestamp;
không suy đoán `dd/MM/yyyy`. Unknown hoặc malformed date đi quarantine.
`VNĐ/Kg`, `đồng/kg`, `VND/kg` → `VND/kg`; `VNĐ` → `VND`.

Giá có comma grouping chỉ được bỏ dấu comma khi mọi nhóm sau có đúng ba chữ
số, ví dụ `12,500.25`; `12,5` không được đổi thành 125. Giữ source market,
commodity/variety và price type; không tự sửa tên tỉnh. Future date hiện
`KEEP` và được lưu warning. Negative/malformed price đi quarantine.

## NSO (`nso` → `nso_rice_statistics`)

Chuẩn hóa 13 bảng CSV thống kê lúa gạo Việt Nam theo metadata [`config/silver/nso_v2_metadata.json`](../../config/silver/nso_v2_metadata.json) và contract [`contracts/silver/nso.yaml`](../../contracts/silver/nso.yaml):
- Unpivot ma trận rộng theo từng bảng (`source_table`), chuẩn hóa năm (`year`), trạng thái sơ bộ (`is_preliminary`), mùa vụ (`season`), chỉ tiêu (`measure`), và loại thống kê (`statistic_kind`: `QUANTITY` hoặc `PERCENT_PREVIOUS_YEAR_100`).
- Quy đổi đơn vị gốc sang đơn vị chuẩn: `Nghìn ha` → `hectare` ($\times 1000$), `Nghìn tấn` → `tonne` ($\times 1000$), `Tạ/ha` → `kg/hectare` ($\times 100$), `%` → `percent` ($\times 1$).
- Phân biệt rõ các ký hiệu khuyết dữ liệu (`..`, `...`) thành `value = NULL` kèm `missing_reason` (`NOT_APPLICABLE_OR_UNAVAILABLE`, `INCOMPLETE_OBSERVATION`, `EMPTY_CELL`) và giữ nguyên các giá trị `0` thực sự (5 quan sát).
- Bảo toàn định danh địa lý gốc (`geography_name_raw`, `source_geography_key`), tên tiếng Việt khôi phục dấu (`geography_name_vi`) và cấp địa lý (`COUNTRY`, `REGION`, `PROVINCE_CITY`). Các bản ghi cấu trúc không phải quan sát số liệu (13 dòng) được tách riêng vào `dead_letters` với `source_dataset = 'nso_exclusions'`.

## Deduplication, missing keys và lỗi

Chỉ dedup **sau DQ**, tránh row mới lỗi che mất row cũ hợp lệ.
Window partition theo business key; giữ `_ingestion_timestamp` mới nhất,
sau đó `_source_file` descending; tie-break các cột còn lại ascending,
NULL first, serialize nested fields thành JSON. Kết quả cùng input deterministic
trong batch, kể cả sau repartition.

MERGE không update khi input timestamp cũ hơn target; vẫn insert business key
mới từ batch đến muộn. Timestamp bằng nhau được phép update nếu data khác;
trong các batch riêng có timestamp bằng nhau, batch chạy sau thắng.
Row không đổi bỏ qua MERGE bằng join kiểm tra changes trước SQL, giữ
`_silver_processed_at` và không tạo Silver snapshot rỗng khi replay.
Quarantine dùng ID hash của dataset + JSON payload có NULL explicit, và
dedup ID trong batch trước MERGE. Business key hash dùng JSON struct để
phân biệt NULL, empty string và các thành phần chứa separator.

Payload quarantine chứa data đã normalize, source diagnostics và
`_source_payload` JSON gốc. Log job chứa dataset/run ID/metrics/traceback,
không dump toàn bộ payload của dataset lên stdout.

## Tài liệu engine tham khảo

`try_cast` trả NULL cho lỗi runtime theo [Spark ANSI documentation](https://downloads.apache.org/spark/docs/3.5.6/sql-ref-ansi-compliance.html).
Iceberg MERGE yêu cầu Spark extensions, theo [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/).
Table properties format/compression theo [Iceberg configuration](https://iceberg.apache.org/docs/1.8.0/configuration/).
