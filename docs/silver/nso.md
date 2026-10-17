> Historical/legacy reference. Current local nine-source routing, NSO V2 and acceptance are documented in [README](README.md), [data catalog](data_catalog.md) and [validation](validation.md). Original bytes are preserved in [archive](archive/manifest.json).

# NSO Vietnam — Silver rice statistics

NSO sử dụng contract READY `contracts/silver/nso_vietnam.yaml`, đọc
`iceberg.bronze.nso_vietnam` và xuất `iceberg.silver.nso_rice_statistics`.
Pipeline dùng chung framework, quality engine, quarantine và MERGE của 8 nguồn
còn lại. Bản triển khai có tham khảo inventory và schema preservation từ
`origin/feat_silver_role2`; không nhập các approval gate hoặc framework dự thảo.

## Input và grain

13 CSV `V06.12.csv`–`V06.24.csv`. V06.12 có tám series diện tích/sản lượng
quốc gia; 12 file còn lại là ma trận địa lý × năm. Inventory từng file, header,
checksum snapshot và unit nằm trong `config/silver/nso_source_tables.json`.
Bronze cần đủ 41 source fields và tám lineage fields (49 trường).

Adapter kiểm kê union header trước chunk đầu, giữ NULL padding cho cột không
thuộc file và cố định tất cả cột nguồn thành **nullable string**. Vì vậy file
quốc gia đầu tiên không khiến padding ma trận tạo schema DOUBLE. Writer từ chối
bỏ mất cột khi align Arrow; schema/type lỗi phải fail trước DELETE retry.
Bronze cũ mất dữ liệu cần phục hồi raw và replay; chỉ thêm cột NULL không
khôi phục dữ liệu. Bảng có 31 cột DOUBLE toàn NULL do lỗi suy luận ban đầu được
sửa bằng utility giữ field IDs cũ dưới tên legacy và bổ sung fields STRING mới.
Bảng mới có 49 trường; bảng đã repair có thêm các trường legacy toàn NULL.
Xem [quy trình phục hồi NSO](rebuild.md).

Business key: `source_table, geography_name_raw, year, season, measure,
statistic_kind`. `geography_level` không nằm trong key: bổ sung mapping sau
này không được tạo thêm một bản sao observation. V06.12 và national rows
trong các ma trận là các series riêng, không cộng với nhau.

## Đơn vị và mùa vụ

| File | Measure | Season | Source unit → Silver unit | Factor |
|---|---|---|---|---:|
| V06.13 | area | annual | 1000 hectare → hectare | 1000 |
| V06.14 | yield | annual | quintal/hectare → kg/hectare | 100 |
| V06.15 | production | annual | 1000 tonne → tonne | 1000 |
| V06.16–18 | area / yield / production | winter_spring | Như ba dòng trên | 1000 / 100 / 1000 |
| V06.19–21 | area / yield / production | summer_autumn_autumn_winter | Như ba dòng trên | 1000 / 100 / 1000 |
| V06.22–24 | area / yield / production | mua | Như ba dòng trên | 1000 / 100 / 1000 |

V06.12 dùng đơn vị ngay trong header: nghìn ha/nghìn tấn ×1000. Header quốc
gia ghi hè thu, nên giữ `summer_autumn` riêng với `summer_autumn_autumn_winter`.
Development index so với năm trước dùng `percent`, factor 1, không biến thành
diện tích/sản lượng. `statistic_kind` phân biệt `quantity` và `development_index`.

Đã kiểm tra trực tiếp bảng lúa trong
[Niên giám NSO 2018](https://www.nso.gov.vn/wp-content/uploads/2019/10/Nien-giam-2018-1.pdf),
trang PDF 508–529, cùng tên series trong `data/raw/nso/metadata.txt` và header
V06.12. Artifact PDF kiểm tra có SHA-256
`e491e8a47a3119f3ad8d8fca935426aad3767cd1ded5e7ea785930385374d3e1`.

## Địa lý và missing values

Theo lựa chọn của người dùng trong phiên làm việc 11/10/2026:

- Giữ nguyên `geography_name_raw`, kể cả `Hà N?i` hoặc whitespace nguồn.
- `geography_name` phục hồi tên theo 71 nhãn trong
  `config/silver/nso_geography_candidates.json`, tham khảo nhánh
  `feat_silver_role2`: `Hà N?i` → `Hà Nội`, `Bà R?a - Vtng Tàu` →
  `Bà Rịa - Vũng Tàu`. Exact lookup sau trim; nhận cả tên có dấu đúng.
- Các nhãn quốc gia xác định → `geography_level=national`, code `704`.
- Candidate phân loại `province`/`region`; nhãn ngoài mapping giữ
  `geography_name=NULL`, level `unknown` để review. Alias mâu thuẫn gây fail.
- Tỉnh/vùng chưa xác minh có `geography_code=NULL`, `province_code=NULL`,
  `mapping_status=needs_review`; observation hợp lệ vẫn vào Silver và có
  `_dq_warnings`. Không fuzzy repair hoặc áp mã tỉnh hiện tại cho năm lịch sử.
- Thiếu hẳn nhãn địa lý hoặc nhãn thống kê không nhận diện đi quarantine.

Tên candidate không xác minh mã địa lý lịch sử. Giữ `Hà Tây` riêng, không
gộp thành Hà Nội hoặc áp các thay đổi tỉnh hiện tại. Raw label vẫn là thành
phần business key; chuẩn hóa tên không tạo observation trùng mới.

Giữ `value_raw`, `value_source DECIMAL(28,8)`, giá trị chuẩn hóa
`value DECIMAL(28,8)`, source/canonical unit và conversion factor.
NULL, blank, `..`, `...`, `…`, `NA`, `N/A`, `NULL` giữ measurement NULL;
`is_missing` và `missing_kind` phân biệt trạng thái. Không fill 0.
Malformed, overflow và negative (kể cả số âm nhỏ bị làm tròn thành 0) đi
quarantine. `try_cast`/`try_multiply` hoạt động khi ANSI bật.

## Unpivot và đối soát

Chỉ unpivot những năm thuộc inventory của file đang xử lý. Cột union padding
không tạo observation. Có giá trị ở period mới chưa kiểm kê thì fail-fast;
cần cập nhật inventory để không âm thầm mất năm mới.

Dòng có **toàn bộ source fields NULL** được loại trước unpivot và lưu audit
trong `dead_letters`, `source_dataset=nso_vietnam_exclusions`, rule
`NSO_EMPTY_SOURCE_RECORD`. NULL measurement trong dòng có nhãn địa lý hợp lệ
vẫn là observation. Không loại/xóa dòng nguồn khỏi Bronze.

Snapshot local đã chạy full, incremental replay và audit. Catalog live đạt
full/audit/replay với cùng counts; Trino xác nhận các NULL/warnings bên dưới.
Xem [bằng chứng nghiệm thu](validation.md).
Sau bổ sung tên candidate, Trino xác nhận **23.060/23.060 observations có tên
chuẩn hóa**, không còn dấu `?` trong `geography_name`; tên gốc vẫn nguyên vẹn.

| Chỉ số | Số lượng |
|---|---:|
| Bronze physical rows | 833 |
| Candidate observations | 23428 |
| Empty source records audited | 13 |
| Candidate slots excluded | 368 |
| Prepared / valid / deduplicated observations | 23060 |
| Quarantined observations | 0 |
| Observations with geography warning | 22140 |
| Missing markers `..` in matrices | 946 |

`observation_count = prepared_count + excluded_count`;
`prepared_count = valid_count + quarantine_count`;
`valid_count = dedup_count + duplicate_count`.
Quarantine count không bao gồm exclusion records; đây là hai grain khác nhau.

## Chạy và sử dụng downstream

```powershell
python scripts/validate_silver_contracts.py
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/job_nso_vietnam.py
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --dataset nso_vietnam --mode incremental --run-id <bronze_run_id>
```

Partition theo `year`, Iceberg v2/ZSTD, MERGE theo grain trên. Final/provisional
là attribute của cùng series/năm; ingestion mới hơn cập nhật observation.
Gold phải lọc measure/statistic/season/geography level/source table đúng mục
đích; không cộng index với quantity, annual với seasons, national với vùng/tỉnh,
hoặc hai source series quốc gia trùng phạm vi. Geographic joins cần mapping
được kiểm chứng riêng; hoàn thành Silver không chứng minh mã tỉnh đã đầy đủ.
