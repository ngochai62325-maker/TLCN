# Chạy lại pipeline sau khi reset dữ liệu

## Phân biệt dữ liệu đã xóa

| Phạm vi reset | Điều kiện và luồng chạy lại |
|---|---|
| Chỉ Silver; giữ Bronze | Trigger `rice_lakehouse_silver_pipeline` với `{"mode":"full"}`. Framework tạo lại target bị thiếu, MERGE và audit. |
| Bronze + Silver; giữ raw | Cần đủ raw local, catalog/storage nhất quán và metadata/checkpoints tương ứng. Ingest Bronze trước, sau đó chạy Silver full. |
| Xóa cả `data/raw/` | Registry dùng `LOCAL_FILE`; adapters không tự tải nguồn từ các endpoint reference. Phải khôi phục raw trước. |
| Chỉ xóa objects trong warehouse | Iceberg catalog có thể vẫn trỏ tới file đã mất. Cần quy trình reset/khôi phục cả catalog và storage; Airflow không tự sửa bằng ingestion thông thường. |

Mở giao diện Airflow hoặc khởi động container không tự chạy pipeline ngay.
Các DAG có lịch ngày 15 hằng tháng và `catchup=False`; để chạy ngay cần trigger.

## Điều kiện hiện tại của checkout

Kiểm tra ngày 11/10/2026: tám trong chín nguồn Silver có raw local; Trade thiếu
`data/raw/faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv`.
Trade 1.226.470 rows trong nghiệm thu trước được đọc từ **Bronze đã tồn tại**.
Fixture validation 2.000 rows không thay thế source Trade production.
Nếu xóa Bronze ngay khi raw Trade còn thiếu, không thể dựng lại đủ chín nguồn.

Airflow scheduler hiện có openpyxl/lxml/PyIceberg/PyArrow/Pandas/YAML; Spark API
đã được kiểm tra. Điều này không thay thế nghiệm thu một scheduled DAG run
trên stack reset hoàn toàn; đợt kiểm tra hiện tại không xóa stack đang dùng.

## Metadata và force reprocess

Checksum/snapshot, batch metadata và checkpoint nằm ngoài Silver, gồm
PostgreSQL và MinIO. Xóa tables mà giữ trạng thái `SUCCESS` có thể khiến
ingestion skip với cùng checksum. Nếu cần đọc lại nguồn trong môi trường có
metadata cũ, Bronze DAG nhận conf:

```json
{"force_reprocess": true}
```

Với FULL loader, force bỏ checksum idempotency gate, không resume checkpoint
cũ và không bỏ qua file đã ingest: nguồn được đọc từ chunk đầu vào một run mới.
Bronze giữ các phiên bản cũ; Silver dedup theo business key và freshness.
Force **không reset metadata**, không sửa catalog đang trỏ tới objects đã mất
và không thay đổi quy tắc incremental loader.

Có thể force riêng NSO để tránh đọc lại các nguồn khác:

```json
{"force_reprocess_sources": ["nso_vietnam"]}
```

## NSO lỗi `raw values do not fit double` sau reset

Log `ingest_nso_vietnam`, attempt 3, ngày 11/10/2026 thất bại ở
`t_nh_th_nh_ph_`: file đầu `V06.12.csv` không có các cột tỉnh/năm ma trận.
Trước bản sửa, Pandas suy luận phần padding toàn NULL thành float64, khiến
Bronze tạo 31 cột DOUBLE. File `V06.13.csv` chứa `C? NU?C` và các chuỗi nguồn
nên writer từ chối ghi để tránh mất dữ liệu.

Adapter hiện ép **toàn bộ union cột nguồn thành Pandas nullable string** trước
khi yield chunk. Bronze mới tạo 41 cột nguồn VARCHAR; việc chuẩn hóa tên địa lý
và cast số tiếp tục thuộc Silver. Thay code không tự sửa bảng DOUBLE đã tồn tại.

Từ root repository, kiểm tra phương án sửa (chỉ đọc bảng):

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/repair_nso_bronze_schema.py
```

Áp dụng khi report là `planned` và đã xác nhận đúng bảng NSO:

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/repair_nso_bronze_schema.py --execute
```

Utility giữ source advisory lock; chỉ nhận các cột nullable DOUBLE thuộc
inventory và **COUNT(column) = 0**. Bất kỳ cột có dữ liệu, kiểu không hỗ trợ,
cột thiếu hoặc legacy name trùng đều làm nó dừng. Một transaction catalog đổi
tên field ID cũ thành `_nso_legacy_null_<column>` rồi thêm field STRING nullable
với tên nguồn và ID mới. Không xóa table, file, row hay snapshot; kiểm tra lại
count và snapshot ID sau commit. Cột legacy toàn NULL được giữ để bảo toàn lịch
sử và không tạo observation Silver. Bảng đúng kiểu trả `already_valid`.

Sau sửa, trigger Bronze DAG với conf force riêng NSO phía trên để toàn bộ
13 CSV thuộc một run SUCCESS; Silver hook chỉ đọc run đó. Nếu dữ liệu đã được
phục hồi như trong [bằng chứng NSO](validation.md), mở DAG run cũ, Clear
`ingest_nso_vietnam`, `silver_nso_vietnam` và task downstream bị `upstream_failed`.
Ingestion có thể SKIPPED vì checksum đã SUCCESS; Silver hook có thể SKIPPED nếu
không còn run pending. Đây là hành vi idempotent hợp lệ.

Đợt phục hồi giữ 71 dòng từ run FAILED cũ và thêm 833 dòng vào run SUCCESS mới:
Bronze vật lý có **904 dòng**, còn Silver có **23.060 observations** sau dedup.
Không so tổng Bronze lịch sử với số dòng raw của một run.

Kiểm chứng tạo bảng mới, ghi đủ 13 CSV qua PyIceberg/REST/MinIO và đọc lại Trino:

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/validate_nso_bronze.py
```

Lệnh này tạo namespace riêng `bronze_nso_validation_<uuid>`, giữ bảng kiểm chứng
để đối soát và không sửa tables production. Kỳ vọng 13 files / 833 records,
41 fields VARCHAR, 71 nhãn địa lý gốc. Hai utility cần runtime PyIceberg của
Airflow scheduler và services MinIO/REST/Trino/PostgreSQL đang hoạt động.

## Luồng xác nhận trên Airflow

1. Đảm bảo đủ raw cho các nguồn enabled và services catalog/storage/database/
   Spark API hoạt động.
2. Nếu Bronze đã reset, trigger `rice_lakehouse_ingestion`. Các Silver hooks
   chạy sau Bronze thành công; kiểm tra task skip và lý do skip từng nguồn.
3. Trigger `rice_lakehouse_silver_pipeline` với `{"mode":"full"}` để gate,
   xử lý đủ chín contract và post-write audit toàn bộ output.
4. Xem metrics, số dòng/quarantine và tín hiệu sau audit; DAG màu xanh có thể
   chứa các task skip, nên không dùng màu DAG làm bằng chứng đủ dữ liệu.

Gate Silver kiểm tra schema và **Bronze không rỗng**, chặn các bảng đã xóa hết
rows nhưng còn schema. Empty incremental batch vẫn được framework chấp nhận
khi nguồn Bronze đã có dữ liệu; đây là trường hợp không có delta.

Kiểm tra Bronze trước khi trigger, chỉ đọc:

```powershell
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --check-bronze
```

Xem [nghiệm thu và lệnh audit](validation.md). Không có lệnh xóa dữ liệu trong
runbook này; người vận hành cần chọn đúng phạm vi reset.

## Rebuild tám nguồn Silver trong môi trường test độc lập

Quy trình mới dùng `compose.silver-test.yml`, project riêng, network `internal`,
không publish host ports, không mount storage shared và không có Airflow.
MinIO/PostgreSQL dùng tmpfs mới; REST Catalog dùng SQLite riêng trong container.
Raw và repository được mount chỉ đọc. Không có bước reset hoặc xóa dữ liệu.

Trade đầy đủ hiện tìm được trong `data_main/raw`, ngoài checkout này. Đặt đường
dẫn tới thư mục Raw đầy đủ; validator dừng trước ingestion nếu thiếu một artifact.
Không thay file Trade bằng ZIP fixture để tuyên bố full coverage.

```powershell
$env:SILVER_TEST_RAW_ROOT = 'C:/Users/hoang/source/repos/final term/tieu luan chuyen nganh/data_main/raw'
$testProject = 'tlcn-silver-rebuild-test-new-unique-id'
docker compose -f compose.silver-test.yml -p $testProject up -d minio-test rest-test postgres-test trino-test
docker compose -f compose.silver-test.yml -p $testProject run --rm runner
```

Dùng một project name chưa tồn tại, chỉ gồm chữ thường/số/dấu gạch nối; thay
`new-unique-id` bằng ID mới. Cache `.pytest_cache/silver_wheels` cần có
pytest 8.4.2, psycopg2-binary 2.9.10, boto3 1.35.99, openpyxl 3.1.5, lxml 5.3.0
và dependencies cho Python 3.10/Linux x86_64. Runner cài offline từ cache.

Validator chạy đúng tám source IDs, không dùng `--dataset all`. NSO V2 vẫn
DRAFT và không nằm trong drill. World Bank theo scope registry `Monthly Prices`;
`Monthly Indices`/Description/Index Weights không được giả thành price series.

Drill kiểm tra checksum và số records độc lập từ Raw, gọi ingestion engine thật,
đối soát Bronze và tám lineage fields, chạy common framework, query Silver,
full/incremental rerun và so snapshot/schema/checksum gồm processing timestamp.
Ingestion lại cùng checksum phải SKIPPED mà Bronze không đổi; Trino kiểm tra
output. `SNAPSHOT_RESULT`/`SNAPSHOT_SUMMARY` là kết quả từ lần chạy thực tế.

Nếu chỉ chạy mode Hadoop mặc định, `bronze_rebuild=NOT_RUN`; không coi đó là
full rebuild. `--rebuild --resume` dành riêng cho tiếp tục cùng test environment
sau lỗi, không reset: chỉ chấp nhận Bronze đầy đủ, checksum đúng và một run
identity. Mode mặc định yêu cầu metadata test ban đầu rỗng. Không dùng `up`
để recreate một test service đang chứa dữ liệu tmpfs cần giữ.

Heap Spark được giới hạn 768 MB, runner 3 GB và MinIO test 1 GB. Full wide payload
chỉ cache cho observation có lỗi DQ; không giữ JSON của hàng triệu valid rows.
Quarantine và Silver là hai transaction riêng: lỗi framework cung cấp
`silver_failure` với stage, commit flags và actual `bronze_read_snapshot_id`.
Kiểm tra dữ liệu đã commit trước khi retry. `_source_snapshot_id` vẫn là lineage
metadata từ Bronze (loader hiện có thể ghi 0), không thay bằng Iceberg snapshot ID.
Audit của quarantine và PostgreSQL run logs có thể cập nhật khi replay, dù
snapshot/dữ liệu Silver không đổi. Existing target sai/thiếu schema bị chặn;
không tự ALTER hoặc overwrite, cần migration được phê duyệt.

## Đề xuất reset an toàn — không thực thi

Ưu tiên một project/catalog/warehouse/metadata database mới. Giữ môi trường cũ
để đối soát; chỉ chuyển routing sau khi owner duyệt kết quả và migration.

| Tài nguyên | Phương án mặc định | Backup/khôi phục và kiểm chứng |
|---|---|---|
| Raw local | Giữ nguyên, mount chỉ đọc | Backup artifacts + SHA-256; xác nhận đủ tám nguồn, đặc biệt Trade |
| MinIO raw/warehouse | Instance/bucket/warehouse mới | Giữ objects cũ; backup cùng catalog, đối soát inventory/checksum |
| Iceberg metadata | REST instance/catalog mới | Backup SQLite/catalog và manifest/data files cùng mốc; xác nhận namespace/table/snapshot IDs |
| PostgreSQL ingestion metadata | Database/schema mới | Dump schema/data, snapshot/checkpoint/watermark/run logs; xác nhận DB name và ban đầu rỗng |
| Airflow metadata/DAG state | Giữ nguyên; drill không sử dụng | Backup metadata DB và DAG conf; test orchestration cần instance/DB riêng |
| Docker volumes | Giữ nguyên volumes hiện tại | Kiểm tra project labels và mount IDs; không xóa volume để dựng môi trường mới |
| Trino catalog/config | Catalog riêng trỏ REST/MinIO test | Backup config; xác nhận endpoint/warehouse rồi DESCRIBE/count/query values |
| Logs/lineage/snapshots lịch sử | Giữ lại | Lưu run IDs, hashes, metrics và snapshot IDs trước/sau; kiểm tra restore đọc được |

Rebuild hoàn tất khi Raw→Bronze đối soát đầy đủ, cả tám Silver source PASS,
khóa duy nhất, missing/quarantine đúng policy, Trino đọc đúng types/values và
full/incremental Silver NOOP. Không dùng màu xanh DAG hoặc count từ fixture làm
tiêu chí nghiệm thu. Shared acceptance, chuyển Airflow routing, migration target
cũ và bất kỳ destructive reset nào đều cần owner approval riêng. Rollback chỉ
có thể bảo đảm nếu catalog và data objects được backup nhất quán; không giả định
khôi phục được lịch sử sau khi mất objects.
