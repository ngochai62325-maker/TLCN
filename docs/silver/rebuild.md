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
