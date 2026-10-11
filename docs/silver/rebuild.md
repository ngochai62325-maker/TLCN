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

Force bỏ checksum idempotency gate; **không phải lệnh reset metadata**.
Loader vẫn có thể resume checkpoint của failed/in-progress run và bỏ file
đã ingest đối với nguồn theo file. Vì vậy phải xử lý state/checkpoint tương
ứng với phạm vi reset, hoặc dùng môi trường/cơ sở metadata mới nhất quán.
Không coi force là cách sửa mọi trạng thái storage bị mất dữ liệu.

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
