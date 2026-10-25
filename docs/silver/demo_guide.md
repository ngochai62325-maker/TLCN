# Hướng dẫn vận hành & trình diễn Local Stack (`tlcn-local`)

Tài liệu này hướng dẫn chi tiết cách khởi động, kiểm tra giao diện, kích hoạt luồng dữ liệu **Raw → Bronze → Silver** từ trạng thái sạch (`0 DagRuns`), truy vấn kết quả bằng **Trino SQL**, cũng như cách dừng/mở lại hoặc reset stack an toàn trên Windows.

---

## A. Chuẩn bị & Khởi động Stack

1. Mở **Docker Desktop** (chế độ Linux containers / WSL2).
2. Kiểm tra file `.env` tại thư mục gốc repository (nếu chưa có, sao chép từ [`.env.example`](../../.env.example)).
3. Mở PowerShell tại thư mục gốc repository và chạy:

```powershell
# Bước 1 (tùy chọn): Kiểm tra cấu hình compose.yaml và 21 file Raw + nso/metadata.txt
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1 -CheckOnly

# Bước 2: Khởi động stack (hoặc mở lại stack đã dừng trước đó)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1
```

- **Lần chạy đầu tiên**: Script tự động build 2 local images (`tlcn/silver-spark:local`, `tlcn/silver-airflow:local`), khởi tạo các volumes của project `tlcn-local`, chạy `airflow-init` đúng 1 lần (migrate DB, tạo bucket `warehouse` trên MinIO, tạo user admin từ `.env`), sau đó bật đầy đủ các dịch vụ.
- **Các lần mở lại sau khi `stop.ps1`**: Script **không** chạy lại `airflow-init` và **không** xóa hay tạo lại container/volume; toàn bộ dữ liệu Bronze/Silver và lịch sử chạy trên Airflow UI được giữ nguyên.
- Script **không bao giờ tự động unpause hoặc trigger DAG**. Khi mới khởi tạo, cả 2 DAG đều ở trạng thái paused với **`0 DagRuns`**.

---

## B. Truy cập các giao diện Localhost

Sau khi `start.ps1` báo `READINESS PASS`, mở trình duyệt tại:

1. **Airflow UI**: `http://localhost:8085`
   - Đăng nhập bằng `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` trong `.env` (mặc định: `admin` / `admin`).
   - Quan sát 2 DAG: `rice_lakehouse_ingestion` và `rice_lakehouse_silver_pipeline` (cả hai đều `Paused`, `Schedule: None`, `0 Runs`).
2. **MinIO Console**: `http://localhost:9001`
   - Đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env` (mặc định: `minioadmin` / `minioadmin123`).
   - Kiểm tra bucket `warehouse` (nơi lưu trữ dữ liệu Bronze raw archive và các bảng Apache Iceberg).
3. **Trino Coordinator Info**: `http://localhost:18080/v1/info`
   - Trả về JSON trạng thái coordinator (`"starting": false`).

Bạn có thể kiểm tra nhanh sức khỏe toàn bộ stack bất cứ lúc nào bằng lệnh:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/status.ps1 -Smoke
```

---

## C. Kích hoạt Pipeline End-to-End (Bronze → Silver)

Khi bạn muốn tự tay chạy luồng dữ liệu từ đầu trên Airflow UI (`http://localhost:8085`):

1. **Unpause theo thứ tự**:
   - Bật (Unpause) DAG **`rice_lakehouse_silver_pipeline`** trước.
   - Bật (Unpause) DAG **`rice_lakehouse_ingestion`** sau.
   - *(Vì cả hai DAG đều có `schedule=None` và `catchup=False`, thao tác unpause sẽ không tự động chạy bất kỳ job nào).*
2. **Trigger DAG Bronze**:
   - Nhấn nút **Trigger DAG** trên `rice_lakehouse_ingestion` với cấu hình mặc định:
     ```json
     {"force_reprocess": false}
     ```
3. **Theo dõi tiến trình tự động**:
   - **Tầng Bronze (`rice_lakehouse_ingestion`)**: 9 task `ingest_*` (bao gồm `ingest_nso`) đọc 21 file Raw từ `/workspace/data/raw`, lưu raw objects lên MinIO, ghi bảng Iceberg `iceberg.bronze.*` và lưu metadata vào PostgreSQL. Sau đó `verify_bronze_batch` kiểm tra tính toàn vẹn của cả 9 nguồn rồi `trigger_silver_release` tự động gọi **đúng 1 child run** bên DAG Silver.
   - **Tầng Silver (`rice_lakehouse_silver_pipeline`)**: Gồm 14 tasks chạy tuần tự qua Spark API (`start` → `check_bronze_Readiness` → 9 task `transform_*` gồm cả `transform_nso` → `silver_data_quality_audit` → `publish_silver_signal` → `end`).

---

## D. Kiểm tra dữ liệu bằng Trino SQL

Sau khi cả 2 DAG hoàn tất (`success`), chạy lệnh dưới đây trong PowerShell để truy vấn trực tiếp các bảng qua Trino:

```powershell
$sql = @'
SELECT 'faostat_production' AS target, count(*) AS rows FROM iceberg.silver_local.faostat_production
UNION ALL SELECT 'faostat_monthly_price', count(*) FROM iceberg.silver_local.faostat_monthly_price
UNION ALL SELECT 'faostat_supply_utilization', count(*) FROM iceberg.silver_local.faostat_supply_utilization
UNION ALL SELECT 'faostat_trade', count(*) FROM iceberg.silver_local.faostat_trade
UNION ALL SELECT 'usda_rice_psd', count(*) FROM iceberg.silver_local.usda_rice_psd
UNION ALL SELECT 'usda_rice_yearbook', count(*) FROM iceberg.silver_local.usda_rice_yearbook
UNION ALL SELECT 'worldbank_commodity_monthly', count(*) FROM iceberg.silver_local.worldbank_commodity_monthly
UNION ALL SELECT 'thitruongnongsan', count(*) FROM iceberg.silver_local.thitruongnongsan
UNION ALL SELECT 'nso_rice_statistics', count(*) FROM iceberg.silver_local.nso_rice_statistics
ORDER BY target
'@
docker compose -p tlcn-local -f compose.yaml exec -T trino trino --server http://localhost:8080 --user local --execute $sql
docker compose -p tlcn-local -f compose.yaml exec -T trino trino --server http://localhost:8080 --user local --execute "SELECT source_dataset, count(*) AS rows FROM iceberg.silver_local.dead_letters GROUP BY source_dataset ORDER BY source_dataset"
```

Kết quả chuẩn đối với bộ 21 file Raw đã pin:
- Tổng cộng **1.414.527** dòng trên 9 bảng Silver (trong đó `nso_rice_statistics` có **23.060** dòng).
- Bảng `iceberg.silver_local.dead_letters` có **17** dòng: `4` dòng xung đột giá từ `thitruongnongsan` và `13` dòng loại trừ cấu trúc từ `nso_exclusions`.

---

## E. Dừng Stack An Toàn (Giữ nguyên dữ liệu)

Khi kết thúc phiên làm việc và không có task/query nào đang chạy dở:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/stop.ps1
```

- Script kiểm tra trạng thái idle của PostgreSQL, Trino và Spark JVM, tự động pause cả 2 DAG, rồi gửi tín hiệu dừng êm (`SIGINT` cho `runner`, `SIGTERM` cho các service còn lại).
- Toàn bộ containers và 4 volumes (`tlcn-local_local_postgres_data`, `tlcn-local_local_minio_data`, `tlcn-local_local_catalog_data`, `tlcn-local_local_airflow_logs`) được giữ nguyên để mở lại nhanh bằng `start.ps1`.

---

## F. Reset Toàn Bộ Dữ Liệu Local (Khi muốn chạy lại từ đầu)

Nếu muốn xóa sạch dữ liệu đã chạy của `tlcn-local` để trở về trạng thái `0 DagRuns` ban đầu:

```powershell
# 1. Xem danh sách containers và volumes sẽ bị xóa (không xóa thật):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1

# 2. Thực hiện xóa (yêu cầu stack đang chạy ở trạng thái idle và gõ chính xác: DELETE tlcn-local):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1 -DeleteData
```

Sau khi `reset.ps1 -DeleteData` hoàn tất, chạy lại `scripts/local_demo/start.ps1` để dựng lại stack sạch từ đầu.
