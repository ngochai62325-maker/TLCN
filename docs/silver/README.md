# Tầng Silver — TLCN Rice Lakehouse (`tlcn-local`)

Hệ thống tích hợp **9 nguồn dữ liệu lúa gạo thực tế** từ tầng Bronze (`iceberg.bronze.*`) sang tầng Silver (`iceberg.silver_local.*`) trên một Docker Compose stack duy nhất ([`compose.yaml`](../../compose.yaml), project `tlcn-local`).

## 1. Phạm vi 9 nguồn dữ liệu chuẩn (Canonical Sources)

1. `faostat_production` → `iceberg.silver_local.faostat_production`
2. `faostat_monthly_price` → `iceberg.silver_local.faostat_monthly_price`
3. `faostat_supply_utilization` → `iceberg.silver_local.faostat_supply_utilization`
4. `faostat_trade` → `iceberg.silver_local.faostat_trade`
5. `usda_psd` → `iceberg.silver_local.usda_rice_psd`
6. `usda_rice_yearbook` → `iceberg.silver_local.usda_rice_yearbook`
7. `worldbank_pinksheet` → `iceberg.silver_local.worldbank_commodity_monthly`
8. `thitruongnongsan` → `iceberg.silver_local.thitruongnongsan`
9. `nso` → `iceberg.silver_local.nso_rice_statistics` (loại trừ cấu trúc lưu tại `dead_letters` với `source_dataset = 'nso_exclusions'`)

Trên bộ 21 file Raw chuẩn trong [`config/local_raw_manifest.json`](../../config/local_raw_manifest.json) (kèm `data/raw/nso/metadata.txt`), tổng số quan sát chuẩn tại tầng Silver là **1.414.527 dòng** (trong đó `nso` gồm **23.060 quan sát**, 946 missing markers, 5 giá trị 0 thực sự và 13 bản ghi loại trừ cấu trúc).

## 2. Đặc điểm vận hành Local Stack

- **Một stack duy nhất (`compose.yaml`)**: Không còn các file Compose chồng chéo. Toàn bộ dữ liệu PostgreSQL, MinIO, Iceberg REST SQLite catalog và Airflow logs được lưu bền vững trong 4 named volumes thuộc project `tlcn-local`.
- **Chế độ Persistent mặc định**: Dùng `scripts/local_demo/stop.ps1` và `scripts/local_demo/start.ps1` để đóng/mở lại stack mà không mất dữ liệu hay lịch sử chạy DAG.
- **Hai DAG điều phối manual-only**:
  - `rice_lakehouse_ingestion`: Ingest 9 nguồn Raw → Bronze, kiểm tra tính toàn vẹn batch (`verify_bronze_batch`), và tự động kích hoạt đúng 1 child run của DAG Silver (`trigger_silver_release`).
  - `rice_lakehouse_silver_pipeline`: Kiểm tra Bronze gate, chạy tuần tự 9 Spark jobs (`transform_*` bao gồm `transform_nso`), thực thi `silver_data_quality_audit` và phát tín hiệu `DQ_PASSED`.
- **Cấu hình linh hoạt qua `.env`**: Tài khoản Airflow (`http://localhost:8085`), MinIO Console (`http://localhost:9001`), PostgreSQL và cổng Trino (`http://localhost:18080`) được quản lý tập trung qua `.env` / [`.env.example`](../../.env.example).

## 3. Danh mục tài liệu Silver

1. [Hướng dẫn trình diễn & vận hành (`demo_guide.md`)](demo_guide.md): Các bước khởi động, đăng nhập UI, chạy pipeline E2E, truy vấn Trino, dừng và reset stack.
2. [Kiến trúc (`architecture.md`)](architecture.md): Luồng điều phối Airflow → Spark API → Iceberg REST / MinIO → Trino, cơ chế khóa tư vấn (advisory lock) và kiểm tra idempotent NOOP.
3. [Data Catalog (`data_catalog.md`)](data_catalog.md): Thống kê số dòng Bronze/Silver, khóa nghiệp vụ (business keys), đơn vị đo lường và quy tắc NSO.
4. [Quy tắc làm sạch (`cleaning_rules.md`)](cleaning_rules.md): Chi tiết chuẩn hóa kiểu dữ liệu, đơn vị, mùa vụ, địa lý và xử lý ngoại lệ cho từng nguồn.
5. [Đặc tả Schema (`schemas.md`)](schemas.md): Cấu trúc bảng Silver của cả 9 nguồn, các cột lineage Bronze và bảng `dead_letters`.
