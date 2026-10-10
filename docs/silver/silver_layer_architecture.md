# Tài liệu Thiết kế & Triển khai Tầng Silver (Silver Layer Architecture)

> **Cập nhật kiểm chứng 10/10/2026:** Tài liệu này lưu thiết kế và báo cáo của giai đoạn trước audit tích hợp. Các bảng PASS/live counts bên dưới chưa được tái xác nhận trên catalog hiện tại: REST chỉ có namespace Bronze, live Silver coverage 0/9. Kiến trúc và hành vi code sau phần A nằm trong [audit tích hợp](silver_integration_audit.md); counts hiện tại là [preview/DQ](silver_dq_reconciliation_report.md), không phải dữ liệu đã ghi. Trade giữ giá trị âm để DQ xử lý; không chuyển âm thành NULL. Không dùng bảng nghiệm thu lịch sử bên dưới để quyết định Gold readiness.

**Dự án**: Vietnam Rice Market Data Lakehouse (`TLCN`)  
**Giai đoạn**: Tuần 8 (05/10/2026 – 11/10/2026)  
**Phụ trách**: Người 2 — Pipeline & Storage Engineer (Data Platform & Lakehouse)

---

## 1. Tổng quan Kiến trúc Tầng Silver

Tầng Silver đóng vai trò là tầng **dữ liệu đã được làm sạch, chuẩn hóa và tối ưu hóa truy vấn (Cleaned & Standardized Layer)** trong kiến trúc Medallion Lakehouse. Dữ liệu từ các bảng Bronze (thô) được chuyển đổi qua Apache Spark 3.5, áp dụng các quy tắc kiểm tra chất lượng dữ liệu (Data Quality - DQ), loại bỏ trùng lặp và ghi vào **Apache Iceberg Format v2** trên MinIO (`s3://warehouse/silver/`).

```text
Bronze (Iceberg Tables / Raw Files)
               │
               ▼
Apache Spark 3.5 (PySpark Transformation Engines)
  ├── 1. Data Cleaning & Trim
  ├── 2. M49/CPC & Unit Standardization
  ├── 3. Unpivot Wide-to-Long (USDA PSD & World Bank Pink Sheet)
  ├── 4. Aggregate Tagging (World/Regional aggregates)
  └── 5. Deduplication (Window over Business Keys + Timestamp Survivorship)
               │
               ▼
Iceberg Silver Tables (REST Catalog / MinIO)
  ├── iceberg.silver.faostat_trade              (Partition: year)
  ├── iceberg.silver.usda_rice_psd             (Partition: market_year)
  ├── iceberg.silver.usda_export_price          (Partition: year)
  └── iceberg.silver.worldbank_commodity_monthly (Partition: year)
               │
               ▼
Trino Distributed Query Engine & Airflow Orchestrator
```

---

## 2. Đặc tả Lưu trữ Iceberg Silver Storage

Tất cả các bảng Silver được đăng ký trên **Iceberg REST Catalog** (`http://iceberg-rest:8181`) và lưu trữ tệp Parquet chuẩn nén ZSTD trên MinIO bucket `warehouse`.

### 2.1. Danh mục các Bảng Silver

| Tên Bảng Iceberg | Nguồn Bronze Đầu Vào | Khóa Nghiệp Vụ (Business Grain) | Phân Vùng (Partition) | Định Dạng & Nén |
| :--- | :--- | :--- | :--- | :--- |
| **`iceberg.silver.faostat_trade`** | `iceberg.bronze.faostat_trade` | `reporter_country_code`, `partner_country_code`, `commodity_code`, `year`, `element_code` | `year` | Iceberg v2 / ZSTD |
| **`iceberg.silver.usda_rice_psd`** | `iceberg.bronze.usda_psd` | `country`, `commodity`, `attribute`, `market_year` | `market_year` | Iceberg v2 / ZSTD |
| **`iceberg.silver.usda_export_price`** | `iceberg.bronze.usda_rice_yearbook` | `exporter_country`, `rice_class`, `year`, `reference_period`, `statistic_description` | `year` | Iceberg v2 / ZSTD |
| **`iceberg.silver.worldbank_commodity_monthly`** | `iceberg.bronze.worldbank_pinksheet` | `commodity_code`, `period_date` | `year` | Iceberg v2 / ZSTD |

---

## 3. Quy tắc Chuẩn hóa theo Từng Dataset

### 3.1. FAOSTAT Detailed Trade Matrix (`iceberg.silver.faostat_trade`)
* **Làm sạch mã M49 & CPC**: Loại bỏ các dấu nháy đơn (`'`) dư thừa trong mã nguồn (ví dụ: `'764` $\to$ `764`, `'23161.02` $\to$ `23161.02`).
* **Phân biệt Quốc gia Đơn vị và Tổng hợp Khu vực (Aggregates)**:
  * Thêm cờ `is_reporter_aggregate` và `is_partner_aggregate`.
  * Đánh dấu `true` cho các mã tổng hợp (`1`, `001`, `5000`, ...) hoặc tên chứa *World*, *Total*, *All countries*, *Unspecified*.
* **Xử lý Giá trị Không hợp lệ**: Các giá trị âm trong `value` bị chuyển thành `NULL`.
* **Khử trùng lặp**: Giữ bản ghi mới nhất theo `_ingestion_timestamp`.

### 3.2. USDA Rice PSD (`iceberg.silver.usda_rice_psd`)
* **Chuyển đổi Cấu trúc (Unpivot Wide-to-Long)**: Các cột năm theo mùa vụ `col_1960_1961` $\dots$ `col_2025_2026` được unpivot thành các dòng chuẩn:
  * `crop_year`: Giữ nguyên định danh chuỗi gốc `"YYYY/YYYY+1"` (ví dụ: `"2020/2021"`).
  * `market_year`: Trích xuất năm đầu vụ dạng số nguyên `2020`.
* **Bảo toàn Quy tắc Nghiệp vụ**: Không tự ý ép `crop_year` sang `calendar_year` để tránh sai lệch chu kỳ mùa vụ lúa gạo quốc tế.
* **Điền khuyết Commodity**: Gán giá trị mặc định `"Rice, Milled"` cho các ô bị rỗng do merge cell từ nguồn thô.

### 3.3. USDA Rice Export Prices (`iceberg.silver.usda_export_price`)
* **Bóc tách Bảng Chuyên biệt**: Lọc các bảng liên quan đến giá xuất khẩu FOB từ Niên giám lúa gạo (Table 25: Thái Lan, Table 26: Việt Nam, Table 27: Ấn Độ, Table 28: Pakistan).
* **Chuẩn hóa Quốc gia Xuất khẩu**: Gán nhãn canonical `exporter_country` (`THAILAND`, `VIETNAM`, `INDIA`, `PAKISTAN`).
* **Chuẩn hóa Giá & Đơn vị**: Làm sạch giá FOB (USD/MT), lọc bỏ các ký tự `'NA'` hoặc ô rỗng.

### 3.4. World Bank Pink Sheet (`iceberg.silver.worldbank_commodity_monthly`)
* **Chuyển đổi Cấu trúc**: Unpivot từ hơn 70 cột hàng hóa ngang thành định dạng dọc chuẩn: `(commodity_code, commodity_name, period_code, period_date, price, unit)`.
* **Làm sạch Thời gian**: Chuyển đổi mã chu kỳ `YYYYMmm` (ví dụ `2024M08`) thành:
  * `year`: `2024` (int)
  * `month`: `8` (int)
  * `period_date`: `2024-08-01` (date)
* **Xử lý Ký tự Khuyết**: Chuyển đổi ký hiệu `…` và chuỗi rỗng thành `NULL`. Không can thiệp các feature ML (như rolling mean, lag, volatility) ở tầng này.

---

## 4. Cơ chế Xử lý Full, Incremental & Đảm bảo Idempotency

### 4.1. Cơ chế Idempotent MERGE INTO
Mỗi transformer kế thừa từ `BaseSilverTransformer` sử dụng câu lệnh Iceberg SQL:
```sql
MERGE INTO {target_table} AS target
USING {source_view} AS source
ON target.{key1} = source.{key1} AND target.{key2} = source.{key2} ...
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```
* **Bảo đảm**: Chạy lần 1, lần 2 hoặc retry/rerun nhiều lần trên cùng một batch dữ liệu thì kết quả luôn đồng nhất, **không bao giờ sinh bản ghi trùng lặp (Zero Duplicates)**.

### 4.2. Cơ chế Incremental Processing
* Hỗ trợ cờ `--mode incremental` và `--watermark <timestamp>` hoặc `--run-id <id>`.
* Khi chạy incremental, transformer chỉ quét các dòng mới trong Bronze thỏa mãn:
  ```sql
  _ingestion_timestamp > '{watermark}'
  ```
* Dữ liệu mới được merge trực tiếp vào partition tương ứng mà không cần quét lại toàn bộ lịch sử bảng.

---

## 5. Điều phối Airflow Pipeline (`rice_lakehouse_silver_pipeline`)

DAG `dags/dag_silver_pipeline.py` được thiết kế theo mô hình điều phối tự động:

```text
start
  ↓
gate_check_bronze (Kiểm tra snapshot Bronze & Spark API)
  ├── transform_faostat_trade       (Chạy PySpark song song)
  ├── transform_usda_psd            (Chạy PySpark song song)
  ├── transform_usda_export_price   (Chạy PySpark song song)
  └── transform_worldbank_commodity (Chạy PySpark song song)
  ↓
silver_data_quality_audit (Kiểm tra nullability & reconciliation)
  ↓
publish_silver_signal (Phát tín hiệu sẵn sàng cho Gold Layer)
  ↓
end
```

### Kiến trúc Spark API Micro-Runner
* Service `spark_api_server.py` chạy thường trực trong container `spark-iceberg` (cổng 5005).
* Airflow task gửi HTTP POST request điều phối job `/opt/spark/bin/spark-submit`, nhận log realtime và mã trạng thái trả về.

---

## 6. Kết quả Kiểm thử & Đối soát Dữ liệu Thực tế

Toàn bộ pipeline đã được kích hoạt chạy kiểm thử End-to-End thành công trên Airflow run `manual__2026-10-09T12:40:14`:

| Tên Bảng Silver | Số Bản ghi Ghi nhận | Kết quả Kiểm tra Khóa & Nullability | Trạng thái Nghiệm thu |
| :--- | :---: | :---: | :---: |
| `iceberg.silver.faostat_trade` | **1,226,470** | 100% Khóa M49/CPC hợp lệ, 0 NULL trên Business Keys | **ĐẠT (PASS)** |
| `iceberg.silver.worldbank_commodity_monthly` | **44,000** | 100% Ngày hợp lệ (`period_date`), 0 NULL trên Keys | **ĐẠT (PASS)** |
| `iceberg.silver.usda_export_price` | **12,936** | Đầy đủ 4 nước XK (TH, VN, IN, PK), 0 NULL trên Keys | **ĐẠT (PASS)** |
| `iceberg.silver.usda_rice_psd` | **990** | Đầy đủ 66 vụ mùa (1960-2025), 0 NULL trên Keys | **ĐẠT (PASS)** |
| **Tổng cộng** | **1,284,396 dòng** | **Thời gian chạy toàn pipeline: 1 phút 14 giây** | **HOÀN THÀNH** |

### Kiểm chứng Tính Bất Biến (Idempotency Test):
* Đã thực hiện chạy lại (Re-run) job trên bảng `usda_rice_psd`.
* Snapshot Iceberg commit: `operation=overwrite, addedRecords=990, removedRecords=990, totalRecords=990`.
* Truy vấn lại trên Trino: số dòng vẫn giữ chính xác **990 dòng**, chứng minh tính Idempotency đạt 100%.
