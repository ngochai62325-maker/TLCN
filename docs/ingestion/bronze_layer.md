# Kiến Trúc Tầng Bronze & Đảm Bảo Độ Tin Cậy (Bronze Layer & Storage Reliability)

> **Dự án**: Vietnam Rice Market Lakehouse (`TLCN`)  
> **Giai đoạn**: Tuần 6 (21/9/2026 – 27/9/2026) — Phân công Người 2 (Data Platform / Storage Engineer)  
> **Trách nhiệm chính**: Thiết kế lưu trữ tầng Bronze an toàn trên MinIO/Iceberg, quản lý cách ly dữ liệu lỗi (Quarantine), kiểm soát tính Idempotent (0 duplicate) và hỗ trợ kiểm toán dữ liệu (Auditability).

---

## 1. Tổng Quan Kiến Trúc Tầng Bronze

Tầng **Bronze** là vùng tiếp nhận dữ liệu thô (Raw Landing & Queryable Bronze Tables) của toàn bộ hệ thống Lakehouse. Nguyên tắc bất biến của tầng Bronze:
1. **Bất biến (Immutable & Append-Only)**: Dữ liệu nguồn sau khi thu thập được bảo toàn nguyên trạng, không áp dụng logic nghiệp vụ (không ép kiểu tùy tiện, không drop null, không deduplicate nghiệp vụ).
2. **Khả năng truy vấn cao (Queryable Lakehouse Tables)**: Sử dụng chuẩn bảng mở **Apache Iceberg** lưu trữ trên **MinIO**, hỗ trợ partition pruning, time travel và metadata commit nguyên tử (atomic commit).
3. **Auditability & Traceability**: Mọi bản ghi dữ liệu đều được tự động gắn 7 cột kỹ thuật (Technical Metadata Columns) nhằm truy vết chính xác nguồn gốc: mã batch, mã run, checksum SHA-256, đường dẫn file nguồn và thời gian ingest.
4. **Cô lập lỗi (Fault Isolation)**: Lỗi dữ liệu cục bộ (file rỗng, sai format, schema drift) được chuyển vào phân vùng `quarantine` kèm file chẩn đoán `.error.json`, tuyệt đối không làm sập pipeline Airflow hay ảnh hưởng đến các nguồn dữ liệu độc lập khác.

```mermaid
flowchart TD
    subgraph Ingestion_Sources["Các Nguồn Dữ Liệu"]
        S1["FAOSTAT (Trade, Prod, Price)"]
        S2["NSO (Tổng cục Thống kê VN)"]
        S3["USDA (PSD, Rice Yearbook)"]
        S4["World Bank (Pinksheet)"]
        S5["ThiTruongNongSan (Giá nội địa)"]
    end

    subgraph Reliability_Engine["Ingestion & Reliability Engine (Người 2)"]
        VAL["BronzeQualityValidator\n- Zero-byte check\n- SHA-256 Checksum\n- Schema Drift Detection"]
        IDEM["IdempotencyController\n- Checksum / Batch Check\n- Zero-Duplicate Skip"]
        QMAN["QuarantineManager\n- Non-crashing isolation\n- Companion .error.json"]
        WRITER["BronzeIcebergWriter\n- Native Parquet Append\n- PyArrow/PyIceberg Bulk Path"]
    end

    subgraph MinIO_Bronze["MinIO S3: Bucket 'bronze'"]
        subgraph Functional_Folders["5 Thư Mục Chức Năng Theo Nhóm Nguồn"]
            RAW["{group}/raw/\n(File thô gốc)"]
            META["{group}/metadata/\n(batch_{batch_id}.json)"]
            MANI["{group}/manifest/\n(run_{run_id}.json)"]
            AUDIT["{group}/audit/\n(audit_{run_id}.json)"]
            QUAR["{group}/quarantine/\n(*.csv + *.error.json)"]
        end
    end

    subgraph Lakehouse_Catalog["Lakehouse Storage & Query"]
        REST["Iceberg REST Catalog\n(SQLite WAL + busy_timeout)"]
        ICEBERG_TBL["Queryable Tables: iceberg.bronze.*\n(faostat_trade: 1.22M rows)"]
        TRINO["Trino SQL Query Engine\n(Port 8080)"]
    end

    Ingestion_Sources --> VAL
    VAL -- "Dữ liệu lỗi / Rỗng / Sai schema" --> QMAN --> QUAR
    VAL -- "Dữ liệu hợp lệ" --> IDEM
    IDEM -- "Trùng lặp batch / Checksum" --> SKIP["Skip an toàn (0 duplicates)"]
    IDEM -- "Batch mới hợp lệ" --> WRITER

    WRITER --> RAW
    WRITER --> META
    WRITER --> MANI
    WRITER --> AUDIT
    WRITER --> REST --> ICEBERG_TBL
    ICEBERG_TBL --> TRINO
```

---

## 2. Cấu Trúc 5 Thư Mục Chức Năng Trên MinIO Object Storage

Mọi nguồn dữ liệu được tự động phân nhóm vào 5 không gian chức năng chuẩn mực trên bucket `bronze` của MinIO thông qua module [`BronzeStorageLayout`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/storage/bronze_storage_layout.py):

```text
bronze/
├── faostat/
│   ├── raw/
│   ├── metadata/
│   ├── manifest/
│   ├── audit/
│   └── quarantine/
├── nso/
│   ├── raw/
│   ├── metadata/
│   ├── manifest/
│   ├── audit/
│   └── quarantine/
├── usda/
│   ├── raw/
│   ├── metadata/
│   ├── manifest/
│   ├── audit/
│   └── quarantine/
├── worldbank/
│   ├── raw/
│   ├── metadata/
│   ├── manifest/
│   ├── audit/
│   └── quarantine/
└── thitruongnongsan/
    ├── raw/
    ├── metadata/
    ├── manifest/
    ├── audit/
    └── quarantine/
```

### Chi tiết vai trò từng thư mục:

| Thư mục | Định dạng file | Mục đích sử dụng | Quy ước đặt tên Key |
| :--- | :--- | :--- | :--- |
| `raw/` | `.csv`, `.zip`, `.xlsx` | Lưu trữ toàn vẹn artifact gốc tải về từ đối tác/crawler | `{group}/raw/{batch_id}/{filename}` |
| `metadata/` | `.json` | Lưu trữ metadata chi tiết cho từng batch ingestion (chuẩn JSON schema) | `{group}/metadata/batch_{batch_id}.json` |
| `manifest/` | `.json` | Snapshot manifest ghi nhận tình trạng thực thi và danh sách chunk | `{group}/manifest/run_{run_id}.json` |
| `audit/` | `.json` | Nhật ký vận hành (Audit trail) ghi nhận thời gian, số lượng dòng, trạng thái | `{group}/audit/audit_{run_id}.json` |
| `quarantine/` | File gốc + `.error.json` | Vùng cách ly dữ liệu lỗi kèm file chẩn đoán lỗi đồng hành | `{group}/quarantine/{ts}_{file}` & `{ts}_{file}.error.json` |

---

## 3. Đặc Tả Metadata & Audit Schema

Được hiện thực tại [`src/ingestion/storage/metadata_schemas.py`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/storage/metadata_schemas.py):

### 3.1. Batch Metadata Schema (`batch_{batch_id}.json`)
```json
{
  "batch_id": "faostat_trade_20260926_ebc0",
  "source_name": "faostat_trade",
  "source_group": "faostat",
  "extracted_at": "2026-09-26 05:35:30 UTC",
  "committed_at": "2026-09-26 05:38:44 UTC",
  "record_count": 1226470,
  "checksum": "697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168",
  "file_size_bytes": 167990400,
  "schema_version": "1.0",
  "status": "COMMITTED",
  "target_table": "iceberg.bronze.faostat_trade"
}
```

### 3.2. Companion Quarantine Diagnostic Schema (`*.error.json`)
Khi một file bị cách ly, hệ thống tự động sinh file chẩn đoán đồng hành với cấu trúc:
```json
{
  "original_file": "corrupted_trade_matrix.csv",
  "quarantined_at": "2026-09-26 12:44:10 UTC",
  "error_type": "SCHEMA_DRIFT",
  "error_message": "Schema drift detected. Missing expected columns: ['element', 'item']",
  "source_id": "faostat_trade",
  "source_group": "faostat",
  "checksum": "d41d8cd98f00b204e9800998ecf8427e",
  "file_size_bytes": 1024,
  "record_count": 0,
  "corrupted_snippet": "bad_col1,bad_col2\nfoo,bar\n",
  "schema_diff": {
    "missing_columns": ["element", "item"],
    "actual_columns": ["bad_col1", "bad_col2"],
    "expected_columns": ["domain", "element", "item", "year", "value"]
  }
}
```

### 3.3. Technical Metadata Columns Trong Bảng Iceberg Bronze
Mỗi dòng dữ liệu trong các bảng Iceberg tầng Bronze đều được tiêm 7 trường kỹ thuật bắt buộc:
1. `_ingestion_run_id` (`VARCHAR`): ID phiên thực thi Airflow/Engine duy nhất.
2. `_ingestion_batch_id` (`VARCHAR`): ID batch phục vụ kiểm soát Idempotency.
3. `_ingestion_timestamp` (`TIMESTAMP(6) WITH TIME ZONE`): Dấu thời gian nạp dữ liệu chuẩn UTC.
4. `_source_id` (`VARCHAR`): Định danh nguồn (ví dụ: `faostat_trade`, `usda_psd`).
5. `_source_file` (`VARCHAR`): Tên file nguồn gốc hoặc URL endpoint.
6. `_source_checksum` (`VARCHAR`): Mã băm SHA-256 của file nguồn.
7. `_source_snapshot_id` (`BIGINT`): Khóa ngoại tham chiếu đến bảng lịch sử `ingestion.source_snapshots`.

---

## 4. Cơ Chế Đảm Bảo Độ Tin Cậy & Quarantine Flow

### 4.1. Quy trình kiểm định chất lượng (Bronze Quality Validator)
Triển khai tại [`src/ingestion/reliability/bronze_quality_validator.py`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/reliability/bronze_quality_validator.py):
* **Zero-byte Verification**: Kiểm tra kích thước file > 0 byte. Chặn đứng các file tải về rỗng hoặc kết nối mạng bị ngắt giữa chừng (`EMPTY_FILE`).
* **Checksum Verification**: Tính mã băm SHA-256 dạng stream block và so sánh với mã nguồn (`CHECKSUM_MISMATCH`).
* **Structural Parsing Check**: Kiểm tra tính hợp lệ cú pháp CSV/JSON. Bóc tách vị trí dòng lỗi và mẫu dữ liệu bị hỏng (`PARSE_FAILURE`).
* **Schema Drift Protection**: Đối chiếu danh sách cột thực tế với danh sách cột bắt buộc (`SCHEMA_DRIFT`).

### 4.2. Cơ chế Quarantine không gây sập Pipeline (Non-Crashing Quarantine)
* Nếu kiểm tra thất bại, [`QuarantineManager`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/reliability/quarantine_manager.py) chuyển file hỏng và file `.error.json` vào MinIO `bronze/{source_group}/quarantine/`.
* Ghi nhật ký vào `bronze/{source_group}/audit/` với trạng thái `QUARANTINED`.
* Trả về kết quả `IngestionResult(status=FAILED, records_quarantined=N)`. Airflow đánh dấu task thất bại có kiểm soát nhưng **các task nguồn dữ liệu độc lập khác vẫn chạy song song bình thường**, không làm nghẽn toàn bộ hệ thống.

---

## 5. Thiết Kế Idempotency & Tối Ưu Hóa Ghi Bulk Dữ Liệu Lớn

### 5.1. Cơ chế đảm bảo tính Idempotent (Zero-Duplicate Re-runs)
Triển khai tại [`src/ingestion/reliability/idempotency_controller.py`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/reliability/idempotency_controller.py):
* Khi nhận yêu cầu nạp dữ liệu, hệ thống tính mã SHA-256 của nguồn và kiểm tra thư mục `metadata/` cũng như cơ sở dữ liệu `source_snapshots`.
* Nếu batch đó hoặc file có checksum trùng khớp đã được ghi nhận trạng thái `COMMITTED`:
  * Tự động trả về quyết định `SKIPPED_IDEMPOTENT`.
  * Không thực hiện append vào bảng Iceberg, **đảm bảo 0 bản ghi trùng lặp (Zero Duplicates)** khi chạy lại (re-run) hoặc backfill trên Airflow.

### 5.2. Giải pháp xử lý tải lớn `faostat_trade` (1.22 Triệu dòng)
* **Vấn đề đã khắc phục**: Việc nạp 1.226.470 dòng qua Trino SQL `INSERT VALUES` từng 500 dòng gây ra gần 2.500 HTTP request và snapshot commit dồn dập, làm tê liệt Trino và REST Catalog.
* **Kiến trúc PyIceberg Parquet Bulk Append**:
  * Đọc chunk 50.000 dòng qua Arrow Table.
  * Ghi trực tiếp thành 1 file Parquet vào MinIO và commit 1 snapshot nguyên tử duy nhất trên REST Catalog cho mỗi chunk.
  * Áp dụng bản vá tương thích `pyarrow/pyiceberg` trong [`BronzeIcebergWriter`](file:///c:/Users/phucb/Documents/TLCN/src/ingestion/storage/bronze_writer.py) giúp toàn bộ 1.226.470 dòng nạp thành công mượt mà trong **chỉ 3 phút 14 giây** (thay vì timeout/treo).
* **Khắc phục Concurrency Lock trên Iceberg REST Catalog**:
  * Cấu hình database backend SQLite của REST Catalog với chế độ **WAL** (`journal_mode=WAL`) và thời gian chờ khóa `busy_timeout=60000ms` trong [docker-compose.yml](file:///c:/Users/phucb/Documents/TLCN/docker-compose.yml), triệt tiêu hoàn toàn lỗi `[SQLITE_BUSY] database is locked` khi nhiều task Airflow ghi đồng thời.

---

## 6. Sổ Tay Kiểm Toán & Truy Vấn Nghiệm Thu (Audit Runbook)

Dưới đây là bộ truy vấn mẫu chuẩn bị sẵn cho Ban Giảng Khảo / Trưởng Nhóm kiểm tra tính toàn vẹn tầng Bronze thông qua Trino CLI hoặc DBeaver (`http://localhost:8080`, catalog `iceberg`):

### Kịch bản 1: Kiểm toán dữ liệu lớn `faostat_trade` (1.22 Triệu dòng)
```sql
-- 1.1. Kiểm tra tổng số dòng thực tế đã nạp vào bảng Bronze
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT _ingestion_batch_id) AS total_batches,
    MIN(_ingestion_timestamp) AS first_ingested_at,
    MAX(_ingestion_timestamp) AS latest_ingested_at
FROM iceberg.bronze.faostat_trade;
-- Kỳ vọng: total_records = 1,226,470

-- 1.2. Kiểm tra tính toàn vẹn của Technical Metadata Columns
SELECT 
    _source_id,
    _source_file,
    _source_checksum,
    _ingestion_run_id,
    COUNT(*) AS records_per_file
FROM iceberg.bronze.faostat_trade
GROUP BY _source_id, _source_file, _source_checksum, _ingestion_run_id;

-- 1.3. Thống kê thương mại lúa gạo Việt Nam (top 5 đối tác xuất khẩu)
SELECT 
    partner_countries AS partner,
    element,
    year,
    unit,
    SUM(value) AS total_volume
FROM iceberg.bronze.faostat_trade
WHERE reporter_countries = 'Viet Nam' 
  AND item LIKE '%Rice%'
  AND year >= 2020
GROUP BY partner_countries, element, year, unit
ORDER BY total_volume DESC
LIMIT 5;
```

### Kịch bản 2: Kiểm toán Idempotency & Lịch sử Snapshot Iceberg
```sql
-- 2.1. Kiểm tra lịch sử snapshot của bảng (chứng minh các lần commit là append có kiểm soát)
SELECT 
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary['added-records'] AS records_added,
    summary['total-records'] AS running_total
FROM iceberg.bronze."faostat_trade$snapshots"
ORDER BY committed_at DESC;

-- 2.2. Kiểm tra chứng minh không có bản ghi trùng lặp trên cùng batch
SELECT 
    _ingestion_batch_id,
    COUNT(*) AS count_records,
    COUNT(DISTINCT _source_checksum) AS unique_checksums
FROM iceberg.bronze.faostat_trade
GROUP BY _ingestion_batch_id;
```

### Kịch bản 3: Kiểm toán Metadata Repository (PostgreSQL) & File System (MinIO)
```sql
-- Kết nối PostgreSQL: postgresql://airflow:airflow@localhost:5432/airflow
-- 3.1. Kiểm tra nhật ký các phiên Ingestion Run thành công
SELECT 
    run_id, 
    source_id, 
    status, 
    records_extracted, 
    records_quarantined, 
    duration_seconds,
    checksum
FROM ingestion.ingestion_runs
WHERE status = 'SUCCESS'
ORDER BY started_at DESC;

-- 3.2. Kiểm tra snapshot metadata đã lưu
SELECT 
    id, 
    source_id, 
    checksum, 
    size_bytes, 
    status, 
    source_uri
FROM ingestion.source_snapshots
ORDER BY retrieved_at DESC;
```

---

## 7. Kết Quả Kiểm Thử (Unit & Integration Test Suite)

Tất cả các tiêu chí nghiệm thu của Người 2 đã được tự động hóa tại [`tests/unit/test_bronze_storage_deliverables.py`](file:///c:/Users/phucb/Documents/TLCN/tests/unit/test_bronze_storage_deliverables.py).

```bash
docker compose exec -u airflow airflow-webserver python -m pytest tests/unit/test_bronze_storage_deliverables.py -v
```

**Bảng tổng kết 19 test cases thành công 100%:**
* `TestBronzeStorageHierarchy`:
  * `test_source_group_resolution` (7 nguồn dữ liệu: `faostat`, `nso`, `usda`, `worldbank`, `thitruongnongsan`): **PASSED**
  * `test_five_functional_folder_paths` (Kiểm tra đường dẫn 5 thư mục chuẩn): **PASSED**
  * `test_batch_metadata_save_and_load` (Lưu và đọc BatchMetadata): **PASSED**
  * `test_audit_entry_save` (Lưu AuditLogEntry): **PASSED**
* `TestQualityValidationAndQuarantine`:
  * `test_zero_byte_file_rejected` (Phát hiện và chặn file 0-byte): **PASSED**
  * `test_checksum_mismatch_detected` (Phát hiện sai lệch SHA-256): **PASSED**
  * `test_corrupted_csv_detected` (Phát hiện lỗi cú pháp định dạng CSV): **PASSED**
  * `test_schema_drift_missing_columns` (Phát hiện thiếu cột / schema drift): **PASSED**
  * `test_quarantine_artifact_routing_and_companion_diagnostic` (Cách ly file và sinh `.error.json`): **PASSED**
* `TestIdempotencyController`:
  * `test_idempotent_skip_on_duplicate_batch` (Skip khi re-run cùng batch/checksum): **PASSED**
  * `test_idempotency_force_reprocess_allowed` (Cho phép ép reprocess khi có cờ force): **PASSED**
* `TestFullLoaderIntegration`:
  * `test_full_loader_generates_bronze_functional_folders` (Sinh đầy đủ 5 thư mục MinIO): **PASSED**
  * `test_full_loader_routes_empty_file_to_quarantine_without_crashing` (Chuyển quarantine an toàn không sập pipeline): **PASSED**

---
*Tài liệu này là căn cứ kỹ thuật chính thức cho phân công Người 2 trong đồ án tốt nghiệp Data Lakehouse lúa gạo Việt Nam.*
