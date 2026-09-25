# Checkpoint and Recovery Architecture

## 1. Overview & Problem Context

Xử lý các tệp dữ liệu lớn (ví dụ: `Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip` nén ~400MB, khi giải nén bung ra ~8GB CSV thô với hàng chục triệu bản ghi) đối mặt với rủi ro đứt gãy giữa chừng (OOM, network glitch, container eviction, timeout).

Nếu không có cơ chế Checkpoint, bất kỳ lỗi nào xảy ra ở 95% tiến trình đều bắt buộc pipeline phải rollback và tải lại từ đầu (start from scratch), gây lãng phí băng thông mạng, tài nguyên CPU và thời gian xử lý.

Hệ thống Data Ingestion Engine của TLCN cài đặt cơ chế **Chunk-level Checkpointing** với metadata lưu trữ bền vững trong PostgreSQL table `ingestion.ingestion_checkpoints`.

---

## 2. Checkpoint Data Model & Persistence

Mỗi chunk được quản lý thông qua `CheckpointEntry` và lưu trữ tại bảng:

```sql
CREATE TABLE IF NOT EXISTS ingestion.ingestion_checkpoints (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    chunk_id INTEGER NOT NULL,
    chunk_key VARCHAR(255),
    row_start BIGINT,
    row_end BIGINT,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',  -- PENDING, PROCESSING, SUCCESS, FAILED
    checksum VARCHAR(128),
    error_message TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_id, run_id, chunk_id)
);
```

### Chu trình xử lý từng chunk:
1. `FullLoader` / `IncrementalLoader` khởi tạo chunk generator từ adapter tương ứng.
2. Trước khi trích xuất hoặc ghi Bronze cho chunk `N`, Loader truy vấn `CheckpointStore.get_last_successful(source_id, run_id)` hoặc tập hợp checkpoints đã thành công.
3. Nếu chunk `N` đã mang trạng thái `SUCCESS`:
   - Bỏ qua hoàn toàn việc tạo Python DataFrame nặng và bỏ qua việc ghi artifact/Bronze Iceberg cho chunk này.
4. Nếu chunk `N` chưa xử lý hoặc ở lần chạy resume:
   - Chunk được đọc, kiểm tra chất lượng sơ bộ, tính checksum SHA-256.
   - Ghi chunk / Bronze partition an toàn.
   - Cập nhật bản ghi checkpoint thành `SUCCESS`.
5. Nếu xảy ra lỗi:
   - Bắt exception, phân loại lỗi qua `ErrorClassifier`.
   - Cập nhật checkpoint chunk hiện tại thành `FAILED` kèm `error_message`.
   - Loader dừng tiến trình an toàn để Airflow retry hoặc trigger recovery.

---

## 3. Bản chất I/O và Giới hạn Kỹ thuật của CSV Scanning (Technical Precision)

> [!IMPORTANT]
> **Đính chính kỹ thuật quan trọng về chi phí I/O khi Resume CSV:**
> 
> * **Không có O(1) Random Seek trên file CSV thô:** File CSV là định dạng văn bản tuần tự (sequential text format) với độ dài dòng biến thiên (variable line length). Hệ điều hành và parser (như `pandas.read_csv` hay Python C-engine) không thể "nhảy cóc" tức thì đến dòng thứ $N$ trong thời gian $O(1)$ nếu không có sẵn file index offset.
> * **Cơ chế `skiprows` thực tế:** Khi thiết lập `skiprows = N`, parser bắt buộc phải đọc luồng byte tuần tự từ đầu file, quét qua ký tự xuống dòng (`\n` hoặc `\r\n`) đúng $N$ lần để xác định vị trí bắt đầu của chunk cần phục hồi.
> * **Chi phí I/O vẫn tồn tại và tỷ lệ thuận với vị trí resume:** Chi phí đọc đĩa (Disk I/O / Page Cache scan) vẫn tiêu tốn thời gian $O(N)$ tỷ lệ thuận với số dòng cần bỏ qua.
> * **Lợi ích thực sự của Checkpoint Resume:**
>   1. **Tiết kiệm RAM & CPU Parser:** Parser không cần phân giải kiểu dữ liệu (type inference), không parse delimiter/quotes, và **không cấp phát (allocate) bộ nhớ cho các DataFrame** của các chunk đã qua.
>   2. **Loại bỏ trùng lặp ghi (Write Idempotency):** Không thực hiện lại thao tác nén parquet, không upload lại các chunk lên MinIO raw landing, và không chèn bản ghi trùng lặp vào Apache Iceberg Bronze table.
>   3. **Giảm thiểu thời gian phục hồi:** Mặc dù mất một lượng thời gian đọc lướt byte trên ổ đĩa, toàn bộ pipeline tiết kiệm được hơn 80-90% thời gian so với việc tái thực thi toàn bộ chu trình xử lý dữ liệu nặng.

---

## 4. Kịch bản Minh họa: FAOSTAT Trade Matrix

Xét cấu hình ingestion cho FAOSTAT Trade Matrix:
- File uncompressed: `Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv` (~8.2 GB, ~38.000.000 dòng).
- Cấu hình: `chunk_size = 50,000` dòng/chunk (tổng cộng ~760 chunks).

### Diễn tiến đứt gãy & Phục hồi:
1. **Lần chạy 1 (Run 1):**
   - Chunks `0` đến `124` (tổng cộng $125 \times 50,000 = 6,250,000$ dòng) được xử lý thành công.
   - Tại chunk `125` (dòng 6,250,000 đến 6,299,999), kết nối mạng hoặc memory pressure gây crash process.
   - Checkpoint ghi nhận: Chunk `124` là `SUCCESS`, Chunk `125` là `FAILED`.
2. **Lần chạy phục hồi (Recovery Run 2):**
   - Loader kết nối PostgreSQL, nhận thấy checkpoint chunk gần nhất thành công là chunk `124` (offset dòng 6,250,000).
   - Adapter mở lại luồng đọc CSV, cấu hình quét qua 6,250,000 dòng đầu tiên.
   - Quá trình scan byte diễn ra nhanh chóng trên filesystem/buffer cache mà không tạo hàng triệu Python objects trong RAM.
   - Khi con trỏ đọc đến dòng 6,250,000, Adapter bắt đầu yield `DataChunk(chunk_id=125)` cho loader tiếp tục ghi dữ liệu vào MinIO và Iceberg.

---

## 5. Checkpoint Lifecycle & Cleanup Policy

- **Trong khi Ingestion đang diễn ra:** Checkpoint entries được duy trì chi tiết ở mức từng chunk nhằm phục vụ failover tức thì.
- **Sau khi toàn bộ Run hoàn tất (Run Status = `SUCCESS`):**
  - Trạng thái tổng hợp (tổng records extracted, tổng chunks, artifact URI, manifest URI, checksum SHA-256) được commit vĩnh viễn vào `ingestion.ingestion_runs` và `ingestion.source_snapshots`.
  - Các checkpoints chi tiết của run đó có thể được dọn dẹp (`clear_checkpoints`) hoặc lưu vết audit theo Retention Policy (mặc định 30 ngày) để tối ưu dung lượng bảng PostgreSQL.
