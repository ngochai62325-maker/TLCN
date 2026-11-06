# Đã nạp rồi thì lần sau nạp gì? FULL, incremental, skip và recovery trong TLCN

Ngày đối chiếu code: **31/10/2026**. HEAD `cd2f1a77b27d5bcba6f1d925e3d9305952551bae`, branch `feat(gold)`.

Tài liệu bổ sung cho [BRONZE_PRESENTATION.md](BRONZE_PRESENTATION.md), giải thích theo code hiện tại. Không sửa cấu hình/logic, chạy pipeline hay ghi dữ liệu trong lần viết này. Mọi bảng dữ liệu và checksum ký hiệu dưới đây là ví dụ minh họa.

## 1. Trả lời ngay câu hỏi của bạn

**Hệ thống hiện không tự chuyển sang incremental chỉ vì đã nạp lần đầu.**

Chiến lược được chọn từ `load_strategy` trong `source_registry.yaml`. Cả chín nguồn thuộc profile `local_nine` đang khai báo:

```yaml
source_type: LOCAL_FILE
load_strategy: FULL
```

Vì vậy lần đầu và lần sau đều đi vào **FullLoader**. Bên trong FullLoader mới có quyết định có cần extract/write nữa không:

```text
Chọn chiến lược từ cấu hình: FULL
        ↓
Kiểm readiness, validation, checksum, metadata và recovery context
        ↓
Nguồn đã ingest thành công, không đổi, proof còn hợp lệ?
        ├─ Có → SKIPPED, không ingest lại dữ liệu
        └─ Không → Nếu đủ điều kiện thì đọc FULL;
                   nếu có drift/unknown commit/lỗi thì chặn
```

**SKIPPED không phải incremental.** Nó có nghĩa lần này không cần nạp lại bản nguồn đã được xác minh, chứ không phải đã tìm ra một số rows mới để nạp.

## 2. Vì sao bạn dễ nghĩ “lần sau là incremental”?

Có ba việc khác nhau thường bị gọi chung là “nạp tăng cường”:

| Khái niệm | Làm gì? | Trạng thái trong dự án |
|---|---|---|
| Incremental extraction | Chỉ lấy những bản ghi mới/thay đổi từ nguồn theo cursor/watermark/change log | Chín adapter nguồn hiện tại không hỗ trợ |
| Idempotent skip | Biết cùng bản nguồn đã thành công nên không extract/write lại | Có trong Bronze FULL |
| Upsert/chỉ ghi thay đổi | Có thể đọc đầy đủ đầu vào nhưng chỉ insert/update phần khác ở target | Có ở Silver MERGE |
| Physical NOOP | Kết quả giống hoàn toàn nên không tạo commit/snapshot thừa | Có cơ chế riêng tại các tầng; Gold đã có evidence replay |
| Recovery | Hoàn tất/xác minh lại cùng một lần nạp bị gián đoạn | Có protocol Bronze theo immutable chunk identity |

Để nói chính xác, luôn tách hai câu hỏi:

1. **Em đọc bao nhiêu dữ liệu từ nguồn?** FULL hay incremental extraction?
2. **Em ghi bao nhiêu dữ liệu vào đích?** Append, upsert, overwrite hay không ghi?

Đọc FULL không bắt buộc phải DROP bảng. MERGE cũng không tự chứng minh đầu vào được đọc incremental.

## 3. Đoạn code nào quyết định dùng loader nào?

File: [ingestion_engine.py](../../src/ingestion/core/ingestion_engine.py), phương thức `_select_loader()`.

Code quyết định theo ý sau, rút gọn các tham số khởi tạo:

```python
if config.load_strategy == LoadStrategy.FULL:
    return FullLoader(...)
elif config.load_strategy == LoadStrategy.INCREMENTAL:
    return IncrementalLoader(...)
```

Không có điều kiện kiểu:

```python
# Đây KHÔNG phải logic hiện tại của dự án:
if source_has_been_loaded_before:
    automatically_switch_to_incremental()
```

Lịch sử ingestion phục vụ skip/recovery và bằng chứng; **không tự đổi cấu hình FULL thành INCREMENTAL**.

## 4. Hệ thống biết “đã nạp rồi” bằng cách nào?

Nó đối chiếu **source ID + checksum nội dung + trạng thái thành công + physical proof**.

### 4.1. Tính checksum của bản nguồn hiện tại

FullLoader gọi `_compute_source_checksum()`.

- Nếu nguồn là file: SHA-256 của nội dung file.
- Nếu nguồn là thư mục như NSO: checksum từ tên tương đối và nội dung các file khớp pattern, theo thứ tự xác định.

Ví dụ ký hiệu:

```text
production_world.csv phiên bản A → checksum H_A
production_world.csv phiên bản B → checksum H_B
```

`H_A`, `H_B` ở đây chỉ là tên minh họa, không phải SHA-256 thật.

Tên file giống nhau không đủ để xác định cùng dữ liệu. Nội dung đổi thì hash thường đổi; chỉ đổi thứ tự rows/format/encoding cũng có thể đổi hash dù ý nghĩa observations gần giống nhau. Hash artifact không phải business-key diff.

### 4.2. Tìm phiên bản nguồn thành công trong PostgreSQL

File: [metadata_repository.py](../../src/ingestion/storage/metadata_repository.py), phương thức `find_snapshot_by_checksum()`.

Query trong code có logic:

```sql
SELECT *
FROM ingestion.source_snapshots
WHERE source_id = %s
  AND checksum = %s
  AND status = 'SUCCESS'
ORDER BY retrieved_at DESC
LIMIT 1;
```

Các tham số là source ID và checksum vừa tính. Đây là bảng **metadata phiên bản artifact nguồn**, không phải bảng chứa toàn bộ observations và không phải Iceberg snapshots table.

Nếu có record, nghĩa là metadata cho biết bản nguồn này đã được ingest thành công trước đó. Nếu run trước FAILED thì không được chỉ nhìn “đã từng chạy” rồi skip.

### 4.3. Không chỉ tin một record SUCCESS

Files: [idempotency_controller.py](../../src/ingestion/reliability/idempotency_controller.py), [full_loader.py](../../src/ingestion/core/full_loader.py).

`IdempotencyController.evaluate_batch()` đề xuất không xử lý khi có source snapshot SUCCESS cùng checksum. FullLoader còn gọi `_check_existing_snapshot()` để kiểm run liên quan:

- Đúng source ID.
- Run đã SUCCESS.
- Checksum run khớp artifact.
- `BronzeIcebergWriter.verify_run()` xác minh physical run membership/row count/source trong Iceberg.

Sau đó local orchestration còn kiểm catalog identity/proof trước khi chấp nhận source result và chuyển Silver.

**Giới hạn của từng proof:** skip-path `verify_run()` kiểm membership/count/source, không tái so exact mọi value của cả artifact với bảng. Exact values/multiplicity được kiểm ở chunk write/readback; không nên nói mỗi lần skip đều parse lại mọi row và so exact toàn nội dung.

### 4.4. Trả kết quả SKIPPED

Kết quả có dạng ý nghĩa:

```text
load_type: FULL
status: SKIPPED
records_extracted: 0
skipped_reason: unchanged_snapshot
```

`load_type` vẫn FULL vì đó là strategy đã chọn. `records_extracted=0` là không extract rows mới trong lần này, không có nghĩa bảng Bronze đang rỗng.

Trong local DAG, SKIPPED có bằng chứng được xử lý như kết quả nguồn hợp lệ cho batch gate; không mặc định bỏ source khỏi batch hoặc chặn Silver vì Airflow task bị skip.

## 5. Ví dụ ba lần chạy để dễ hình dung

Giả sử có file gồm hai quan sát:

```text
year,production
2022,100
2023,110
```

### Lần 1 — Chưa ingest bản nguồn này

```text
Checksum = H_A
Chưa có source snapshot SUCCESS cho H_A
→ FullLoader đọc cả 2 rows
→ Ghi Bronze, readback, persist metadata
→ SUCCESS nếu toàn bộ điều kiện đạt
```

### Lần 2 — Chạy lại cùng file, cùng nội dung

```text
Checksum vẫn H_A
Có source snapshot/run SUCCESS và physical proof hợp lệ
→ SKIPPED
→ Không extract/write lại 2 rows
```

Đây là **idempotent skip**, không phải incremental extraction.

Skip vẫn có chi phí: đọc bytes để tính hash, kiểm validation/metadata/catalog và ghi metadata lần chạy theo đường xử lý. Không đồng nghĩa không đọc ổ đĩa, không gọi dịch vụ hay toàn DAG không làm gì.

### Lần 3 — File được cập nhật, thêm năm và sửa số cũ

```text
year,production
2022,100
2023,112
2024,120
```

```text
Checksum đổi thành H_B
Không có SUCCESS cho H_B
→ Khi input/schema/recovery admission cho phép,
  FullLoader đọc cả 3 rows của bản mới
→ Không chỉ đọc mỗi row 2024
```

Bronze giữ các phiên bản theo ingestion lineage, không tự so business key để chỉ append row 2024. Trong ví dụ một bảng chỉ có hai lần ghi thành công này, không có writes khác, Bronze history có thể gồm **2 + 3 = 5 rows**, với phiên bản 2023 khác nhau thuộc các physical runs.

Silver đọc admitted Bronze snapshot, áp source conflict rules và business-key handling; với nguồn được phép latest-based dedup, có thể giữ quan sát 2023 mới hơn. **Không áp kết luận đó cho mọi nguồn:** NSO có checksum/schema approval và conflict guards riêng, có thể chặn bản cập nhật chứ không tự lấy bản mới nhất.

Đây là ví dụ giải thích cơ chế, không phải hướng dẫn sửa Raw rồi replay chín nguồn thật trên baseline đã publish.

## 6. Nếu chỉ đổi một file NSO thì chỉ nạp file đó không?

**Không có cơ chế file-level incremental tự động như vậy trong FullLoader hiện tại.**

NSO có checksum từng file để lineage và checksum cả tập file theo pattern để nhận diện artifact thư mục. Nếu một file matching pattern thay đổi, directory checksum thay đổi.

Nếu bản nguồn mới được phép ingest, `extract_full()` của NSO vẫn duyệt tập file matching pattern theo thứ tự, không chỉ tự chọn file vừa đổi. Checksum từng file không tự tạo ra delta extraction.

Silver NSO hiện kiểm checksum/layout với metadata đã pin; bản nguồn đổi chưa được profile/duyệt có thể bị chặn. Không đổi strategy hoặc checksum evidence chỉ để vượt gate.

## 7. Vậy incremental thật sự sẽ hoạt động thế nào?

Phần này là **khái niệm và yêu cầu cho mở rộng tương lai**, không mô tả chín nguồn hiện đang chạy incremental.

Giả sử nguồn cung cấp một change feed có trường `updated_at` đáng tin cậy:

```text
Đã xử lý thành công đến watermark W
        ↓
Lần sau yêu cầu nguồn trả thay đổi sau W
        ↓
Nhận cả bản ghi mới và bản ghi cũ vừa được sửa
        ↓
Ghi dữ liệu, xác minh commit thành công
        ↓
Commit watermark mới theo protocol an toàn
```

Ví dụ ý tưởng điều kiện nguồn:

```sql
WHERE updated_at > :last_successful_watermark
```

Điều kiện này chỉ để minh họa. Implementation thật cần xử lý timestamp trùng, pagination, stable ordering/tie-break IDs, late arrivals, backdated updates và deletes. Nhiều nguồn cần composite cursor, overlap window + dedup hoặc CDC, không chỉ một phép `>`.

**Không dùng `year > max(year)` một cách mù quáng:** nguồn có thể sửa số liệu năm 2023 trong khi đã có năm 2024. Lọc chỉ năm mới sẽ bỏ sót correction đó.

Lần đầu của một nguồn incremental có thể bootstrap toàn bộ hoặc dùng mốc khởi đầu được cấu hình. Đó là thiết kế của nguồn/adapter; không có quy tắc chung “đã tồn tại bảng thì tự lấy delta”.

## 8. Dự án đã có IncrementalLoader, sao không bật luôn?

File: [incremental_loader.py](../../src/ingestion/core/incremental_loader.py).

Nhánh này hiện có cấu trúc:

1. Đọc watermark qua `WatermarkStore`.
2. Gọi `adapter.extract_incremental(...)`.
3. Duyệt chunks, lưu checkpoint và trả metrics/watermark proposal.
4. Trả `NO_NEW_DATA` nếu adapter không trả rows.

Engine chỉ gọi `watermark_store.set()` khi strategy INCREMENTAL, loader result SUCCESS và có watermark sau xử lý. Watermarks được lưu ở PostgreSQL `ingestion.source_watermarks` qua [watermark.py](../../src/ingestion/core/watermark.py).

Nhưng phải đọc kỹ active code thay vì chỉ tin docstring:

- Tất cả adapter của chín nguồn đang dùng đều có `extract_incremental()` báo `NotImplementedError` cho snapshot sources.
- `_select_loader()` không truyền `BronzeIcebergWriter` cho IncrementalLoader; loader hiện không có bước write/physical readback như FullLoader.
- Watermark proposal hiện lấy max của trường trong từng chunk rồi gán lại; chưa chứng minh global max nếu chunks không ordered hoặc đầy đủ semantics của cursor.
- Trong engine, watermark persistence nằm trước final `complete_run()`; không có transaction bao trùm physical commit/watermark/run status.
- Unit tests với mock adapter kiểm các nhánh logic không phải physical acceptance cho incremental Bronze end-to-end.

Vì vậy đây là **khung mở rộng chưa được nghiệm thu thành đường incremental ingestion thật**, không được chỉ đổi YAML rồi tuyên bố đã có incremental an toàn.

Registry chặn INCREMENTAL thiếu `watermark_column`; đó mới là điều kiện cấu hình tối thiểu, không đủ chứng minh adapter, writer và recovery đã sẵn sàng. Local profile còn pin registry fingerprint; đổi YAML tùy ý sẽ bị routing guard phát hiện.

## 9. Recovery có phải incremental không?

**Không. Recovery là tiếp tục/xác minh cùng bản nguồn bị gián đoạn.**

Ví dụ một FULL source có ba chunks:

```text
Chunk 0: đã commit
Chunk 1: đã commit
Chunk 2: lỗi/không rõ outcome
```

Khi recovery, Bronze yêu cầu durable source checksum/layout và physical identity phù hợp. Với writer hiện tại, loader đọc lại các chunks:

- Chunk đã có và exact content/multiplicity khớp: không append lại.
- Chunk chưa có: append theo immutable identity nếu recovery được phép.
- Chunk có nhưng khác/partial: chặn, không xóa/overwrite tự động.
- `UNKNOWN_COMMIT` chưa giải quyết: chặn automatic replay để operator xác minh catalog/content trước.

Việc chỉ append phần chưa commit không có nghĩa adapter đang lấy delta của **một phiên bản nguồn mới**. Đây là recovery của cùng artifact/layout. Checkpoint không tự cho phép skip physical proof.

`force_reprocess` cũng không phải incremental: nó yêu cầu reprocess đầy đủ và có thể giữ thêm history. Local_nine DAG hiện từ chối forced reprocessing; không dùng như nút “thử lại cho nhanh”.

## 10. Silver/Gold liên quan thế nào đến chuyện “nạp lại”?

| Tầng | Phạm vi đọc / nhận diện thay đổi | Cách ghi hiện tại |
|---|---|---|
| Bronze | FULL theo cấu hình; checksum/proof cho skip; recovery cho cùng artifact | Immutable chunk append, giữ history |
| Silver | Local workflow mặc định full pinned Bronze read; framework có bộ lọc incremental riêng | Dedup + change/freshness-aware MERGE |
| Gold | Đọc chín admitted Silver snapshots; batch fingerprint theo inputs/code/refs | Exact content compare, full snapshot write nếu đổi, physical NOOP nếu giống |

Silver có `--mode incremental` để lọc Bronze run/timestamp là một cơ chế **đọc Bronze đã có**, không biến adapter Raw → Bronze thành incremental. Tên “incremental” ở hai vị trí không có nghĩa toàn pipeline đang lấy delta từ nguồn ngoài.

Silver MERGE cũng có thể chạy với đầu vào full: key mới INSERT, key cũ thay đổi đủ mới UPDATE, key không đổi không ghi. Source-specific conflict policy vẫn áp dụng.

Raw mới mất một row không tự làm Silver xóa row đó; current retention/upsert giữ absent keys. Gold chỉ phản ánh những rows thực sự có trong admitted Silver snapshot, không tự suy source withdrawal từ Raw.

Bronze SKIPPED không đảm bảo toàn DAG Silver/Gold cũng sẽ skip: các tầng còn có gates, reads, audit và logic riêng. Physical NOOP là không đổi dữ liệu vật lý, không có nghĩa không tốn thời gian tính toán/validation.

## 11. Bảng quyết định để học nhanh

| Tình huống | Bronze hiện làm gì? | Có phải incremental không? |
|---|---|---|
| Chưa có SUCCESS cùng checksum | FULL nếu input/admission hợp lệ | Không |
| Có SUCCESS cùng checksum và proof hợp lệ | SKIPPED | Không |
| File nội dung đổi | FULL bản mới nếu được chấp nhận; drift/approval có thể chặn | Không tự lấy delta |
| Run trước dở dang, cùng checksum/layout | Recovery có exact readback theo physical identity | Không |
| Run trước unknown commit | Chặn để xác minh/recovery | Không |
| Đổi YAML thành INCREMENTAL nhưng adapter không hỗ trợ | Không có incremental hoạt động; guard/lỗi chặn | Không |
| Nguồn mới có delta API + writer/watermark/recovery được triển khai và nghiệm thu | Có thể thiết kế incremental thật | Là phạm vi tương lai |

## 12. Khi thầy hỏi, em nên trả lời thế nào?

**Câu hỏi: “Nạp lần đầu full, lần sau hệ thống tự chuyển sang incremental à?”**

> “Không tự chuyển ạ. Chiến lược của chín nguồn hiện tại được cấu hình FULL vì đầu vào là các snapshot files. Khi chạy lại, hệ thống tính checksum và kiểm tra metadata cùng physical proof. Nếu cùng bản nguồn đã thành công thì skip, không nạp lặp. Nếu bản nguồn thay đổi và đủ điều kiện thì vẫn đọc FULL bản mới. Đây là FULL snapshot ingestion có idempotency, không phải incremental extraction.”

**Câu hỏi: “Vậy tối ưu ở đâu?”**

> “Bronze tránh ingest lại artifact không đổi, CSV lớn được đọc theo chunk và chunk recovery tránh append trùng. Silver dùng MERGE có change detection; Gold có exact physical NOOP. Những cơ chế này giảm ghi thừa nhưng không có nghĩa nguồn đã được đọc theo delta.”

**Câu hỏi: “Sao không chỉ lấy năm lớn hơn năm cuối?”**

> “Vì dữ liệu lịch sử có thể được sửa. Chỉ lọc năm mới sẽ bỏ sót revisions. Muốn incremental thật cần updated_at/cursor hoặc change feed có semantics đáng tin cậy, kèm xử lý late updates/deletes và commit watermark an toàn.”

**Câu hỏi: “Đã có incremental_loader.py thì đã dùng chưa?”**

> “Chưa dùng cho chín nguồn hiện tại. Nhánh đó mới có khung watermark và mock tests; adapter snapshot báo không hỗ trợ, đường ghi/readback chưa hoàn chỉnh như FullLoader. Em không coi sự tồn tại của class là bằng chứng incremental end-to-end đã được nghiệm thu.”

## 13. Các file nên mở để chỉ bằng chứng

| File / phương thức | Điều chứng minh |
|---|---|
| [source_registry.yaml](../../config/ingestion/source_registry.yaml) | Cả chín nguồn local đang FULL |
| [IngestionEngine._select_loader](../../src/ingestion/core/ingestion_engine.py) | Chọn strategy theo config, không theo số lần chạy |
| [FullLoader.execute](../../src/ingestion/core/full_loader.py) | Checksum, validation, skip, recovery và extract_full |
| [IdempotencyController.evaluate_batch](../../src/ingestion/reliability/idempotency_controller.py) | Đề xuất skip từ successful source snapshot |
| [MetadataRepository.find_snapshot_by_checksum](../../src/ingestion/storage/metadata_repository.py) | Tra lịch sử source/checksum/SUCCESS trong PostgreSQL |
| [BronzeIcebergWriter](../../src/ingestion/storage/bronze_writer.py) | Immutable chunk admission/readback và verify_run |
| [FaostatBulkAdapter](../../src/ingestion/adapters/faostat_bulk_adapter.py) | CSV chunked FULL; incremental không hỗ trợ |
| [NsoVietnamAdapter](../../src/ingestion/adapters/nso_adapter.py) | Multi-file FULL, không tự chỉ lấy file đổi |
| [IncrementalLoader](../../src/ingestion/core/incremental_loader.py) | Nhánh khung cho tương lai và các giới hạn hiện tại |
| [tests/unit/test_incremental_loader.py](../../tests/unit/test_incremental_loader.py) | Tests dùng mock adapter, không phải physical E2E |

**Câu nhớ nhanh:** “Đã nạp rồi” giúp hệ thống kiểm tra **có cần nạp lại không**; nó không tự quyết định **nguồn đã hỗ trợ lấy phần tăng thêm**.
