# B4 — kế hoạch shared NSO recovery đã duyệt

**B4 đã được duyệt có điều kiện và execute thành công ngày 10/10/2026.** [Kết quả](nso_b4_shared_recovery_results.md), [evidence tổng hợp](nso_b4_shared_validation_evidence.json): current snapshot `3499764399995403202`, 49 fields/833 rows; rerun NOOP. Nội dung dưới đây giữ baseline/kế hoạch đã duyệt để audit, không phải current snapshot trước một write mới. B4 không cấp quyền Silver/unit/province mapping.

## Phạm vi thay đổi đã duyệt — baseline trước B4

Target duy nhất: `iceberg.bronze.nso_vietnam`, approved baseline snapshot **8554544144555949741**, schema ID 0/18 nullable fields. Schema đầy đủ và ID/type từng field nằm trong `schema_before` của evidence. Thêm **31 string nullable**: `t_nh_th_nh_ph_`, `col_1995` đến `col_2023` và `so_b_2024`; 18 fields cũ giữ tên/type/ID. Output 49 fields, vẫn **833 rows**, không tăng physical row count. Khôi phục 22.500 non-null matrix cells (21.554 numeric + 946 missing markers); tất cả national/lineage values giữ nguyên.

Giữ raw ZIP, manifests, 13 checksums, source snapshots/checkpoints/runs và các Iceberg snapshots. Không cập nhật ingestion status/watermark hay `_source_snapshot_id=0`. Chỉ dùng PostgreSQL session advisory source lock, không ghi metadata row. Không tác động chín Bronze sources khác. B4 không duyệt NSO Silver contract, unit conversion hay province mapping.

ZIP hash được duyệt: `eb231503f3a1051d226100c699983c7f1b420f11e205cc1e419a90373f5d700d`; directory content hash `b4bbdad365bf03e4a1867c4ff3b73829335955047d6e6f007da74bcb5993580a`. Verify lại ngay trước write; nhiều raw runs, checksum/lineage/count/schema/snapshot drift đều phải stop và re-audit.

## Staging và transaction boundary

Chạy job trong scheduler hiện có, dùng credentials đang hoạt động; không sửa secrets. Host PostgreSQL login bằng defaults/.env bị auth failure nên không dùng host cho shared source lock. Scheduler preflight GET raw/catalog và SELECT metadata đã pass.

Scheduler runtime PyIceberg 0.12.0/PyArrow 16.1.0 được xác minh read-only; transaction recovery đã pass với cùng hai library versions trên portable network-none catalog ([evidence](nso_arrow16_isolated_evidence.json)). Đây là compatibility test, shared REST commit vẫn là bước cần duyệt.

1. Lấy `MetadataRepository.source_lock("nso_vietnam")`, fail-fast nếu ingestion/cleanup đang giữ lock.
2. Pin baseline snapshot, parse verified ZIP, tạo union payload trong memory; ghi commit-intent/before-snapshot journal vào file `/tmp` và giữ approved evidence trên host.
3. `table.transaction()` + `transaction.update_schema()` (nullable additions) + `transaction.overwrite(verified_arrow)`; commit catalog một lần. Parquet/manifest files staged trước commit, không tạo/switch shared staging table, không DELETE+append.
4. Đọc lại table, cell-level multiset/rows/lineage reconciliation, lưu snapshot after. So sánh Trino schema/counts/file grouping với evidence trước khi mở downstream.

Không có multi-table transaction; chỉ một NSO target. Library có thể tạo các snapshots nội bộ cho overwrite nhưng visibility đổi tại một catalog commit. Concurrent update được optimistic validation bảo vệ; không ép retry bỏ qua conflict. REST write chưa được nghiệm thu live.

Readers tiếp tục thấy snapshot cũ trước commit. Dự toán cửa sổ source lock **1–2 phút**, chưa benchmark shared write; isolated transaction đo dưới một giây. Tạm hoãn NSO ingestion/Silver trong cửa sổ này, không pause toàn bộ platform. Không claim zero downstream effect: queries sau commit thấy additional fields nhưng NSO Silver vẫn blocked do contract.

## Exact commands — chỉ sau approval B4

Chạy ở repository root; scheduler/container và bind mounts hiện đã kiểm chứng. Nếu runtime/credentials/table thay đổi, dừng thay vì đổi secret hoặc fallback.

```powershell
# Recheck read-only, then compare against the reviewed host evidence.
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode plan --local-dir /opt/airflow/data/raw/nso --output /tmp/nso_recovery_preflight.json

# Copy the approved immutable plan; execute validates its hashes, identities/counts and snapshot.
docker cp docs/silver/nso_recovery_evidence.json airflow-scheduler:/tmp/nso_recovery_approved_plan.json
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode execute --plan /tmp/nso_recovery_approved_plan.json --local-dir /opt/airflow/data/raw/nso --output /tmp/nso_shared_recovery_result.json
docker cp airflow-scheduler:/tmp/nso_shared_recovery_result.json docs/silver/nso_shared_recovery_result.json
```

Trino SELECT checks (dùng API/CLI đã cấu hình; không INSERT/ALTER):

```sql
DESCRIBE iceberg.bronze.nso_vietnam;
SELECT count(*) FROM iceberg.bronze.nso_vietnam;
SELECT _source_file, _source_checksum, _ingestion_run_id, _ingestion_chunk_id, count(*)
FROM iceberg.bronze.nso_vietnam GROUP BY 1,2,3,4 ORDER BY 1;
SELECT count(t_nh_th_nh_ph_), count(col_1995), count(so_b_2024)
FROM iceberg.bronze.nso_vietnam;
SELECT snapshot_id, parent_id, operation FROM iceberg.bronze."nso_vietnam$snapshots";
```

Exact per-file cell counts được tính từ raw trong evidence; job kiểm tra toàn bộ 30 measurement columns, không dùng chỉ ba SELECT aggregates để claim cell-level pass.

## Partial failures và rollback

| Failure | Hành động |
|---|---|
| Parse/checksum/schema/lineage mismatch, source lock unavailable | Stop trước staging/write; snapshot cũ giữ nguyên |
| Arrow/data-file staging thất bại hoặc trước commit | Table metadata/data cũ không đổi; giữ file/evidence để điều tra, không purge orphan files |
| Optimistic concurrent modification | Stop/re-audit; không override snapshot khác |
| REST/network timeout lúc commit | Outcome chưa biết: đọc current snapshot + `repair-id` + cell multiset trước khi retry; không blind append/rollback |
| Commit thành công nhưng Trino/cache/reconciliation lỗi | Chặn downstream; giữ snapshots, kiểm tra server visibility rồi rollback nếu data mismatch đã xác nhận |
| Retry sau commit thành công | Exact multiset → NOOP; cùng approved plan, không tạo snapshot mới |

Rollback về baseline chỉ khi current snapshot bằng `snapshot_after` trong result và không có concurrent changes. **Rollback snapshot không xóa 31 nullable schema additions**; old data projection trở về baseline, added matrix values trở thành NULL ở baseline snapshot. Giữ tất cả snapshots/raw/files. Nếu schema cũng cần trở về 18 fields, phải có review/approval khác; không DROP COLUMN tự động.

```powershell
$repairResult = Get-Content -Raw -Encoding UTF8 docs/silver/nso_shared_recovery_result.json | ConvertFrom-Json
$afterSnapshot = $repairResult.shared_recovery.snapshot_after
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode rollback --from-snapshot $afterSnapshot --to-snapshot 8554544144555949741 --output /tmp/nso_shared_rollback_result.json
docker cp airflow-scheduler:/tmp/nso_shared_rollback_result.json docs/silver/nso_shared_rollback_result.json
```

Request B4 là quyền thực thi đúng target/count/schema/lock/journal/recovery này. Không gồm shared Silver, business-policy changes, replay các source khác hay ingestion DAG trigger.
