# B4 — kết quả Shared NSO Bronze Recovery

**B4_COMPLETE_AND_VERIFIED.** Thực thi theo phê duyệt có điều kiện của người dùng ngày 10/10/2026, duy nhất `iceberg.bronze.nso_vietnam`. Overall Silver vẫn **PARTIALLY_COMPLETE**, live coverage **0/9**; chưa chạy Phase C2/E hoặc Silver writer.

Evidence tổng hợp: [JSON](nso_b4_shared_validation_evidence.json). Approved B3 evidence được giữ nguyên tại [plan JSON](nso_recovery_evidence.json); không đổi snapshot baseline trong plan để hợp thức hóa write.

| Kiểm chứng | Kết quả thực tế |
|---|---|
| Preflight | PASS: snapshot, schema, ZIP SHA-256, 13 checksum/row counts, toàn bộ tám lineage fields và manifest khớp approved plan |
| Snapshot before | `8554544144555949741` |
| Snapshot after | `3499764399995403202` |
| Schema | ID 0/18 fields → ID 1/49 fields; giữ ID/type của 18 fields cũ, thêm 31 nullable strings |
| Physical rows / source files | 833 / 13; không tăng số dòng |
| Recovered matrix cells | 22.500 non-null = **21.554 numeric + 946 `..` missing markers** |
| PyIceberg và Trino | Full-row/cell multiset equality PASS, cả 49 fields; DESCRIBE types PASS; 18-field baseline projection giữ nguyên tuyệt đối |
| Source lock | Đã lấy PostgreSQL session advisory NSO lock; kiểm tra một granted lock trong execution; đã nhả sau khi hoàn tất |
| Runtime state | Không active ingestion/replay/tasks; hai DAG giữ paused; không trigger task/Silver |
| Rerun | **NOOP**, snapshot before/after đều `3499764399995403202`; metadata location và toàn bộ catalog inventory không đổi |
| Files mới | 1 Parquet, 833 rows, 104.403 bytes; tổng 6 objects gồm data, manifests, manifest lists và metadata JSON |
| Rollback readiness | Snapshot baseline và metadata còn tồn tại; HEAD xác nhận đủ 13 Parquet baseline, kích thước không đổi; không rollback |

ZIP SHA-256 vẫn `eb231503f3a1051d226100c699983c7f1b420f11e205cc1e419a90373f5d700d`. Directory checksum vẫn `b4bbdad365bf03e4a1867c4ff3b73829335955047d6e6f007da74bcb5993580a`. Raw ZIP/manifest object inventory và metadata ingestion được fingerprint trước/sau: 20 ingestion runs, 10 source snapshots, watermarks/checkpoints/dead letters đều không đổi. Chín bảng Bronze khác (tám nguồn nghiệp vụ và một fixture) không đổi schema/snapshot/history. Namespace Silver chưa tồn tại.

## Transaction và các files

Native `table.transaction()` thêm schema và overwrite payload, commit REST Catalog một lần. PyIceberg ghi hai snapshots nội bộ trong transaction: `2995248014055073657` (logical delete) rồi `3499764399995403202` (append replacement). Đây là staging của overwrite đã duyệt; không phải blind append hoặc hai catalog commits. Current snapshot có đủ 833 rows. Snapshot history tăng **14→16**, giữ nguyên 14 snapshots cũ. Logical delete không xóa vật lý 13 files baseline.

Repair identity: `nso-eb231503f3a1051d`. Danh sách đủ sáu objects/ETags/bytes nằm trong [file/schema evidence](nso_b4_file_and_schema_evidence.json); Parquet mới:

`s3://warehouse/bronze/nso_vietnam-7fe07be6a54e43059e5fc487c8e48749/data/00000-0-39a4103f-2e5c-46cb-bd71-bbf55f4fc680.parquet`

Commit không timeout, không có partial commit hoặc post-commit data mismatch. Không tự retry write hay rollback. Nếu cần rollback sau này: xác nhận current snapshot vẫn đúng snapshot after, ngăn concurrent NSO operations và xin quyết định người dùng; dùng procedure trong [approved B4 plan](nso_shared_recovery_plan.md). Rollback snapshot giữ 31 nullable schema additions; không DROP fields/files hoặc expire snapshots.

## Preflight guards và kiểm thử

Lượt audit ban đầu dừng trước write vì native schema dùng tuple còn JSON dùng list. Chuẩn hóa `schema.model_dump(mode="json")` giải quyết khác biệt biểu diễn; không có schema drift thật. Trino REST client mặc định làm tròn timestamp hiển thị xuống milliseconds; server-side `CAST(_ingestion_timestamp AS varchar)` xác nhận đủ microseconds và dùng cho full-lineage comparison. Không sửa table timestamps, Trino settings hoặc secrets.

Sau sửa, preflight strict PASS; guard checks từ chối snapshot/checksum/lineage timestamp/type drift và active tasks. **17 schema/native recovery regression tests pass** ([log](nso_b4_regression.log)); tests chỉ dùng SQLite/filesystem isolated và dummy REST URI. Các helper read-only để lập file/NOOP evidence đã sửa import/null-period handling; không ảnh hưởng shared transaction. Final checks đều PASS.

## Evidence và commands

- [Preflight](nso_b4_preflight_evidence.json), [so sánh approved plan](nso_b4_preflight_comparison.json), [baseline audit](nso_b4_before_audit.json).
- [Commit result](nso_shared_recovery_result.json), [post-commit audit](nso_b4_after_audit.json), [execute log](nso_b4_execute.log), [post-commit log](nso_b4_post_commit.log).
- [NOOP result](nso_b4_noop_result.json), [final NOOP checks](nso_b4_noop_validation.json), [NOOP log](nso_b4_noop.log), [final check log](nso_b4_noop_validation.log).
- [Physical files/schema/profile](nso_b4_file_and_schema_evidence.json), [file/schema log](nso_b4_file_and_schema.log).

Các commands đã chạy trong scheduler:

```powershell
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode plan --local-dir /opt/airflow/data/raw/nso --output /tmp/nso_b4_preflight.json
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/audit_nso_b4.py --phase before --plan /tmp/nso_recovery_approved_plan.json --output /tmp/nso_b4_before_audit.json
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode execute --plan /tmp/nso_recovery_approved_plan.json --local-dir /opt/airflow/data/raw/nso --output /tmp/nso_shared_recovery_result.json
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/audit_nso_b4.py --phase after --plan /tmp/nso_recovery_approved_plan.json --before /tmp/nso_b4_before_audit.json --result /tmp/nso_shared_recovery_result.json --output /tmp/nso_b4_after_audit.json
# Approved rerun, returned NOOP:
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/recover_nso.py --mode execute --plan /tmp/nso_recovery_approved_plan.json --local-dir /opt/airflow/data/raw/nso --output /tmp/nso_b4_noop_result.json
docker exec -w /opt/airflow airflow-scheduler python jobs/bronze/verify_nso_b4_files.py --plan /tmp/nso_recovery_approved_plan.json --before /tmp/nso_b4_before_audit.json --result /tmp/nso_shared_recovery_result.json --output /tmp/nso_b4_file_and_schema_evidence.json
```

Không có NSO Silver contract/unit/province approval trong B4. Các quyết định cụ thể trước E nằm tại [NSO Phase E decisions](nso_phase_e_decisions.md).
