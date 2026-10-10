# Blockers còn lại

Overall: **PARTIALLY_COMPLETE**. Phần A đã được duyệt; 8 sources integrated/previewed và tested isolated; live business coverage 0/9. Công việc độc lập an toàn đã thực hiện, không dừng ở kế hoạch.

B1/B2/B3 và **B4 shared NSO recovery đã hoàn thành** ([report](nso_b4_shared_recovery_results.md)): 833 rows/49 fields, 21.554 numeric + 946 missing-marker cells, shared PyIceberg/Trino equality và NOOP pass. Current snapshot `3499764399995403202`; metadata/raw/history và các bảng khác giữ nguyên. Còn approval **C2** theo [Silver shared plan](silver_shared_execution_plan.md), và [NSO contract/unit/province decisions](nso_phase_e_decisions.md) trước E. B4 không cấp quyền C2/NSO Silver/business policies.

| Blocker | Evidence/ảnh hưởng | Bước cần thực hiện |
|---|---|---|
| NSO contract/unit/geography chưa sẵn sàng | Bronze matrix loss đã giải quyết; contract vẫn UNKNOWN/NEEDS_PROFILING | Duyệt grain/schema/targets/DQ và unit-season table; exact historical province lookup, missing/footer/index rules theo NSO E decisions |
| Silver live chưa tồn tại | REST chỉ namespace bronze; Trino không thể query outputs chưa được ghi | Approval C: tạo namespace/8 target tables + quarantine và chạy counts/schema/keys/retry/Trino validation; NSO chỉ sau B + contract |
| Spark service stopped | Compose ps không có spark-iceberg running | Sau approval execution: start service với contracts mount; kiểm tra API health, không tự trigger ingestion |
| Source snapshot references 0 | PostgreSQL có 10 snapshots nhưng Bronze `_source_snapshot_id=0` | Approved lineage repair/cross-reference; actual Iceberg snapshot đã lưu riêng; không tự đổi Bronze metadata |
| SUA policy | 236 Residuals âm, 12-element balance bị thiếu thành phần nếu Gold chỉ đọc valid rows | Business decision D; giữ quarantine hiện tại |
| PSD rate semantics | Source HTML đã ghi Milling Rate (.9999) kèm 1000 MT, values 6250…6600 | Xác nhận dimensionless scale/unit; hiện quarantine 66, không tự chia 10.000 |
| Master mappings/duplicate interpretation | Country lịch sử, province labels, 195 Yearbook/11 Domestic duplicate removals | Duyệt mapping và review conflicting values; hash deterministic không thay business truth |

## Checkpoint cụ thể

Xem [B4 đã kiểm chứng](nso_b4_shared_recovery_results.md). Recovery duy nhất NSO Bronze đi từ snapshot `8554544144555949741` sang `3499764399995403202`, 18→49 fields, giữ 833 rows/13 checksums/lineage. Source lock, atomic transaction, shared NOOP, Trino và rollback readiness đều PASS; không thực thi rollback hoặc xóa/expire lịch sử.

C targets: `iceberg.silver.faostat_production`, `faostat_monthly_price`, `faostat_supply_utilization`, `faostat_trade`, `usda_rice_psd`, `usda_export_price`, `worldbank_commodity_monthly`, `thitruongnongsan`, và `iceberg.silver.dead_letters`. Expected output 1.391.161 + quarantine 302 theo preview; exact schemas/snapshot IDs trong JSON evidence. Chưa có shared Silver namespace nên C bao gồm tạo namespace/tables; nếu catalog khác lúc chạy, dừng để review schema/row diff. Recovery giữ bảng mới ở trạng thái unpublished hoặc rollback bảng cũ về recorded snapshot, không xóa table/files.

B4 được phê duyệt riêng và đã thực thi đúng scope. Chưa execute shared Silver, task/DAG trigger hoặc metadata-status updates; hai DAG vẫn paused. C2, NSO contract/mapping/unit decisions và shared NSO Silver approval còn độc lập. Không gọi overall Silver COMPLETE_AND_E2E_VERIFIED khi các gates này chưa qua kiểm chứng.
