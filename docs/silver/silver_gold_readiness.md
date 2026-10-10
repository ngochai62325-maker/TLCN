# Gold readiness checklist

**Chưa sẵn sàng bắt đầu Gold production.** Repo integration/preview không thay thế live Silver outputs và business mapping review. Không sửa/triển khai Gold trong mission này.

Finalization: **B4 NSO Bronze đã execute và verified** ([report](nso_b4_shared_recovery_results.md)): 833 rows/49 fields, shared NOOP, Trino và raw/lineage/snapshot preservation PASS. NSO contract/geography/unit/season mapping vẫn chưa được duyệt ([E decisions](nso_phase_e_decisions.md)); live Silver vẫn 0/9. C2 validation fixtures chỉ chạy sau approval, không phải business outputs hoặc Gold input. Đây là cập nhật trạng thái báo cáo; Gold implementation/policies không đổi.

| Nhu cầu Gold | Silver hiện xử lý | Phần còn thiếu |
|---|---|---|
| Country/product identity | M49/CPC source codes và names; rice/milled rice tách riêng | Approved dim_country/product, historical countries/aggregates, không suy đoán ISO |
| Calendar month/year | Price annual/monthly/index và WB monthly giữ rõ | Gold phải lọc đúng time_grain; không gộp annual với monthly |
| Crop/marketing year | PSD giữ YYYY/YYYY+1; Yearbook giữ reference_period/table_name | Xác nhận calendar/MARKETING semantics theo source table; không tự dựng calendar date |
| Province/region/history | Domestic giữ market labels; NSO Bronze đã phục hồi raw labels | Reviewed province/region master/effective dates và approved NSO contract |
| Production/area/yield | Source codes, units/conversions và flags được giữ | Live write + Trino validation, thống nhất rice-equivalent policy ngoài Silver |
| Supply/utilization | 12 long elements, Stock Variation có dấu | Quyết định 236 negative Residuals, accounting consequences |
| Trade | Reporter/partner, direction, quantity/value, aggregate flags | Tránh đếm country totals cộng bilateral; review unknown historical mappings |
| Producer/export/domestic prices | Separate datasets; Price currencies; FOB unit; Domestic daily grain | Xác định comparable series/quality, currency conversion thuộc downstream |
| PSD units | Mass/area/yield units giữ nguyên | 66 Milling Rate definition/unit/scaling chưa approved |
| WB commodity series | 71 named series; known price/index units; blank padding loại có evidence | Business selection rice series versus macro indices, source metadata descriptions |
| Quality flags/provenance | Raw payload, eight technical fields, warnings, actual pinned snapshot | `_source_snapshot_id=0` chưa liên kết ingestion source snapshots |
| Uniqueness/corrections | Deterministic ties, stale guard, no-op actual isolated tests | Review conflicting same-timestamp duplicates; shared concurrency/failure checks |
| Persistent Silver/quarantine | Code và isolated tests thực thi | Approval C, live write và Trino queries/counts/quarantine validation |
| Orchestration readiness | 9 task registrations; thực gate/audit, fixture exclusion | Spark service đang stopped; NSO gate intentionally blocks complete batch |

Điều kiện mở Gold: Bronze NSO đầy đủ; contract/geography/unit/policy được duyệt; tất cả business outputs được ghi và đọc qua Trino; counts/schema/keys/DQ/quarantine/lineage được đối chiếu theo từng run/snapshot; không dùng fixture; không có tín hiệu READY giả. Pipeline chỉ phát `E2E_VALIDATED` sau query thực và giữ `gold_status=REQUIRES_MAPPING_REVIEW`.
