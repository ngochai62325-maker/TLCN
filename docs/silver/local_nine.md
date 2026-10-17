# local_nine profile

Nguồn/thứ tự/target được khai báo tường minh trong
[src/silver/local_profile.py](../../src/silver/local_profile.py).
Không tự lấy tất cả registry entries enabled: fixture và NSO legacy Silver bị loại.

Namespace là `iceberg.silver_local`; dead_letters cùng namespace. Tám contracts
READY giữ nguyên business semantics, NSO DRAFT dùng exact-hash in-memory overlay.
Hai DAG manual-only, max_active_tasks/max_active_runs=1 và retries=0.
Bronze batch proof pin snapshot thật, child deterministic, không reset khi trùng.

[Kiến trúc/workflow](architecture.md), [mapping/schema](data_catalog.md),
[local runbook](local_runbook.md), [validation](validation.md).

Các trạng thái Phase2 BLOCKED trước đây đã được thay thế bởi actual accepted
runs, nhưng evidence gốc vẫn giữ nguyên tại [index](evidence/index.md).
Bản đầy đủ trước consolidation: [archive manifest](archive/manifest.json).
