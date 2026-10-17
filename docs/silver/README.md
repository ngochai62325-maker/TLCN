# Silver — TLCN Rice Lakehouse chạy local

Profile hiện tại: **9 nguồn** dưới `iceberg.silver_local`. Bộ Raw đã pin từng
được nghiệm thu **1.414.527 observations** trong clean-room; initial
Airflow/PyIceberg/Trino và full/incremental physical NOOP PASS trên đủ19 bảng.
Runtime demo mới `tlcn-local` đã smoke PASS nhưng **chưa chạy pipeline**.
Xem bằng chứng lịch sử và kết quả runtime riêng biệt tại
[evidence index](evidence/index.md). Không coi equality row count là NOOP.

Tám nguồn V2 giữ release CLOSED_MANUAL_ONLY. NSO production contract vẫn **DRAFT**;
profile `local_nine` chỉ dùng READY overlay trong bộ nhớ. Không Gold, cron hay
production cutover. Initial và replay đã có interruption/targeted recovery được
lưu minh bạch; đây không phải một lần chạy liên tục không lỗi/retry.

## Đọc và chạy project

Trình diễn với một local stack: [Demo guide](demo_guide.md), Airflow tại
`http://localhost:8085`. Runtime cũ đã cleanup theo approval; demo mới giữ hai DAG
paused/manual-only để owner tự trigger. Chạy `scripts/local_demo/start.ps1 -CheckOnly` để
kiểm tra cấu hình/Raw mà không thay runtime.

1. [Local runbook](local_runbook.md): laptop mới, Raw bundle, build images, Compose,
   Airflow UI/CLI, Trino, RAM và shutdown/recovery an toàn.
2. [Kiến trúc](architecture.md): orchestration, catalog/storage, Spark, DQ và lineage.
3. [Data catalog](data_catalog.md): chín source/targets, counts, keys, units và schema.
4. [NSO V2](nso_v2.md): matrix normalization, geography, seasons, missing và exclusions.
5. [Validation](validation.md): actual run IDs, full/incremental NOOP và tests.
6. [Evidence index](evidence/index.md): các bằng chứng cần để đối chiếu nghiệm thu.

Bộ21 Raw files được cung cấp riêng theo
[manifest](../../config/local_raw_manifest.json), kèm `nso/metadata.txt`.
Không giả định Git chứa full Trade CSV. Dockerfiles và dependency locks phục vụ
build local; không cần cloud, published private images hoặc volumes của máy cũ.

Các tài liệu lịch sử được giữ ngoài navigation chính; bản trước consolidation
được lưu trong [archive](archive/manifest.json). Không dùng lệnh reset/repair
legacy làm quy trình mặc định của fresh laptop.
