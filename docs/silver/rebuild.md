# Local rebuild và recovery

Quy trình hiện tại: [local_runbook.md](local_runbook.md). Dùng một project name
mới và bộ Raw đã pin để tạo storage độc lập. Không cần reset stack đang có.

- Fresh Raw → Bronze → Silver: startup mới theo runbook; trigger chỉ Bronze.
- Silver-only replay: common runner với source proof/pinned snapshot và namespace
  local rõ ràng; không tự tạo Silver DagRun trái origin gate.
- Interrupted execution: giữ scheduler stopped, xác minh actual commits trước
  clear/retry; preserve tasks SUCCESS và exact existing run IDs.
- NSO legacy repair: lịch sử, không phải bước mặc định của môi trường mới.

Không dùng kỳ vọng NSO legacy41-column/904-row. NSO V2 local có23.060
observations từ13 CSV, production DRAFT. Current nine-source acceptance và
NOOP nằm trong [validation](validation.md), [evidence index](evidence/index.md).

Runbook/timeline cũ được bảo toàn byte-for-byte trong
[documentation archive](archive/manifest.json). Active clean-room overlay vẫn ở
[evidence path gốc](evidence/nso_v2/phase3_cleanroom_20261016_154548/compose.cleanroom.yml).
Không xóa/move file đó hoặc apply cấu hình laptop mới vào containers đã nghiệm thu.
