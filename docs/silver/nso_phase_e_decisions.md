# NSO — quyết định còn thiếu trước Phase E

B4 đã phục hồi và kiểm chứng shared Bronze ([kết quả](nso_b4_shared_recovery_results.md)): snapshot `3499764399995403202`, 833 rows/49 fields. **Chưa bắt đầu normalization hoặc publish NSO Silver.** Contract YAML vẫn `NEEDS_PROFILING`; B4 không phê duyệt business semantics hay shared Silver.

Profile mới đọc trực tiếp snapshot đã phục hồi: [evidence](nso_b4_file_and_schema_evidence.json). Có 762 matrix rows, 71 raw geography labels riêng biệt, 22.500 non-null matrix cells (21.554 numeric, 946 `..`), 360 matrix cells NULL. V06.12 có 35 quantity rows + 35 development-index rows + **1 row có toàn bộ source fields NULL**; 560 non-null national measure cells. Giữ nguyên row NULL đó ở Bronze; không tự coi là observation hợp lệ hoặc tự xóa trong B4. `so_b_2024` có 750 matrix cells non-null.

| Quyết định cần duyệt | Phạm vi / evidence | Đề xuất để review; chưa áp dụng |
|---|---|---|
| Contract/targets/schema | `bronze_input`, `silver_output`, grain, keys, DQ, dedup, quarantine, incremental, lineage, Gold dependency hiện UNKNOWN | Chốt source table × source geography identity/level × year × season × measure × statistic kind; giữ raw value/unit/labels và eight lineage fields. Cần tên target, schema/types và schema quarantine cụ thể trước khi chuyển READY. |
| Province/region/historical master | 71 raw labels/762 matrix rows, gồm national, vùng và tỉnh; nhãn như `Hà N?i`, `C? NU?C` đã nằm trong source, không phải lỗi B4 | Lookup **exact** có source references, geography level, parent và effective dates theo năm; không fuzzy-repair hoặc áp master hiện tại cho toàn bộ 1995–2024. Duyệt cách xử lý unmatched: quarantine/needs_review, hoặc approved exclusion phạm vi rõ ràng. Không đoán mã tỉnh. |
| Unit theo source table/measure/statistic | 12 matrix tables không mang unit trong header; niên giám chỉ corroborate examples, chưa là contract approval toàn bộ series | Duyệt bảng dưới đây: area nghìn ha → ha ×1000; yield tạ/ha → kg/ha ×100; production nghìn tấn → tấn ×1000. Giữ `source_unit` và factor. National quantity dùng unit header; development index giữ percent, factor 1, không nhân như quantity. |
| Crop-season equivalence và aggregation | V19–21 ghi **hè thu và thu đông**; national V12 header chỉ hè thu | Giữ `summer_autumn_autumn_winter` riêng `summer_autumn`; không coi hai series tương đương. Giữ annual/winter_spring/mùa; không cộng annual với seasons hoặc national/region/province. Cần rule tránh cộng national V12 với national rows trong matrices. |
| Statistic/year/provisional | 35 national quantity + 35 index rows; `So b? 2024` và `so_b_2024`, gồm 750 matrix non-null cells | Duyệt exact raw-label mappings → quantity/development_index, year 2024 và `is_provisional`; giữ source label. Index là so với năm trước=100, không phải quantity hoặc cùng grain với quantity. |
| Missing/null/non-observation | 946 token `..`, 360 matrix NULL cells, 1 all-source-NULL V12 row | `..` → value NULL + missing flag/raw token, không numeric 0; duyệt giữ missing observations hay exclusion có reconciliation riêng. Phân biệt NULL cells thật và columns không thuộc source. Duyệt exclude/footer rule cho all-source-NULL row (8 synthetic national candidates nếu unpivot bừa) hoặc quarantine; không tính vào valid observations. |
| DQ/dedup/survivorship và lineage reference | Khôi phục raw cells không chứng minh province/unit validity; `_source_snapshot_id=0` giữ nguyên trên 833 rows | Duyệt missing/negative/unknown geography/unit/year/provisional rules, severity/actions, business key và xử lý duplicate/conflicts. Giữ original run/file/checksum, lưu actual input Iceberg snapshot riêng. Không dùng Iceberg snapshot ID thay PostgreSQL source_snapshot FK; sửa liên kết 0 cần scope/approval riêng nếu muốn. |

## Bảng unit và season đề xuất cần ký duyệt

| Source table | Measure | Season giữ riêng | Source unit đề xuất | Canonical unit / factor |
|---|---|---|---|---|
| V06.13 | area | annual | 1000 ha | ha ×1000 |
| V06.14 | yield | annual | quintal/ha (tạ/ha) | kg/ha ×100 |
| V06.15 | production | annual | 1000 t | t ×1000 |
| V06.16 | area | winter_spring | 1000 ha | ha ×1000 |
| V06.17 | yield | winter_spring | quintal/ha | kg/ha ×100 |
| V06.18 | production | winter_spring | 1000 t | t ×1000 |
| V06.19 | area | summer_autumn_autumn_winter | 1000 ha | ha ×1000 |
| V06.20 | yield | summer_autumn_autumn_winter | quintal/ha | kg/ha ×100 |
| V06.21 | production | summer_autumn_autumn_winter | 1000 t | t ×1000 |
| V06.22 | area | mua | 1000 ha | ha ×1000 |
| V06.23 | yield | mua | quintal/ha | kg/ha ×100 |
| V06.24 | production | mua | 1000 t | t ×1000 |

Source definitions/official-yearbook corroboration và các giới hạn đã ghi trong [business review](silver_business_quality_review.md). Bảng này là **đề xuất**, chưa gán units hoặc province codes vào Silver.

## Gate trước execution E

1. Người dùng duyệt NSO Data Contract và bảng unit/season, exact historical geography lookup cùng missing/footer/DQ/dedup rules. Chọn rõ phạm vi giữ/quarantine/exclusion nếu mappings chưa đủ.
2. Thực hiện normalization và isolated E2E, đối soát national/matrix observations, 946 markers/360 NULL cells/footer và source lineage riêng; không hardcode output hợp lệ từ raw counts.
3. Trình plan và xin **approval shared NSO Silver riêng** trước writer. Approval B4 không thay thế gate này hoặc approval C2 cho tám nguồn khác.

Hiện chưa đổi YAML, mapping/unit/DQ policies, không unpause DAG, không trigger Silver writer và không sửa Gold implementation.


## E1 implementation v? proposal m?i

[S?u t?i li?u E1 v? k?t qu? ki?m th?](nso_silver_isolated_test_results.md) li?n k?t contract, unit/season, historical geography, DQ v? shared write plan. Inventory ??y ?? c? 71 raw labels v? 750 source-table/label pairs; runtime approved historical mapping hi?n r?ng, kh?ng c? province code t? g?n. Matrix unit applicability v?n NEEDS_APPROVAL.

Profile E1 ph?t hi?n **m?i CSV trong 13 source ??u c? m?t all-source-NULL record**: 12 matrix rows ch?a to?n b? 360 NULL cells v? 1 national row t??ng ?ng 8 candidate slots. Proposal audit 13 records, exclude 368 candidate slots tr??c unpivot, gi? 946 markers v? m?i actual NULL trong nonempty row; kh?ng x?a Bronze. Expected candidates 23.428, eligible 23.060, conservative preview valid 560 / quarantine 22.500 / excluded 368. ??y l? counts c?a draft policy, kh?ng ph?i ph? duy?t shared write ho?c Gold readiness.

Framework c? gate ch?n writer khi contract ch?a READY ho?c c? critical blockers; preview ???c opt-in read-only. C?n duy?t exact series units, historical boundaries/codes, missing/footer/DQ/provisional/index policy, target/grain/schema v? plan shared Silver ri?ng.
