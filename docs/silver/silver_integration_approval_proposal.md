# Đề xuất cần phê duyệt trước khi tích hợp

Ngày kiểm tra: 10/10/2026. **A đã được duyệt; B4 được duyệt có điều kiện và đã COMPLETE_AND_VERIFIED cho duy nhất NSO Bronze. C2/business policies/NSO Silver chưa được duyệt.** [B4 results](nso_b4_shared_recovery_results.md), [NSO decisions trước E](nso_phase_e_decisions.md). Nội dung B dưới đây ghi baseline/plan đã duyệt, không cấp quyền ghi các bảng khác.

## A. Contract và hành vi pipeline (chỉ sửa repository)

- Giữ `SilverTransformationFramework` của Hải làm nơi điều phối DQ, quarantine, dedup và write. Giữ class/method của Phúc; chuyển `BaseSilverTransformer.execute` vào cùng các kiểm soát qua adapter nội bộ. Không tạo framework thứ ba.
- FAOSTAT Production/Price/SUA: chạy cả source DQ và contract DQ; bổ sung SQL tương đương đúng quy tắc hiện hành vào YAML. Giữ nguyên policy âm của SUA (chỉ Stock Variation 5071 được âm).
- Price: bổ sung `time_grain`, `month` nullable, `currency`, `price_kind`; giữ annual 7021 và monthly 7001–7012, không nội suy. Giá trị index không mang currency.
- USDA Yearbook: chốt `iceberg.silver.usda_export_price` cùng schema đang dùng bởi transformer/DAG của Phúc (`exporter_country`, `rice_class`, `reference_period`, `statistic_description`, `price_fob`, `unit`), thay target/field không khớp trong YAML hiện tại. Giữ cả kỳ năm/crop-year và tháng, không coi năm USDA là calendar year nếu chưa xác minh.
- Trade/PSD/World Bank: bổ sung executable DQ đúng contract; giữ WARNING/LOG/KEEP trong báo cáo thay vì quarantine. Những thay đổi negative-to-null, lọc bỏ khóa lỗi, suy đoán currency/unit của World Bank phải thay bằng chẩn đoán và quarantine có lineage; không suy đoán mã ISO hay tỉnh.
- Domestic: sửa `source_column` YAML sang header UTF-8 thực; grain đề xuất `market × commodity × price_type × date × unit × currency` để không gộp hai báo giá khác đơn vị. Parse ngày thực tế `M/d/yyyy h:mm:ss a`; chuẩn hóa chỉ VNĐ→VND, VNĐ/Kg→VND/kg đã quan sát. Province/product master mapping vẫn pending.
- Tất cả target: giữ đủ tám metadata Bronze; thêm `_source_payload`/raw numeric và source flags cần thiết. `_source_snapshot_id` hiện bằng 0 trên dữ liệu đang lưu: không coi đây là Iceberg snapshot; lưu snapshot Bronze thực riêng khi đọc.
- Dedup đã triển khai: thứ tự timestamp → source_file → payload hash cố định; hash giữ các metadata/raw fields nhưng loại `_silver_processed_at` và read-time Iceberg snapshot. MERGE chỉ UPDATE khi có thay đổi và timestamp mới hơn; tie cùng timestamp dùng cùng thứ tự file/hash. Batch cũ không đè dữ liệu mới; rerun không đổi snapshot. Quarantine retry không lặp payload; cùng lần retry không cập nhật timestamp vô ích.
- Runner/DAG: thêm các nguồn được contract chấp thuận; NSO vẫn fail rõ khi Bronze chưa đầy đủ. Loại fixture `faostat_trade_validation` khỏi lịch production (vẫn có thể chạy test thủ công). Gate và Gold signal phải có kiểm chứng thật và không báo READY khi nguồn còn blocked.

Phạm vi A không cho phép ghi catalog/MinIO/PostgreSQL dùng chung. Kiểm thử chỉ read-only hoặc Hadoop warehouse tạm.

## B. Khôi phục Bronze NSO (phê duyệt riêng)

Target duy nhất của B4: **`iceberg.bronze.nso_vietnam`**. Baseline trước recovery 833 rows/13 CSV, schema 18 fields bị mất 31 matrix columns. Sau recovery: snapshot `3499764399995403202`, 49 fields/833 rows, 21.554 numeric + 946 missing markers; PyIceberg/Trino equality và shared NOOP PASS.

Đề xuất sau khi chứng minh archive raw/checksum: thêm các cột matrix dưới dạng string nullable (không đổi cột cũ); replay **833 rows thay thế cùng run/chunk identity**, không append thêm bản sao. Dùng raw archive đã lưu, đối chiếu SHA-256 từng CSV và lưu repair run/audit riêng. Không bịa số liệu từ các hàng NULL hiện tại.

**Sửa nhãn checksum sau B1:** SHA-256 của ZIP raw là `eb231503f3a1051d226100c699983c7f1b420f11e205cc1e419a90373f5d700d`. Giá trị `b4bbdad365bf03e4a1867c4ff3b73829335955047d6e6f007da74bcb5993580a` trong manifest là checksum thư mục CSV (filename + bytes theo thứ tự), không phải ZIP hash. Cả 13 CSV khớp file gốc. Isolated transaction phục hồi đủ 833 rows và 22.500 ô matrix non-null (21.554 numeric, 946 missing markers), giữ tám lineage fields và 13 checksum. Schema cần thêm **31 string nullable fields**: `t_nh_th_nh_ph_`, `col_1995`…`col_2023`, `so_b_2024`; tổng schema từ 18 lên 49 fields. Không tự xác định mã tỉnh hay đổi đơn vị matrix. Xem [evidence mới](nso_recovery_evidence.json) và [kế hoạch B4 cụ thể](nso_shared_recovery_plan.md).

Kế hoạch B4 dùng native PyIceberg transaction: thêm schema và overwrite 833 rows trong một catalog commit dưới source lock, đã kiểm chứng shared REST/Trino và rerun NOOP. Không fallback DELETE+append hoặc ALTER từng cột. Rollback readiness PASS, không execute rollback; rollback snapshot giữ nullable additions và history/raw. B4 không duyệt unit/province/contract hoặc shared Silver.

## C. Ghi Silver E2E cho tám nguồn READY (sau A và kết quả preview)

Targets dự kiến: `iceberg.silver.faostat_production`, `faostat_monthly_price`, `faostat_supply_utilization`, `faostat_trade`, `usda_rice_psd`, `usda_export_price`, `worldbank_commodity_monthly`, `thitruongnongsan`; NSO chỉ thêm khi B và contract/geography/unit được duyệt. Quarantine dùng `iceberg.silver.dead_letters` để giữ layout hiện tại; không tự chuyển sang `silver_system`.

Tám nguồn READY có thể được duyệt/chạy độc lập với B; dùng CLI `run_all_silver.py --dataset <source_id> --mode full --pipeline-run-id <stable_id>` cho từng nguồn đã duyệt, không dùng `--dataset all` khi NSO unresolved. Review schemas/snapshot IDs trong [preview evidence cuối](silver_live_preview_evidence.json), sau đó kiểm tra full rerun/no-op, incremental cùng Bronze run ID, quarantine và Trino counts/keys trước khi nghiệm thu. Đây là command plan cho C, chưa được thực thi.

Mission finalization đã chạy lại preview, inventory, DQ và isolated idempotency; counts giữ nguyên. Lệnh canary, từng nguồn, namespace, rollback và phạm vi Airflow nằm trong [kế hoạch shared Silver](silver_shared_execution_plan.md). [Business review](silver_business_quality_review.md) tách SUA/PSD/NSO mapping/unit khỏi quyền ghi C.

Catalog hiện chưa có namespace Silver: chưa có bảng để rollback. Phải chốt output/quarantine count theo preview ngay trước run, xin phê duyệt tạo namespace/tables; ghi run ID, snapshot before/after và Trino counts. Với bảng đã tồn tại lúc chạy: không tự evolve schema hoặc overwrite; dừng và trình diff schema/rowcount, snapshot rollback riêng. Đề xuất này **không tự cấp quyền chạy C**.

Số liệu read-only sau A (10/10): Production 22.738; Price 15.334; SUA 36.144; Trade 1.226.470; PSD 924; Yearbook 12.936; World Bank 56.232; Domestic 20.383 output rows. Quarantine dự kiến **302** (236 SUA + 66 PSD Milling Rate). World Bank loại 13.464 candidate cells thuộc 17 cột Excel padding trống, giữ raw payload; Yearbook dedup 195, Domestic dedup 11. Toàn bộ output còn warning do `_source_snapshot_id=0`; actual Iceberg snapshot lưu riêng.

Recovery nếu C thất bại: dừng nguồn và không phát tín hiệu Gold, giữ mọi table/file để điều tra. Với bảng có snapshot trước run, rollback về snapshot đã ghi; với bảng mới, giữ bảng ở trạng thái chưa được downstream phê duyệt, không xóa bảng. B và contract NSO chưa duyệt nên không ghi NSO Silver; full DAG vẫn dừng ở gate thay vì báo complete sai.

## D. Residuals SUA (quyết định nghiệp vụ riêng)

236 giá trị âm thuộc element 5166 đang bị source DQ quarantine. Giữ nguyên trong lần sửa an toàn. Có thể cân nhắc giữ signed residuals có quality flag sau khi xác nhận ý nghĩa FAO và yêu cầu Gold; không lấy absolute value, không biến thành NULL hay đổi rule lặng lẽ.
