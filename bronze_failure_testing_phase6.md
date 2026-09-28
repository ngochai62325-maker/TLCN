# BRONZE FAILURE TESTING & RECOVERY VALIDATION (PHASE 6)

## 1. Objective
Kiểm chứng khả năng failure recovery và fault tolerance của Bronze Ingestion architecture (vốn đã được thiết lập từ Phase 1 đến Phase 5). Đảm bảo pipeline phản ứng chính xác, không ghi trùng (duplicate), không sai watermark, recover từ chunk failures an toàn và báo lỗi FAILED/SKIP chuẩn xác cho Airflow. 

## 2. Scenarios Tested
- **Scenario A: Extraction Failure:** Adapter thất bại trước khi viết Chunk.
- **Scenario B & E: Chunk Write Failure & Recovery:** Viết Chunk 1, 2 thành công nhưng Chunk 3 thất bại -> Resume retry bỏ qua 1, 2 và tiếp tục ghi từ Chunk 3.
- **Scenario C: Post-write Failure:** Chạy thành công extraction/write nhưng fail metadata/upload -> Kích hoạt Orphan Cleanup bằng API đã thiết kế ở Phase 3.
- **Scenario D: Watermark Safety:** Incremental extraction bị fail giữa chừng -> Watermark Store *tuyệt đối không tịnh tiến*, bảo vệ tính toàn vẹn của batch lần sau.
- **Scenario F: Idempotent Retry:** Writer Iceberg chạy DELETE WHERE theo `(run_id, chunk_id)` rồi mới APPEND.
- **Scenario G: Advisory Lock Failure:** Fail-fast contention khi cùng chạy đồng thời 2 task chung 1 source (Đã test ở Phase 4 `test_advisory_lock.py`).
- **Scenario H & I & J: Readiness Failure, Exception Propagation, Airflow Retry Semantics:** Lỗi Engine trả về `FAILED` hoặc `NOT_READY` -> task wrapper quăng `AirflowException` / `AirflowSkipException` đẩy Retry logic hoàn toàn về phía Orchestrator.

## 3. Implementation Changes (Fixes Applied)
1. **Fix `test_pipeline_tasks.py` Imports:** Fix module `ImportError` do sử dụng legacy functions đã bị xoá ở Phase 5. Đã refactor `test_pipeline_tasks.py` để mock `IngestionEngine` đúng chuẩn và trigger `AirflowException`. 
2. **Fix `test_data_contract_test` in `test_bronze_identity.py`:** Add mock handler `writer.execute_query = MagicMock(return_value=([], []))` cho `DELETE FROM` query ở bước pre-append. Lỗi này là test infrastructure issue (do không có live Trino để bắt request HTTP), hoàn toàn không phải do regression của business logic. Fix này khiến toàn bộ Regression Suite về màu xanh (100% Pass).

## 4. Test Results
- **Targeted Unit Tests (Phase 6 Scenarios):** `tests/unit/test_phase6_failure_recovery.py` - PASS
- **Targeted Unit Tests (Phase 5 Wrapper):** `tests/unit/test_pipeline_tasks.py` - PASS
- **Regression Suite:** 210 passed.
- **Integration Tests:** BLOCKED — Missing live Trino/Iceberg REST Catalog/MinIO/PostgreSQL infrastructure to perform true E2E execution tests. All semantics proved strictly via injected mocks checking internal state.

## 5. Remaining Technical Debt
- **Strict SQL Post-Write Validation:** Ở Phase 5, hàm legacy `post_audit` đã bị tháo. Pipeline hiện tự tin tưởng row count trả về từ method `writer.write_chunk()` của PyIceberg. Việc query ngược lại Trino bằng `SELECT count(*)` đã bị gỡ. Nếu có yêu cầu Enterprise Audit 2-chiều, có thể add một Optional Validation Step vào `IngestionEngine.run()`.

## 6. Architecture Impact
Phase 6 KHÔNG thay đổi Architecture. Nó cung cấp safety net vững chắc xác nhận Architecture của Phase 1-5 đáp ứng đúng Semantic và Data Quality guarantees.

## 7. Conclusion
**READY FOR V6 FINAL AUDIT**
