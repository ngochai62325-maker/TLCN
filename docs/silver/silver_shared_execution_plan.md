# C2 — shared Silver execution plan cần duyệt riêng

Ngày 10/10/2026. **Plan chưa execute.** C1 đã chạy lại [live preview](silver_finalization_preview_evidence.json), [inventory](bronze_finalization_inventory_evidence.json), [source profiles](bronze_finalization_profile_evidence.json), isolated full/incremental và [runtime/DagBag](silver_finalization_runtime_evidence.json). REST hiện chỉ namespace Bronze; live Silver coverage **0/9**. B4 và C2 có thể được duyệt độc lập; không chạy NSO hoặc `--dataset all` để né gate.

## Exact targets/counts

Quyền C2 bao gồm tạo namespace `iceberg.silver`, tám bảng dưới đây và quarantine `iceberg.silver.dead_letters`. Kế hoạch còn đề xuất hai validation tables riêng `iceberg.silver_validation.reliability_controls` và `iceberg.silver_validation.reliability_dead_letters`, mỗi bảng một fixture row sau test. Không đổi Bronze hoặc business policies. Counts là kết quả preview hiện tại, không phải assertions hardcode trong source tests.

| Canonical CLI/Bronze source | Silver target | Output rows | Quarantine candidates | Pinned Bronze snapshot |
|---|---|---:|---:|---|
| faostat_production | iceberg.silver.faostat_production | 22.738 | 0 | 3250087836768729857 |
| faostat_monthly_price | iceberg.silver.faostat_monthly_price | 15.334 | 0 | 7180512843532517583 |
| faostat_supply_utilization | iceberg.silver.faostat_supply_utilization | 36.144 | 236 | 5236669643868971539 |
| faostat_trade | iceberg.silver.faostat_trade | 1.226.470 | 0 | 5969648367205304379 |
| usda_psd | iceberg.silver.usda_rice_psd | 924 | 66 | 794008497907757228 |
| usda_rice_yearbook | iceberg.silver.usda_export_price | 12.936 | 0 | 5981613253177296156 |
| worldbank_pinksheet | iceberg.silver.worldbank_commodity_monthly | 56.232 | 0 | 1491155964568568519 |
| thitruongnongsan | iceberg.silver.thitruongnongsan | 20.383 | 0 | 8924386602570103506 |

Tổng 1.391.161 valid output + 302 quarantine candidates; blank WB padding 13.464 excluded, Yearbook dedup 195, Domestic dedup 11. Output schemas chính xác có trong preview JSON (cả raw payload/diagnostics/eight metadata + read snapshot). `usda_rice_psd` là target name, canonical source/CLI là `usda_psd`.

## Preflight và execution boundary

MinIO/REST read đã thành công bằng temporary Spark preview; credentials sẵn có trong runtime, không in/copy secrets. Spark REST/S3 endpoint config và contracts mount có trong Compose. Service `spark-iceberg` hiện chưa chạy; C2 gồm quyền start service với mount hiện tại, không recreate services khác. Host PostgreSQL defaults không login; advisory lock/metadata processing dùng scheduler đã SELECT thành công. Trino SELECT Bronze thành công dù healthcheck unhealthy.

Trước write phải recheck namespace/tables/snapshot IDs. Nếu target đã xuất hiện hoặc schema/count thay đổi, stop trình diff; không tự evolve/overwrite. Nếu Bronze snapshot đổi, rerun preview và review counts mới. Source jobs pin snapshot khi đọc; không giả `_source_snapshot_id=0` là snapshot thực.

Read-only [Airflow state evidence](silver_finalization_dag_state_evidence.json) hiện cho thấy cả hai DAG paused, không có queued/running/retry/scheduled/deferred tasks. Hai ingestion DAG runs lịch sử SUCCESS không chứng minh Silver live tồn tại. Verify lại không có active writers trước start Spark; giữ DAG paused và chỉ chạy task manual được duyệt, không tự unpause hoặc trigger full ingestion.

Một source một lần, canary **Production**. Serialize manual runs; không chạy đồng thời ingestion và source writer. Manual `run_all_silver.py` không tự lấy PostgreSQL source lock, vì vậy chạy nó trong scheduler-held lock như command dưới đây. Source lock chỉ bảo vệ source đó, không multi-source transaction. Mỗi source commits quarantine rồi valid MERGE; nếu MERGE fail sau quarantine, retry cùng ID giữ quarantine không lặp (isolated đã kiểm chứng).

## Exact commands — chỉ sau approval C2

```powershell
docker compose up -d --no-deps spark-iceberg
docker compose exec -T spark-iceberg /opt/spark/bin/spark-sql --master 'local[2]' --driver-memory 2g --conf spark.sql.shuffle.partitions=4 -e 'CREATE NAMESPACE IF NOT EXISTS iceberg.silver'
```

Canary và các lượt tiếp theo dùng scheduler lock và Spark API. Chưa thực thi POST `/run` trong mission này. Code API đã đặt spark-submit `local[2]`, driver 2g, shuffle partitions 4 như preview (override qua `SILVER_SPARK_MASTER`, `SILVER_DRIVER_MEMORY`, `SILVER_SHUFFLE_PARTITIONS` nếu được review). Không sửa global spark-defaults/secrets. Worker service đang stopped nên chưa claim deployment; C2 canary phải xác minh limits/health trước khi tiếp tục Trade. Nếu service đang dùng code/config khác thì stop, không nới DQ.

Lệnh Spark trực tiếp sau approval, khi source lock đã được giữ qua orchestration, có dạng:

```powershell
docker compose exec -T -w /home/iceberg spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g --conf spark.sql.shuffle.partitions=4 jobs/silver/run_all_silver.py --dataset faostat_production --mode full --pipeline-run-id silver-finalization-20261010-faostat_production
```

Không chạy lệnh trực tiếp này mà thiếu lock. Lệnh scheduler/API dưới đây lấy lock đúng source; sau khi worker limits được verify, đổi `$sourceId` và `$bronzeRunId` theo từng nguồn, không dùng all:

```powershell
$sourceId = 'faostat_production'
$mode = 'full'
$bronzeRunId = '-'
$lockedRun = @'
import json, sys
from ingestion.storage.metadata_repository import MetadataRepository
from dag_silver_pipeline import trigger_spark_job
source, mode, run = sys.argv[1:]
pipeline = 'silver-finalization-20261010-' + source
with MetadataRepository().source_lock(source):
    report = trigger_spark_job('run_all_silver.py', mode, None if run == '-' else run, source, pipeline)
print('APPROVED_SILVER_RESULT_JSON=' + json.dumps(report))
'@
$lockedRun | docker exec -i -e PYTHONPATH=/opt/airflow/dags:/opt/airflow/src -w /opt/airflow airflow-scheduler python - $sourceId $mode $bronzeRunId
```

Trino kiểm tra count/schema, unique/null business keys, quarantine queryability, sample raw lineage và actual snapshots. Ví dụ canary:

```sql
SELECT count(*) FROM iceberg.silver.faostat_production;
SELECT count(*) FROM (
  SELECT country_code, commodity_code, year, element_code
  FROM iceberg.silver.faostat_production
  GROUP BY 1,2,3,4 HAVING count(*) > 1
);
SELECT count(*) FROM iceberg.silver.faostat_production
WHERE country_code IS NULL OR commodity_code IS NULL OR year IS NULL OR element_code IS NULL;
SELECT snapshot_id, parent_id, operation FROM iceberg.silver."faostat_production$snapshots";
```

Rerun full với cùng pipeline ID, assert changed rows 0 và snapshot không đổi. Sau đó `$mode='incremental'`, `$bronzeRunId` lấy đúng `files[].lineage._ingestion_run_id`/inventory hiện tại của source đó, rerun same batch, kiểm tra no-op. Lưu report/log/snapshots/Trino queries cho mỗi lượt. Không lấy run ID NSO hoặc pipeline ID làm filter cho nguồn khác.

Tiếp tục Price → SUA → PSD → Yearbook → WB → Domestic → Trade chỉ sau canary pass. SUA/PSD giữ quarantine policy hiện tại; C2 không cho phép sửa dấu/unit scaling. Mỗi source failed thì stop source, không báo overall complete.

## Correction/retry/Airflow acceptance

Late correction/stale batch/failed write đã test trên isolated Iceberg. **Không chèn fake correction vào business shared tables** chỉ để chứng minh E2E. `jobs/silver/validate_shared_controls.py` có plan-only default, đã được chuẩn bị để dùng cùng merge/quarantine engines trên hai targets `iceberg.silver_validation.reliability_controls` và `reliability_dead_letters`. Fixture có ID/source `VALIDATION_ONLY`, không vào registry/schedules/marts hoặc coverage numerator; không dùng NSO observations. Sau test mỗi target một row; value 10→20 theo timestamp là synthetic correction chỉ ở validation namespace. Failure sau quarantine/trước merge và retry/no-op/stale được kiểm tra; table/file/history được giữ, không DROP. Hai targets này cần được duyệt cùng C2 trước write.

```powershell
# Default is read-only plan; this command is the approved write probe only.
docker compose exec -T -w /home/iceberg spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g --conf spark.sql.shuffle.partitions=4 jobs/silver/validate_shared_controls.py --execute
```

Đọc lại hai validation targets và snapshots bằng Trino, đối chiếu fixture report; không dùng probe này để khẳng định từng business correction đã xảy ra. Business full/incremental no-op vẫn phải kiểm tra riêng mỗi source.

Sau write checks, có thể duyệt riêng task `silver_faostat_production` trong ingestion DAG để validate Airflow → lock → API → Spark → Trino. Task này cập nhật `ingestion.ingestion_runs.source_metadata['silver_status']`; quyền đó phải thuộc approval C2, khác với CLI manual không cập nhật status. Không trigger ingestion tasks hoặc replay. Nếu không còn pending run, task sẽ SKIPPED, không tính là E2E PASS. Repeat cho sources với pending run được xác nhận.

Full Silver DAG có NSO readiness gate nên chưa thể pass toàn DAG trước NSO recovery + contract approval. Sau E phải nghiệm thu cả chín sources và readiness audit, không bypass gate hoặc phát Gold READY. Stage này chưa đáp ứng acceptance Airflow executions/shared idempotency/corrections/Trino outputs.

Lệnh task canary chỉ sau quyền cập nhật `ingestion.ingestion_runs.source_metadata` được duyệt:

```powershell
docker exec airflow-scheduler airflow tasks test rice_lakehouse_ingestion silver_faostat_production 2026-10-10T00:00:00+07:00
```

## Recovery nếu write fail

Catalog hiện chưa có Silver targets; rollback cho table mới là giữ table/snapshots/files ở trạng thái unpublished, chặn downstream và sửa/retry theo cùng IDs. Không DROP/TRUNCATE hoặc xóa bucket/volume. Nếu đã có before snapshot lúc chạy, ghi ID trước mỗi source; rollback chỉ khi current snapshot đúng recorded after và không có concurrent changes. Giữ quarantine/audit, không xóa evidence của lỗi. Pipeline không có table-wide atomicity giữa tám nguồn; không dùng tổng count để che source thất bại.

Request C2 không bao gồm B4, NSO Silver, Gold implementation hay business-policy changes. Approval cần chốt cả API resource configuration và phạm vi Airflow task/status updates trước execution.
