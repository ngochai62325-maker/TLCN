# Trình diễn TLCN: Fresh Bronze → Silver trên Windows

Chỉ dùng project **`tlcn-local`** với profile `local_nine`. Runtime demo đã
**smoke PASS**: Airflow/Trino hoạt động, chín sources, 14 Silver tasks, hai DAG
paused/manual-only, **0 DagRuns** tại nghiệm thu. Xem [Phase B evidence](evidence/demo_phase_b_20261017_131025/README.md).
Owner đã duyệt xóa runtime cũ và miễn backup volumes; Raw Windows và Git evidence
được giữ nguyên. Fresh Bronze/Silver E2E **chưa chạy**: bạn tự trigger bên dưới.
Canonical startup dùng các Compose local, không dùng base `docker-compose.yml`.

## A. Khởi động

Mở Docker Desktop ở chế độ Linux containers/WSL2. Từ PowerShell tại repository:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1 -CheckOnly
# Start lần đầu hoặc mở lại project hiện hữu:
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1
```

Lần đầu script build hai execution images từ Dockerfiles/lockfiles, tạo storage,
chạy initializer một lần rồi start execution services. Những lần sau start các
container hiện hữu theo dependency, kiểm tra persisted paused/idle state trước
scheduler; không rerun initializer hoặc tự recreate project chưa hoàn chỉnh.
Script không trigger/unpause DAG. Nếu gặp lỗi, dừng và xem trạng thái thực tế;
không dùng reset để xử lý unknown commit.

Raw phải được cung cấp riêng: 21 files và `nso/metadata.txt` đúng
[manifest](../../config/local_raw_manifest.json). `.env.local` chỉ cần nếu muốn đổi
`SILVER_TEST_RAW_ROOT`, local image tags hoặc Trino port; lấy mẫu từ `.env.local.example`.
Không ghi đè `.env.local` đã có. Airflow demo cố định **127.0.0.1:8085**;
biến `TLCN_AIRFLOW_PORT` cũ không còn điều khiển canonical overlay.

Laptop tham chiếu: Windows 16 GB, WSL khoảng 8 GB. Trước workload, đo ba mẫu cách
30–60 giây: Windows available khoảng ≥2 GiB, WSL có headroom cho runner tăng tới
3 GiB, reserve ≥1 GiB và infrastructure growth. Không cộng memory caps để suy ra
usage; dùng `status.ps1` xem actual usage, kiểm tra OOM nếu có lỗi. Không chạy
đồng thời shared/clean-room. Spark giữ driver 768 MiB, `local[2]`, sources tuần tự.

## B. Airflow UI

Mở **http://localhost:8085**. Tài khoản **demo local công khai**, không dùng trên
mạng công cộng: username `local_nine`, password `local-nine-test-only`.
PostgreSQL/MinIO/REST/Spark không publish host ports; Trino ở localhost:18080.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/status.ps1 -Smoke
```

Smoke phải PASS actual `docker port`, Windows HTTP, metadata DB/scheduler health,
DAG parse, chín sources, 14 Silver tasks, NSO DRAFT overlay và registry checksum.
Smoke không phải bằng chứng Bronze/Silver E2E trong project mới.

## C. Unpause đúng thứ tự

Trong UI, unpause `rice_lakehouse_silver_pipeline` trước, sau đó
`rice_lakehouse_ingestion`. Cả hai có `schedule=None`, `catchup=False`, retries=0.
Unpause không tạo lịch định kỳ. Không trigger Silver thủ công.

## D. Trigger Bronze

Chọn DAG `rice_lakehouse_ingestion` → Trigger DAG, nhập:

```json
{"force_reprocess": false}
```

Chỉ trigger một lần. Chín source tasks ingest các Raw thật vào Bronze; namespace
đích Silver là `iceberg.silver_local`. Không NSO legacy, Trade fixture hay Gold.

## E. Bronze → một Silver child

Theo dõi chín ingestion tasks rồi `verify_bronze_batch` và `trigger_silver_release`.
Trigger operator tạo child ID xác định từ Bronze run, chờ Silver hoàn tất rồi
Bronze mới kết thúc. Kiểm tra đúng một child trong Silver DAG Runs.
Airflow ingestion task SUCCESS có thể chứa source outcome checksum-SKIPPED khi
Raw không đổi; đây chỉ hợp lệ nếu commit/snapshot proof được batch gate xác minh.

## F. Silver và audit

Silver DAG có 14 tasks: start, Bronze gate, chín transformers tuần tự,
`silver_data_quality_audit`, `publish_silver_signal`, end. Chú ý
`transform_nso_vietnam_v2`: production YAML vẫn DRAFT, local-only READY overlay
chỉ tồn tại trong bộ nhớ. Publish signal là `DQ_PASSED`, không `READY_FOR_GOLD`.
Nếu một writer FAIL/timeout, không clear/retry ngay: đọc catalog, snapshots và
metadata trước; giữ history, không DROP/reset/rollback snapshots để che lỗi.

## G. Trino SQL

Trong PowerShell, tạo cùng Compose arguments (chỉ cần một lần mỗi shell):

```powershell
$envFile = '.env.local.example'
if (Test-Path .env.local) { $envFile = '.env.local' }
$dc = @('compose', '--env-file', $envFile, '-p', 'tlcn-local',
    '-f', 'compose.silver-test.yml', '-f', 'compose.local-nine-test.yml', '-f', 'compose.local.yml')
$sql = @'
SELECT 'faostat_production' AS target, count(*) AS rows FROM iceberg.silver_local.faostat_production
UNION ALL SELECT 'faostat_monthly_price', count(*) FROM iceberg.silver_local.faostat_monthly_price
UNION ALL SELECT 'faostat_supply_utilization', count(*) FROM iceberg.silver_local.faostat_supply_utilization
UNION ALL SELECT 'faostat_trade', count(*) FROM iceberg.silver_local.faostat_trade
UNION ALL SELECT 'usda_rice_psd', count(*) FROM iceberg.silver_local.usda_rice_psd
UNION ALL SELECT 'usda_rice_yearbook', count(*) FROM iceberg.silver_local.usda_rice_yearbook
UNION ALL SELECT 'worldbank_commodity_monthly', count(*) FROM iceberg.silver_local.worldbank_commodity_monthly
UNION ALL SELECT 'thitruongnongsan', count(*) FROM iceberg.silver_local.thitruongnongsan
UNION ALL SELECT 'nso_rice_statistics_v2', count(*) FROM iceberg.silver_local.nso_rice_statistics_v2
ORDER BY target
'@
docker @dc exec -T trino-test trino --server http://localhost:8080 --user local --execute $sql
docker @dc exec -T trino-test trino --server http://localhost:8080 --user local --execute 'SELECT source_dataset, count(*) FROM iceberg.silver_local.dead_letters GROUP BY source_dataset'
```

Với bundle pin, kết quả tham chiếu: tổng **1.414.527** observations, NSO **23.060**,
dead letters **17** = 4 market conflicts + 13 NSO structural exclusions. Đối chiếu
từng nguồn với [data catalog](data_catalog.md), units, keys và lineage; không ép
counts vào pipeline. Chỉ tuyên bố E2E PASS sau khi run thực tế và SQL đã hoàn tất.

## H. Dừng an toàn, giữ dữ liệu

Chờ cả Bronze/Silver terminal, không có writer/query/lock active rồi chạy:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/stop.ps1
```

Script kiểm tra idle, pause hai DAG và gửi một graceful signal/service, chờ tối
đa 60 giây. Idle Spark API dùng SIGINT; các service còn lại SIGTERM. Timeout thì
dừng, không forced kill. Containers, volumes, Airflow history và Raw được giữ.
Không tắt Docker Desktop trong lúc writer đang chạy.

## I. Reset demo — cần xác nhận riêng

```powershell
# Chỉ xem danh sách, không xóa:
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1
# Chỉ khi thực sự muốn mất vĩnh viễn dữ liệu/lịch sử demo và stack đang healthy/idle:
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/reset.ps1 -DeleteData
```

Lệnh thứ hai yêu cầu gõ chính xác `DELETE tlcn-local`, kiểm tra idle, graceful stop
và chỉ xóa resources mang project label `tlcn-local`, không force. Raw Windows,
Git evidence và images không bị xóa. Muốn giữ lịch sử phải backup volumes trước.
Script này không phải công cụ xóa shared/Phase2/clean-room cũ. Nếu stack đã stopped
hoặc recovery chưa rõ, reset từ chối khi không lấy được idle proof; kiểm tra riêng.
