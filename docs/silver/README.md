# Silver Layer — hướng dẫn sử dụng

Cập nhật ngày **11/10/2026**. Silver dùng **PySpark 3.5 + Apache Iceberg**, theo
contract YAML tại `contracts/silver/`. CLI, Spark API và hai Airflow DAG cùng gọi
`SilverTransformationFramework`; các job theo nguồn chỉ là entry point tương thích.

## Tài liệu

- [Kiến trúc và workflow](silver_layer_architecture.md).
- [Quy tắc làm sạch và chuẩn hóa](cleaning_rules.md).
- [Schema đầu vào/đầu ra và business keys](schemas.md).
- [Kiểm thử, kết quả và giới hạn môi trường](validation.md).
- [NSO: schema, unit, địa lý và đối soát](nso.md).
- [Chạy lại sau khi reset dữ liệu](rebuild.md).
- [Audit FAOSTAT/NSO trước tích hợp](faostat_nso_repository_audit.md): tài liệu
  lịch sử, không đại diện trạng thái triển khai hiện tại.

## Phạm vi hiện tại

**9 nguồn READY**: FAOSTAT Production, Monthly Price, Supply Utilization,
Trade; NSO Vietnam; USDA PSD, Rice Yearbook; World Bank Pink Sheet; IPSARD.

NSO xuất `iceberg.silver.nso_rice_statistics`; giữ tên địa lý nguồn, mã chưa
xác minh là NULL và warning theo lựa chọn của người dùng. Tên có dấu riêng
được phục hồi từ bảng candidates, chẳng hạn `Hà N?i` → `Hà Nội`. Milling Rate USDA
được chuẩn hóa thành fraction, giữ giá trị/đơn vị nguồn. `--dataset all` chạy
đủ chín contract. Xem [NSO](nso.md) và [cleaning rules](cleaning_rules.md).

Nghiệm thu trước reset: **1.414.287 dòng Silver**, full/audit/replay đạt cả chín
nguồn; Trino đối chiếu counts thành công. Sau reset đã sửa lỗi NSO Bronze suy
luận cột toàn NULL thành DOUBLE, kiểm chứng tạo bảng mới 13 CSV / 833 dòng và
phục hồi Silver NSO **23.060 observations**. Suite độc lập **103 passed**;
regression NSO/force/repair **34 passed**. Xem [bằng chứng](validation.md) và
[cách phục hồi task NSO lỗi](rebuild.md).

## Chạy pipeline

Thực hiện từ root repository. Stack nghiệm thu hiện mount đúng checkout này.
Spark service chạy API foreground; jobs dùng `local[2]` trong cấu hình dev.
Entrypoint image mặc định chỉ thực thi argument đầu nên Compose phải override
entrypoint. Các port notebook/master cũ không có daemon trong chế độ này.
Contract tự tìm đường dẫn theo repository, không phụ thuộc working directory.
API xử lý HTTP healthcheck độc lập trong khi job lock giữ Spark jobs tuần tự.

```powershell
docker compose up -d --no-deps spark-iceberg
docker compose ps spark-iceberg
```

```powershell
python scripts/validate_silver_contracts.py
docker inspect spark-iceberg --format '{{json .Mounts}}'

# Sau khi khởi động Compose với các volume đúng repository hiện tại:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --check-bronze
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --dataset all
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --audit

# Chạy một nguồn:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --dataset faostat_production

# Incremental theo Bronze run đã thành công:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --dataset usda_psd --mode incremental --run-id <bronze_ingestion_run_id>

# Hoặc watermark UTC, exclusive (> watermark):
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --dataset usda_psd --mode incremental --watermark 2026-10-01T00:00:00+00:00
```

`--pipeline-run-id` là định danh lần xử lý Silver; `--run-id` là bộ lọc run
Bronze, chỉ có hiệu lực trong incremental. Không được chạy incremental thiếu
run ID và watermark. Nếu cung cấp cả hai, pipeline lấy giao của hai điều kiện.

CLI trả `SILVER_RESULT=<JSON>` và exit code khác 0 khi lỗi. Có thể lưu JSON bằng
`--output-json <path>`; đường dẫn thuộc filesystem nơi script đang chạy.
Tên alias `usda_export_price` được chuyển sang contract `usda_rice_yearbook`;
đầu ra chính thức là `iceberg.silver.usda_rice_yearbook`.

## Chạy kiểm thử độc lập

```powershell
# Lần đầu tải pytest wheel; cần image tabulario/spark-iceberg:latest đã có local.
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1

# Các lần sau chạy hoàn toàn offline:
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1 -SkipDownload
```

Script dùng `--network none`, mount repository chỉ đọc và tạo warehouse tạm.
Suite không kết nối catalog REST, MinIO hoặc PostgreSQL đang dùng chung.
Xem [validation.md](validation.md) để chạy kiểm tra toàn bộ snapshot CSV/Excel.

## Dependencies và cấu hình

Spark runtime đã có trong image: Spark **3.5.5**, Java **17**, Iceberg **1.8.1**
trong môi trường đã kiểm thử. Host Windows dùng Python 3.13/Java 22 nên ưu tiên
container cho Spark. Nếu cài local, dùng Python 3.11 và Java 17 với
`python -m pip install -e ".[silver,dev]"`; chạy từ root repository.

Spark job chỉ đọc Bronze, không cần cài các parser Excel. Kiểm tra snapshot từ
raw cần thêm `openpyxl` và `lxml` cho adapter USDA HTML.
`pyproject.toml` giữ phần ingestion và bổ sung optional extra `silver`.

| Biến môi trường | Mục đích |
|---|---|
| `ICEBERG_REST_URI` | REST catalog, mặc định `http://iceberg-rest:8181` |
| `MINIO_ENDPOINT` | S3 endpoint, mặc định `minio:9000` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Credentials từ môi trường triển khai |
| `SILVER_SHUFFLE_PARTITIONS` | Số shuffle partitions, mặc định 8 cho môi trường dev |
| `SPARK_API_URL` | Airflow → Spark API, mặc định `http://spark-iceberg:5005/run` |

Spark session dùng UTC và bật Adaptive Query Execution. Compose mount thêm
`contracts/` vào `/home/iceberg/contracts:ro` và `config/silver/` vào
`/home/iceberg/config/silver:ro` để job đọc specification và inventory NSO.

## Vận hành và khôi phục

Lỗi schema thiếu bắt buộc hoặc sai kiểu bảng Silver làm job fail, log traceback
kèm dataset/pipeline run ID. Không tự suy đoán country/province hoặc thay đổi
kiểu của bảng cũ. Engine cho phép thêm cột mới nhưng chặn thay đổi kiểu hiện có;
ví dụ bảng cũ có `value DOUBLE` cần migration sang contract `DECIMAL` trước
khi dùng pipeline mới. Sao lưu/review migration riêng; không drop bảng dùng chung.

Quarantine và Silver là hai Iceberg commit riêng. Nếu job lỗi sau commit
quarantine nhưng trước MERGE Silver, chạy lại cùng Bronze input: quarantine
không nhân bản payload và MERGE không nhân bản business key. Metadata Bronze
chỉ đánh dấu Silver SUCCESS sau khi Spark job thực sự trả thành công.

Full mode là **đọc toàn bộ Bronze rồi UPSERT**, không xóa business keys đã
vắng mặt khỏi snapshot mới. Nếu cần semantics snapshot replacement/deletion,
cần bổ sung contract và chiến lược riêng; không dùng full như một overwrite.

NSO yêu cầu đủ ma trận Bronze. Adapter hiện kiểm kê union trước chunk đầu;
writer từ chối mất cột/schema mismatch trước khi ghi. Cột period mới có dữ liệu
nhưng chưa được kiểm kê gây fail để cập nhật inventory rõ ràng.

## Nghiệm thu đầy đủ catalog đang chạy

Lệnh dưới ghi các bảng Silver/quarantine theo contract, audit rồi replay từng
nguồn. Chạy khi không có ingestion hoặc Silver job khác đang xử lý catalog.
Không thay trạng thái run trong PostgreSQL hoặc tự trigger/unpause DAG.

```powershell
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/validate_silver_e2e.py --output-json /home/iceberg/jobs/silver/validation_result.json

# Đối chiếu chỉ đọc qua Trino:
docker compose exec -T spark-iceberg python3 /home/iceberg/jobs/silver/verify_silver_trino.py --spark-report /home/iceberg/jobs/silver/validation_result.json --output-json /home/iceberg/jobs/silver/trino_result.json

# Kiểm tra lại các bảng hiện có, không ghi Silver:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/validate_silver_e2e.py --audit-only --output-json /home/iceberg/jobs/silver/audit_result.json
```

Evidence gồm counts, schema, Bronze/Silver snapshot IDs và checksum rows;
replay phải giữ cả data, processing timestamp và Silver snapshot ID.
Quarantine cập nhật detection/run khi replay nên snapshot quarantine có thể đổi.

Bảng legacy **rỗng** có audit timestamp NTZ có thể dùng utility migration:

```powershell
# Dry run; kiểm tra SQL trước khi thực thi:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/migrate_empty_silver_schema.py --dataset faostat_production
# Sau review thêm --execute. Utility giữ cột cũ *_legacy_ntz, chuyển obsolete required diagnostics sang nullable; không drop bảng.
```

Utility từ chối bảng có dữ liệu; không dùng nó thay migration dữ liệu đã công bố.
Nếu PSD cũ có `value DECIMAL(18,2)`, cần migration precision tới `(18,4)` trước
khi chạy contract mới; writer sẽ chặn type mismatch thay vì cắt mất fraction.
