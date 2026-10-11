# Silver Layer — hướng dẫn sử dụng

Cập nhật ngày **11/10/2026**. Silver dùng **PySpark 3.5 + Apache Iceberg**, theo
contract YAML tại `contracts/silver/`. CLI, Spark API và hai Airflow DAG cùng gọi
`SilverTransformationFramework`; các job theo nguồn chỉ là entry point tương thích.

## Tài liệu

- [Kiến trúc và workflow](silver_layer_architecture.md).
- [Quy tắc làm sạch và chuẩn hóa](cleaning_rules.md).
- [Schema đầu vào/đầu ra và business keys](schemas.md).
- [Kiểm thử, kết quả và giới hạn môi trường](validation.md).
- [Audit FAOSTAT/NSO trước tích hợp](faostat_nso_repository_audit.md): tài liệu
  lịch sử, không đại diện trạng thái triển khai hiện tại.

## Phạm vi hiện tại

**8 nguồn READY**: FAOSTAT Production, Monthly Price, Supply Utilization,
Trade; USDA PSD, Rice Yearbook; World Bank Pink Sheet; IPSARD.

**NSO chưa được xuất bản**: contract còn `NEEDS_PROFILING`; Bronze đã mất các
cột ma trận tỉnh/năm của V06.13–24 và chưa có mapping/unit được xác nhận.
`--dataset all` chạy tám contract READY và log rõ nguồn pending.
`--dataset nso_vietnam` báo lỗi trước khi đọc/ghi bảng. Transformer NSO hiện có
vẫn dùng được để preview dữ liệu đầy đủ và kiểm tra thiếu schema.

## Chạy pipeline

Thực hiện từ root repository. Kiểm tra Docker đang mount **đúng checkout**;
container đang có sẵn trong môi trường nghiệm thu mount một checkout khác.
Không dùng kết quả chạy từ checkout khác để nghiệm thu code này.

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
`contracts/` vào `/home/iceberg/contracts:ro` để job đọc specification.

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

NSO cần khôi phục schema Bronze từ toàn bộ 13 CSV, nạp lại các file bị mất cột,
xác nhận đơn vị ma trận và mapping địa lý rồi cập nhật contract. Chỉ đổi status
sang READY không thể khôi phục dữ liệu đã mất.
