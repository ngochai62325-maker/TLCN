# Kiểm thử Silver và kết quả nghiệm thu

Cập nhật **11/10/2026**, timezone `Asia/Bangkok`. **9/9 nguồn READY** đã chạy
full, audit và replay trên catalog REST/MinIO, với **1.414.287 dòng Silver**.
Container hiện mount đúng checkout này. Trino đối chiếu số dòng thành công
trên cả chín bảng.

## Bằng chứng

- [Spark full + audit + replay](evidence/live_validation.json): metrics,
  schema thực tế, checksum rows, Bronze/Silver snapshot IDs và audit. Thời gian
  UTC 14:24:01–14:28:37, tương ứng khoảng 21:24–21:28 Bangkok.
- [Đối chiếu Trino](evidence/trino_validation.json): counts qua engine khác,
  NSO missing/geography và USDA fraction/type.
- [Migration legacy](evidence/legacy_schema_migrations.json): DDL đã chạy trên
  bảng Production rỗng trước nghiệm thu; giữ bảng, cột cũ và lịch sử.
- [NSO sau phục hồi tên](evidence/nso_geography_validation.json) và
  [đối chiếu tên bằng Trino](evidence/nso_geography_trino.json): full/audit/replay
  sau bổ sung bảng candidates. Đây là snapshot NSO mới hơn report chín nguồn
  phía trên; business keys/counts giữ nguyên, tên/level và checksum được cập nhật.

Evidence mô tả snapshot được nghiệm thu. Chạy lại các lệnh dưới đây sau khi
input thay đổi để lấy kết quả mới.

## Môi trường và các kiểm tra

| Thành phần | Runtime / kết quả |
|---|---|
| Spark image | `tabulario/spark-iceberg:latest`, image local tại thời điểm kiểm tra |
| Spark / Scala / Java | 3.5.5 / 2.12 / 17 |
| Python Spark container / pytest | 3.10.16 / 8.4.2 |
| Iceberg runtime | 1.8.1 cho Spark 3.5 |
| Contract validator | 9 executable READY contracts đạt |
| Suite độc lập | **103 passed**, không skip, **163,66 giây** |
| Regression Bronze liên quan writer | **30 passed**, **14,53 giây** trên host |
| Airflow DAG parse/dependencies | **1 passed**, **9,61 giây**, image `apache/airflow:2.10.4-python3.11` |
| Live Spark | Full + replay + post-write audit 9 nguồn PASSED |
| Live Trino | Counts khớp Spark; USDA decimal(18,4) đọc được |
| Airflow → Spark HTTP | Scheduler gọi NSO audit, trả PASSED / 23.060 rows |
| Spark API health | `healthy`; `/health` phản hồi trong lúc NSO audit đang chạy |
| Bronze readiness sau cập nhật gate | Schema hợp lệ và có rows cho cả chín Bronze tables; PASSED |

Suite độc lập dùng HadoopCatalog trong `/tmp`, repository read-only và
`--network none`. Nó gồm Silver unit/integration, 13 tests Bronze schema
preservation và union 13 CSV NSO. Regression Bronze 30 tests gồm identity,
chunk idempotency, failure recovery và schema preservation; có tests trùng
phạm vi suite độc lập nên không cộng các con số thành tổng test duy nhất.
Nên pin digest image đã nghiệm thu khi triển khai môi trường khác.

## Kết quả trên Bronze live

| Dataset | Bronze rows | Prepared | Valid | Quarantine | Silver / dedup | Duplicates |
|---|---:|---:|---:|---:|---:|---:|
| FAOSTAT Monthly Price | 15.334 | 15.334 | 15.334 | 0 | 15.334 | 0 |
| FAOSTAT Production | 22.738 | 22.738 | 22.738 | 0 | 22.738 | 0 |
| FAOSTAT SUA | 36.380 | 36.380 | 36.144 | 236 | 36.144 | 0 |
| FAOSTAT Trade | 1.226.470 | 1.226.470 | 1.226.470 | 0 | 1.226.470 | 0 |
| NSO Vietnam | 833 | 23.060 | 23.060 | 0 | 23.060 | 0 |
| IPSARD | 20.394 | 20.394 | 20.394 | 0 | 20.383 | 11 |
| USDA PSD | 15 | 990 | 990 | 0 | 990 | 0 |
| USDA Rice Yearbook | 13.131 | 13.131 | 13.131 | 0 | 12.936 | 195 |
| World Bank Pink Sheet | 792 | 56.232 | 56.232 | 0 | 56.232 | 0 |

Mỗi dataset đạt `prepared = valid + quarantine` và `valid = dedup + duplicates`.
Replay giữ nguyên số dòng, checksum gồm `_silver_processed_at`, schema và
**Silver snapshot ID**. Bronze snapshot trước/sau giữ nguyên. Quarantine retry
có thể cập nhật run/detection timestamp và snapshot riêng.

NSO có **23.428 candidate slots**, loại **368 slots** thuộc **13 record
all-source-NULL** trước unpivot. Dead letters lưu 13 record dưới dataset
`nso_vietnam_exclusions`; `23.428 = 23.060 + 368`. Giữ **946 missing
observations** với value NULL và **22.140 observations** geography cần review
với raw name/code NULL, theo quyết định người dùng. Trino xác nhận không có
review code khác NULL hoặc missing value khác NULL. Xem [NSO](nso.md) trước
khi join/aggregate theo địa lý.

Exact candidates phục hồi tên ở trường riêng `geography_name` và phân loại
province/region; raw names, NULL codes và warnings giữ nguyên. Trino kiểm tra
**23.060/23.060 observations có tên chuẩn hóa**, zero tên chuẩn hóa chứa `?`.
Tỉnh lịch sử Hà Tây vẫn riêng biệt; không áp modern province-code mapping.

USDA có **66 Milling Rate observations**: nguồn 6250–6600 → 0.6250–0.6600,
`unit=fraction`, factor 0.0001, giữ value/unit gốc. Trino xác nhận zero
conversion errors và precision `DECIMAL(18,4)`.

236 negative Residuals SUA vào quarantine theo contract; negative Stock
Variation được phép. Đây là routing dữ liệu theo quy tắc.

## Test cases quan trọng

| Case | Hành vi đã kiểm tra |
|---|---|
| Missing/schema | Required column thiếu fail-fast; optional NULL đúng kiểu |
| Casting | Malformed/overflow/fraction-to-integer quarantine, kể cả ANSI bật |
| Raw negative | Phát hiện dấu âm trước rounding |
| SQL UNKNOWN, LOG/KEEP | NULL predicate là fail; warning giữ row; cast error vẫn chặn |
| FAOSTAT | M49/CPC padding; unit/element; annual month NULL; currency/index tách riêng |
| NSO | Unit/season/index đúng; raw geography giữ nguyên, code NULL + warning |
| NSO schema drift | Unknown file/period có dữ liệu fail; NULL padding không tạo observation |
| NSO candidate names | Exact source/canonical aliases; tỉnh lịch sử giữ riêng; unknown không đoán; mapping mâu thuẫn gây fail |
| NSO missing | All-source-NULL được audit; NULL measurement có identity vẫn giữ |
| USDA milling | 6250 → 0.6250; malformed/out-of-range quarantine, missing NULL |
| World Bank | Invalid period không bị filter mất; chỉ loại synthetic column toàn NULL |
| Bronze preservation | Missing/extra/type mismatch và sanitizer collision không silent data loss |
| Dedup/freshness | Latest timestamp, deterministic tie-break; batch cũ không overwrite |
| MERGE replay | Giữ processing timestamp và snapshot; giữ extra legacy target columns |
| Quarantine retry | ID không nhân bản payload; raw payload dùng điều tra |
| Incremental | Đúng run/watermark; empty batch zero metrics |
| Bronze reset | Gate từ chối bảng còn schema nhưng zero rows; chấp nhận sau ingest |
| API/Airflow | Whitelist, structured result, watermark, chín task sau Bronze; health phản hồi trong lúc job chạy, job subprocess vẫn tuần tự |

## Lệnh chạy lại

Từ root repository:

```powershell
python scripts/validate_silver_contracts.py
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1
# Đã có wheels thì dùng -SkipDownload để chạy offline.
```

Kiểm tra live khi Bronze ổn định và không có job khác ghi cùng catalog:

```powershell
docker compose up -d --no-deps spark-iceberg
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/validate_silver_e2e.py --output-json /home/iceberg/jobs/silver/validation_result.json
docker compose exec -T spark-iceberg python3 /home/iceberg/jobs/silver/verify_silver_trino.py --spark-report /home/iceberg/jobs/silver/validation_result.json --output-json /home/iceberg/jobs/silver/trino_result.json

# Chỉ audit bảng hiện có, không ghi Silver:
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/validate_silver_e2e.py --audit-only --output-json /home/iceberg/jobs/silver/audit_result.json

# HTTP bridge từ Airflow:
docker compose exec -T airflow-scheduler python -c "from silver.orchestration import trigger_spark_job; print(trigger_spark_job(dataset='nso_vietnam', action='audit'))"
```

Trino utility nhận report full/replay, không dùng report `--audit-only`.
Report được bind về `jobs/silver/` trên host; có thể sao chép sang evidence
để lưu vào Git. Không có password trong report.

Chạy DAG test offline với wheels từ script trên:

```powershell
$repositoryRoot = (Get-Location).Path
$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$dagCommand = 'python -m pip install --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/airflow_test_deps pytest==8.4.2 && python -m pytest tests/unit/test_dag_structure.py -q -p no:cacheprovider'
docker run --rm --network none --mount $mountArgument --workdir /workspace -e PYTHONPATH=/tmp/airflow_test_deps:/workspace/src --entrypoint /bin/bash apache/airflow:2.10.4-python3.11 -c $dagCommand
```

Snapshot offline còn được hỗ trợ qua `scripts/validate_silver_local_snapshots.py`;
cần openpyxl/lxml cho adapters. Trade offline dùng ZIP 2.000 rows vì CSV đầy đủ
thiếu trong checkout; **live** ở trên dùng toàn bộ **1.226.470 rows hiện có
trong Bronze**.

## Migration, performance và phạm vi

Production legacy ban đầu rỗng, có timestamp NTZ và obsolete required diagnostics.
Utility `migrate_empty_silver_schema.py` giữ timestamp cũ dưới
`_ingestion_timestamp_legacy_ntz`, thêm timestamp UTC và chuyển bảy diagnostic
cũ thành nullable. Giữ bảng, history và extra fields; utility từ chối nonempty
table. PSD cũ `value DECIMAL(18,2)` cần migration riêng tới `(18,4)`; writer
chặn type mismatch.

Đã kiểm tra Airflow parse/dependencies và HTTP bridge thật. Đợt này không
trigger/unpause scheduled DAG hoặc cập nhật trạng thái PostgreSQL bằng test;
lifecycle scheduled run đầy đủ chưa được trigger.
Không nghiệm thu reset toàn bộ stack từ raw: Trade CSV local đang thiếu và
metadata/checkpoint cũ có thể ảnh hưởng rebuild. Xem [runbook rebuild](rebuild.md).

Regression toàn repository ở đợt trước: 206 passed/3 failed, do Trade CSV thiếu
(hai tests) và USDA file-size assertion hardcoded. `test_readiness.py` trước
đó thiếu `responses`. Đợt này chạy lại 30 Bronze tests liên quan và suite nêu
trên; không tuyên bố toàn repository xanh.

Performance dùng Spark expressions, `array + explode`, AQE, year partition,
hash distribution, fanout=false, ZSTD và heap dev 1 GB. Pre-MERGE join tránh
snapshot rỗng nhưng thêm lần đọc target. Chưa benchmark workload Trade hàng
chục triệu rows; cần đo shuffle và partition pruning theo workload triển khai.
