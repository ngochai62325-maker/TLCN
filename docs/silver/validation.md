# Kiểm thử Silver và kết quả nghiệm thu

Ngày thực hiện: **11/10/2026**, timezone người dùng `Asia/Bangkok`.
Nghiệm thu xử lý và ghi Iceberg trong catalog tạm độc lập. Không nghiệm thu
triển khai live REST/MinIO vì các container hiện có mount checkout khác.

## Môi trường

| Thành phần | Runtime được chạy |
|---|---|
| Spark test image | `tabulario/spark-iceberg:latest`, local image tại thời điểm kiểm tra |
| Spark / Scala | 3.5.5 / 2.12 |
| Java | 17 |
| Python Spark container | 3.10.16 |
| Iceberg runtime | 1.8.1 cho Spark 3.5 |
| pytest | 8.4.2 |
| Excel / HTML parsers | openpyxl 3.1.5, lxml 5.3.0 trong container tạm |
| Catalog kiểm thử | HadoopCatalog trong `/tmp`, không REST/S3 |
| Cách ly | `--network none`; repository bind read-only; container tự xóa |

Image tag `latest` không pin digest. Khi triển khai, ghi nhận/pin digest
image đã nghiệm thu để tránh thay Spark/Iceberg/Python không chủ ý.

## Các kiểm tra đã thực hiện

- Validator: 8 READY contracts đạt executable checks, NSO PENDING đúng thiết kế.
- Suite Silver: **69 passed**, không skip, trong **114,63 giây**; kiểm tra
  source transformation, quality engine, MERGE, quarantine, API, incremental
  và toàn bộ tám pipeline. Xem lệnh dưới đây để tái hiện.
- Airflow: test parse hai DAG và dependencies chạy trong image
  `apache/airflow:2.10.4-python3.11`; **1 passed**. Không trigger DAG thật.
- Syntax: `python -m compileall` cho Silver/jobs/DAG/scripts/tests mới.
- Whitespace: `git diff --check`.

## Test cases chính

| Case | Bằng chứng / expected behavior |
|---|---|
| M49/CPC bị infer numeric | `'704`, `113.0` → `704`, `0113`; không suy đoán mã lạ |
| Source unit conversion | ha/kg/t/hg conversions, unit/element agreement |
| Null value | Giữ NULL, không thay bằng zero |
| Malformed/overflow | Row vào quarantine và payload vẫn chứa raw value |
| Decimal rounding | Dấu âm được phát hiện trước rounding |
| ANSI mode | `try_cast` không làm toàn job fail vì một row malformed |
| Fraction → integer | Numeric `2020.0` hợp lệ; `2020.5` tạo `SYS_CAST_ERR` |
| SQL UNKNOWN | Predicate NULL tạo DQ error |
| Không có YAML DQ | System cast error vẫn quarantine |
| LOG/KEEP | Giữ row, lưu `_dq_warnings` |
| FAOSTAT annual/index | Annual month NULL; currency/index tách riêng |
| SUA negative stock | Cho phép Stock Variation; Residuals âm quarantine |
| PSD unpivot | Market year và crop year giữ đúng nguồn; missing country không default |
| World Bank period | Invalid month không bị filter mất trước quarantine |
| Excel trailing columns | Bỏ synthetic header rỗng chỉ khi mọi value NULL; giữ named series missing |
| NSO thiếu ma trận | Fail rõ; không xuất bản UNKNOWN contract |
| Missing source column | Required schema thiếu gây fail-fast |
| Reconciliation | Prepared = valid + quarantine; valid = dedup + duplicates |
| Deterministic dedup | Latest timestamp; stable tie-break trong batch |
| Real Iceberg rerun | Không nhân bản keys; `_silver_processed_at` không đổi cho row không đổi |
| Freshness | Batch cũ không ghi đè key mới hơn; key chưa có vẫn được insert |
| Schema mismatch | Writer từ chối target có type khác contract; không drop/recreate |
| Quarantine retry | Payload giống nhau không nhân bản ID; cập nhật pipeline run ID |
| Incremental filter | Chỉ chọn Bronze run yêu cầu; batch rỗng trả zero metrics |
| API boundary | Chặn script ngoài whitelist; truyền argv và parse structured result |

Suite nằm trong `tests/unit/test_silver_*.py`, `tests/unit/silver/`,
`tests/integration/test_faostat_nso_silver.py` và
`tests/integration/test_silver_pipeline_isolated.py`.
Các fixture cũ all-NULL/map errors được sửa thành explicit Spark schema;
test PSD preprocess kiểm tra raw string trước cast rồi kiểm tra DQ trên
projection contract thật.

## Kết quả trên toàn bộ snapshot local

Các adapter hiện có tạo Bronze tables tạm với cột sanitized và audit fields,
sau đó chạy shared framework, MERGE, incremental replay và audit target.
Trade dùng validation ZIP **2.000 row** do full raw Trade CSV thiếu trong
checkout. Không thay fixture này vào production registry.

| Dataset | Bronze rows | Prepared observations | Valid | Quarantine | Dedup / Silver rows | Duplicates |
|---|---:|---:|---:|---:|---:|---:|
| FAOSTAT Monthly Price | 15.334 | 15.334 | 15.334 | 0 | 15.334 | 0 |
| FAOSTAT Production | 22.738 | 22.738 | 22.738 | 0 | 22.738 | 0 |
| FAOSTAT SUA | 36.380 | 36.380 | 36.144 | 236 | 36.144 | 0 |
| FAOSTAT Trade ZIP | 2.000 | 2.000 | 2.000 | 0 | 2.000 | 0 |
| IPSARD | 20.394 | 20.394 | 20.394 | 0 | 20.383 | 11 |
| USDA PSD | 15 | 990 | 990 | 0 | 990 | 0 |
| USDA Yearbook | 13.131 | 13.131 | 13.131 | 0 | 12.936 | 195 |
| World Bank Pink Sheet | 792 | 56.232 | 56.232 | 0 | 56.232 | 0 |

Full và incremental replay đều đạt reconciliation, giữ số row Silver;
post-write audit PASSED trên tám bảng thử nghiệm. World Bank được kiểm tra
lại riêng sau xử lý 17 trailing synthetic columns hoàn toàn NULL, với kết quả
56.232 observations và zero quarantine. Các series có missing price vẫn giữ.

SUA quarantine 236 negative Residuals theo contract hiện hành; 1.280 negative
Stock Variation vẫn được chấp nhận theo rule. Đây là routing dữ liệu theo
specification, không phải pipeline failure.

Snapshot thử nghiệm có heap 1 GB. Lần đầu writer IPSARD partition theo từng
ngày/fanout bị Java heap OOM. Sau bổ sung year partition và các table properties
hash distribution/fanout=false, full pipeline và replay chạy thành công.

## Lệnh tái hiện

```powershell
python scripts/validate_silver_contracts.py
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1
```

Kiểm tra snapshot offline; cần tải wheel trước khi bật network isolation:

```powershell
python -m pip download --only-binary=:all: --platform manylinux_2_28_x86_64 --implementation cp --python-version 310 --abi cp310 --dest .pytest_cache/silver_wheels openpyxl==3.1.5 lxml==5.3.0

$repositoryRoot = (Get-Location).Path
$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$snapshotCommand = 'python3 -m pip install --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/silver_snapshot_deps openpyxl==3.1.5 lxml==5.3.0 && /opt/spark/bin/spark-submit --master local[2] --conf spark.sql.defaultCatalog=spark_catalog /workspace/scripts/validate_silver_local_snapshots.py'
docker run --rm --network none --hostname localhost --mount $mountArgument --workdir /workspace -e SPARK_LOCAL_IP=127.0.0.1 -e SPARK_LOCAL_HOSTNAME=localhost -e PYTHONPATH=/tmp/silver_snapshot_deps:/workspace/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $snapshotCommand
```

Script hỗ trợ `--dataset worldbank_pinksheet` để chạy riêng một nguồn.
Kết quả stdout có `SNAPSHOT_RESULT` từng dataset và `SNAPSHOT_SUMMARY`.
Warehouse/data files tạm tự xóa khi kết thúc; kết quả này không tạo bảng
`iceberg.silver.*` trong catalog live.

## Regression ngoài Silver và giới hạn

Regression Bronze trên host: **206 passed, 3 failed** trong 209 tests được
chọn, sau chạy ngoài sandbox để pytest có quyền ghi Windows temp.
Ba failure có sẵn từ snapshot/checkouts:

1. Trade large-file extraction thiếu
   `data/raw/faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv`.
2. Test all canonical FAOSTAT sources cũng fail vì cùng Trade file thiếu.
3. USDA readiness test hardcode file size 45.542 bytes; file local hiện 45.549.

`test_readiness.py` chưa được chạy vì host thiếu dependency `responses`;
`test_dag_structure.py` được chạy riêng trong Airflow image. Không tuyên bố
toàn bộ repository tests đã xanh. Không sửa các assertion/source artifacts
ngoài Silver để tạo kết quả pass giả.

NSO vẫn pending: Bronze thiếu queryable matrix columns và contract chưa có
schema/unit/province mapping production. Test national/matrix proposal và
fail-fast có chạy, nhưng không chứng minh NSO production output hoàn chỉnh.

Không chạy các integration PoC có DROP bảng/shared catalog hoặc gọi nguồn
external. Nghiệm thu này chứng minh cleaning, DQ, dedup, quarantine và
Iceberg writes trên local snapshots, chưa chứng minh throughput full Trade
hàng chục triệu rows hay lifecycle của stack live đang chạy checkout khác.
