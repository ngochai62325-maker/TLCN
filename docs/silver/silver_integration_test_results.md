# Kết quả kiểm thử Silver integration

Ngày: 10/10/2026. Không thực thi destructive live tests (`test_faostat_poc.py` có DROP/overwrite bảng dùng chung). B4 riêng cho NSO Bronze đã được duyệt và execute; không trigger ingestion DAG/Silver writer.

## Mission finalization — kết quả mới

**B4_COMPLETE_AND_VERIFIED** ([report](nso_b4_shared_recovery_results.md), [JSON](nso_b4_shared_validation_evidence.json)): preflight strict PASS; source lock/paused DAG/no active operations PASS; shared atomic recovery 833/49, matrix 21.554 numeric + 946 markers; PyIceberg/Trino full cells/schema/lineage PASS. Snapshot `8554544144555949741` → `3499764399995403202`, rerun NOOP không đổi snapshot/metadata location/catalog. HEAD đủ 13 baseline Parquet và preserved snapshots; 6 objects mới được liệt kê, không rollback. Sau schema JSON serialization fix, **17 focused regression pass** ([log](nso_b4_regression.log)); overlap với suite trước, không cộng totals.

| Check | Environment / command | Kết quả | Evidence |
|---|---|---|---|
| Bronze/unit regression + native recovery | Host Python 3.13, PyIceberg 0.12.0, PyArrow 21, REST dummy `127.0.0.1:9` | 233 passed, 0 failed/skipped (229 unit + 4 native integration) | [log](nso_bronze_regression.log) |
| Recovery focused regression | Cùng host, 13 schema unit + 4 actual SQLite/filesystem integration | 17 passed, 0 failed/skipped; overlap với 233 | [log](nso_recovery_tests.log) |
| NSO raw→isolated repair | `recover_nso.py --mode isolated` GET shared raw/baseline, write local only | 833 rows/49 fields, retained projection/cell equality, failure/no-op/rollback PASS | [JSON](nso_recovery_evidence.json), [log](nso_recovery_isolated.log) |
| Portable native recovery on worker Arrow version | Temporary container, network none, PyIceberg 0.12.0/PyArrow 16.1.0, verified local input copies | 833/49, transaction/failure/no-op/rollback PASS | [JSON](nso_arrow16_isolated_evidence.json), [log](nso_arrow16_isolated.log) |
| Eight-source controls + reliability probe | Spark 3.5.5, network none, fresh temporary Hadoop Iceberg | 66 passed, 0 failed/skipped, 137,64 giây | [log](silver_finalization_isolated.log) |
| Spark API command boundary | Host; subprocess mocked, không chạy Spark | 2 passed; limits/run IDs/report/path guard | [log](silver_api_regression.log) |
| Fresh live Bronze preview | Temporary Spark local[2], network lakehouse, repo read-only | 8 PREVIEW_VALIDATED + NSO BLOCKED; exit 1 expected | [JSON](silver_finalization_preview_evidence.json), [log](silver_finalization_preview.log) |
| Current runtime/DagBag/Pg SELECT | Running scheduler, read-only | 9+9 source tasks, no import errors, PyIceberg 0.12/PyArrow 16.1, Pg available | [JSON](silver_finalization_runtime_evidence.json) |
| Airflow metadata state | Read-only SQL transaction | Hai DAG paused, không active tasks; không execute task | [JSON](silver_finalization_dag_state_evidence.json) |
| Shared NSO Bronze B4 | Shared REST Catalog/MinIO + Pg advisory lock + Trino | PASS, RECOVERED + NOOP | [commit](nso_shared_recovery_result.json), [post audit](nso_b4_after_audit.json), [NOOP](nso_b4_noop_validation.json) |
| Shared Silver writer + Airflow task execution | Shared catalog | NOT RUN, C2/E pending | Không ghi Silver/status hoặc trigger task |

Không cộng các suites overlap hoặc dùng mocked API test thay live Spark API E2E. Portable recovery đã thử đúng PyArrow 16.1 như scheduler; native test ghi SQLite/filesystem local, không kiểm chứng shared REST commit.

Commands đã chạy:

```powershell
$env:PYTHONPATH='src;.pytest_cache/nso_recovery_deps;.pytest_cache/silver_dev_deps'
$env:ICEBERG_REST_URI='http://127.0.0.1:9'
python -m pytest tests/unit --ignore=tests/unit/silver --ignore=tests/unit/test_silver_quality_engine.py --ignore=tests/unit/test_silver_merge_engine.py --ignore=tests/unit/test_dag_structure.py tests/integration/test_nso_recovery_isolated.py -q -p no:cacheprovider
python -m pytest tests/unit/test_bronze_schema_preservation.py tests/integration/test_nso_recovery_isolated.py -q -p no:cacheprovider
python -m pytest tests/unit/test_silver_api_execution.py -q -p no:cacheprovider
# In a fresh shell with normal read-only service settings (no dummy REST URI):
$env:PYTHONPATH='src;.pytest_cache/nso_recovery_deps'
python jobs/bronze/recover_nso.py --mode isolated --output docs/silver/nso_recovery_evidence.json
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test_faostat_nso_isolated.ps1 -SkipDownload
```

Dependency PyIceberg/SQLAlchemy được cài task-local `.pytest_cache/nso_recovery_deps`; không upgrade scheduler dependencies. Initial native tests 14 pass/3 fail do Windows file-URI `/C:/`; dùng native Windows path, không sửa assertions. Lượt sau 16 pass/1 fail vì concurrent update ném `ValidationException` sau retry thay vì chỉ `CommitFailedException`; test chấp nhận hai rejection types và vẫn assert data/schema concurrent writer được giữ. Kết quả cuối focused 17 pass, broad 233 pass. Portable offline test ban đầu thiếu boto3 vì import MinIO ở module top; chuyển import vào live-input path để offline không cần service SDK. Không sửa unit/DQ rules để làm tests pass.

Host Pg credentials từ defaults/.env auth failure, nên shared plan chọn scheduler connection đã SELECT pass, không đổi secrets. NSO ZIP hash label cũ đã được sửa dựa vào byte hashing trực tiếp; không đổi artifact để khớp label sai.

Portable compatibility command cuối (Linux wheels cache tải bằng pip download, không install vào service):

```powershell
$repositoryRoot=(Get-Location).Path
$mountArgument="type=bind,source=$repositoryRoot,target=/workspace,readonly"
$offlineCommand='python3 -m pip install --no-index --find-links /workspace/.pytest_cache/nso_linux_wheels --target /tmp/nso_deps pyiceberg[sql-sqlite]==0.12.0 && python3 -m pip install --no-index --find-links /workspace/.pytest_cache/nso_linux_wheels --no-deps --target /tmp/nso_deps pyarrow==16.1.0 && PYTHONPATH=/tmp/nso_deps:/workspace/src:$PYTHONPATH python3 jobs/bronze/recover_nso.py --mode isolated --offline-archive .pytest_cache/nso_verified_raw.zip --offline-baseline .pytest_cache/nso_existing_baseline.parquet --offline-plan docs/silver/nso_recovery_evidence.json --output /tmp/nso_arrow16_evidence.json && cat /tmp/nso_arrow16_evidence.json'
docker run --rm --network none --mount $mountArgument --workdir /workspace --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $offlineCommand
```

Offline copies được tải GET/scan ở snapshot `8554544144555949741`; CLI kiểm tra archive bytes/local member equality và directory checksum trước khi recovery. Lượt container chưa pin Arrow dùng 17.0, nên đã chạy lại 16.1 đúng runtime cần kiểm tra. Lệnh version `python -c` ban đầu bị lỗi Windows quoting; chuyển runtime versions vào report JSON, không dùng lượt lỗi đó làm evidence. Source suite 65 đã pass trước khi thêm probe; final suite 66 includes actual isolated probe + repeat/no-op. Shared production tasks vẫn chưa chạy.

## Phần A — lịch sử kiểm thử

## Kết quả theo tầng

| Suite | Môi trường | Kết quả cuối | Phạm vi |
|---|---|---|---|
| compileall | Host Python 3.13 | PASS | src/silver, Bronze writer, jobs, DAG syntax |
| Compose config + diff check | Host/Docker CLI | PASS | `docker compose config --quiet`, `git diff --check`; không apply Compose |
| validate_silver_contracts | Host | 9 YAML structurally PASS; 8 READY, NSO NEEDS_PROFILING | Keys/types/SQL DQ presence, không khẳng định NSO executable |
| Unit regression ingestion/contract | Host Python 3.13, pytest 8.4.2 | 219 passed | Không chạy Spark/Airflow/live tests ở host |
| Final focused regression | Host Python 3.13, pytest 8.4.2 | 39 passed | Bao gồm test mới chặn schema loss ở Trino fallback; có overlap với 219 tests, không cộng thành 258 unique |
| Source + shared controls + actual isolated Iceberg | Docker Spark 3.5.5/Python 3.10.16, network none | 65 passed | Full/incremental từng READY source, no-op/stale/correction/quarantine/schema guards, NSO recovery |
| DAG structure | Docker Airflow 2.10.4/Python 3.11, network none | 1 passed | Task dependencies và fixture exclusion; không chạy task |
| Live Airflow DagBag import | Running scheduler, read-only graph | PASS | 9 source Silver tasks; không import PySpark trong scheduler |
| Live Bronze preview | Temporary Spark, network lakehouse, repo read-only | 8 PREVIEW_VALIDATED + 1 BLOCKED (NSO) | DQ/dedup/counts/schema; exit 1 có chủ ý vì NSO |
| Shared Silver/Iceberg/Trino E2E | Catalog chung | NOT RUN — approval C pending | Catalog chỉ có Bronze |

Không coi exit 1 của all-source preview là thành công toàn bộ. Không coi test mocked writer hay temporary Hadoop table là live Silver integration. Eight-source full/incremental integration sử dụng actual Iceberg writes, nhưng mọi path nằm trong warehouse tạm.

## Commands tái lập

Chạy tại repository root:

```powershell
python -m compileall -q src/silver src/ingestion/storage/bronze_writer.py jobs/silver dags
python scripts/validate_silver_contracts.py
docker compose config --quiet
git diff --check
$env:PYTHONPATH = 'src;.pytest_cache/silver_dev_deps'
python -m pytest tests/unit --ignore=tests/unit/silver --ignore=tests/unit/test_silver_quality_engine.py --ignore=tests/unit/test_silver_merge_engine.py --ignore=tests/unit/test_dag_structure.py -q -p no:cacheprovider
python -m pytest tests/unit/test_bronze_schema_preservation.py tests/unit/test_bronze_identity.py tests/unit/test_chunk_idempotency.py tests/unit/test_phase6_failure_recovery.py tests/unit/test_silver_contract_loader.py tests/unit/test_source_registry.py -q -p no:cacheprovider
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test_faostat_nso_isolated.ps1 -SkipDownload
```

`responses` đã được cài trong `.pytest_cache/silver_dev_deps` theo dev dependency của pyproject, không thay global environment. `-SkipDownload` dùng pytest wheels đã lưu; bỏ flag nếu cần download trước khi container network none chạy.

Live inventory: PostgreSQL export SELECT nằm trong [metadata evidence](ingestion_metadata_evidence.json); host PostgreSQL mặc định trả authentication failure, vì vậy dùng read-only psql trong container, không sửa credentials.

```powershell
$env:PYTHONPATH = 'src'
python jobs/silver/audit_bronze_coverage.py --metadata-file docs/silver/ingestion_metadata_evidence.json
```

Preview đã chạy bằng lệnh sau. Không bỏ `--preview` khi chưa có approval C.

```powershell
$repositoryRoot = (Get-Location).Path
$mountArgument = "type=bind,source=$repositoryRoot,target=/workspace,readonly"
$previewCommand = 'PYTHONPATH=/workspace/src:$PYTHONPATH python3 jobs/silver/run_all_silver.py --dataset all --preview'
docker run --rm --network tlcn_lakehouse-net --memory 3g --mount $mountArgument --workdir /workspace -e AWS_REGION=us-east-1 -e 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 2g --conf spark.sql.shuffle.partitions=4 pyspark-shell' --entrypoint /bin/bash tabulario/spark-iceberg:latest -c $previewCommand
```

DAG test dùng container Airflow tạm, không cần metadata DB:

```powershell
$dagTestCommand = 'python -m pip install --no-index --find-links /workspace/.pytest_cache/hoang_wheels --target /tmp/silver_dag_test_deps pytest==8.4.2 && PYTHONPATH=/tmp/silver_dag_test_deps:/workspace/src:$PYTHONPATH python -m pytest tests/unit/test_dag_structure.py -q -p no:cacheprovider'
docker run --rm --network none --mount $mountArgument --workdir /workspace -e AIRFLOW_HOME=/tmp/airflow --entrypoint /bin/bash apache/airflow:2.10.4-python3.11 -c $dagTestCommand
```

## Failures được phát hiện và xử lý

- Host sandbox: 23 pass/2 temp-permission errors; rerun outside sandbox, không bỏ assertions.
- Host collection thiếu `responses`; cài dependency dev ở task-local directory. Regression lần đầu 201 pass/12 fail: old Bronze mocks chưa trả DESCRIBE schema và USDA test hardcode file size. Sửa fixtures theo metadata preflight, giữ test DELETE-failure; file size so với actual local artifact. Kết quả cuối 219 pass.
- Preview mặc định local[*] làm JVM dừng trong workload Trade; giới hạn local[2], 2 GB driver. Source rules không bị nới lỏng để tránh lỗi.
- NSO recovery test đọc Spark CSV bỏ all-empty footer (70 thay vì 71 rows). Sửa isolated replay dùng pandas parser như adapter; không hạ expected row count. Kiểm chứng 833 rows và cell equality.
- Airflow import thất bại do framework import PySpark. Tách pure registry, giữ Spark ở job runtime. DAG test ban đầu dùng get_dag gây query SQLite chưa init; chuyển sang imported DagBag.dags cho test structure. Live DagBag và isolated graph pass.
- Lượt chạy Spark isolated và live preview đồng thời gặp Docker `unexpected EOF`; không coi lượt lỗi đó là PASS. Rerun tuần tự: isolated 65 passed trong 123,98 giây, gồm retry sau failure giữa quarantine và Silver; preview đối chiếu bằng log/JSON cuối. Shared services không được restart hoặc thay đổi.

Logs: [unit](silver_unit_regression.log), [final regression](silver_final_regression.log), [isolated](silver_isolated_tests.log), [DAG](silver_dag_tests.log), [preview](silver_live_preview.log). PowerShell redirection tạo log UTF-16; JSON evidence là UTF-8. Exact output schemas và pinned Bronze snapshot nằm trong [preview evidence](silver_live_preview_evidence.json).

Các suite pytest cuối: 0 failed, 0 skipped. Shared E2E chưa chạy; NSO có trạng thái BLOCKED trong live preview, không tính là skipped test hoặc nguồn đã nghiệm thu.
