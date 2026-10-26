# TLCN Rice Lakehouse — Project Status

> **Final G12 26/10/2026: GOLD_MVP_COMPLETE.** 17 physical tables / 1.414.527 fact observations, 9-source exact reconciliation, DQ01–18, 4 full-scope semantic SQL queries, immutable publication/readback và consumers PASS. `manual__gold_g12_20261026_01` và `manual__gold_g12_noop_20261026_01` đều 16/16 SUCCESS; formal replay 36,07 phút giữ nguyên 17 UUID/snapshot/metadata/file sets và manifest hash. Full Trade 156 partitions / 1.226.470 observations / 526.986 groups, zero differences, Trino 448/512 MiB. Historical failed runs giữ FAILED; external Docker interruption được recover bằng standard Airflow task resume sau actual metadata/RAM checks. Không upgrade/reset/migrate hoặc sửa/trigger Bronze–Silver. [VALIDATION](../gold/VALIDATION.md), [evidence](../gold/runtime_acceptance.json), [demo/recovery](../gold/IMPLEMENTATION.md). Body 23/10 là lịch sử; H01–H05/official geo/FX/ML ngoài MVP không tự đóng.

Audit ngày **2026-10-23**, timezone **Asia/Bangkok (UTC+07)**; mốc ghi nhận baseline **19:42 +07**. Branch `main`, HEAD `1bb662c49cac2f82af14ce4b5c9a5d909c2a0e05`. Phạm vi: Git, static source/config, lightweight tests và read-only probes. Không start/build/reset Docker, trigger DAG hay ghi Iceberg/MinIO/PostgreSQL. [Kiến trúc và vận hành](PROJECT_CONTEXT.md) · [Backlog](NEXT_STEPS.md).

## Mục lục

- [Cách hiểu trạng thái](#cách-hiểu-trạng-thái)
- [Git baseline và thay đổi gần đây](#git-baseline-và-thay-đổi-gần-đây)
- [Tiến độ theo subsystem](#tiến-độ-theo-subsystem)
- [Kiểm tra mới và kết quả thực tế](#kiểm-tra-mới-và-kết-quả-thực-tế)
- [Bugs và correctness risks](#bugs-và-correctness-risks)
- [Gold chưa đủ điều kiện triển khai](#gold-chưa-đủ-điều-kiện-triển-khai)
- [Drift và khả năng bàn giao](#drift-và-khả-năng-bàn-giao)
- [Chưa xác minh và việc còn lại](#chưa-xác-minh-và-việc-còn-lại)

## Cách hiểu trạng thái

| Trạng thái | Ý nghĩa trong báo cáo |
|---|---|
| `COMPLETE_VERIFIED` | Phạm vi hẹp được nêu có code và kiểm tra mới; không tự mở rộng sang runtime |
| `IMPLEMENTED_UNVERIFIED` | Có implementation, chưa chứng minh hoạt động trên dữ liệu/runtime hiện tại |
| `PARTIAL` | Thiếu phần bắt buộc hoặc có lỗi đã thấy |
| `DESIGN_ONLY` | Chỉ có schema/tài liệu/đề xuất |
| `NOT_STARTED` | Chưa tìm thấy implementation của feature được nêu |
| `BLOCKED` | Kiểm chứng bị dependency/môi trường/lỗi cụ thể chặn |
| `LEGACY` | Ngoài active runtime, còn cần rà usage trước khi bỏ |
| `UNKNOWN` | Không có đủ bằng chứng để kết luận |

Không có cơ sở chấm một tỷ lệ hoàn thành tổng thể. Bronze/Silver có code khá đầy đủ, nhưng **HEAD hiện tại chưa được nghiệm thu E2E**.

## Git baseline và thay đổi gần đây

- Đầu audit: `git status --porcelain=v1 --untracked-files=all` rỗng, không có diff tracked hay untracked không bị ignore. Không có `docs/handoff/` trước khi tạo bộ tài liệu này. Không tìm thấy `AGENTS.md` áp dụng trong repo/ancestor đã kiểm tra.
- 242 tracked files; 145 Python files. Không fetch, pull, checkout, reset, stash, clean, merge, rebase, commit hay push.
- `main`, cached `origin/main`, `handoff/local-nine-phase2` và cached tracking branch cùng HEAD. Branch `feat_silver_intergrate` ở `5c57169`, ahead 1 so với cached tracking ref của chính nó. Đây là **local refs**, không phải remote freshness check.
- `origin` dùng GitHub SSH host alias `github.com-personal`; URL không chứa credential được nhúng. Alias phụ thuộc cấu hình máy.
- `.env` vắng, `.env.local` có và bị ignore; lifecycle helpers không đọc `.env.local`. Raw trade lớn có tại máy này nhưng bị ignore. Ignored inventory có permission/long-path errors, nên không tuyên bố đã duyệt hết mọi ignored file.

| Commit (thời gian +07) | Diff có ý nghĩa với người nhận |
|---|---|
| `1bb662c` — 22/10 16:44 | Message “update gold layer schema”, nhưng diff thực là thêm 5 audit/review files (6,412 dòng), gồm Gold evidence/mapping và `senior_de_review.md`; không sửa runtime/DBML trong commit này |
| `b9c698f` — 22/10 14:42 | Thêm Gold README, `dim_fact_v3.dbml`, cập nhật `.gitignore` |
| `34b4003` — 19/10 23:35 | Refactor ingestion/storage: bỏ Raw archive/physical manifest, giản lược quarantine và committed-batch handling; xóa `bronze_storage_layout.py` và test deliverables tương ứng; 12 files, +91/-872 |
| `4d1a801` — 19/10 11:31 | Revert commit `4dd9bb3`; phải xét diff cuối cùng sau `34b4003`, không dừng ở revert |
| `4dd9bb3` — 19/10 11:24 | Thử bỏ Raw landing uploads/reliability; đã bị revert ngay sau đó |
| `50e6a7d` — 18/10 00:25 | Gộp stack thành `compose.yaml`, bỏ compose/docs/evidence cũ; đổi contract NSO v2 thành `nso.yaml`, bỏ contract NSO cũ, sửa registry và hash pins |
| `f91337f`, `e4cd47d`, `82bfec4`, `5c57169` | Các mốc local demo/nine-source/NSO/eight-source trước refactor; chỉ dùng làm lịch sử |

`git diff 34b4003..HEAD -- src dags jobs compose.yaml contracts tests` rỗng: code đang audit vẫn là phiên bản sau refactor 19/10, dù HEAD 22/10 có tên liên quan Gold. Attribution chỉ dựa vào Git author/diff, không suy diễn ai đang giữ thay đổi ngoài repo.

Các file đã xóa nêu trong bảng là **đường dẫn lịch sử**, không phải dependency cần tìm lại trong working tree. Không phục hồi/xóa file nào trong audit.

## Tiến độ theo subsystem

| Feature / phạm vi | Trạng thái | Bằng chứng mới và giới hạn |
|---|---|---|
| Cấu hình local 1 Compose, project, profile, ports/mounts | `COMPLETE_VERIFIED` | Resolve [compose.yaml](../../compose.yaml) thành JSON thành công: 8 services, 4 named volumes, localhost ports đúng. Chưa dựng images/services |
| Lifecycle scripts: syntax | `COMPLETE_VERIFIED` | PowerShell AST parse 7 `.ps1` dưới `scripts/`: 0 parse errors |
| Lifecycle start/resume/idle guards/stop/reset | `IMPLEMENTED_UNVERIFIED` | [_common.ps1](../../scripts/local_demo/_common.ps1), [start](../../scripts/local_demo/start.ps1), [stop](../../scripts/local_demo/stop.ps1), [reset](../../scripts/local_demo/reset.ps1); config tests dùng mocks có chạy, không thực thi lifecycle thật |
| Raw inventory hiện có | `COMPLETE_VERIFIED` | [Manifest](../../config/local_raw_manifest.json): 21 input + 1 support tồn tại, size khớp; tổng input 183,728,724 bytes. Không hash lại toàn bộ |
| Raw bàn giao clean clone | `PARTIAL` | Trade CSV lớn bị ignore; manifest không tự cung cấp file. Cần data provisioning và Raw root mới |
| Registry/local routing và 9 executable contracts | `COMPLETE_VERIFIED` | [MAPPINGS](../../src/silver/local_profile.py), registry checksum khớp pins; validator 9/9 hợp lệ. 10 enabled registry entries gồm 1 fixture; không phải 10 production sources |
| Bronze FULL/adapters/9 targets/schema | `IMPLEMENTED_UNVERIFIED` | [FullLoader](../../src/ingestion/core/full_loader.py), [adapters](../../src/ingestion/adapters/), [writer](../../src/ingestion/storage/bronze_writer.py); unit checks một số paths đạt, chưa ghi/đọc 9 physical tables tại HEAD |
| Bronze source locks/checksum/checkpoint/recovery | `PARTIAL` | Có implementation và mock tests; chunk replace DELETE/APPEND chưa atomic, metadata error có best-effort catches. Xem risks bên dưới |
| Bronze quarantine/diagnostic | `PARTIAL` | [QuarantineManager.quarantine_artifact](../../src/ingestion/reliability/quarantine_manager.py) trả thành công nhưng không persist; reproducer mới xác nhận không gọi sink |
| Bronze generic incremental extraction | `PARTIAL` | [IncrementalLoader](../../src/ingestion/core/incremental_loader.py) có code nhưng không write qua Bronze writer như FULL; không active với 9 LOCAL_FILE/FULL sources. Không đánh đồng với Silver incremental read |
| Bronze → Silver Airflow gates | `IMPLEMENTED_UNVERIFIED` | Hai [DAG files](../../dags/), persisted results/physical proofs/parent-run checks trong pipeline_tasks và silver.orchestration; Airflow host thiếu, runtime chưa kiểm tra |
| Spark API allowlist, serialization, commit readback | `IMPLEMENTED_UNVERIFIED` | [spark_api_server](../../jobs/silver/spark_api_server.py), [run_all_silver](../../jobs/silver/run_all_silver.py); lock/readback mocks đạt sau process-local import-path fix; chưa chạy Spark writer |
| Silver 9 transformers/normalization/DQ | `IMPLEMENTED_UNVERIFIED` | [framework](../../src/silver/framework.py), [transformers](../../src/silver/transformers/), [contracts](../../contracts/silver/); static schema/selected semantics checks đạt, không có fresh row-level acceptance |
| NSO semantics/metadata identity | `IMPLEMENTED_UNVERIFIED` | [nso.py](../../src/silver/transformers/nso.py), [nso.yaml](../../contracts/silver/nso.yaml), [semantics tests](../../tests/unit/silver/test_nso_v2_semantics.py) trong nhóm test mới; chưa xác minh physical NSO table/counts |
| Silver quarantine/exclusions/lineage | `IMPLEMENTED_UNVERIFIED` | [quarantine](../../src/silver/engine/quarantine.py), [quality](../../src/silver/engine/quality.py), [framework](../../src/silver/framework.py); Bronze quarantine bug không đồng nghĩa Silver manager giống vậy. Host test_silver_quarantine chưa collect được do thiếu PyIceberg |
| Silver deterministic dedup/change-aware MERGE/NOOP | `IMPLEMENTED_UNVERIFIED` | [merge.py](../../src/silver/engine/merge.py), framework; phải so snapshot/metadata path trước-sau tại runtime, unit mock không đủ chứng nhận physical NOOP |
| Full host unit suite | `BLOCKED` | Collection thiếu `airflow`, `responses`, `pyiceberg`; subset có 5 setup errors do `%TEMP%` permissions và 4 lỗi import helper ở invocation mặc định |
| Docker hiện hành / physical rows / snapshots / runtime RAM | `UNKNOWN` | Docker client không kết nối được `dockerDesktopLinuxEngine`; không có catalog/DB/HTTP evidence mới |
| Gold DBML và source mapping audit | `DESIGN_ONLY` | [DBML](../gold/dim_fact_v3.dbml), [mapping CSV](../gold/design_audit_2026-10-22_field_mapping.csv), [mapping report](../gold/design_audit_2026-10-22_schema_and_mapping.md); 27 design tables, 261 mapping rows; còn mismatches |
| Gold ETL/contracts/jobs | `NOT_STARTED` | Không tìm thấy Gold implementation tree; tài liệu/mapping CSV không phải executable ETL |
| Analytics views/dashboard/ML training | `NOT_STARTED` | Không tìm thấy implementation active. [EDA notebooks](../../tlcn_explain-for_eda/) là khám phá dữ liệu chưa kiểm chứng lại |
| Transformers kiểu cũ/direct query/old harness | `LEGACY` | `_transformer.py`, [query_silver_usda.py](../../query_silver_usda.py), một số scripts và tests trỏ compose cũ; phải rà usage trước khi sửa/bỏ |

## Kiểm tra mới và kết quả thực tế

Tất cả kết quả phần này được chạy trong audit 23/10 tại HEAD nêu trên. Không cài dependency mới. Host: Python **3.13.0**, pytest **8.4.2**; có PySpark/PyArrow/boto3/psycopg, thiếu `airflow`, `responses`, `moto`, `pyiceberg`. Không suy ra container thiếu những package tương tự.

| Check | Kết quả và phạm vi |
|---|---|
| Python AST + absolute project import paths | **145 tracked `.py`**, không SyntaxError; các module paths tuyệt đối `ingestion/silver/common` được resolve về source. Không phải runtime import/dependency/symbol check toàn bộ |
| `python -B scripts/validate_silver_contracts.py` | **9/9 contracts hợp lệ**; không đọc physical table |
| `docker compose --env-file .env.example -p tlcn-local -f compose.yaml config --format json` | Resolve được; chỉ trích metadata không chứa credential values vào kết luận |
| PowerShell AST | **7 scripts, 0 errors**; không execute scripts |
| Raw file metadata | **21 inputs + support**, sizes khớp manifest; **chưa hash lại SHA-256** |
| Registry/NSO contract checksums | Registry SHA-256 khớp `REGISTRY_SHA256` trong local_profile và pipeline_tasks; NSO contract khớp `NSO_CONTRACT_SHA256`; không bypass hash guard |
| `docker ps` read-only | Lỗi named pipe `dockerDesktopLinuxEngine` không tồn tại; không start engine/stack |
| Unit collection | **333 tests collected, 4 collection errors**, không phải 333 tests passed |
| Selected unit suite | **102 passed, 4 failed, 5 errors, 3 deselected**, 3.39s. Xem nguyên nhân và follow-up riêng bên dưới |
| Follow-up 4 import failures | **4 passed**, 1.60s, sau thêm `jobs/silver` vào `sys.path` của đúng process test; không sửa repo |
| Quarantine read-only reproducer | Trả `is_quarantined=True`; artifact/diagnostic không tồn tại và injected metadata/storage mocks không được gọi. Không ghi file/DB/object |
| Tracked text secret screen | 195 text files của HEAD, high-confidence private key/token patterns: 0 hits. Có dev credential defaults trong config; không chép giá trị. Không scan toàn bộ Git history/binary/datasets/local `.env` |

Lệnh collection đã chạy:

```powershell
python -B -m pytest --collect-only -q -p no:cacheprovider --tb=line tests/unit
```

Bốn collection errors: `test_dag_structure.py`, `test_local_nine_dags.py` thiếu Airflow; `test_readiness.py` thiếu responses; `test_silver_quarantine.py` thiếu PyIceberg. Moto cũng thiếu theo dependency probe nhưng không gán nó làm nguyên nhân bốn lỗi trên.

Lệnh subset đã chạy (PowerShell continuation):

```powershell
python -B -m pytest -o addopts= -q -p no:cacheprovider --tb=line `
  tests/unit/test_bronze_schema_preservation.py `
  tests/unit/test_bronze_identity.py `
  tests/unit/test_metadata_silver_recovery.py `
  tests/unit/test_orphan_cleanup.py `
  tests/unit/test_advisory_lock.py `
  tests/unit/test_pipeline_tasks.py `
  tests/unit/test_local_nine_profile.py `
  tests/unit/test_silver_orchestration.py `
  tests/unit/silver/test_nso_v2_semantics.py `
  tests/unit/test_local_demo.py::LocalDemoConfigTests `
  -k 'not nso_overlay_refuses_contract_drift and not missing_or_mismatched_unit_evidence_fails_closed and not health_responds_during_job_and_jobs_remain_serial'
```

- 4 failures: `test_release_lock_fails_closed_and_covers_outcome`, hai parameter cases của `test_release_unknown_commit_reads_catalog_under_lock_without_retry`, `test_local_runner_holds_bronze_source_lock_through_commit_readback`; đều `ModuleNotFoundError: validate_silver_e2e`.
- 5 setup errors: bốn `test_release_registry_rejects_drift` cases và `test_release_preflight_failure_prevents_every_write`; `PermissionError` tại `%TEMP%/pytest-of-hoang`. Không kết luận business assertion sai từ setup failure.
- 3 deselections được chọn trước để tránh các temporary-file/network paths không cần cho audit. Không coi các test bị bỏ là đạt.

Lệnh xác minh riêng import-path diagnosis:

```powershell
python -B -c "import sys; sys.path.insert(0, 'jobs/silver'); import pytest; raise SystemExit(pytest.main(['-o','addopts=','-q','-p','no:cacheprovider','--tb=line','tests/unit/test_silver_orchestration.py::test_release_lock_fails_closed_and_covers_outcome','tests/unit/test_silver_orchestration.py::test_release_unknown_commit_reads_catalog_under_lock_without_retry','tests/unit/test_silver_orchestration.py::test_local_runner_holds_bronze_source_lock_through_commit_readback']))"
```

Kết quả 4 passed chỉ giải thích 4 failures ban đầu, không làm toàn suite xanh. File helper thực sự có tại [jobs/silver/validate_silver_e2e.py](../../jobs/silver/validate_silver_e2e.py); `pyproject.toml` chỉ thêm `src` vào pytest pythonpath, trong khi container thêm cả `jobs/silver`. Chưa sửa import packaging hay `%TEMP%` ACL.

Các tests đã chọn dùng mocks cho storage/catalog/database; không chạy writers thật. Pytest có thể tạo ignored cache/temp artifacts tự động; audit không cleanup/xóa chúng và không tạo bộ evidence files riêng.

## Bugs và correctness risks

### P0 — Bronze quarantine báo thành công nhưng không có diagnostic bền vững

Evidence: [quarantine_manager.py](../../src/ingestion/reliability/quarantine_manager.py), `QuarantineManager.__init__`/`quarantine_artifact`; [FullLoader](../../src/ingestion/core/full_loader.py), nhánh validation failure; diff `34b4003`.

Constructor bỏ qua mọi injected kwargs. Method tính checksum/size/snippet/schema_diff nhưng không persist chúng; tự ghép `quarantine/...` và `.error.json`, chỉ log rồi trả `is_quarantined=True`. Read-only probe với synthetic invalid path, explicit error/run/batch, injected sink mocks và logger mock cho kết quả: status true, paths không tồn tại, sinks không được gọi. Kết hợp source inspection xác nhận đây không phải storage upload bị chậm mà là thiếu implementation.

FullLoader trả FAILED và kèm diagnostic URI đó; invalid input không vì lỗi này tự lọt vào Silver. Tác động là mất diagnostic có thể truy vết, báo cáo cách ly sai và giảm khả năng phục hồi lỗi sau refactor. Không đủ chứng cứ kết luận mọi happy-path ingestion đã hỏng.

Hướng sửa H01: dùng persistence phù hợp chính sách Raw-local. [MetadataRepository.save_dead_letter](../../src/ingestion/storage/metadata_repository.py) và [ingestion.dead_letter_records DDL](../../src/ingestion/storage/init_db.sql) đã có nhưng không có active caller được tìm thấy. Wire repository vào manager, persist structured diagnostics, thể hiện đúng Raw vẫn local và không trả artifact/JSON URI chưa tạo. Persistence failure phải observable, không báo cách ly thành công. Không tự phục hồi MinIO Raw archive đã bỏ.

### P1 — Verifier và test entrypoint chưa theo active contracts

[verify_silver_trino.py](../../jobs/silver/verify_silver_trino.py), nhánh non-release NSO, query `geography_name_vi` và `geography_level='UNMAPPED'`; [nso.yaml](../../contracts/silver/nso.yaml) dùng `geography_name_candidate` và enum lowercase, không có cột kia. Validator sẽ query sai schema khi đi vào nhánh này. Runtime DAG còn có [silver.audit](../../src/silver/audit.py) riêng; không quy lỗi query utility thành bằng chứng DAG thất bại.

H02 sửa query theo contract và kiểm tra chính sách NSO unknown/provisional/missing thực tế. H03 sửa package-safe helper imports, test dependencies và fixture/temp strategy có thể tái lập; không che lỗi bằng skip toàn bộ tests hay chạy admin để vượt ACL.

### P1 — Recovery và source snapshot semantics cần xác minh sau refactor

[Bronze writer](../../src/ingestion/storage/bronze_writer.py) preflight trước DELETE, nhưng replace chunk vẫn DELETE → APPEND hai thao tác; lỗi giữa chừng có thể để thiếu chunk đến khi phục hồi. [FullLoader](../../src/ingestion/core/full_loader.py) có metadata exceptions bị catch/best-effort, truyền `_source_snapshot_id=0`. [IdempotencyController.register_committed_batch](../../src/ingestion/reliability/idempotency_controller.py) nay chỉ log/cập nhật object; durable snapshots vẫn phụ thuộc MetadataRepository. Chưa có runtime failure-injection tại HEAD để chứng minh retry/resume không thiếu/trùng dữ liệu.

Silver MERGE xử lý upsert và physical NOOP có điều kiện; không tự loại row đã bị rút khỏi FULL source. Cần quy định withdrawal/correction policy trước khi gọi semantics đó là snapshot replacement. H04 tập trung failure tests và policy trước, không tự rewrite toàn bộ writer.

### P1 — Chưa có nghiệm thu runtime hiện hành

Không truy cập được Docker engine. Không biết image đã build đúng locks chưa, volumes hiện chứa gì, catalog có bao nhiêu bảng, Airflow DagRuns cuối là gì, Raw nào được bind khi chạy trước đó. Không có fresh count, duplicate/null/unit audit, Silver schema readback, physical NOOP hoặc Spark/Trino parity. H05 phải bắt đầu read-only nếu runtime được owner cung cấp; việc start/ingest/replay là scope cần cho phép riêng.

## Gold chưa đủ điều kiện triển khai

Status: **`DESIGN_ONLY` cho schema/mapping; `NOT_STARTED` cho ETL**. Inventory mới của [DBML](../gold/dim_fact_v3.dbml): **27 tables = 12 Dim + 3 Xref + 8 Fact + 3 Agg + 1 Forecasting mart**. [Mapping CSV](../gold/design_audit_2026-10-22_field_mapping.csv) có 261 rows; đó là field audit, không phải approved executable mapping.

| Vấn đề đối chiếu trực tiếp | Tác động | Việc cần trước ETL |
|---|---|---|
| Gold assumptions dùng `silver`, local pipeline dùng `silver_local` | Đọc nhầm namespace hoặc không có table | Explicit input namespace/profile |
| NSO enum `QUANTITY`, `COUNTRY`, `PROVINCE_CITY` và các geo/preliminary fields/seasons không khớp contract lowercase/current columns | Filter rỗng, query lỗi, sai classification | Rewrite mappings theo [nso.yaml](../../contracts/silver/nso.yaml), giữ nguồn chứng minh |
| Official province/region/boundary history và transition weights không có trong Silver; geography_code/boundary_version NULL có chủ đích | Không derive được official geo keys hoặc phân bổ lịch sử tin cậy | Owner cung cấp/duyệt crosswalk/version; không tạo weights bằng giả định |
| National NSO fact không bảo toàn `source_table` của Silver business key | Gộp các publication/grain khác nhau; nguy cơ double counting | Chốt publication precedence hoặc giữ source grain rõ ràng |
| Daily VN fact PK date/province/variety/price_type thiếu market/source/unit/currency so với quote key Silver | Grain collision, mất quotes hoặc average không có quy tắc | Redesign grain theo quote hoặc aggregation policy có chứng cứ |
| SUA dùng các codes `5123/5131/5153/5170` thay các codes thực `5016/5023/5165/5166`; có field đòi elements chưa có | Sai loss/processed/other/residual mappings; field không derive được | Audit source elements, NULL/unsupported rõ, không suy đoán closing stock/nutrition |
| NSO quantities và PSD milling fraction đã scale tại Silver | Double conversion làm sai độ lớn | Unit matrix: source unit → Silver unit → Gold unit, convert đúng một lần |
| Country/product/price benchmark identities và crop/calendar year còn khác hệ quy chiếu | Gộp sai commodity, quốc gia, time basis | Crosswalk và benchmark/time policies tách biệt |
| Forecast mart dùng targets tương lai nhưng thiếu availability/vintage/PIT policy | LEAD row kế tiếp không đảm bảo đúng tháng, dễ target leakage | Chốt calendar, horizon, as-of availability và split; không tuyên bố leakage-free |

Chi tiết tham khảo: [audit 22/10](../gold/design_audit_2026-10-22.md), [schema/mapping audit](../gold/design_audit_2026-10-22_schema_and_mapping.md), [evidence cũ](../gold/audit_2026-10-22_evidence.json). Những lỗi static ở bảng trên được đối chiếu lại với HEAD; các counts/profile results nằm trong evidence 22/10 vẫn là **historical**, không phải row scans mới. Đề xuất Gold MVP trong audit cũ chưa phải quyết định triển khai của owner.

## Drift và khả năng bàn giao

| Evidence hiện hành | Nguyên nhân / tác động / cách xử lý |
|---|---|
| [senior_de_review.md](../../senior_de_review.md) mô tả BronzeStorageLayout/MinIO Raw archive | Review đã lệch refactor `34b4003`; không dùng làm truth của active path. Giữ như historical reference đến khi owner cho cleanup |
| [schemas.md](../silver/schemas.md), [data_catalog.md](../silver/data_catalog.md), [cleaning_rules.md](../silver/cleaning_rules.md) còn NSO fields/enums cũ | Dễ viết SQL/Gold sai. SUA docs nói residual `5166` âm bị quarantine trong khi contract cho signed values/log policy. Sửa docs dựa trên contracts và tests |
| [validate_silver_local_snapshots.py](../../scripts/validate_silver_local_snapshots.py) và [isolated integration test](../../tests/integration/test_silver_pipeline_isolated.py) còn nhắc `compose.silver-test.yml` đã bị xóa | Không dùng làm canonical local workflow; script còn có chế độ rebuild/write, không chạy thử mù. Rà từng reference; negative test cố ý nhắc old IDs không phải bug rename |
| [query_silver_usda.py](../../query_silver_usda.py) dùng namespace/table/config cũ | Utility legacy có thể bypass profile; ưu tiên canonical runner/Trino, không chạy tiện ích này làm evidence |
| `.env.local` ignored và không được helpers đọc; fixed ports trái kỳ vọng override template | Đổi file/port có thể không có tác dụng hoặc bị guard từ chối. Chốt một env workflow và port policy |
| [`.gitignore`](../../.gitignore) có `docs/*`, chưa allowlist `docs/handoff/` | Handoff tồn tại local nhưng không tự vào Git. Lần sau owner chọn track hoặc chuyển file thủ công; audit không sửa config hay stage files |
| Trade Raw bị ignore, SSH alias riêng máy, absolute Raw bind, Windows/WSL memory probes | Clean clone chưa đủ chạy; cung cấp data pack theo manifest và checklist máy nhận, không hardcode lại đường dẫn máy hiện tại |
| [Dockerfiles](../../docker/) và [locks](../../requirements/) khác Python host | Chưa build/probe image; version assertions trong Dockerfiles không phải evidence image hiện hành chạy đúng. Không khẳng định dependency mismatch chỉ từ host |
| [compose.yaml](../../compose.yaml): 8,384 MiB caps dài hạn, init 768 MiB | Chưa đo thực tế; giữ jobs tuần tự, tránh full scans song song trên máy 16 GB. Không tự tăng RAM/parallelism |
| Reset có project/idle/confirmation/external-volume guards trong source | Không phát hiện lý do phải bypass guards; destructive safety vẫn chưa được verify runtime. `stop` còn pause DAGs nên không phải read-only |

Secret review chỉ là pattern screening có giới hạn. Dev credential defaults hiện được track trong config, template và utility; không nhầm chúng với private credentials đã kiểm tra toàn diện. Không đưa giá trị vào ba handoff files, không mở local secrets để chép vào evidence.

## Chưa xác minh và việc còn lại

**Historical context, không phải kết quả mới:** 9 Bronze/9 Silver tables, khoảng 1,414,527 Silver observations, 23,060 NSO observations, 17 dead letters và FULL/INCREMENTAL physical NOOP từng được báo cáo. Chưa có fresh runtime evidence tại HEAD để xác nhận lại bất kỳ count/NOOP claim nào.

Chưa chạy: full unit suite thành công, integration suite, Spark transformations trên fixtures, Docker image builds, lifecycle operations, DAG import qua Airflow thực, HTTP readiness, SQL catalog/data scans, E2E/replay/failure-injection, checksum toàn Raw, Gold DBML official parser hoặc Gold ETL. Không tạo datasets/output evidence files riêng.

Mốc tiếp theo: xử lý diagnostic giả H01 → verifier H02 → test environment/imports H03 → recovery H04 → local acceptance H05. Sau đó mới chốt Gold mapping H06 và phát triển phần đã được owner duyệt. Portability H07 là điều kiện bàn giao máy khác. Cleanup legacy chưa được ưu tiên trước correctness. Xem task IDs, dependencies và acceptance cụ thể trong [NEXT_STEPS](NEXT_STEPS.md).
