# Rebuild và triển khai Silver

## Release V2: SILVER_V2_8_SOURCES_CLOSED_MANUAL_ONLY

Owner đã phê duyệt release closure cho đúng tám nguồn ngày 15/10/2026. Fresh
read-only verification và disarm runtime PASS. Trạng thái cuối thực tế:

| Hạng mục | Kết quả |
|---|---|
| SHARED_ACCEPTED | 8/8 |
| Fresh Raw → Bronze → Silver → Trino rebuild | PASS |
| Shared Iceberg/Trino, Airflow canary, Bronze-trigger-Silver E2E | PASS |
| Representative nonzero incremental E2E | PASS |
| SILVER_ACCEPTED_RELEASE | silver_v2_release_20261013 |
| Production / canary flags | 0 / 1 |
| Bronze / Silver DAG | paused / paused; schedule=None; cron trống |
| Automatic Bronze-trigger-Silver | Disarmed; không có trigger task |
| Silver / quarantine | 1.391.467 rows / 4 logical records |
| Catalog / PostgreSQL ingestion runs | 29 identities không đổi / 29 runs |
| Active/queued runs, Spark jobs, source locks | 0 |
| NSO V2 | DRAFT / excluded |
| Production scheduling | NOT_ENABLED |
| Consumer/Gold cutover | NOT_PERFORMED |

Đúng hai Airflow services được recreate để disarm bằng immutable image đã nghiệm
thu; Spark, PostgreSQL, MinIO, REST Catalog, Trino và volumes giữ nguyên. Actual
flags, parse/imports, serialized scope tám nguồn, paused/manual-only và health
PASS. Data, quarantine audit identity và PostgreSQL/Trino histories không đổi
so với activation canary; không trigger hoặc unpause DAG trong bước closure.

Record cuối: [closure.json](evidence/release_20261013/closure.json), gồm immutable
image references, identities, metadata hashes và committed file manifest. Giữ
nguyên byte/SHA của cả chín historical evidence artifacts; `.gitattributes`
ngăn Git normalize CRLF/LF cho evidence. Không sửa transformers/contracts/NSO V2.

Git release là một local commit có message
`feat(silver): finalize v2 eight-source accepted release`. Resolve SHA của chính
commit chứa record bằng:

```powershell
git log -1 --format=%H -- docs/silver/evidence/release_20261013/closure.json
```

SHA cụ thể được ghi sau commit trong local receipt
`$(git rev-parse --git-dir)/silver-v2-release-closure.json` và báo cáo owner;
commit không thể chứa SHA của chính nó. Không amend, push, merge, remote tag
hoặc publish image. Các mục deployment/canary phía dưới là lịch sử, không phải
trạng thái runtime hiện tại. Tám nguồn đã đóng phát triển; chỉ mở lại nếu có lỗi mới.

Periodic scheduling vẫn cần business cron/timezone/SLA và approval activation/
unpause riêng sau fresh preflight; chỉ Bronze sở hữu cron, Silver trigger-only.
Chưa có consumer/Gold cutover được duyệt.

### Lịch sử nghiệm thu trước closure

Activation canary được owner phê duyệt và đã hoàn tất ngày 15/10/2026: đúng một
manual Bronze batch tự trigger đúng một Silver run, cả hai SUCCESS. Cả hai DAG
hiện **paused**, schedule=None; periodic scheduling và consumer cutover chưa
được phê duyệt. Evidence: [activation_canary.json](evidence/release_20261013/activation_canary.json)
và [sanitized logs](evidence/release_20261013/activation_canary_logs.zip).

Code orchestration và execution images đã triển khai theo approval deployment-only,
**DEPLOYMENT_PASS** ngày 15/10/2026. Bộ **89 test cases PASS** thuộc bước chuẩn bị;
lần deploy này kiểm tra runtime thực tế và read-only readback, không chạy writer.
Đúng ba execution services được recreate, release flags đã khôi phục. Cả hai DAG
đã giữ paused, schedule=None, production=0/canary=1 ở bước deployment. Activation
canary sau đó dùng production=1/canary=0, cron trống; cuối test pause lại hai DAG.
Evidence bền vững: [deployment.json](evidence/release_20261013/deployment.json).
Ở cuối activation canary chưa recreate để reset flags; bước closure phía trên
đã disarm về production=0/canary=1. Scheduling định kỳ, chuyển consumer hoặc
cấu hình runtime tiếp theo vẫn cần approval riêng.

Namespace được duyệt: `iceberg.silver_v2_release_20261013`. Tám YAML READY và
routing CLI mặc định giữ nguyên. Chỉ cờ `--release silver_v2_release_20261013`
mới route tám source được allowlist sang namespace này, gồm cả dead letters.
NSO legacy, NSO V2 DRAFT và Trade fixture không thuộc release.

- **Fresh rebuild tám nguồn:** Raw đầy đủ → Bronze mới → Silver mới trong môi trường mới được duyệt.
- **Silver-only:** giữ Bronze và PostgreSQL, dựng target Silver mới từ approved snapshot/run identity.
- **Full Raw → Bronze → Silver:** cần đủ artifacts và successful ingestion, không tự reset metadata.
- **Legacy NSO recovery:** chỉ áp dụng cho sự cố lịch sử ở phần riêng bên dưới.

Shared rollout dùng Silver-only cho bảy nguồn. Trade đã được owner duyệt bổ sung
Raw và ingest riêng ngày 15/10/2026. File external ở `data_main/raw/faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv`
có 167.990.400 bytes, SHA-256
`697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
Đã copy, giữ bản gốc, vào cùng relative path dưới `data/raw/`; SHA/bytes ở
host và `/opt/airflow/data/raw/` khớp. Đã ingest đúng `faostat_trade`, không dùng
fixture 2.000 rows hoặc force bảy nguồn khác. Bước Trade chỉ ghi Bronze; Silver
được phê duyệt và nghiệm thu kỹ thuật riêng ở phần kết quả shared bên dưới.

Các đoạn runtime `/tmp` dưới đây ghi nhận giai đoạn CLI trước canary. Runtime
hiện tại đã dùng override/mount được duyệt ở phần Airflow canary; không chạy lại
các bước chuẩn bị lịch sử hoặc `--execute` với preflight cũ.

Spark runtime cần PyIceberg, `psycopg2-binary`, và `INGESTION_DB_URL` shared.
Dependency có thể cài từ wheel đã kiểm tra vào `/tmp/silver-release-deps` khi
owner cho phép; không recreate services. Không có unlocked fallback.
Shared Spark hiện chưa mount registry ingestion; copy nguyên bản registry vào
`/tmp/silver-release-source-registry.yaml` khi owner cho phép, giữ SHA-256 và
truyền `--registry` bên dưới. Đây là cấu hình runtime, không phải ingestion/reset.

Ngày 15/10/2026: đã chuẩn bị psycopg2-binary 2.9.10 trong thư mục private trên,
registry SHA-256 `9f37db79754fd9669d0d8e1b621b4c7b2cbbe7371eb51c7ec55102bf79bacda6`.
Cấu hình DB chỉ nằm trong `/tmp/silver-release-runtime.json` mode 0600, lấy từ
environment hiện hữu; không đưa credentials vào lệnh, log hoặc global API env.
Preflight bảy nguồn trước ingestion đạt `PREFLIGHT_PASSED`, report tại
`/tmp/silver-v2-preflight-seven-20261015.json`. Baseline lúc đó: 19 bảng hiện hữu,
12 ingestion runs, chưa có Trade Bronze hoặc namespace V2; hai DAG paused.
Trino SELECT 1 và Spark API `/health` cổng 5005 PASS, API đã quảng bá đúng
release capability. Kết quả sau bước Trade được ghi ngay bên dưới.
Runtime PASS ở đây áp dụng cho CLI launcher có child environment riêng. Trước
Airflow cutover, worker/API child process vẫn cần được cấu hình dependency path
và DB environment được duyệt; health capability không chứng minh job write PASS.

### Trade shared Bronze — đã chạy theo approval riêng ngày 15/10/2026

Block lệnh dưới đây đã chạy thành công, exit 0: copy một Raw file (giữ bản gốc),
tạo `iceberg.bronze.faostat_trade`, ghi artifacts/warehouse và metadata ingestion
của riêng Trade. Không force các nguồn khác, không ghi Silver hoặc trigger Airflow.
**Không chạy lại block này:** file/table hiện đã tồn tại; không overwrite hoặc retry.

- Ingestion và PostgreSQL: **SUCCESS**, 1.226.470 rows, checksum khớp Raw.
- Run ID: `faostat_trade_20261015080856_b62b4b96`.
- UUID: `943d69c1-1d8d-4be0-bfec-d3cae26f24f1`; schema ID: `0`.
- Current Iceberg snapshot: `642330937032309860`.
- Schema 24 columns (16 source + 8 lineage); PyIceberg/Trino types khớp;
  Trino readback pinned snapshot xác nhận count, run/checksum/source và không có NULL lineage.
- Final readback sau preflight: identities (UUID/snapshot/metadata/location/schema)
  của cả 19 bảng trước đó không đổi; 12 ingestion runs cũ không đổi, tổng nay 13.
  Hai DAG vẫn paused, không có advisory lock còn giữ, namespace Silver V2 chưa tồn tại.

Fresh preflight read-only **8/8 PASS**, hoàn tất `2026-10-15T08:12:37.885338+00:00`.
Report: `spark-iceberg:/tmp/silver-v2-preflight.json`; code fingerprint
`bc50ba29c2af37f4df2b949c71138d6755852a23bec68c3b0ddf2221af834711`.
Tám targets đều chưa tồn tại; routing target/quarantine vẫn là
`iceberg.silver_v2_release_20261013` / `iceberg.silver_v2_release_20261013.dead_letters`.
Tại thời điểm preflight chưa chạy `--execute`; owner đã duyệt report này để
chạy nghiệm thu kỹ thuật ở bước sau. Owner acceptance được ghi nhận riêng ở
phần kết quả shared bên dưới; giữ nguyên report preflight lịch sử.

| Source | Bronze rows | Bronze Iceberg read snapshot ID | Preflight |
|---|---:|---|---|
| faostat_production | 22.738 | `2102081992938536361` | PASS |
| faostat_monthly_price | 15.334 | `6157409281295192226` | PASS |
| faostat_supply_utilization | 36.380 | `2304096287387620151` | PASS |
| faostat_trade | 1.226.470 | `642330937032309860` | PASS |
| usda_psd | 15 | `8466254684668503650` | PASS |
| usda_rice_yearbook | 13.131 | `1548408886735471323` | PASS |
| worldbank_pinksheet | 792 | `2882972560001480195` | PASS |
| thitruongnongsan | 20.394 | `7501198324700066346` | PASS |

Đây là actual Iceberg read snapshot IDs; không thay thế field lineage
`_source_snapshot_id` bằng các ID này. Không có blocker kỹ thuật trong Bronze/
preflight. Owner đã duyệt acceptance và canary; production cutover vẫn cần
approval riêng.

```powershell
$ErrorActionPreference = 'Stop'
$tradeSource = 'C:\Users\hoang\source\repos\final term\tieu luan chuyen nganh\data_main\raw\faostat\Trade_DetailedTradeMatrix_E_All_Data_(Normalized)\rice_trade_data.csv'
$tradeTarget = Join-Path (Get-Location).Path 'data\raw\faostat\Trade_DetailedTradeMatrix_E_All_Data_(Normalized)\rice_trade_data.csv'
$tradeSha = '697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168'
if ((Get-Item -LiteralPath $tradeSource).Length -ne 167990400 -or (Get-FileHash -LiteralPath $tradeSource -Algorithm SHA256).Hash.ToLowerInvariant() -ne $tradeSha) { throw 'Source Trade differs from approved artifact' }
if (Test-Path -LiteralPath $tradeTarget) { throw 'Trade destination already exists; inspect before continuing' }
New-Item -ItemType Directory -Path (Split-Path -Parent $tradeTarget) -Force | Out-Null
Copy-Item -LiteralPath $tradeSource -Destination $tradeTarget
if ((Get-Item -LiteralPath $tradeTarget).Length -ne 167990400 -or (Get-FileHash -LiteralPath $tradeTarget -Algorithm SHA256).Hash.ToLowerInvariant() -ne $tradeSha) { throw 'Copied Trade integrity check failed' }
@'
import csv, hashlib, json, sys
from pathlib import Path
sys.path[:0] = ['/opt/airflow/src', '/opt/airflow']
from ingestion.core.ingestion_engine import IngestionEngine
from ingestion.storage.bronze_writer import sanitize_column_name
from jobs.silver.verify_silver_trino import query
path = Path('/opt/airflow/data/raw/faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv')
sha = '697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168'
assert path.stat().st_size == 167990400
with path.open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == sha
with path.open(encoding='utf-8-sig', newline='') as stream:
    reader = csv.reader(stream)
    raw_columns = [sanitize_column_name(c) for c in next(reader)]
    raw_rows = sum(1 for _ in reader)
engine = IngestionEngine.create_default()
assert Path(engine.registry.get_source('faostat_trade').local_path).resolve() == path
catalog = engine.bronze_writer.get_iceberg_catalog()
assert not catalog.table_exists(('bronze', 'faostat_trade')), 'Inspect existing Trade before retry'
def identities():
    result = {}
    for namespace in catalog.list_namespaces():
        for name in catalog.list_tables(namespace):
            if name == ('bronze', 'faostat_trade'):
                continue
            table = catalog.load_table(name)
            snapshot = table.current_snapshot()
            result['.'.join(name)] = (str(table.metadata.table_uuid), table.metadata_location,
                                     str(snapshot.snapshot_id) if snapshot else None)
    return result
before = identities()
result = engine.run_with_readiness_check('faostat_trade')
assert result.status.value == 'SUCCESS', 'Read catalog/commit state before any retry'
assert result.records_extracted == raw_rows and result.checksum == sha
with engine.metadata_repo.source_lock('faostat_trade'):
    table = catalog.load_table(('bronze', 'faostat_trade'))
    snapshot = table.current_snapshot()
    assert snapshot and int(snapshot.summary['total-records']) == raw_rows
    fields = {f.name: str(f.field_type) for f in table.schema().fields}
    audit = {'_ingestion_run_id': 'string', '_ingestion_batch_id': 'string',
             '_ingestion_chunk_id': 'long', '_ingestion_timestamp': 'timestamptz',
             '_source_id': 'string', '_source_file': 'string',
             '_source_checksum': 'string', '_source_snapshot_id': 'long'}
    assert set(fields) == set(raw_columns) | set(audit)
    assert all(fields[c] == kind for c, kind in audit.items())
    nulls = ' OR '.join(f'"{c}" IS NULL' for c in audit)
    readback = query('http://trino:8080', f'''SELECT count(*) AS rows,
        count_if({nulls}) AS invalid_lineage,
        count(DISTINCT _ingestion_run_id) AS runs,
        min(_ingestion_run_id) AS run_id, min(_source_checksum) AS checksum,
        count_if(_source_id <> 'faostat_trade' OR _source_checksum <> '{sha}'
          OR _source_file <> '{path.name}') AS invalid_source
        FROM iceberg.bronze.faostat_trade FOR VERSION AS OF {snapshot.snapshot_id}''')[0]
    assert readback['rows'] == raw_rows and readback['runs'] == 1
    assert readback['invalid_lineage'] == readback['invalid_source'] == 0
    assert readback['run_id'] == result.run_id and readback['checksum'] == sha
    assert identities() == before, 'Other shared table identities changed; stop'
    print(json.dumps({'status': 'BRONZE_VERIFIED', 'target': 'iceberg.bronze.faostat_trade',
          'rows': raw_rows, 'run_id': result.run_id, 'uuid': str(table.metadata.table_uuid),
          'snapshot_id': str(snapshot.snapshot_id), 'schema': fields,
          'other_tables_unchanged': True}))
'@ | docker exec -i airflow-scheduler python -
if ($LASTEXITCODE -ne 0) { throw 'Trade operation stopped; inspect actual commit state before retry' }
```

Sau ingestion đã chạy preflight đủ tám nguồn bằng lệnh bên dưới. Owner cần
duyệt report trước Silver write; approval Trade không bao gồm Silver/cutover.

Block preflight read-only sau đã chạy thành công; giữ report để owner review.
Nếu code/source/target thay đổi, tạo fresh report và xin duyệt lại trước write:

```powershell
$releaseSources = 'faostat_production,faostat_monthly_price,faostat_supply_utilization,faostat_trade,usda_psd,usda_rice_yearbook,worldbank_pinksheet,thitruongnongsan'
$releaseLauncher = @'
import json, os, subprocess, sys
from pathlib import Path
env = os.environ.copy()
env.update(json.loads(Path('/tmp/silver-release-runtime.json').read_text()))
env['PYTHONPATH'] = '/tmp/silver-release-deps:/home/iceberg/src:/home/iceberg/jobs/silver'
command = ['/opt/spark/bin/spark-submit', '--driver-memory', '768m',
           '/home/iceberg/jobs/silver/validate_silver_e2e.py',
           '--release', 'silver_v2_release_20261013', '--dataset', sys.argv[1],
           '--registry', '/tmp/silver-release-source-registry.yaml', *sys.argv[2:]]
raise SystemExit(subprocess.run(command, env=env).returncode)
'@
$releaseLauncher | docker exec -i spark-iceberg python - $releaseSources --preflight --output-json /tmp/silver-v2-preflight.json
```

Lệnh sau đã chạy theo approval Shared Silver ngày 15/10/2026. Không chạy lại
với preflight cũ có targets chưa tồn tại; phải kiểm tra actual state trước retry:

```powershell
$releaseLauncher | docker exec -i spark-iceberg python - $releaseSources --execute --approved-preflight /tmp/silver-v2-preflight.json --output-json /tmp/silver-v2-acceptance.json
```

Preflight kiểm tra mọi input trước write: Raw/PG/Bronze checksum, complete run
identity, current snapshot/UUID, target schema, legacy identities, code fingerprint.
Execute lấy lại source lock, kiểm tra drift, pin approved Bronze snapshot;
giữ lock qua write, outcome readback, Spark/PyIceberg/Trino checks và full/incremental
NOOP dùng real run ID. Counts lấy từ shared run thực tế. Unknown outcome cần
catalog readback trước retry; không automatic retry hoặc reset.

Kết quả đạt checks là `VALIDATED_PENDING_OWNER`, chưa phải `SHARED_ACCEPTED`.
Giữ hai DAG paused. Ingestion DAG chỉ ghi Bronze; Silver DAG sở hữu Silver scheduling.
Routing DAG V2 chỉ bật sau owner cutover qua
`SILVER_ACCEPTED_RELEASE=silver_v2_release_20261013`, không qua DAG conf.
Đặt flag ở cả hai DAG runtimes: Bronze DAG khi đó chỉ ingest đúng tám nguồn,
loại NSO legacy/DRAFT và Trade fixture. Chưa đặt flag trên shared.
DAG phát `DQ_PASSED`, không phát `READY_FOR_GOLD` hoặc chạy Gold.

Target basenames: `faostat_production`, `faostat_monthly_price`,
`faostat_supply_utilization`, `faostat_trade`, `usda_rice_psd`,
`usda_rice_yearbook`, `worldbank_commodity_monthly`, `thitruongnongsan`;
quarantine: `dead_letters`. Bảng legacy giữ nguyên.

### Kết quả Shared Silver V2 — SHARED_ACCEPTED theo owner approval

Đã chạy đúng release validator `--execute --approved-preflight`, exit 0,
từ `2026-10-15T08:20:57.665532+00:00` đến `2026-10-15T08:26:40.191780+00:00`.
Report kỹ thuật gốc giữ trạng thái **VALIDATED_PENDING_OWNER** tại thời điểm chạy.
Owner đã phê duyệt acceptance cho đúng tám nguồn; trạng thái hồ sơ release hiện
tại là **SHARED_ACCEPTED**. Không sửa report gốc để tạo cảm giác validator tự
cấp owner acceptance. Bản gốc runtime: `spark-iceberg:/tmp/silver-v2-acceptance.json`.
Evidence bền vững trong checkout (không phụ thuộc lifecycle container):

- [Approved preflight nguyên bản](evidence/release_20261013/preflight.json), SHA-256
  `76cbbc7cf6884ced13dc74be1e85fb04aa85ec780ea4ec47d829433c5f9a4718`.
- [Technical acceptance nguyên bản](evidence/release_20261013/acceptance.json), SHA-256
  `7bebe47c87f3943b3b59fa292d7686cd0de72caa6dc85c6f00ce2bf6b1cac2ee`.

Đã đối chiếu byte/hash của cả hai bản copy. Chưa commit/push theo giới hạn owner;
đây là evidence trên filesystem workspace, chưa phải bản backup remote.
Approved preflight giữ nguyên, SHA-256
`76cbbc7cf6884ced13dc74be1e85fb04aa85ec780ea4ec47d829433c5f9a4718`;
code fingerprint không đổi so với preflight.

Targets trong bảng đều có prefix `iceberg.silver_v2_release_20261013.`.
Counts là số đo shared thực tế; valid tính trước dedup. Business keys unique
bằng Silver rows ở cả tám nguồn. Schema Spark/PyIceberg/Trino, values, units,
missing reasons, flags và tám Bronze lineage fields đều qua validator.

| Target | Valid | Quarantine | Excluded | Duplicates | Silver rows | Trino | Full NOOP | Incremental NOOP |
|---|---:|---:|---:|---:|---:|---|---|---|
| faostat_production | 22.738 | 0 | 0 | 0 | 22.738 | PASS | PASS | PASS |
| faostat_monthly_price | 15.334 | 0 | 0 | 0 | 15.334 | PASS | PASS | PASS |
| faostat_supply_utilization | 36.380 | 0 | 0 | 0 | 36.380 | PASS | PASS | PASS |
| faostat_trade | 1.226.470 | 0 | 0 | 0 | 1.226.470 | PASS | PASS | PASS |
| usda_rice_psd | 990 | 0 | 0 | 0 | 990 | PASS | PASS | PASS |
| usda_rice_yearbook | 13.131 | 0 | 0 | 195 | 12.936 | PASS | PASS | PASS |
| worldbank_commodity_monthly | 56.232 | 0 | 13.464 | 0 | 56.232 | PASS | PASS | PASS |
| thitruongnongsan | 20.390 | 4 | 0 | 3 | 20.387 | PASS | PASS | PASS |

Tổng Silver: **1.391.467 rows**. SUA có 236 warnings theo policy Residuals âm;
giá nông sản có 29 warnings. World Bank excluded là các observations từ cột
synthetic rỗng, được đối soát riêng, không phải quarantine.

| Target | UUID | Current snapshot sau full/incremental NOOP |
|---|---|---|
| faostat_production | `10c64549-dcee-47c2-919e-a58cff5f091e` | `7833012846717654768` |
| faostat_monthly_price | `99f6b25e-14e2-4cea-a181-0bbea2357526` | `8339508013034672563` |
| faostat_supply_utilization | `5990fc5f-ef32-4dc8-a232-565387336bfd` | `6996323947372016333` |
| faostat_trade | `61823cf7-5c7f-4c85-9028-32157be45573` | `1757023266116193085` |
| usda_rice_psd | `f2417e03-5f86-43c6-b6b1-063cf0b4604f` | `2364532576229946037` |
| usda_rice_yearbook | `cb41cc38-d110-4f53-8fd7-dc837472ca13` | `2178026287867944899` |
| worldbank_commodity_monthly | `32b55281-d428-4298-82af-84f38f17f55c` | `6867808954567920629` |
| thitruongnongsan | `53c2d07b-34ee-4f83-aa91-f76c5fcfa10b` | `2921921961555442382` |

Cả tám target có schema ID `0`; commit được đọc lại từ catalog và đối chiếu
đúng UUID/schema/snapshot trong acceptance report. Full/incremental rerun giữ
snapshot/schema/row checksum (gồm processing timestamp); incremental dùng real
Bronze run identities từ approved preflight.

Quarantine được xác minh riêng qua Trino: `dead_letters` có **4 rows**, đều của
`thitruongnongsan`; UUID `5adc1ff5-c9a9-4aa3-b649-5c6d358111d6`, current snapshot
`6572719367556595160`, schema ID `0`. Silver/quarantine là các transaction riêng.

Final readback **PASS**: toàn bộ 20 bảng trước write giữ nguyên UUID/snapshot/
schema/location/metadata identity, gồm tất cả Bronze, Silver legacy và NSO.
13 PostgreSQL ingestion runs giữ nguyên, không ingest thêm Bronze. Chỉ tạo
đúng 9 bảng trong namespace V2 (8 targets + dead_letters), không có source ngoài
scope. Hai DAG vẫn paused, không bật Airflow release flag, không giữ advisory
lock sau chạy. Không OOM/timeout/partial failure, không retry/reset, không thay
transformers/contracts/business policies/NSO V2/Gold, không commit/push/merge.

Owner final verification PASS lúc `2026-10-15T08:41:31.813575+00:00`: tám UUID/
snapshots/schema/metadata vẫn khớp report, dead_letters vẫn 4 rows với UUID và
snapshot ở trên, fingerprint không drift; 20 bảng trước release và 13 ingestion
runs không đổi. Không có queued/running DAG run, hai DAG vẫn paused.
Readback cuối sau khi chuẩn bị canary lúc `2026-10-15T09:04:01.593817+00:00`
cũng PASS: tám targets/dead_letters không drift, 20 bảng cũ và 13 ingestion runs
giữ nguyên, không active/queued DAG runs, flags dịch vụ vẫn chưa áp dụng;
Silver code fingerprint vẫn khớp acceptance report.

| Source ID | Owner release status |
|---|---|
| faostat_production | SHARED_ACCEPTED |
| faostat_monthly_price | SHARED_ACCEPTED |
| faostat_supply_utilization | SHARED_ACCEPTED |
| faostat_trade | SHARED_ACCEPTED |
| usda_psd | SHARED_ACCEPTED |
| usda_rice_yearbook | SHARED_ACCEPTED |
| worldbank_pinksheet | SHARED_ACCEPTED |
| thitruongnongsan | SHARED_ACCEPTED |

Đây là hồ sơ owner acceptance trong release documentation. Không có cơ chế
owner-acceptance chính thức trong PostgreSQL hiện hữu; không sửa ingestion
status, silver_status, source snapshots hoặc metadata DB để biểu diễn acceptance.
NSO legacy/DRAFT và fixture không thuộc receipt này.

### Airflow canary — kế hoạch được duyệt và đã thực hiện

Phần kế hoạch dưới đây ghi nhận cấu hình trước triển khai. Override và đúng ba
manual runs đã được thực hiện theo approval; xem kết quả thực tế cuối phần này.

Đã chuẩn bị `compose.silver-canary.yml`, chỉ có hiệu lực khi operator chủ động
truyền override sau approval. Hai DAG có cờ `SILVER_CANARY_ONLY=1`: `schedule=None`,
chỉ manual runs; cờ này yêu cầu đúng approved V2 release và từ chối giá trị sai.
Mặc định không đặt cờ vẫn giữ lịch hiện hữu. Trong canary, **mọi task của cả hai
DAG có retries=0**, gồm init/gate/audit; Silver transforms luôn retries=0.
Đây là guard vận hành, không thay transformations/contracts/business rules.

Airflow 2.10.4 scheduler bỏ qua paused DAGs khi cấp queued runs/tasks. Vì vậy
canary cần approval tạm unpause, và phải xác minh manual-only timetable trước
khi unpause để không tạo thêm run theo lịch tháng đang đến hạn.

Kiểm tra chuẩn bị đã PASS: compose config validation (không áp dụng), dependency
mount CPython 3.10 import psycopg2 2.9.10 trong container tạm `--network none`,
parse hai DAG ở cả scheduler/webserver với default và preview V2/manual-only.
Preview có đúng tám ingest/transform source IDs, không NSO/fixture, ingestion
không có Silver hooks, chỉ Silver DAG viết Silver, không Gold, signal `DQ_PASSED`.
Regression cuối: **4 tests PASS**, gồm lịch mặc định, scope/manual-only/retries=0
của canary, missing-release và invalid-flag rejection. Parse live cuối ở cả hai
containers xác nhận default Bronze 10 IDs/retries=2, preview Bronze/Silver đúng
8 IDs/retries=0 và schedules None; flags của services thực tế vẫn chưa được đặt.
LocalExecutor hiện tại chạy task trong scheduler; không có Celery worker riêng.

| Runtime | Hiện tại | Thay đổi cần owner phê duyệt cho canary |
|---|---|---|
| airflow-scheduler + airflow-webserver | Không có release/canary flags; Bronze default vẫn gồm 10 IDs (có NSO/fixture) | Đặt `SILVER_ACCEPTED_RELEASE=silver_v2_release_20261013`, `SILVER_CANARY_ONLY=1`; load lại DAG serialization thành 8 sources, lịch None |
| Spark API process/children | Health/capability đúng release; thiếu DB env và psycopg2 ở default import path | Inject cùng ingestion DB secret hiện hữu của Airflow vào `INGESTION_DB_URL`; thêm dependency mount vào PYTHONPATH |
| Spark dependencies | CLI đã dùng psycopg2 2.9.10 dưới /tmp; API không kế thừa child env của CLI | Bộ đã kiểm tra được copy ra `.pytest_cache/silver_release_deps_cp310`; mount readonly tại `/opt/silver-release-deps` qua override, giữ thư mục này đến hết canary |
| Spark registry | Default path chưa mount; bản /tmp đúng checksum | Mount `./config/ingestion:/home/iceberg/config/ingestion:ro`, giữ registry SHA `9f37db79754fd9669d0d8e1b621b4c7b2cbbe7371eb51c7ec55102bf79bacda6` |
| Airflow additional packages | Startup hiện dùng openpyxl/pyiceberg chưa pin | Override pin `openpyxl==3.1.5 pyiceberg==0.12.0`, đúng phiên bản đang chạy; kiểm tra lại dependencies trước khi unpause |

Spark PYTHONPATH dự kiến:
`/opt/silver-release-deps:/home/iceberg/src:/home/iceberg/jobs:/home/iceberg/jobs/silver`.
Không đặt credentials trong override/evidence; `SILVER_INGESTION_DB_URL` được
inject vào session triển khai từ cùng secret hiện hữu, không log giá trị.
Spark routing nhận explicit release từ DAG/API request, không cần một default
release flag bật mọi CLI. Driver mặc định API hiện 1g/local[2]; CLI acceptance
đã chạy 768m. Override không thay driver memory hoặc dịch vụ storage/catalog.

Thay environment của containers cần recreate, `docker restart` đơn thuần không
nạp environment mới. Lệnh sau **đã chạy một lần theo approval canary**, sau khi
inject secret và kiểm tra dependency directory; đây là execution record, không
phải yêu cầu chạy lại:

```powershell
docker compose -p tlcn -f docker-compose.yml -f compose.silver-canary.yml up -d --no-deps --no-build --pull never --force-recreate spark-iceberg airflow-scheduler airflow-webserver
```

Chỉ ba execution services được recreate; không chọn airflow-init, PostgreSQL,
MinIO, REST hoặc Trino, không down/reset volumes. Không pull image mới. Image IDs
đã đối chiếu với containers hiện tại: Spark
`sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460`,
Airflow `sha256:5afa55b7c6087112770605f9b9851afd665f5c7f3719b390fcb4bd28a47a7f9a`.

Quy trình đã duyệt gồm 1 Bronze và 2 Silver DAG runs, tuần tự:

1. Trước runtime change, xác nhận lại accepted identities/code/checksums, không
   concurrent CLI/API writer hoặc active/queued DAG run. Giữ hai DAG paused.
2. Sau runtime change được duyệt, kiểm tra actual API child import psycopg2,
   DB connectivity + source locks, default registry SHA, image/dependency versions,
   API health/capability; parse và serialized DAG phải đúng flags/tám sources/
   `schedule=None`. Kiểm tra read-only readiness/audit qua API. Nếu lệch, dừng
   trước unpause, không tự nâng dependencies hoặc sửa evidence cho khớp runtime.
3. Tạm unpause riêng Bronze DAG và trigger một manual run, conf
   `{"force_reprocess":false}`. Cùng Raw checksum: initialize/start SUCCESS,
   tám `ingest_*` SKIPPED vì idempotency; `end_lakehouse_ingestion` SKIPPED theo
   trigger_rule all_success. DAG có thể SUCCESS với leaf SKIPPED: phải đối chiếu
   data, không dựa màu DAG. Tối đa tám ingestion run records SKIPPED mới là tác
   động metadata dự kiến; Bronze UUID/snapshot/rows và Silver phải giữ nguyên.
   Pause Bronze sau khi run kết thúc, không force nguồn khác.
4. Tạm unpause Silver DAG, manual full run conf
   `{"mode":"full","release":"silver_v2_release_20261013"}`. Gate SUCCESS,
   tám transforms SUCCESS với metrics thực tế, audit/signal/end SUCCESS.
   Snapshot/schema/rows/checksum gồm processing timestamp của tám Silver targets
   phải NOOP so với accepted baseline; Trino kiểm tra lại keys, values, units,
   flags, missing reasons và lineage theo durable acceptance report.
5. Manual incremental run trên cùng inputs, conf
   `{"mode":"incremental","release":"silver_v2_release_20261013","watermark":"2026-10-15T08:09:33.016911+00:00"}`.
   Watermark trên là actual max Bronze ingestion timestamp đo bằng Trino;
   xác nhận nó vẫn khớp trước canary. Filter hiện hữu là timestamp > watermark,
   nên delta của cả tám sources bằng 0: transforms SUCCESS với bronze/prepared/
   valid/quarantine/dedup counts 0; gate/audit/signal/end SUCCESS, tám Silver
   targets vẫn NOOP và Trino đọc đủ dữ liệu accepted. DAG conf hiện chỉ có một
   bronze_run_id chung, nên không truyền run ID của một source cho cả tám nguồn.
   Replay với real run ID riêng từng source đã PASS trong CLI acceptance.
6. Đối soát riêng dead_letters: UUID giữ nguyên, 4 logical records vẫn của giá
   nông sản. Full replay có thể cập nhật audit timestamp/pipeline_run_id và
   snapshot quarantine; không yêu cầu snapshot dead_letters NOOP như Silver.
   Bronze/legacy UUID/snapshots không đổi, không Gold/READY_FOR_GOLD. Pause Silver
   sau canary, giữ consumer routing, lưu task states/run IDs/Trino evidence vào
   hồ sơ trước khi đề nghị cutover scheduling/consumer riêng.

Khi cần Trino readback, copy durable acceptance JSON vào một path tạm trong
Spark và dùng existing `verify_silver_trino.py --spark-report ... --output-json ...`;
đồng thời đối chiếu snapshot/schema/checksum với accepted baseline. Không dùng
`validate_silver_e2e.py --execute` với preflight cũ có targets chưa tồn tại.

Rollback nếu lỗi: pause cả hai DAG, chặn run/task mới, không clear/retry writer;
đọc source lock, current catalog UUID/snapshot, commit flags và PostgreSQL/run
logs trước quyết định tiếp. Pause không tự hủy Spark job đang chạy: phải xác
nhận outcome, không tự kill/recreate hoặc coi timeout là chưa commit. Giữ
accepted namespace/data/evidence và consumer routing; không DROP/ALTER/reset
hoặc restore snapshot tự động. Nếu rollback cấu hình được duyệt, bỏ override
và khôi phục environment cũ nhưng tiếp tục giữ cả hai DAG paused, không tự chạy
Silver legacy. Acceptance dữ liệu và Airflow E2E là hai trạng thái riêng.

### Kết quả Airflow canary shared — AIRFLOW_E2E_PASS

Hoàn tất kiểm tra cuối lúc `2026-10-15T10:01:16.153989+00:00`. Evidence thực tế:
[canary.json](evidence/release_20261013/canary.json) và
[canary_logs.zip](evidence/release_20261013/canary_logs.zip) (35 log files).
Giữ nguyên hai report gốc `acceptance.json` và `preflight.json`; trạng thái owner
của đúng tám nguồn vẫn **SHARED_ACCEPTED**.

Pre-change PASS: Git HEAD `b2bb3091aaaa42f3caeb1e112653f96464e6beb0`, Silver
fingerprint `bc50ba29c2af37f4df2b949c71138d6755852a23bec68c3b0ddf2221af834711`,
29 catalog identities khớp baseline, không active/queued runs hoặc source locks.
Đã lưu rollback config, Docker runtime identities và working-tree diff trong
`.pytest_cache/silver_canary_private_20261015`, ACL riêng cho owner. Backup chứa
secret runtime, không đưa vào evidence/Git hoặc in ra log.

Runtime thực tế: chỉ recreate `spark-iceberg`, `airflow-scheduler`,
`airflow-webserver`; bốn storage/catalog/Trino services giữ nguyên container ID
và StartedAt. Đã áp dụng release/canary flags, PYTHONPATH, readonly registry và
dependency mounts, DB env từ secret hiện hữu. Actual API child import/DB/locks,
registry checksum, dependency versions và API health đều PASS. Scheduler và
metadata DB healthy; actual parsed/serialized DAGs đúng tám nguồn,
`schedule=None`, mọi task `retries=0`, không NSO/fixture/Gold. DAG regression
**4 tests PASS**, gồm guard retries=0 cho cả init/gate/audit.

| Run | DAG | Actual manual run ID | Kết quả task |
|---|---|---|---|
| Bronze | `rice_lakehouse_ingestion` | `silver_v2_canary_20261015T091831Z_bronze` | SUCCESS; init/start SUCCESS, 8 ingest SKIPPED, end SKIPPED hợp lệ |
| Silver full | `rice_lakehouse_silver_pipeline` | `silver_v2_canary_20261015T091831Z_full` | SUCCESS; 13/13 tasks SUCCESS, 8 targets NOOP |
| Silver incremental | `rice_lakehouse_silver_pipeline` | `silver_v2_canary_20261015T091831Z_incremental` | SUCCESS; 13/13 tasks SUCCESS, delta 0 cho cả 8 nguồn, 8 targets NOOP |

Đúng ba manual runs tuần tự, 37 task instances, không failed attempt hoặc retry
writer. Bronze end bị SKIPPED do all_success và không thực thi (try_number=0),
không phải ingestion failure. Watermark incremental được đọc lại bằng Trino
ngay trước trigger, vẫn là `2026-10-15T08:09:33.016911+00:00`.

Các targets dưới đây có prefix `iceberg.silver_v2_release_20261013.`. UUID,
schema ID 0, metadata identity và row checksum (gồm processing timestamp) giữ
nguyên accepted baseline sau cả full và incremental. Trino schema, keys,
values/units, flags/missing reasons và tám lineage fields PASS 8/8 ở cả hai lần.

| Target | Rows | Snapshot trước = sau full = sau incremental | Trino |
|---|---:|---|---|
| faostat_production | 22.738 | `7833012846717654768` | PASS |
| faostat_monthly_price | 15.334 | `8339508013034672563` | PASS |
| faostat_supply_utilization | 36.380 | `6996323947372016333` | PASS |
| faostat_trade | 1.226.470 | `1757023266116193085` | PASS |
| usda_rice_psd | 990 | `2364532576229946037` | PASS |
| usda_rice_yearbook | 12.936 | `2178026287867944899` | PASS |
| worldbank_commodity_monthly | 56.232 | `6867808954567920629` | PASS |
| thitruongnongsan | 20.387 | `2921921961555442382` | PASS |

Tổng Silver **1.391.467 rows**. `dead_letters` vẫn đúng **4 logical records**,
cùng UUID `5adc1ff5-c9a9-4aa3-b649-5c6d358111d6`, logical payload/errors hash
không đổi. Full replay chỉ cập nhật audit metadata theo thiết kế: snapshot từ
`6572719367556595160` thành `5285064570972783895`; incremental giữ snapshot sau
full. Không rollback snapshot quarantine.

Bronze rows và toàn bộ 20 bảng trước release giữ nguyên UUID/snapshot/schema/
location/metadata identity. PostgreSQL ingestion_runs từ 13 thành 21: đúng tám
run records SKIPPED mới, mỗi source một record; 13 records cũ không đổi. Các
bảng ingestion checkpoints/source snapshots/watermarks/dead_letter_records
không đổi. Airflow có đúng ba DagRuns, 37 TaskInstances, 23 XCom records và logs
của canary; không clear/retry writer hoặc reset metadata.

Trạng thái cuối: **cả hai DAG paused**, `schedule=None`, không queued/running
run, không giữ advisory lock, không Gold/READY_FOR_GOLD, consumer routing không
đổi. Giữ runtime canary và dependency directory; không xóa cache mount đang
được sử dụng. Không chạy lại `--execute` hoặc trigger run thứ tư.

Chưa có blocker kỹ thuật trong phạm vi canary. Bước tiếp theo cần owner duyệt
production scheduling và consumer cutover riêng: quyết định lịch/catchup,
bỏ manual-only có kiểm soát trong khi giữ V2 routing/secret/dependency mounts,
kiểm tra timetable kế tiếp trước khi unpause. Không bỏ toàn bộ override để bật
lịch vì có thể khôi phục routing/scope legacy. Chưa thực hiện các thay đổi này.

### Production readiness trước implementation — baseline lịch sử

Phần này ghi nhận review trước khi owner duyệt implementation. Các thay đổi
được đề xuất tại đây đã được thực hiện và kiểm tra trong phần kết quả tiếp theo.

Owner xác nhận **chưa chốt lịch business; chỉ chuẩn bị cấu hình**. Giữ cả hai
DAG paused/manual-only. Production BLOCKED về orchestration và khả năng tái tạo
runtime; không phủ nhận SHARED_ACCEPTED hoặc AIRFLOW_E2E_PASS của dữ liệu/canary.
Không thay DAG/runtime/services trong lần kiểm tra readiness này.

Read-only verification sau test mới: HEAD vẫn
`b2bb3091aaaa42f3caeb1e112653f96464e6beb0`, Silver fingerprint vẫn khớp report;
29 identities không đổi, Trino tám targets = 1.391.467 rows, quarantine = 4,
PostgreSQL ingestion runs = 21, không active/queued run hoặc advisory lock.
Không ghi shared, trigger/unpause, restart/recreate, commit/push hoặc sửa NSO V2.

Production preview trong process riêng (giữ release, đặt canary=0) đã PASS:

| DAG | Lịch mặc định khi bỏ manual-only | catchup | max_active_runs | max_active_tasks |
|---|---|---|---:|---:|
| rice_lakehouse_ingestion | `0 6 15 * *`, Asia/Bangkok | false | 1 | 4 |
| rice_lakehouse_silver_pipeline | `0 8 15 * *`, Asia/Bangkok | false | 1 | 1 |

Scope tám nguồn được lọc bằng `SILVER_ACCEPTED_RELEASE`, độc lập với canary flag.
Preview và regression mới xác nhận bỏ manual-only vẫn giữ đúng tám source IDs,
không NSO/fixture; DAG/API truyền explicit release và common runner route Silver
cùng dead letters sang V2. Không có hook Silver trong ingestion DAG hoặc Gold
task; chỉ Silver DAG là scheduled Silver writer dự kiến. CLI/API vẫn là lối
chạy thủ công, phải hạn chế bằng quyền vận hành và cùng source advisory lock.

**Orchestration hiện tại chưa an toàn để chỉ bật hai lịch:** hai DAG chạy độc
lập, không trigger/sensor/dependency qua DAG. Hai giờ chênh lịch không bảo đảm
Bronze xong. `check_bronze` kiểm tra schema/nonempty, không xác nhận ingestion
cycle hiện tại thành công; source vừa FAILED vẫn có thể để lại Bronze cũ hợp lệ.
Một source FAILED chỉ làm fail Bronze DAG, chưa chặn Silver DAG. Đồng thời
`run_ingestion_task` chuyển cả NOT_READY lẫn checksum SKIPPED thành task SKIPPED;
không được coi mọi SKIPPED là nguồn đã sẵn sàng.

Phương án ít thay đổi nhất, **mới là đề xuất, chưa sửa production code**:

1. Chỉ Bronze sở hữu lịch business sau khi owner chốt; Silver giữ `schedule=None`
   trong approved release và được Bronze trigger đúng một lần sau batch.
2. Giữ đúng tám enabled sources bằng assertion fail-closed khi registry thiếu,
   disabled hoặc ngoài scope. Giữ writers retries=0 cho approved release, độc
   lập với canary flag; hiện bỏ canary sẽ khôi phục Bronze writer retries=2.
3. Trong wrapper ingestion, thêm tùy chọn NOT_READY là failure cho release;
   chỉ checksum SKIPPED được đi qua gate. Bronze end dùng `none_failed` để tám
   checksum skips vẫn cho phép tiếp tục; bất kỳ FAILED/upstream_failed nào chặn
   toàn bộ batch Silver tám nguồn. Đây là lựa chọn chặn toàn batch đơn giản,
   không giả định đã có gating riêng từng source.
4. Sau end thêm existing `TriggerDagRunOperator` trỏ tới Silver DAG, conf
   `{"mode":"full","release":"silver_v2_release_20261013"}`, run ID xác định
   từ upstream run ID, `reset_dag_run=False`, `retries=0`,
   `wait_for_completion=True`. Bronze batch tiếp theo không bắt đầu khi batch
   hiện tại còn chờ Silver. Không thêm DAG hoặc writer Silver.
5. Production ban đầu dùng full có freshness-aware MERGE/NOOP đã nghiệm thu;
   không tự dùng lại watermark canary hoặc một bronze_run_id cho cả tám nguồn.
   Incremental có dữ liệu mới được kiểm chứng riêng bên dưới.

API job timeout hiện 3300s, client HTTP timeout 3600s, Silver transforms retries=0.
Runner giữ source lock qua write/outcome readback và trả recovery state khi lỗi.
Các transforms là tasks độc lập: task lỗi không tự dừng những tasks còn lại;
audit/publish bị chặn, operator phải pause cả hai DAG ngay khi partial/timeout,
đọc catalog/quarantine/locks/PG/logs trước mọi retry. Không clear writer, trigger
trùng, force reprocess hoặc rollback snapshot tự động. Chỉ resume khi xác định
được commit outcome; timeout không chứng minh transaction chưa commit.

Tham chiếu operator đúng Airflow 2.10.4:
[TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/operators/trigger_dagrun/index.html).

**Dependency durability: BLOCKED.** `.pytest_cache/silver_release_deps_cp310`
là readonly runtime mount thật, không phải release artifact bền vững; xóa cache
sẽ làm API child mất psycopg2. `_PIP_ADDITIONAL_REQUIREMENTS` cài lúc container
startup vẫn phụ thuộc package index/transitive resolution. DB secret được inject
vào phiên compose hiện tại; recreate phải lấy lại secret từ nguồn quản lý hiện
hữu. Không lấy private rollback backup làm secret delivery mechanism.

Đề xuất đóng gói hai execution images với base digest đã kiểm tra, lock đúng
runtime versions và dependency hashes, cài trong build thay vì startup/cache:

- Spark base `tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460`;
  psycopg2 2.9.10, PyIceberg 0.7.1, PyArrow 17.0.0, pandas 2.2.3, PyYAML 6.0.2.
- Airflow base `apache/airflow@sha256:5afa55b7c6087112770605f9b9851afd665f5c7f3719b390fcb4bd28a47a7f9a`;
  Airflow 2.10.4, psycopg2 2.9.10, PyIceberg 0.12.0, PyArrow 16.1.0,
  pandas 2.1.4, PyYAML 6.0.2, openpyxl 3.1.5, lxml 5.3.0. Lock transitive
  dependencies từ runtime đã kiểm tra; không tự nâng phiên bản để build dễ hơn.
- Production override giữ `SILVER_ACCEPTED_RELEASE` cố định, explicit canary=0
  chỉ sau approval, readonly registry mount và required DB secret interpolation.
  Giữ PYTHONPATH trỏ tới source/jobs; bỏ cache path sau khi dependency có trong
  image. Không ghi secret vào Dockerfile/image/compose/evidence; inject từ secret
  store/env hiện hữu ở deployment, không dùng credential fallback mặc định.

Monitoring finding riêng: shared Trino SQL PASS nhưng Docker health unhealthy.
Actual `/etc/trino/config.properties` có `http-server.http.port=8080\r`;
health script dùng grep/cut không strip CR, tạo URL sai (`curl: (3)`). Đề xuất
chuẩn hóa LF cho đúng file config này và enforce LF bằng `.gitattributes`, không
đổi Trino version/port/memory/catalog. Chưa sửa bind-mounted config/runtime.
Không normalize registry hoặc Raw có checksum đã duyệt cùng thay đổi này.

**Real incremental E2E: PASS, một source representative và common framework.**
Test mới trong `tests/integration/test_silver_pipeline_isolated.py` dùng adapter
FAOSTAT Production thật, PostgreSQL/MinIO/REST/Trino test có sẵn, Raw CSV sinh
từ fixture FAOSTAT đã kiểm chứng. Runner chỉ nối isolated internal network,
workspace readonly, database/namespace mới; không nối network hoặc ghi shared.
Giữ nguyên snapshot/rows của các bảng isolated rebuild trước đó. Không suy diễn
đây là nonzero incremental test cho cả tám transformer hoặc Airflow E2E mới.

| Check | Actual result |
|---|---|
| Raw revisions | 2 rows baseline → 5 rows revised full artifact; checksum khác nhau |
| New observation | Năm 2021, value 20.00 |
| Changed observation | Năm 2020, 12.50 → 15.50 theo ingestion timestamp mới hơn |
| Dedup/DQ | 4 valid trước dedup, 1 duplicate, 1 quarantine giá trị âm |
| Bronze | Append-only 2 → 7 rows, snapshot `1197141491795587222` → `6426987507485842038`; old pinned snapshot vẫn 2 rows |
| Silver incremental | Filter bằng real run ID `faostat_production_20261015102330_ba37c22a`; prepared 5, dedup 3, target 3 unique keys |
| Silver snapshot | `9195611327046029348` → `9079120738595403269`, UUID giữ nguyên |
| Lineage/units/values | Tám lineage fields khớp Bronze; source checksum/run đúng revision mới; Spark/Trino PASS |
| Replay | Full và incremental NOOP giữ cả snapshot/schema/row checksum; Bronze replay SKIPPED |

Log actual: [incremental_new_raw.log](evidence/release_20261013/incremental_new_raw.log).
Test DB `silver_incremental_4b4c3bb20faf`, namespace
`rebuild_test.incremental_4b4c3bb20faf`; test objects được giữ, không reset/drop.
Test session phải `refreshTable` sau PyIceberg external commit; production API
mỗi request khởi tạo Spark job mới. Regression DAG **5 PASS** (thêm production
preview), incremental E2E **1 PASS**. Không chạy lại bộ 84 tests hoặc canary.

Chạy lại riêng test khi independent test services đã chạy (không tự start/
recreate chúng; mỗi test tạo namespace/database mới):

```powershell
$env:SILVER_TEST_RAW_ROOT = (Resolve-Path 'data/raw').Path
$incrementalTestCommand = 'python3 -m pip install --quiet --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/incremental-deps pytest==8.4.2 psycopg2-binary==2.9.10 boto3==1.35.99 && export PYTHONPATH=/tmp/incremental-deps:/workspace/src:/workspace:/workspace/jobs/silver:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip && python3 -m pytest tests/integration/test_silver_pipeline_isolated.py::test_new_raw_incremental_revisions_e2e -q -s -p no:cacheprovider'
docker compose -p tlcn-silver-fresh-20261015 -f compose.silver-test.yml run --rm --no-deps --entrypoint /bin/bash runner -c $incrementalTestCommand
```

**Exact minimal files còn cần chuẩn bị sau approval phương án:**
`dags/dag_ingestion_pipeline.py` (batch gate, exact scope, no automatic writer
retry, trigger); `dags/dag_silver_pipeline.py` (production trigger-only);
`src/ingestion/orchestration/pipeline_tasks.py` (release NOT_READY failure);
`tests/unit/test_dag_structure.py` và `tests/unit/test_pipeline_tasks.py`
(success/checksum-skip/NOT_READY/FAILED/duplicate-trigger behavior).
Đóng gói runtime có thể dùng hai Dockerfiles `docker/silver-spark.Dockerfile`,
`docker/silver-airflow.Dockerfile`, hai lock manifests dưới `requirements/`, và
inactive `compose.silver-production.yml`. Trino health fix chỉ cần LF của
`config/trino/etc/config.properties` cùng `.gitattributes`. Các production file
mới/thay đổi này **chưa được tạo hoặc áp dụng**; không sửa transformers/contracts.

**Git release plan:** HEAD chưa chứa code đã nghiệm thu; working tree có 14
tracked changes từ release preparation và untracked canary override/evidence.
Giữ DAGs/common runner/framework/orchestration, validators, compose configs,
tests và runbook/evidence. Chỉ thay hai test files/runbook và thêm một evidence
log trong readiness lần này. Chuẩn bị một focused release commit chứa đúng
code/config/tests cùng durable evidence, gắn immutable commit/image digests
vào deployment record sau khi owner cho commit. Không dùng `git add .`.
Không commit `.pytest_cache/`, wheel/dependency cache, Raw lớn, Airflow logs
thô hoặc `.env`/private rollback backup/runtime env chứa secret. Hai report gốc
và canary evidence không sửa; credential pattern scan evidence mới sạch.

**Controlled cutover đề xuất, chưa thực hiện:** owner chốt lịch/SLA và duyệt
orchestration/dependency package; build/lock và test đúng phần thay đổi; fresh
read-only preflight code/image/registry hashes, tám source snapshot/run identities,
V2 targets/dead_letters/legacy/PG baselines, không concurrent writer. Backup
config/env references/serialized DAG timetable và container/image identities,
giữ secret backup riêng. Chỉ apply execution config đã duyệt khi DAGs paused;
parse/serialized scope đúng tám, Silver trigger-only, retries/gate đúng,
DB/API/registry/imports/Trino health PASS và xem next run/catchup trước unpause.

Sau cutover được phê duyệt, theo dõi batch ingestion và triggered Silver run IDs,
actual metrics/DQ/lineage/snapshot transitions/locks/timeout; Trino đối soát
rows/unique keys/units/values/quarantine với actual Bronze batch, không yêu cầu
1.391.467 rows cố định khi có dữ liệu mới. Lỗi hoặc unknown commit: pause cả hai,
read catalog/PG/logs trước quyết định resume, rollback config chỉ khi outcome rõ
và vẫn paused; không xóa/restore snapshot tự động hoặc quay writer về legacy.
Chưa có deployed Gold/consumer service được phát hiện; `query_silver_usda.py`
chỉ là script query legacy thủ công, không tự chuyển nó hoặc tạo consumer giả.

### Bronze-trigger-Silver activation canary — BRONZE_TRIGGER_SILVER_E2E_PASS

Đã thực hiện đúng approval ngày 15/10/2026. Fresh preflight read-only PASS: code,
immutable images, registry checksum, 29 identities, PostgreSQL metadata và tám
Raw checksums khớp baseline; mỗi checksum có prior SUCCESS hợp lệ. Không có
active/queued runs, concurrent writers hoặc advisory locks; hai DAG paused.

Chỉ recreate `airflow-scheduler` và `airflow-webserver` để nạp environment mới;
Spark giữ nguyên. Không đổi images/dependencies hoặc restart storage/database/
catalog/Trino. Actual flags trên cả hai Airflow services:

```text
SILVER_ACCEPTED_RELEASE=silver_v2_release_20261013
SILVER_PRODUCTION_ENABLED=1
SILVER_CANARY_ONLY=0
SILVER_BRONZE_SCHEDULE=
```

Actual parse/serialization đúng tám sources, retries=0, schedule=None; Bronze
có `verify_bronze_batch` và `TriggerDagRunOperator`, `wait_for_completion=True`,
không reset existing runs. Đã unpause Silver trước, target guard PASS, rồi
unpause Bronze và gọi trigger đúng một lần với `{"force_reprocess":false}`.

| Run | Actual run ID | Kết quả |
|---|---|---|
| Manual Bronze | `silver_v2_activation_20261015T131911Z_55ae7846` | 13/13 tasks SUCCESS |
| Automatically triggered Silver | `silver_v2_bronze__a273985357d5bc94b784f6e942947084fa25cdb9e82d0e107b36d37d68e7b02f` | 13/13 tasks SUCCESS |

Tám ingestion tasks SUCCESS với logical status SKIPPED, `unchanged_snapshot`,
zero extracted records và prior successful checksum proof. Batch readiness
gate PASS, source run IDs khớp PostgreSQL; trigger XCom khớp deterministic Silver
run ID. Không trigger Silver thủ công. Airflow ghi run_type MANUAL cho cả run
do operator tạo; provenance được chứng minh qua operator logs/XCom và một lần
manual invocation của Bronze, không suy ra từ run_type đơn lẻ.

Silver chạy từ `13:20:07.286927Z` đến `13:29:07.339236Z`; Bronze kết thúc lúc
`13:29:09.465968Z` sau khi trigger nhận Silver SUCCESS. Gate, tám transforms,
audit, signal và end đều SUCCESS; signal chỉ `DQ_PASSED`. Mỗi ingestion/Silver
writer đúng một attempt, không retry/clear/force reprocess. Spark API có đúng
24 POST thành công: tám readiness checks, tám transforms và tám audits.

Readback Spark/PyIceberg/Trino PASS 8/8: tám targets NOOP, UUID/schema/snapshots
và toàn bộ row checksums (gồm processing timestamp) khớp acceptance. Tổng Silver
**1.391.467 rows**; World Bank excluded observations đo lại từ Bronze là **13.464**.
Bronze và đủ 20 bảng tồn tại trước V2 giữ nguyên identities; inventory vẫn 29 bảng.

Dead letters vẫn **4 logical records**, IDs/payload/errors giữ nguyên. Snapshot
audit đổi hợp lệ từ `5285064570972783895` sang `5171447547413260487`; UUID/schema/
location giữ nguyên. Đây là latest verified quarantine baseline cho lần sau.
Không yêu cầu giữ snapshot audit cũ hoặc dùng report lịch sử để chạy --execute.

PostgreSQL ingestion runs **21 → 29**, đúng tám checksum-SKIPPED runs mới, hash
của 21 runs cũ giữ nguyên. Bốn bảng metadata còn lại không đổi. Airflow chỉ thêm
hai DAG runs, 26 task instances và 199 XCom records. Không duplicate run, Gold,
READY_FOR_GOLD, consumer cutover hoặc thay business rules/contracts/NSO V2.

Đã pause cả hai DAG sau khi cả hai runs kết thúc; final readback xác nhận không
còn active/queued runs, active tasks hoặc advisory locks. Cron trống, hai
schedules=None. Flags runtime vẫn **production=1/canary=0**; không thực hiện
vòng recreate bổ sung. Mặc định của compose override vẫn production=0/canary=1.
Nếu owner muốn đưa runtime về flags mặc định, fresh zero-jobs/locks/code/image
check rồi inject secrets hiện hữu và recreate đúng hai Airflow services bằng
cùng images, giữ hai DAG paused; cần approval rõ ràng cho transition đó.

Report bền vững: [activation_canary.json](evidence/release_20261013/activation_canary.json).
Archive gồm 22 sanitized worker task logs, runtime logs và run/task evidence;
bốn EmptyOperators được scheduler đánh SUCCESS không có worker log. SUA log
đã đọc và lưu từ container vì host enumeration không liệt kê file đó. Không
lộ credentials. Không sửa code/config nghiệp vụ hoặc commit/push/merge.

Dừng tại **BRONZE_TRIGGER_SILVER_E2E_PASS**, chờ owner duyệt release closure.
Không tự bật cron, unpause hoặc chuyển consumer.

### Controlled runtime deployment — DEPLOYMENT_PASS, baseline trước activation

Owner đã duyệt triển khai hai immutable images, không duyệt activation. Apply
hoàn tất lúc `2026-10-15T12:52:32.241755+00:00`, exit 0; chỉ chạy một lần bằng
base compose và `compose.silver-production.yml`, `--no-deps --no-build --pull never`.
Compose dry-run chỉ có đúng ba services trước khi apply.

| Service | Actual container ID (prefix) | Actual immutable reference |
|---|---|---|
| spark-iceberg | `ae0ee2eca06c` | `tlcn/silver-spark@sha256:79723a677138a36a03d195d82564c5e9039abeb0a2e149b7dd89d8b289403843` |
| airflow-scheduler | `ab79d268eb34` | `tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96` |
| airflow-webserver | `1548dd592c90` | `tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96` |

Docker inspect xác minh RepoDigests và OCI Descriptor digest, không chỉ image ID.
PostgreSQL, MinIO, REST Catalog, Trino, airflow-init, mc-init và volumes giữ nguyên.
Spark healthy; Airflow webserver/metadatabase/scheduler health PASS (hai Airflow
services không khai báo Docker HEALTHCHECK). Package inventory khớp 146/377/377
packages; pip check PASS, không startup pip install hoặc dependency cache mount.

Fresh preflight và post-deployment readback PASS: cả 29 table identities không đổi;
1.391.467 Silver rows; 4 quarantine records, kể cả audit digest giữ nguyên theo
latest verified baseline. Spark/PyIceberg schema và toàn bộ row checksums của tám
targets khớp acceptance; Trino kiểm tra schema, keys, values, units, flags, missing
reasons và tám lineage fields PASS. Bronze rows/snapshots và năm bảng ingestion
metadata giữ nguyên (21 ingestion runs). DAG run/task/XCom history không đổi.

API child imports, registry checksum/readonly mounts, DB và tám source-lock probes
PASS; locks đã giải phóng. Actual API Spark child chạy `check-bronze` read-only
PASS 8/8. Cả scheduler/webserver parse và serialized DAG state đều đúng tám nguồn,
retries=0, catchup=False, max_active_runs=1, schedule=None và paused. Không có task
automatic trigger, Gold hoặc READY_FOR_GOLD. Production=0, canary=1, cron trống.

Điều tra drift trước deploy: labels chỉ base compose, bảy containers đã start trong
`12:22:02Z..12:22:31Z`; runtime không đổi thêm trước apply. Docker chỉ còn 256 events
trong `12:43:18Z..12:45:58Z`, đều healthcheck exec, không còn lifecycle create/start.
Logs/events không xác định được caller hoặc lệnh gây drift; nguyên nhân **chưa rõ**.
Không quan sát concurrent deployment/writer. Backup runtime/config/secret references
và DAG serialization lưu private ngoài Git; report không chứa credentials.

Code fingerprint `bc50ba29c2af37f4df2b949c71138d6755852a23bec68c3b0ddf2221af834711`
và execution artifact hashes giữ nguyên. Working tree còn các thay đổi chưa commit;
chưa có published Git release. Không sửa business rules/contracts/NSO V2/Gold.

Bước tiếp theo cần owner approval riêng: activation canary với production=1/canary=0,
cron vẫn trống, fresh preflight và một batch Bronze-trigger-Silver được xác định rõ.
Chỉ unpause/trigger trong phạm vi approval đó, Silver trước Bronze; nghiệm thu rồi
pause lại. Chưa duyệt scheduling định kỳ hoặc chuyển consumer. Không chạy lại
Silver `--execute` với preflight lịch sử và không tự recreate lần nữa.
Đây là trạng thái tại thời điểm deployment trước approval activation. Lần
Bronze-trigger-Silver thực tế đã PASS ở mục activation canary phía trên;
AIRFLOW_E2E_PASS của mục canary cũ vẫn là evidence lịch sử riêng.

### Safe orchestration & reproducible runtime — baseline chuẩn bị trước deployment

Owner chỉ duyệt code/config, image build và isolated tests. Không có approval
deploy, unpause/trigger shared, scheduling hoặc consumer cutover. Business cron
chưa chốt. Evidence mới: [runtime_preparation.json](evidence/release_20261013/runtime_preparation.json),
status **PREPARED_RUNTIME_DRIFT_BLOCKED**; code/build/tests **PASSED**.
Không sửa các report acceptance/preflight/canary/incremental gốc.

**Dependency đã triển khai trong code hiện hữu:**

- Silver luôn `schedule=None`; chỉ Bronze có thể nhận explicit
  `SILVER_BRONZE_SCHEDULE` sau khi owner duyệt. Mặc định không có cron, kể cả
  mất flags; `catchup=False`, `max_active_runs=1` giữ nguyên ở cả hai DAG.
- Production trigger chỉ xuất hiện khi approved release đúng,
  `SILVER_PRODUCTION_ENABLED=1`, `SILVER_CANARY_ONLY=0`. Các mode default,
  release chưa activate và canary không tạo trigger task. Run IDs canary cũ bị
  từ chối trước ingestion khi chạy trong production mode.
- Approved release bắt buộc đúng tám enabled IDs, registry checksum đã duyệt;
  missing/disabled/unexpected IDs hoặc config drift làm fail-closed. Source
  locks/common runner/contracts/transformations/DQ/units không đổi.
- Trong production, checksum SKIPPED trở thành task SUCCESS với logical status
  SKIPPED: phải có checksum và prior SUCCESS phù hợp trong PostgreSQL. SUCCESS
  cũng phải khớp persisted run/source/checksum/records. NOT_READY, FAILED và
  skip reason không được chứng minh đều làm task FAILED, không phát trigger.
- `verify_bronze_batch` phụ thuộc all_success của đủ tám ingestion tasks,
  đối chiếu lại cả tám kết quả với PostgreSQL; không dựa vào Bronze nonempty.
  Bất kỳ failed/upstream_failed nào chặn toàn bộ Silver batch.
- Existing `TriggerDagRunOperator` dùng run ID
  `silver_v2_bronze__<sha256(upstream_run_id)>`, conf
  `{"mode":"full","release":"silver_v2_release_20261013"}`,
  `reset_dag_run=False`, `retries=0`, `wait_for_completion=True`,
  `deferrable=False`; thời gian chờ bị giới hạn 2 giờ, không cần triggerer.
- Guard đọc actual DagModel/SerializedDagModel trước trigger: target phải
  available, unpaused, không import error, đúng release/registry/eight sources,
  schedule=None/writers retries=0, không active hoặc duplicate target run.
  `pre_execute` kiểm tra lại để target bị pause sau gate cũng fail rõ ràng.
  Operator/DB uniqueness còn chặn duplicate race, không clear/reset existing run.
- Mọi writer của approved release retries=0, độc lập với canary. Partial/timeout
  không tự retry; source lock giữ qua write/outcome readback của common runner.
  Pause không tự hủy job đã chạy: đọc catalog/quarantine/PG trước resume.

| Mode | Bronze schedule | Silver schedule | Auto trigger |
|---|---|---|---|
| Không release | None | None | Không; không phải V2 production scope |
| Release, production=0 | None | None | Không, gồm cả canary |
| Release, production=1, canary=0 | Explicit owner cron, hoặc None nếu chưa có | None | Sau verified batch + target admission |

**Images đã build và kiểm chứng**, chỉ Linux amd64, không deploy:

| Image | Immutable reference | Verification |
|---|---|---|
| Spark | `tlcn/silver-spark@sha256:79723a677138a36a03d195d82564c5e9039abeb0a2e149b7dd89d8b289403843` | 146 packages khớp accepted inventory; pip check PASS |
| Airflow | `tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96` | 377 packages khớp accepted inventory; pip check PASS |

Hai Dockerfiles pin base digest đã nghiệm thu. Hash-locked manifests chỉ liệt kê
delta trên immutable base: Spark 1 package (psycopg2-binary 2.9.10), Airflow 7
packages đúng runtime đã nghiệm thu. `--no-deps --require-hashes` không tự nâng
transitive versions; full inventory sau build được so sánh với accepted runtime.
Đây là reproducible dependency set và immutable artifact identities, không tuyên
bố rebuild bit-for-bit cùng image digest. `.dockerignore` chỉ đưa Dockerfiles và
locks vào build context, không secrets/Raw/cache/source data.

```powershell
docker build --pull=false -f docker/silver-spark.Dockerfile -t tlcn/silver-spark:release-20261013 .
docker build --pull=false -f docker/silver-airflow.Dockerfile -t tlcn/silver-airflow:release-20261013 .
```

Các lệnh build trên đã chạy thành công. Images được giữ local; chưa publish.
`compose.silver-production.yml` **INACTIVE**, mặc định production=0/canary=1,
không cron. Operator phải inject `SILVER_SPARK_IMAGE`/`SILVER_AIRFLOW_IMAGE` bằng
đúng immutable references đã verify, và hai secret variables
`SILVER_INGESTION_DB_URL`, `SILVER_AIRFLOW_DB_URL` từ secret source hiện hữu.
Không in giá trị hoặc dùng private rollback backup làm secret source.
Override chỉ đổi ba execution services; registry readonly, không cache mount,
không pip startup, không stack thêm canary override. Compose config-only PASS.
Default API child imports + registry checksum + API health đã PASS trong image
Spark mới, network-none, không `.pytest_cache` mount hoặc POST writer.

Trino fix chỉ đổi CRLF → LF của `config/trino/etc/config.properties`; property
values/version/port/catalog/storage không đổi. `.gitattributes` giữ LF cho file
này và execution artifacts; registry giữ CRLF đã được checksum acceptance.
Actual health script exit 0 và Docker health healthy, không chạy restart Trino.

**Tests actual:** 10 DAG parsing/config + 27 ingestion orchestration unit + 11
Airflow SQLite metadata integration + 31 Silver unit/compatibility + 10 native
Iceberg fixture integration = **89 distinct cases PASS**. Default manual-only
guard có targeted test PASS sau runtime-drift finding; không tăng double-count.
Metadata tests dùng disposable SQLite/network-none, real operator tạo queued
test run và xác minh duplicate rejection; không chạy external Silver tasks hoặc
dùng mock SUCCESS để tuyên bố production E2E. Tám fixture pipelines kiểm tra
thực Iceberg write/replay/routing; không lặp full Raw rebuild. Representative
incremental evidence cũ giữ nguyên, không rerun vì data path/package inventory
không đổi. Read-only gate mới cũng PASS với tám checksum-SKIPPED canary records
thực trong PostgreSQL và prior successful ingestions.

**Runtime drift cuối nhiệm vụ — không tự khắc phục bằng deployment:**

- Cả bảy shared containers có IDs/StartedAt khác canary; observed start window
  khoảng **19:22 ngày 15/10/2026, Asia/Bangkok**. Các tools của nhiệm vụ này không
  chạy apply/restart/recreate shared. Không suy đoán tác nhân gây thay đổi.
- Scheduler/webserver mất `SILVER_ACCEPTED_RELEASE` và `SILVER_CANARY_ONLY`;
  shared execution services vẫn dùng base images, chưa dùng images mới đã build.
- Cả hai DAG **paused**, `schedule=None`, không active/queued run hoặc advisory
  lock. Bronze hiện serialize default 10 sources (có NSO/fixture), chưa phải V2
  release scope; không được unpause/trigger cấu hình này. Source scope tám được
  chứng minh ở production-preview tests, chưa được restore trong actual runtime.
- Toàn bộ **29 table identities** vẫn khớp canary; Silver 1.391.467 rows,
  quarantine 4. Counts và content hashes của cả năm ingestion metadata tables
  không đổi, gồm 21 ingestion runs. Các evidence lịch sử giữ nguyên checksum.
- Không ghi shared data/Gold, sửa NSO V2, chuyển consumer hoặc commit/push.

**Files của implementation lần này:** hai DAGs,
`src/ingestion/orchestration/pipeline_tasks.py`, ba unit test files
(`test_dag_structure.py`, `test_pipeline_tasks.py`, `test_silver_orchestration.py`),
`tests/integration/test_release_orchestration.py`, hai Dockerfiles dưới `docker/`,
hai locks dưới `requirements/`, `compose.silver-production.yml`, `.dockerignore`,
`.gitattributes`, Trino properties, runbook và `runtime_preparation.json`.
Các release changes có sẵn từ trước được giữ nguyên, không overwrite/reset.

Git preparation: giữ code/config/tests cùng durable evidence trong release
commit có chọn lọc sau approval; không `git add .`. Không commit `.env`, Raw lớn,
`.pytest_cache`, dependency wheels, private credentials/rollback backup hoặc test
logs tạm. Report ghi hashes của execution artifacts và immutable image refs;
không có credentials trong images/build context/report. Chưa commit/push/merge.

**Quy trình deployment đã được owner duyệt và thực hiện ở mục trên (lịch sử):**

1. Owner chốt release commit/artifact identities, duyệt sửa runtime drift và
   deployment đúng ba execution services. Giữ business cron trống khi chưa chốt.
2. Fresh read-only preflight actual code/registry/source snapshots/target
   identities/metadata/runtime flags; không tái dùng preflight cũ với --execute.
   Backup runtime config và secret references riêng, không reset storage/catalog.
3. Inject immutable image refs + existing DB secrets. Dùng base compose cùng
   production override, mặc định production=0/canary=1; tiếp tục paused/manual.
   Lệnh sau đã chạy đúng một lần theo approval deployment; không tự chạy lại:

   ```powershell
   docker compose -p tlcn -f docker-compose.yml -f compose.silver-production.yml up -d --no-deps --no-build --pull never --force-recreate spark-iceberg airflow-scheduler airflow-webserver
   ```

4. Verify actual API children/dependencies/DB/locks/registry, serialized scope
   đúng tám, retries=0, schedules=None, runtime/container identities và Trino.
   Không chọn PostgreSQL/MinIO/REST/Trino hoặc volumes cho recreate.
5. Activation (`production=1`, `canary=0`), bất kỳ controlled manual batch mới
   hoặc periodic cron/unpause đều cần approval rõ ràng riêng. Silver phải được
   unpause trước để target guard cho phép trigger; chỉ Bronze sở hữu cron đã duyệt.
   Không tạo consumer routing giả hoặc chuyển script query legacy tự động.
6. Khi được phép vận hành, monitor actual batch/triggered run IDs, outcomes,
   snapshot transitions, quarantine và Trino reconciliation. Partial/unknown
   commit: pause cả hai và read back trước retry; rollback config chỉ sau khi
   outcome rõ, giữ paused, không tự rollback snapshots/reset data hoặc route legacy.

## Recovery khi dữ liệu đã bị xóa — không phải bước triển khai mặc định

| Phạm vi reset | Điều kiện và luồng chạy lại |
|---|---|
| Chỉ Silver; giữ Bronze | Trigger `rice_lakehouse_silver_pipeline` với `{"mode":"full"}`. Framework tạo lại target bị thiếu, MERGE và audit. |
| Bronze + Silver; giữ raw | Cần đủ raw local, catalog/storage nhất quán và metadata/checkpoints tương ứng. Ingest Bronze trước, sau đó chạy Silver full. |
| Xóa cả `data/raw/` | Registry dùng `LOCAL_FILE`; adapters không tự tải nguồn từ các endpoint reference. Phải khôi phục raw trước. |
| Chỉ xóa objects trong warehouse | Iceberg catalog có thể vẫn trỏ tới file đã mất. Cần quy trình reset/khôi phục cả catalog và storage; Airflow không tự sửa bằng ingestion thông thường. |

Mở giao diện Airflow hoặc khởi động container không tự chạy pipeline ngay.
Các DAG có lịch ngày 15 hằng tháng và `catchup=False`; để chạy ngay cần trigger.

## Ghi nhận lịch sử ngày 11/10/2026

Kiểm tra ngày 11/10/2026: tám trong chín nguồn Silver có raw local; Trade thiếu
`data/raw/faostat/Trade_DetailedTradeMatrix_E_All_Data_(Normalized)/rice_trade_data.csv`.
Trade 1.226.470 rows trong nghiệm thu trước được đọc từ **Bronze đã tồn tại**.
Fixture validation 2.000 rows không thay thế source Trade production.
Nếu xóa Bronze ngay khi raw Trade còn thiếu, không thể dựng lại đủ chín nguồn.

Airflow scheduler hiện có openpyxl/lxml/PyIceberg/PyArrow/Pandas/YAML; Spark API
đã được kiểm tra. Điều này không thay thế nghiệm thu một scheduled DAG run
trên stack reset hoàn toàn; đợt kiểm tra hiện tại không xóa stack đang dùng.

## Metadata và force reprocess

Checksum/snapshot, batch metadata và checkpoint nằm ngoài Silver, gồm
PostgreSQL và MinIO. Xóa tables mà giữ trạng thái `SUCCESS` có thể khiến
ingestion skip với cùng checksum. Nếu cần đọc lại nguồn trong môi trường có
metadata cũ, Bronze DAG nhận conf:

```json
{"force_reprocess": true}
```

Với FULL loader, force bỏ checksum idempotency gate, không resume checkpoint
cũ và không bỏ qua file đã ingest: nguồn được đọc từ chunk đầu vào một run mới.
Bronze giữ các phiên bản cũ; Silver dedup theo business key và freshness.
Force **không reset metadata**, không sửa catalog đang trỏ tới objects đã mất
và không thay đổi quy tắc incremental loader.

Có thể force riêng NSO để tránh đọc lại các nguồn khác:

```json
{"force_reprocess_sources": ["nso_vietnam"]}
```

## Legacy NSO recovery — ngoài release V2 tám nguồn

Kỳ vọng 41-column/904-row bên dưới thuộc recovery lịch sử, không phải fresh rebuild.

### NSO lỗi `raw values do not fit double` sau reset

Log `ingest_nso_vietnam`, attempt 3, ngày 11/10/2026 thất bại ở
`t_nh_th_nh_ph_`: file đầu `V06.12.csv` không có các cột tỉnh/năm ma trận.
Trước bản sửa, Pandas suy luận phần padding toàn NULL thành float64, khiến
Bronze tạo 31 cột DOUBLE. File `V06.13.csv` chứa `C? NU?C` và các chuỗi nguồn
nên writer từ chối ghi để tránh mất dữ liệu.

Adapter hiện ép **toàn bộ union cột nguồn thành Pandas nullable string** trước
khi yield chunk. Bronze mới tạo 41 cột nguồn VARCHAR; việc chuẩn hóa tên địa lý
và cast số tiếp tục thuộc Silver. Thay code không tự sửa bảng DOUBLE đã tồn tại.

Từ root repository, kiểm tra phương án sửa (chỉ đọc bảng):

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/repair_nso_bronze_schema.py
```

Áp dụng khi report là `planned` và đã xác nhận đúng bảng NSO:

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/repair_nso_bronze_schema.py --execute
```

Utility giữ source advisory lock; chỉ nhận các cột nullable DOUBLE thuộc
inventory và **COUNT(column) = 0**. Bất kỳ cột có dữ liệu, kiểu không hỗ trợ,
cột thiếu hoặc legacy name trùng đều làm nó dừng. Một transaction catalog đổi
tên field ID cũ thành `_nso_legacy_null_<column>` rồi thêm field STRING nullable
với tên nguồn và ID mới. Không xóa table, file, row hay snapshot; kiểm tra lại
count và snapshot ID sau commit. Cột legacy toàn NULL được giữ để bảo toàn lịch
sử và không tạo observation Silver. Bảng đúng kiểu trả `already_valid`.

Trong lần recovery lịch sử, Bronze DAG dùng conf force riêng NSO phía trên để toàn bộ
13 CSV thuộc một run SUCCESS; Silver hook lúc đó chỉ đọc run đó. Hook này đã được
loại khỏi ingestion DAG; không áp dụng các thao tác NSO này cho release tám nguồn.
Nếu dữ liệu đã được
phục hồi như trong [bằng chứng NSO](validation.md), mở DAG run cũ, Clear
`ingest_nso_vietnam`, `silver_nso_vietnam` và task downstream bị `upstream_failed`.
Ingestion có thể SKIPPED vì checksum đã SUCCESS; Silver hook có thể SKIPPED nếu
không còn run pending. Đây là hành vi idempotent hợp lệ.

Đợt phục hồi giữ 71 dòng từ run FAILED cũ và thêm 833 dòng vào run SUCCESS mới:
Bronze vật lý có **904 dòng**, còn Silver có **23.060 observations** sau dedup.
Không so tổng Bronze lịch sử với số dòng raw của một run.

Kiểm chứng tạo bảng mới, ghi đủ 13 CSV qua PyIceberg/REST/MinIO và đọc lại Trino:

```powershell
docker compose exec -T airflow-scheduler python /opt/airflow/jobs/bronze/validate_nso_bronze.py
```

Lệnh này tạo namespace riêng `bronze_nso_validation_<uuid>`, giữ bảng kiểm chứng
để đối soát và không sửa tables production. Kỳ vọng 13 files / 833 records,
41 fields VARCHAR, 71 nhãn địa lý gốc. Hai utility cần runtime PyIceberg của
Airflow scheduler và services MinIO/REST/Trino/PostgreSQL đang hoạt động.

## Luồng xác nhận trên Airflow

1. Đảm bảo đủ raw cho các nguồn enabled và services catalog/storage/database/
   Spark API hoạt động.
2. Nếu Bronze đã reset trong recovery được duyệt, trigger `rice_lakehouse_ingestion`.
   DAG này chỉ ingest Bronze; kiểm tra task skip và lý do từng nguồn.
3. Trigger `rice_lakehouse_silver_pipeline` với `{"mode":"full"}` để gate,
   xử lý tám source IDs được allowlist và post-write audit; NSO ngoài scope.
4. Xem metrics, số dòng/quarantine và tín hiệu sau audit; DAG màu xanh có thể
   chứa các task skip, nên không dùng màu DAG làm bằng chứng đủ dữ liệu.

Gate Silver kiểm tra schema và **Bronze không rỗng**, chặn các bảng đã xóa hết
rows nhưng còn schema. Empty incremental batch vẫn được framework chấp nhận
khi nguồn Bronze đã có dữ liệu; đây là trường hợp không có delta.

Kiểm tra Bronze trước khi trigger, chỉ đọc:

```powershell
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/jobs/silver/run_all_silver.py --check-bronze
```

Xem [nghiệm thu và lệnh audit](validation.md). Không có lệnh xóa dữ liệu trong
runbook này; người vận hành cần chọn đúng phạm vi reset.

## Rebuild tám nguồn Silver trong môi trường test độc lập

Ngày 15/10/2026, project mới `tlcn-silver-fresh-20261015` đã chạy xong, exit 0:
catalog namespaces = 0, MinIO buckets = 0, PostgreSQL user tables = 0 trước
ingestion. Raw → Bronze → Silver → Trino **8/8 PASSED**, tổng Silver sau dedup
**1.391.467 rows**; cả tám Bronze replay `SKIPPED`, full/incremental Silver NOOP.
Đây là số đo của lần fresh run, không phải giá trị hardcode để nghiệm thu shared.

| Source | Valid trước dedup | Quarantine | Excluded | Duplicate | Silver rows |
|---|---:|---:|---:|---:|---:|
| faostat_production | 22.738 | 0 | 0 | 0 | 22.738 |
| faostat_monthly_price | 15.334 | 0 | 0 | 0 | 15.334 |
| faostat_supply_utilization | 36.380 | 0 | 0 | 0 | 36.380 |
| faostat_trade | 1.226.470 | 0 | 0 | 0 | 1.226.470 |
| usda_psd | 990 | 0 | 0 | 0 | 990 |
| usda_rice_yearbook | 13.131 | 0 | 0 | 195 | 12.936 |
| worldbank_pinksheet | 56.232 | 0 | 13.464 | 0 | 56.232 |
| thitruongnongsan | 20.390 | 4 | 0 | 3 | 20.387 |

SUA giữ 236 warnings theo policy Residuals âm; giá nông sản có 29 warnings.
Regression parse DAG: **2 tests PASS** (default và approved-release scope).
**AIRFLOW_E2E_NOT_RUN trong isolated project**: project test không có Airflow
riêng, không suy ra orchestration PASS từ CLI. Shared canary được nghiệm thu
riêng ở phần kết quả bên trên. Test services được
giữ nguyên để đối soát; không restart chúng vì MinIO/PostgreSQL dùng tmpfs.

Quy trình mới dùng `compose.silver-test.yml`, project riêng, network `internal`,
không publish host ports, không mount storage shared và không có Airflow.
MinIO/PostgreSQL dùng tmpfs mới; REST Catalog dùng SQLite riêng trong container.
Raw và repository được mount chỉ đọc. Không có bước reset hoặc xóa dữ liệu.

Trade đầy đủ hiện tìm được trong `data_main/raw`, ngoài checkout này. Đặt đường
dẫn tới thư mục Raw đầy đủ; validator dừng trước ingestion nếu thiếu một artifact.
Không thay file Trade bằng ZIP fixture để tuyên bố full coverage.

```powershell
$env:SILVER_TEST_RAW_ROOT = 'C:/Users/hoang/source/repos/final term/tieu luan chuyen nganh/data_main/raw'
$testProject = 'tlcn-silver-rebuild-test-new-unique-id'
docker compose -f compose.silver-test.yml -p $testProject up -d minio-test rest-test postgres-test trino-test
docker compose -f compose.silver-test.yml -p $testProject run --rm runner
```

Dùng một project name chưa tồn tại, chỉ gồm chữ thường/số/dấu gạch nối; thay
`new-unique-id` bằng ID mới. Cache `.pytest_cache/silver_wheels` cần có
pytest 8.4.2, psycopg2-binary 2.9.10, boto3 1.35.99, openpyxl 3.1.5, lxml 5.3.0
và dependencies cho Python 3.10/Linux x86_64. Runner cài offline từ cache.

Validator chạy đúng tám source IDs, không dùng `--dataset all`. NSO V2 vẫn
DRAFT và không nằm trong drill. World Bank theo scope registry `Monthly Prices`;
`Monthly Indices`/Description/Index Weights không được giả thành price series.

Drill kiểm tra checksum và số records độc lập từ Raw, gọi ingestion engine thật,
đối soát Bronze và tám lineage fields, chạy common framework, query Silver,
full/incremental rerun và so snapshot/schema/checksum gồm processing timestamp.
Ingestion lại cùng checksum phải SKIPPED mà Bronze không đổi; Trino kiểm tra
output. `SNAPSHOT_RESULT`/`SNAPSHOT_SUMMARY` là kết quả từ lần chạy thực tế.

Nếu chỉ chạy mode Hadoop mặc định, `bronze_rebuild=NOT_RUN`; không coi đó là
full rebuild. `--rebuild --resume` dành riêng cho tiếp tục cùng test environment
sau lỗi, không reset: chỉ chấp nhận Bronze đầy đủ, checksum đúng và một run
identity. Mode mặc định yêu cầu metadata test ban đầu rỗng. Không dùng `up`
để recreate một test service đang chứa dữ liệu tmpfs cần giữ.

Heap Spark được giới hạn 768 MB, runner 3 GB và MinIO test 1 GB. Full wide payload
chỉ cache cho observation có lỗi DQ; không giữ JSON của hàng triệu valid rows.
Quarantine và Silver là hai transaction riêng: lỗi framework cung cấp
`silver_failure` với stage, commit flags và actual `bronze_read_snapshot_id`.
Kiểm tra dữ liệu đã commit trước khi retry. `_source_snapshot_id` vẫn là lineage
metadata từ Bronze (loader hiện có thể ghi 0), không thay bằng Iceberg snapshot ID.
Audit của quarantine và PostgreSQL run logs có thể cập nhật khi replay, dù
snapshot/dữ liệu Silver không đổi. Existing target sai/thiếu schema bị chặn;
không tự ALTER hoặc overwrite, cần migration được phê duyệt.

Release Trino checks so sánh các bộ tám lineage fields bằng `EXCEPT`, tránh
correlated join trên nhiều rows có cùng batch identity. Với target lớn có `year`
trong business key, kiểm tra exact distinct keys theo partition năm rồi đối soát
coverage/count toàn bảng. Không dùng approximate distinct hoặc isolated counts.

## Đề xuất reset an toàn — không thực thi

Ưu tiên một project/catalog/warehouse/metadata database mới. Giữ môi trường cũ
để đối soát; chỉ chuyển routing sau khi owner duyệt kết quả và migration.

| Tài nguyên | Phương án mặc định | Backup/khôi phục và kiểm chứng |
|---|---|---|
| Raw local | Giữ nguyên, mount chỉ đọc | Backup artifacts + SHA-256; xác nhận đủ tám nguồn, đặc biệt Trade |
| MinIO raw/warehouse | Instance/bucket/warehouse mới | Giữ objects cũ; backup cùng catalog, đối soát inventory/checksum |
| Iceberg metadata | REST instance/catalog mới | Backup SQLite/catalog và manifest/data files cùng mốc; xác nhận namespace/table/snapshot IDs |
| PostgreSQL ingestion metadata | Database/schema mới | Dump schema/data, snapshot/checkpoint/watermark/run logs; xác nhận DB name và ban đầu rỗng |
| Airflow metadata/DAG state | Giữ nguyên; drill không sử dụng | Backup metadata DB và DAG conf; test orchestration cần instance/DB riêng |
| Docker volumes | Giữ nguyên volumes hiện tại | Kiểm tra project labels và mount IDs; không xóa volume để dựng môi trường mới |
| Trino catalog/config | Catalog riêng trỏ REST/MinIO test | Backup config; xác nhận endpoint/warehouse rồi DESCRIBE/count/query values |
| Logs/lineage/snapshots lịch sử | Giữ lại | Lưu run IDs, hashes, metrics và snapshot IDs trước/sau; kiểm tra restore đọc được |

Rebuild hoàn tất khi Raw→Bronze đối soát đầy đủ, cả tám Silver source PASS,
khóa duy nhất, missing/quarantine đúng policy, Trino đọc đúng types/values và
full/incremental Silver NOOP. Không dùng màu xanh DAG hoặc count từ fixture làm
tiêu chí nghiệm thu. Shared acceptance, chuyển Airflow routing, migration target
cũ và bất kỳ destructive reset nào đều cần owner approval riêng. Rollback chỉ
có thể bảo đảm nếu catalog và data objects được backup nhất quán; không giả định
khôi phục được lịch sử sau khi mất objects.
