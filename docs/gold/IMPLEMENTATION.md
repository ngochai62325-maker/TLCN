# Gold implementation — gold-20261025.1

Owner đã duyệt **17 physical Iceberg tables + 4 version-controlled Semantic SQL Query Definitions** ngày 26/10/2026. Trino giữ **448**, REST catalog/SQLite và Docker services giữ nguyên. Không tạo native/temporary/materialized views hoặc bốn aggregate tables. Trạng thái nghiệm thu và số liệu thực tế: [VALIDATION.md](VALIDATION.md).

**G12 final: GOLD_MVP_COMPLETE.** Hai actual Gold DagRuns SUCCESS; đủ 17 tables, 1.414.527 fact observations, DQ01–18, four full-scope semantic queries, immutable publication/readback và published consumers PASS. Formal replay cùng inputs/code/refs/SQL giữ nguyên đủ 17 UUID/snapshots/metadata/data-file sets và manifest. Full Trade phủ 156 partitions / 1.226.470 observations / 526.986 groups. [Runtime evidence](runtime_acceptance.json), [timings/limitations](VALIDATION.md).

## Semantic delivery

SQL definitions tại [queries/gold](../../queries/gold/), version `gold-semantic-20261026.1`:

- `vw_nso_source_observations.sql`
- `vw_fao_production_yield.sql`
- `vw_sua_supported_balance.sql`
- `vw_trade_reported_unit_values.sql`

Giữ nguyên SELECT logic/grain của thiết kế đã duyệt. Tên `vw_*` là query identifier, **không phải catalog object**. Loader chấp nhận đúng bốn tên, một SELECT, exact table references; từ chối extra/missing files, DDL/DML, multiple statements/comments và unapproved relations. Placeholder chỉ bind table names từ canonical inventory cùng positive int64 snapshot IDs, không nội suy SQL do người dùng cung cấp.

Mỗi definition có hash deterministic của version/name/normalized SQL. Candidate validation dùng Trino parse/bind, kiểm exact column names/types, rồi thực thi trên **actual candidate output snapshots**. Kiểm source/group coverage, presence vs NULL, contributor keys, unpivot multiset hai chiều và paired ratios. Queries được kiểm đầy đủ qua các năm rời nhau để giới hạn aggregation memory trên Trino 448; không lấy vài năm làm đại diện cho toàn dataset.

Manifest lưu cả bốn tên/version/definition hash/bound SQL hash, UUID/snapshot bindings, expected output schema và validation outcomes/coverage. Consumer xác minh chúng với SQL files hiện hành trước execution. SQL khác hash phải được validate/publish bằng batch mới; không tự dùng definition mới với manifest cũ.

## Canonical schema và transformations

[DBML](dim_fact_v3.dbml) giữ **17 table bodies / 262 columns / 40 logical FKs**. [CSV](source_to_gold_mapping.csv) giữ **488 source branches**. Không có schema copy trong contracts/gold. `src/gold/spec.py` đọc approved DBML subset và kiểm mapping với current Silver contract hashes. Iceberg không tự enforce logical PK/FK; ETL phải kiểm chúng.

- Input `iceberg.silver_local`, output `iceberg.gold_local`, scope đúng 9 nguồn local_nine.
- K1: canonical typed JSON → full lowercase SHA-256; exact Unicode/source identities, không row number/sequence/delimiter concat.
- G1/P1/S1: source-scoped geography/product/series; chỉ WB có zero-hash non-geographic member. Không fabricated ISO/admin/product/FX conformance.
- Production/SUA/NSO giữ normalized values; trade monetary `1000 USD ×1000`; PSD `1000 MT/1000 HA ×1000`, milling fraction/yield ×1. Missing/zero/signed values giữ nguyên, checked Decimals không đổi rounding/range.
- NSO giữ full publication grain; FAO price monthly/annual phân hoạch rời; PSD market year và Yearbook source periods không calendar FK.
- Facts giữ 8 audits, Silver processing time/warnings, exact source BK/context, actual Silver UUID/pinned snapshot. Upstream `_source_snapshot_id=0` không là input pin.

## Modules và execution

| Module | Trách nhiệm |
|---|---|
| `spec.py`, `keys.py`, `rules.py` | Canonical schema/mapping, K1/JSON, finite unit/aggregation/precision rules |
| `admission.py`, `preflight.py` | Persisted Silver SUCCESS proofs, current UUID/snapshots/schema, writer state, ownership/resources |
| `dimensions.py` | Eight dimensions, exact dictionaries, FD checks và finite calendar spines |
| `facts.py` | Nine source-specific fact transformations; shared monthly fact replay giữ cả WB và FAO |
| `quality.py` | Requiredness/PK/FK/time/source checks, distributed exact `EXCEPT ALL` reconciliation |
| `catalog.py` | Owned tables, full snapshot overwrite, content-aware NOOP, commit/readback/recovery |
| `views.py` + SQL files | Read-only semantic loader/binder, Trino client/schema/execution/coverage/cell validations |
| `publication.py` | Immutable manifests + working/published pointers; existing pyarrow S3 adapter |
| `consumer.py` | Published read window, writer shared lock, manifest/hash/schema/snapshot checks, allowlisted query samples |
| `pipeline.py`, `api.py`, `orchestration.py` | Serial stages, strict job allowlist, runner startup code-fingerprint handshake |
| `jobs/gold/` | `preflight_gold.py`, `run_gold.py`, `query_gold.py` |
| `dags/dag_gold_pipeline.py` | Manual Gold DAG, 16 tasks, one active run/task; no Bronze/Silver trigger |

G01 source/year/month/date rồi dependent dimensions; facts **WB → production → SUA → trade → PSD → NSO → VN → FAO prices → Yearbook**. G11 validates toàn scope và semantic SQL trước publication. Spark writers nối tuần tự; không join tất cả sources thành một mega-frame hoặc cache full trade/provenance dataset.

## Publication, replay và recovery

Existing PostgreSQL advisory lock bảo vệ Gold writer; existing source locks bảo vệ admission. API dùng cùng serial job lock với Silver. Persistent ownership giữa các Spark stages ở `active.json`; heartbeat trước mutations. Không tự tiếp quản incomplete batch khác owner/context.

Full-snapshot overwrite phản ánh pinned Silver state, gồm Silver rows bị rút. Gold không sửa withdrawal semantics của Silver đối với Raw. Same inputs/code/refs và same content → không gọi Iceberg writer; physical NOOP kiểm UUID/snapshot/metadata/data-file set. G02 replay không xóa FAO branch đã có trong shared monthly fact.

Unknown write response phải đọc catalog/content trước retry. Verified committed content được nhận diện, không ghi lại chỉ vì response mất. Không DROP/rollback/expire snapshots để sửa lỗi. Partial/failed commits giữ lại và không publish. Retry đúng owner/phase/context; không sửa pointer mù. `jobs/gold/recover_gold.py` chỉ cho phép transition từ failed read-only validation, khi đủ 17 owned outputs có exact UUID/snapshot/metadata/files/rows, và inputs/model/reference/scope không đổi; code fingerprint mới được audit vào batch mới. Original failed state giữ nguyên. Recovery không ghi tables, batch mới phải chạy lại toàn DAG/DQ và semantic validations.

Lịch sử pre-G12: SUA unpivot bằng 12 `UNION ALL` lặp pivot plans đã heap OOM; single-pivot `UNNEST` qua được SUA nhưng full Trade còn OOM năm 2017. G12 giữ nguyên semantic SQL, chia Trade theo year/reporter bucket, exact multiset hai chiều và checkpointed provenance; full scope đã PASS trên Trino 448/512 MiB. Không suy chính xác cơ chế JVM của failure cũ khi không còn heap dump/query info. Cả hai failed states được giữ tại VALIDATION.md/evidence.

Prefix: `s3://warehouse/gold_publications/local_nine/`. Immutable `<batch>.json` giữ input UUID/snapshots, model/code/reference fingerprints/scope, output UUID/snapshots/files/rows, reconciliation và semantic validation. Full DQ/progress ở `working/<batch>.json`; `active.json` ownership, `published.json` pointer. Chỉ publish sau tất cả required gates/readbacks. Không tuyên bố atomic multi-table commit.

Consumer `query_gold.py` giữ Gold shared lock, từ chối unpublished/incomplete batch, kiểm context fingerprint, exact inventory, definition hashes/version/bindings, actual physical schema/UUID/snapshots. Nó bind SQL trực tiếp vào published snapshots, không query `iceberg.gold_local.vw_*`. Mặc định sample tối đa 20 observations, bắt đầu năm mới nhất; `--year` chỉ chọn năm trong validated coverage. Query/grain không bị aggregate lại. Direct Trino clients có thể bypass application gate; demo chỉ dùng raw table SQL sau published SUCCESS, không trong lúc ETL đang chạy.

## Chạy và trình diễn trên PowerShell

Từ repository root, stack/data hiện hữu được giữ qua các lần stop/start:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1
```

Mở Airflow `http://localhost:8085`. Chạy Bronze→Silver bằng workflow hiện hành nếu cần dữ liệu mới; **Gold không tự chạy hoặc chạy lại chúng**. Chỉ sau Silver SUCCESS, kiểm tra:

```powershell
python -B scripts/validate_gold_contracts.py
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/preflight_gold.py
```

Preflight phải READY. DAG Gold manual-only; có thể trigger qua UI hoặc CLI (đặt run ID mới):

```powershell
docker exec tlcn-local-airflow-scheduler-1 airflow dags unpause rice_lakehouse_gold_pipeline
docker exec tlcn-local-airflow-scheduler-1 airflow dags trigger --run-id manual__gold_demo_01 rice_lakehouse_gold_pipeline
docker exec tlcn-local-airflow-scheduler-1 airflow dags list-runs --dag-id rice_lakehouse_gold_pipeline --output table
```

Chờ Gold SUCCESS và published verification, rồi:

```powershell
docker exec tlcn-local-trino-1 trino --execute 'SHOW TABLES FROM iceberg.gold_local'
docker exec tlcn-local-trino-1 trino --execute 'SELECT count(*) FROM iceberg.gold_local.fact_nso_observation'
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/query_gold.py --view vw_nso_source_observations
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/query_gold.py --view vw_fao_production_yield
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/query_gold.py --view vw_sua_supported_balance
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/query_gold.py --view vw_trade_reported_unit_values --year 2024
docker exec tlcn-local-runner-1 python3 -c "import sys; sys.path.insert(0,'/workspace/src'); from collections import Counter; from gold.publication import Publication; p=Publication(); a=p.get('active.json'); s=p.get('working/'+a['batch']+'.json'); print(a); print(Counter((r['rule_id'],r['status']) for r in s['dq'])); print(p.get('published.json'))"
```

Replay cùng inputs/code/refs qua run ID mới sau run đầu SUCCESS. Đọc output `write=NOOP` và so snapshots/metadata/datafiles; count không đủ là chứng minh replay. Bằng chứng chạy thực tế tại [VALIDATION.md](VALIDATION.md).

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/stop.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1
```

Stop kiểm tra idle rồi pause cả ba DAG, giữ volumes/data/history. Start không tự trigger pipeline. Không dùng reset/delete volumes để trình diễn. Khi source code runner đổi, cần controlled restart riêng runner sau idle check; health phải có semantic version và startup code fingerprint khớp repository trước Gold request.

Chờ Gold SUCCESS trước khi dùng stop script. Một external Docker stop giữa NSO
đã được nghiệm thu recovery trong G12: đủ 9 Silver/17 Gold pins/schema/files còn
nguyên; restore Airflow sau RAM admission, pause Gold, standard task clear đúng
run/logical date cho NSO + downstream, scheduler reset orphan rồi resume. Không
dùng failed-validation recovery helper cho interrupted fact stage và không sửa
publication pointers/mark SUCCESS thủ công. Workflow thường ngày không cần clear.

Initial full Trade exact validation có thể mất ~51 phút do heap/history admission
ở 512 MiB; sum of SQL runtimes ~4,71 phút. Compatible sealed checkpoints được verify
và reuse, không giả là query mới. Formal full Gold replay đã chạy ~36,07 phút;
Trade fact dù NOOP vẫn cần exact recomputation/reconciliation (~12,82 phút).
Chuẩn bị DagRun SUCCESS trước buổi demo; consumer samples dùng year/bucket và
trả tối đa20 rows. Không cam kết unrestricted full-history interactive queries.

## G12 — exact Trade validation bounded by year/reporter bucket

`src/gold/trade_validation.py` giữ nguyên bốn SQL definitions. Trade được chia theo
`(year_key, mod(from_base(substr(reporter_geography_key,1,2),16),4))`. Reporter là
non-null K1 key đã kiểm; một semantic group `(year, reporter, partner, product)`
luôn nằm trong đúng một bucket. Quantity/value/import/export cùng group không
bị tách. Domain được enumerate từ actual pinned fact với LEFT JOIN metric admission,
giữ NULL-valued observations; tổng domain phải bằng physical snapshot rows.

Mỗi partition kiểm exact full-grain/metric/contributor/value `EXCEPT ALL` **hai
chiều**, unique semantic grain, presence/NULL/absent và cả hai unit-value ratios.
Một SQL trả bốn scalar result rows, không fetch toàn dataset. DISTINCT group
counts và row counts bổ sung cho multiset proof, không thay thế proof.

Checkpoints tại `gold_publications/local_nine/validation/<scope>/`: mỗi partition
chỉ được ghi immutable ACCEPT sau đủ assertions. Scope chứa validator version,
full code/reference/input context, semantic hashes/bindings và đủ 17 output
UUID/snapshot/metadata/rows/files. Checkpoint seal, result counters và query ID
được verify trước reuse. Incomplete progress không được publish. Runtime timings
được giữ ở checkpoints/progress; published summary deterministic để replay không
conflict với immutable manifest.

Trino giữ 448/512 MiB. Client có 64 MB query-memory cap, planning 30 s, execution
2 m, total query 3 m, tối đa 2.048 result rows và 4 MiB/page. Fetch failure/timeout
cancels the outstanding query without retry. Admission đọc actual JVM heap và
query history; chờ khi heap >=70% hoặc có >=80 queries trong 15 phút, dừng sau
960 s nếu không đủ headroom. Không ép GC, đổi server config hoặc restart loop.
Query khác đang active cũng chặn admission. Sau query, chờ tối đa 90 s cho natural
GC đưa used heap về <70% trước ACCEPT; không submit lại query. Nếu không hồi phục,
checkpoint vẫn INCOMPLETE. Một phiên bản gate đo tức thời >=90% đã dừng an toàn
trong G12 dù query đúng và JVM không crash; cooldown có giới hạn thay thế phép
đo đơn đó, không thay đổi bất kỳ exact DQ assertion nào.

Ba subject nhỏ NSO/production/SUA được kiểm toàn scope một lần khi actual admitted
fact <=40.000 observations (hiện 23.060/22.738/36.380), vẫn exact multiset và full
grain/metric. Dataset lớn hơn dùng year work units. Cách này giảm số retained
plans; không giảm historical coverage. Trade luôn dùng bốn buckets/năm.

Published consumer Trade lấy tối đa 20 rows qua validated year/bucket, vẫn bind
published snapshots và kiểm checkpoint coverage. Không cam kết unrestricted
full-history interactive performance. `--year` vẫn chỉ nhận validated years.

Read-only diagnostic commands trước recovery (active batch phải là failed validation):

```powershell
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/validate_trade.py --year 2017 --bucket 0
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/validate_trade.py --year 2017
docker exec tlcn-local-runner-1 python3 /workspace/jobs/gold/validate_trade.py
```

Partial commands có status INCOMPLETE đối với toàn Trade, kể cả subset đã PASS.
Không chỉnh batch pointers để nhận checkpoints. Explicit recovery chỉ chạy sau
full Trade ACCEPT và independent 9-input/17-output readback. Xem actual G12
evidence tại VALIDATION.md/runtime_acceptance.json; các failed runs trước vẫn giữ.

## Tests và giới hạn

```powershell
python -B -m pytest -o addopts= -q -p no:cacheprovider tests/unit/gold
docker exec -e GOLD_SPARK_TEST=1 tlcn-local-runner-1 /opt/spark/bin/spark-submit --master 'local[1]' --driver-memory 768m --conf spark.ui.enabled=false /workspace/tests/integration/gold/test_transformations.py
$env:GOLD_TRINO_TEST='1'
$env:TRINO_HOST='127.0.0.1'
$env:TRINO_PORT='18080'
python -B tests/integration/gold/test_views_trino.py
```

Unit/Spark/Trino fixtures không thay physical acceptance. Runtime giữ runner cap 3 GiB, driver 768 MiB, serial local[2], shuffle=2; cần Windows/WSL headroom và theo dõi spill/OOM cho trade. Official geo hierarchy/transition, cross-provider equivalence, FX, global shares/ranks và ML/DQ19 vẫn deferred. H01–H05 không tự đóng bởi Gold acceptance.
