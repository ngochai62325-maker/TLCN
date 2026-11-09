# Gold physical acceptance — 26/10/2026

> **Final G12 status: GOLD_MVP_COMPLETE.** Hai actual Gold DagRuns SUCCESS,
> 17 tables / 1.414.527 fact observations, nine-source exact reconciliation,
> DQ01–18, bốn semantic queries, immutable publication/readback và consumers PASS.
> Formal post-publication NOOP giữ nguyên đủ 17 UUIDs/snapshots/metadata/files và
> manifest hash. Full Trade phủ 156 partitions / 1.226.470 observations /
> 526.986 groups, zero differences. Các failure records được giữ như lịch sử.

## G12 diagnosis và targeted optimization

Fresh baseline vẫn branch `feat(gold)`, HEAD `c5ec191f756af7dc33bbd22b4acc75e05fb06003`,
active batch `c5fe15d109c8eb740b558f8eb56c5e16f2ee897fb2391994b94fb2400e4e90b8`
FAILED_NOT_PUBLISHED, published pointer absent, no active Airflow runs/tasks.
Seven services healthy where healthchecks exist, Trino 448 actual max heap
536.870.912 bytes, WSL available ~5 GiB, Windows free ~2,97 GiB. Fresh source
proof/current catalog and 17-table exact schema/UUID/snapshot/metadata/files
readback match the preserved candidate. No table writes in G12 diagnostics.

Old 2017 one-direction comparison: 2.170 SQL bytes, 12.469 plan bytes, six
distributed fragments, one metric join, four aggregations and a multiset window.
Year filter reaches fact scan; this is not evidence of fragment explosion.
No EXPLAIN ANALYZE/full failing old workload was executed. Old second-failure
query details were lost with the JVM; exact planning/execution/history causation
is still unverified. Python consumed only scalar results and overwrote each HTTP
payload; no full Trade result was buffered by the old validation loop.

New validation leaves the semantic SQL file byte-identical. Each year has four
disjoint reporter-K1 buckets. Actual pinned domain is 1986–2024, 39 years ×4 =156
nonempty partitions, 1.226.470 observations. Reporter is in the complete grouping
identity; filtering on year/reporter bucket commutes with group-by and keeps
all four quantity/value metrics together. Domain enumeration includes present
NULL rows, rejects invalid keys/metric joins, and sums to snapshot row count.
Full-grain + metric + observation key + Decimal value EXCEPT ALL in both
directions proves exact cells; counts alone never certify equality.

One bounded composite query returns four scalar rows for expected/actual groups,
presence/paired ratios and both multiset directions. The actual plan has 18
fragments, six metric joins, 12 scans (six fact/six dimension), two windows.
It has more fragments than one old directional query because it combines
assertions. **All six join filters contain the reporter bucket before pivot
aggregation.** Bucket is not Parquet scan pushdown; fact projection is only
seven columns, and year reaches scan filtering. This bounds group/window state,
not all physical I/O. Initial controlled partitions read ~34,18 MB each and
returned four rows; planning ~0,30–0,39 s, execution ~2,69–5,50 s, tracked query
peak ~23,01–39,40 MiB, spill 0. Heap transiently rose; admission waited without
forcing GC, changing limits or restarting. Initial and final-code 2017 both
passed; checkpoints from different code fingerprints were not carried forward.

Implementation code fingerprint sau bounded cooldown:
`ce5f23517281b552d02f25529a70b064af320902976b4bf9c5291c99a564fa98`.
Checkpoint scope:
`0081168fb4ee788426cf068e8249d5c2fe21cc5bb36ef5c0ecc313adeb680955`.
138 unit tests and nine small Trino fixtures PASS. Fixtures compare old/new
rows/schema/multiplicity and detect altered values, omitted groups, duplicate
metrics; include reversed reporter/partner, present NULL/absent, zero, signed,
high-precision values, empty and skewed partitions. Not a full E2E claim.

G12 có một safe resource-gate failure trước full acceptance: query đầu của full
scope trả exact results đúng nhưng used-heap sample >=90%, nên không ghi ACCEPT.
JVM không crash (running/exit0/OOMKilled=false/restartCount0), heap sau đó ~257 MiB.
Gate được sửa thành bounded idle cooldown tối đa 90 s tới <70%, không retry SQL
hoặc ép GC. Final-code 2017 đã PASS lại đủ bốn buckets trước khi chạy full scope.
Scope/code của các lượt trước không được reuse; failed resource progress giữ nguyên.

Checkpoint/recovery/resource budgets and bounded consumer behavior are documented
in [IMPLEMENTATION.md](IMPLEMENTATION.md). Consolidated G12 evidence is appended
under `phase_g12` in [runtime_acceptance.json](runtime_acceptance.json), preserving
the two failed attempts below. Full coverage đã được xác minh lại bằng mọi
checkpoint seals; publication, Airflow và formal replay evidence chỉ được ghi
PASS sau actual acceptance.

## G12 full Trade standalone result

156/156 partitions ACCEPT, zero forward/reverse/presence/ratio differences.
Full-loop elapsed **3.073,921 s (~51,23 phút)**, sum of producer query elapsed
**282,602 s**; phần lớn wall time là conservative heap/history admission và
natural-GC cooldown. Bốn controlled-2017 checkpoints cùng code/SQL/refs/snapshots
được verify/reuse; 152 partitions còn lại thực thi trong full loop. Query peak
**42.305.692 bytes (~40,35 MiB)**, spill **0**, sampled heap peak **525.012.000
bytes** dưới max 536.870.912, 304 samples, no monitor errors, Trino OOM counters 0.
Standalone sampled runner current peak 179.249.152 bytes; WSL available minimum
4.246.716 KiB. Trong standalone phase không Trino restart/config change/forced GC
hoặc table writes; external runtime interruption sau đó được ghi riêng bên dưới.

Final-code actual 2024/bucket3 query `20261026_090259_00245_gxw77`: 4.144 SQL bytes,
18 fragments, six bucket-filtered joins before pivot, seven projected fact
columns, 1,95 s elapsed / 242,76 ms planning / 16.126.463 B tracked memory / spill0.
This confirms bounded state, not Parquet bucket pushdown or fragment reduction.
Exact old failing-query heap cause remains unverified without its lost JVM data.

Fresh 9-input/17-output readback after standalone/Spark regressions preserved
all UUIDs/snapshots/metadata/files. 138 unit / 7 Spark (71,438 s) / 9 Trino fixture
tests PASS. Controlled idle runner restart loaded fingerprint `ce5f2351…`.
Explicit recovery admitted batch
`ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235`,
from previous failed batch with **17 verified tables / 0 table writes**.
Manual run `manual__gold_g12_20261026_01` đã SUCCESS, đủ 16/16 tasks; batch trên
đã PUBLISHED. Full DQ/semantic task SUCCESS **593,1 s**, publication task **24,8 s**.
Actual required DQ01–18 đều ACCEPT, nine-source exact reconciliation PASS,
immutable manifest/readback PASS. Bốn published consumers trả 20 rows/query;
SUA dùng latest validated year 2023, ba queries còn lại dùng 2024. Collector
chọn SUA 2024 trước đó đã bị consumer reject đúng vì ngoài coverage.

Formal run duy nhất `manual__gold_g12_noop_20261026_01` được trigger sau readback
baseline và idle/resource admission; **SUCCESS / final physical NOOP PASS**.

## G12 external runtime interruption và standard Airflow resume

Existing Docker containers dừng lúc **26/10 09:31:21 UTC / 16:31:21 Asia/Bangkok**,
trong khi task NSO đang chạy. Airflow containers exit137 nhưng `OOMKilled=false`;
runner/Trino exit0. Không quy kết initiator khi chưa có evidence. Core services
khởi động lại khoảng **12:12 UTC / 19:12**, Airflow còn tắt; không còn Spark JVM
hoặc writer locks nhưng NSO metadata vẫn RUNNING. Đây không phải semantic OOM.

Readback sau gián đoạn xác nhận chín Silver UUID/snapshots, đủ 17 Gold exact
schemas/UUID/snapshots/metadata locations/data-file sets, reference hashes và
code fingerprint không đổi; published pointer vẫn absent. Checkpoints và
temporary benchmark evidence còn nguyên. No unknown/drifted Gold commit.

Windows available ban đầu chỉ **1.049.736 KiB**, dưới startup gate 2 GiB; agent
giữ Airflow tắt. Owner giải phóng RAM, actual recheck **3.141.192 KiB** và WSL
6.590.596 KiB trước restore. Chỉ start lại hai existing Airflow containers.
Bronze/Silver DAGs vẫn paused, không trigger; không rebuild/reset/migrate.

Gold được pause trước standard `airflow tasks clear`, chọn đúng logical date
`2026-10-26T09:07:42+00:00`, task `^fact_nso$` và downstream. Scheduler tự reset
orphan RESTARTING sang scheduled/running; NSO attempt2 bắt đầu **12:32:39 UTC**.
Trade và mọi stage đã SUCCESS giữ nguyên; hai failed historical runs vẫn FAILED.
Không chỉnh publication JSON/DB để ép SUCCESS. NSO attempt2 SUCCESS **75,8 s**.
First DagRun wall time có external downtime, không coi nó là uninterrupted
performance benchmark. Incident này xảy ra trước formal replay; final replay
result và uninterrupted timings được ghi ở mục tiếp theo.

## G12 final publication, consumer và formal NOOP acceptance

| Acceptance | Actual result |
|---|---|
| First Gold run | `manual__gold_g12_20261026_01`, **16/16 tasks SUCCESS**, NSO attempt2 sau external interruption |
| Published batch | `ec421297c194804dc7a3a63aacfda37a65dacfac07245d118e34b34506ca2235` |
| Formal replay (exactly one) | `manual__gold_g12_noop_20261026_01`, **16/16 tasks SUCCESS**, same inputs/model/code/refs/SQL definitions |
| Replay runtime | **2.164,223366 s / 36,07 phút**, 19:54:31 → 20:30:35 Asia/Bangkok, 26/10/2026 |
| Trade fact replay | **769,0 s**, full recomputation/exact reconciliation, physical NOOP |
| Replay final DQ/semantic task | **550,8 s**, SUCCESS |
| First-run DQ | **1.185 ACCEPT**, all required DQ01–18, zero FAIL/WARN |
| Replay fresh DQ | **1.185 new ACCEPT**, all DQ01–18; working-state cumulative 2.370 |
| Actual NOOP readbacks | **18 new DQ16**, **17 unique tables**; shared monthly checked at both source stages |
| Physical comparison | **17/17 same UUID, snapshot ID, metadata location, rows and exact data-file sets**; no new Iceberg commit |
| Logical comparison | Fresh full source→Gold key/value/NULL/sign/lineage reconciliation repeated for all9 sources |
| Immutable publication | Same batch and canonical manifest SHA256 `631c0d372fcb37dfd81466622012a42311b867c028f570b4465658a9136a046b`; manifest readback PASS |
| Consumer read window | Actual rejection `Gold writer active; read window closed` during replay with published pointer present; all4 consumers reopened after SUCCESS |
| Input protection | Nine Silver UUID/snapshots unchanged; no Bronze/Silver writes, triggers or contract/business-logic changes |

Actual full semantic results, identical in published run and formal replay:

| Query | Full semantic rows/groups | Years | Published consumer |
|---|---:|---|---|
| vw_nso_source_observations | 23.060 | 1990–2024 (35 years) | 20 rows, 2024, PASS |
| vw_fao_production_yield | 7.714 | 1961–2024 (64 years) | 20 rows, 2024, PASS |
| vw_sua_supported_balance | 4.825 | 2010–2023 (14 years) | 20 rows, 2023, PASS |
| vw_trade_reported_unit_values | 526.986 | 1986–2024 (39 years / 156 partitions) | 20 rows, 2024 / bounded reporter bucket, PASS |

Trade full-scope sealed proofs were produced on actual pinned snapshots before
recovery; both DAGs independently enumerated the domain and verified/reused all
156 compatible checkpoints. Same code/context/definitions/17 output metadata/file
sets are required, so this is audited proof reuse, not a claim of new query
execution per cached partition. All other subjects and global Spark reconciliation
were executed again. Fixtures are separate: **138 unit / 7 Spark / 9 Trino PASS**.

Replay memory sampled 207 times: runner current peak **3.221.037.056 bytes**, Trino
heap peak **524.884.992 bytes**, minimum WSL available **2.920.380 KiB**. Runner cap
3 GiB caused reclaim events (`max` 2.998→9.255), but **oom=0 / oom_kill=0**, no
JVM crash or replay service restart. No heap/cap/image/catalog changes. Admission
was checked before each stage; peak pressure during exact processing is recorded,
not hidden. Initial uncached Trade validation remains slow at 512 MiB (~51 min,
SQL sum ~4,71 min); bounded samples/checkpointed replay are the supported demo path.

All 17 table bodies / 262 columns / 40 logical references / 488 mapping branches
and four semantic SQL files remain unchanged from the G12 baseline. Git branch
`feat(gold)`, HEAD `c5ec191f756af7dc33bbd22b4acc75e05fb06003`, uncommitted work kept;
no commit/push. Official geography/conformance/FX/ML/DQ19 and H01–H05 foundation
backlog stay outside this completed local Gold MVP. Machine evidence is under
`phase_g12.first_published_run`, `replay_published_run`, `formal_replay` in
[runtime_acceptance.json](runtime_acceptance.json).

## Historical pre-G12 status and evidence

**Final status: `GOLD_BLOCKED`.** Đã có đúng 17 physical tables/schema/data và bốn executable semantic SQL definitions, nhưng Trino Java heap OOM ngăn hoàn tất trade semantic validation. **Không publication, không Gold DagRun SUCCESS, không GOLD_MVP_COMPLETE.** Runtime retries đã dừng, dữ liệu/snapshots được giữ. Consolidated machine-readable evidence: [runtime_acceptance.json](runtime_acceptance.json).

## Baseline và phạm vi

Branch `feat(gold)`, HEAD `c5ec191f756af7dc33bbd22b4acc75e05fb06003`. Working tree đã có uncommitted Gold implementation từ phiên trước, được giữ và cập nhật theo owner decision. Không commit/push; không sửa Bronze/Silver contracts/business logic/DAG hoặc Compose/images; không reset/rebuild/migrate catalog.

Fresh preflight xác minh 7 existing project containers, không active runs/tasks/advisory locks/ingestion IN_PROGRESS/Trino queries và không Spark JVM. Gold namespace/prefix ban đầu trống. Windows available ~2.69 GiB (sau đó ~3.03 GiB), WSL MemAvailable ~5 GiB. Controlled restart **riêng runner** sau idle check; startup health semantic version `gold-semantic-20261026.1` và code fingerprint `370b62c7f695e3ae5cbd3d38d1e820fc9bdce3d57ec388949668e708495d5761` khớp repository trước Gold trigger.

Model giữ **17 table bodies / 262 columns / 40 logical FK refs / 488 mapping branches**. DBML chỉ cập nhật lifecycle/semantic-delivery notes, canonical CSV và historical 22/10 artifacts không đổi.

## Fresh Silver inputs

Actual UUID/snapshots khớp latest persisted Silver SUCCESS run:

`local_nine_bronze__01642403687b19185d020dc67609ba14016444a733e68e54d789c5e4a567984b`

| Source | Actual Silver UUID | Pinned snapshot | Rows |
|---|---|---|---:|
| faostat_production | 1a77df77-5818-447e-8013-cc0b8373f7f1 | 4148714342754332207 | 22,738 |
| faostat_monthly_price | fdb60dcf-42d4-4d5d-845f-7d316be7b8d6 | 4111066513304943577 | 15,334 |
| faostat_supply_utilization | fba03ec7-0b17-4264-a5ba-37932eba3b7b | 8123855978320232066 | 36,380 |
| faostat_trade | 875dab2c-6eaa-4e0a-91e1-82ed6a43ad75 | 8505324700177525410 | 1,226,470 |
| usda_psd | afd22c11-134e-4f9e-bb84-8695aaf2f82c | 7561675754441800451 | 990 |
| usda_rice_yearbook | 12f9f1a7-030b-4b4f-99cf-6eb4a4b0357f | 2558498974942784461 | 12,936 |
| worldbank_pinksheet | f9a342d8-f3ee-4287-9966-f57b0672e2ab | 4338448473297469539 | 56,232 |
| thitruongnongsan | 231603f2-5d5a-493f-8bff-8c479ed52174 | 6096019421881505089 | 20,387 |
| nso | 3fa9ef1e-24dc-4803-9ca9-7b123a7453b4 | 139672476149787931 | 23,060 |

Preflight metadata/proof counts đã được kiểm lại bằng full pinned Spark input admission trong Gold DAG: schema, required/BK/lineage non-null, duplicate keys, contract blocking rules và physical completeness. Đây là fresh Gold admission; không tự đóng toàn H01–H05 hoặc chứng nhận Bronze recovery.

## Static/fixture regression results

| Command/check | Actual result |
|---|---|
| `python -B scripts/validate_gold_contracts.py` | 17 tables / 262 columns / 488 branches / 40 refs PASS |
| `python -B -m pytest -o addopts= -q -p no:cacheprovider tests/unit/gold` | **101 PASS**, thêm SQL/hash/publication/consumer/schema và explicit recovery admission/drift cases |
| Existing runner `spark-submit ... tests/integration/gold/test_transformations.py` | **7/7 PASS, 81.680 s**, 8 dims/9 facts/schema/PK/FK/repartition/NULL/sign/lineage/shared-monthly replay |
| `GOLD_TRINO_TEST=1 python -B tests/integration/gold/test_views_trino.py` | **6/6 PASS**, bốn original SELECT fixtures + exact output schemas + single-plan unpivot NULL/absent/signed multiset regression |
| Trino bounded `EXCEPT ALL` multiset fixture | PASS; multiplicity được giữ |
| Airflow real `DagBag` import | 0 errors, 16 tasks, schedule=None, catchup=False, max_active_runs/tasks=1 |

Trino decimal output types được xác minh trực tiếp: production computed yield `decimal(25,3)`, trade unit values `decimal(24,2)`. HTTP client khai báo `PARAMETRIC_DATETIME` để metadata timestamp giữ precision 6, không fallback legacy millisecond representation. Source SELECT logic/physical schema không đổi.

Prior implementation baseline (trước owner đổi semantic delivery): 75 unit, 7 Spark, 4 Trino fixtures PASS nhưng native views bị block; có một existing local_nine regression setup error do Windows temp ACL. Không biến các kết quả cũ hoặc fixtures mới thành physical E2E PASS.

## Actual Gold execution

Run ID: **`manual__gold_sql_e2e_20261026_01`**, triggered manually through existing Airflow.

Candidate batch: **`98cc5232699c19f650985a8f30ba4f5eaf057993867290db8649f4931476d4d6`**.

Đã hoàn tất runtime stages của run đầu: source admission, đủ 8 dimensions/9 facts và full Spark PK/FK/key/value/NULL/sign/lineage reconciliation. Trade **1,226,470 rows**, stage SUCCESS **882.8 s**. Semantic SQL validation tiếp tục sau đó; xem failure/recovery dưới đây. Chưa có final SUCCESS/NOOP claim.

Consumer thử đọc giữa ETL đã **từ chối đúng**: `Gold writer active; read window closed`. Không query unpublished Gold như một batch complete.

## 17 candidate physical tables

Independent PyIceberg readback xác nhận exact column names/types/requiredness của đủ 17 tables. Counts dưới đây từ actual Iceberg snapshot metadata, khớp Spark write/readback counts; source PK/FK/value checks thực thi trong DAG. Đây là candidate inventory, chưa thay thế publication/NOOP acceptance.

| Table | Rows | Actual snapshot | Data files |
|---|---:|---|---:|
| dim_source_system | 9 | 5212267582300439758 | 2 |
| dim_year | 67 | 3316235776078407392 | 2 |
| dim_month | 800 | 8295574410069434036 | 2 |
| dim_date | 2,038 | 7665947661409225898 | 2 |
| dim_source_geography | 987 | 8664595598201008097 | 2 |
| dim_source_product | 295 | 8514812747103222137 | 1 |
| dim_observation_type | 34 | 1046146616745412073 | 2 |
| dim_price_series | 1,042 | 2896601742107309180 | 4 |
| fact_fao_production_observation | 22,738 | 4557995850181557281 | 16 |
| fact_fao_sua_observation | 36,380 | 5998541611407626400 | 4 |
| fact_fao_trade_observation | 1,226,470 | 9089157317142504426 | 10 |
| fact_usda_psd_observation | 990 | 1998504400080667561 | 17 |
| fact_price_monthly | 61,573 | 4322554606104063046 | 21 |
| fact_fao_price_annual | 9,993 | 6561274137596983252 | 9 |
| fact_usda_price_reference | 12,936 | 640990871150888729 | 11 |
| fact_vn_market_quote_daily | 20,387 | 8271577785702045919 | 2 |
| fact_nso_observation | 23,060 | 4536483924352632178 | 9 |

## Actual failure và controlled recovery

Run `manual__gold_sql_e2e_20261026_01` **FAILED** tại `dq_reconciliation_views_and_snapshot_readback`: Trino 448 Java heap OOM khi validation SUA 2017 dùng 12 `UNION ALL` lặp pivot/join/scan plans. Trino exited code 3, `OOMKilled=false` (JVM heap failure, không kernel cgroup kill). Gold state `FAILED_NOT_PUBLISHED`, **866 DQ outcomes đã lưu**, published pointer absent; toàn bộ 17 data snapshots được giữ. Đây là failure thực tế, không đổi thành PASS.

Root fix chỉ đổi implementation của phép kiểm unpivot: một pivot + `UNNEST(ARRAY[ROW(...)])`, vẫn full cells/key/value/NULL/multiplicity và exact `EXCEPT ALL` hai chiều. Real SUA 2017 tại pinned snapshot trả **0/0 differences**. Không đổi bốn SELECT definitions/grains/units hoặc physical schemas; không tăng RAM hay nâng image/config.

Sau khi verify fail/idle: start lại đúng Trino container cũ và reload riêng runner. Explicit recovery đã đọc independent actual schema/requiredness, UUID/snapshot/metadata/data-file set/rows của đủ 17 tables và xác nhận inputs/reference/model/scope không đổi. **Table writes trong recovery: 0**. Failed state cũ không bị ghi đè.

Recovery run: **`manual__gold_sql_e2e_20261026_02`**. New batch: **`c5fe15d109c8eb740b558f8eb56c5e16f2ee897fb2391994b94fb2400e4e90b8`**. Code fingerprint: **`1a1943b4a4cd99e9f02195b5d0cf651e3b5aed55f4c3c5a51599f99f8f06b240`**. Toàn DAG đã chạy lại; đủ 17 tables xác nhận writer `NOOP`, không đổi UUID/snapshot/metadata/files. Tuy nhiên full semantic workload lại làm Trino Java heap OOM tại trade 2017; run **FAILED**, active batch **FAILED_NOT_PUBLISHED**. Recovery cải thiện plan và qua được toàn SUA, nhưng chưa chứng minh đủ tài nguyên cho toàn workload. Không khẳng định cơ chế memory retention/planner cụ thể của failure thứ hai khi chưa profile JVM.

Trino exited code 3 lúc **07:07:21 UTC / 14:07:21 Asia/Bangkok**, `OOMKilled=false`. Zero active Airflow tasks và không Spark JVM sau failure. Đã start lại đúng Trino container để khôi phục availability, không đổi config/image/catalog hoặc rerun Gold. Actual `/etc/trino/jvm.config` vẫn `-Xmx512M`. Consumer thực tế tiếp tục từ chối: `Gold batch not published; read window closed`.

## Real semantic SQL results

Tất cả bốn query names đã parse/bind và có exact expected output schema trên actual candidate snapshots. Validation outcomes được lấy từ persisted DQ, không từ fixtures:

| Query | Coverage/actual results | Status |
|---|---|---|
| vw_nso_source_observations | 1990–2024, đủ 35/35 years, source row coverage; 36 accepted checks | Candidate validation PASS |
| vw_fao_production_yield | 1961–2024, đủ 64/64 years, cell multiset/presence/paired yield; 257 accepted checks | Candidate validation PASS |
| vw_sua_supported_balance | 2010–2023, đủ 14/14 years, 12 cells/NULL/absent/signed exact; 43 accepted checks | Candidate validation PASS |
| vw_trade_reported_unit_values | Expected 1986–2024 (39 years); group coverage tới 2017 (32 years), exact cell comparison hoàn tất tới 2016 (31 years); 158 accepted checks | **INCOMPLETE / BLOCKED** tại cell validation 2017 |

Không có native persisted views hoặc aggregate tables. Không công bố bốn query definitions là đã hoàn tất published-batch acceptance.

## DQ, reconciliation, publication và physical NOOP

Full pinned Silver admission và exact distributed row/key/value/NULL/sign/context/audit/lineage reconciliation đã chạy cho **9/9 sources**, tổng **1,414,527 fact observations**. PK/unique và 40 logical FK checks đã thực thi trên physical output; no observed duplicate/orphan failures. FAO prices: 5,341 monthly + 9,993 annual = 15,334 input rows; shared monthly total 61,573 gồm WB 56,232.

Recovery state có **1,040 ACCEPT outcomes**, không có known data-mismatch result; execution exception vẫn làm toàn batch FAIL. DQ01–09/11/14/15 đã thực thi; DQ10/12 mới partial vì trade chưa đủ coverage. DQ13/18 final assertions chưa được gọi vì semantic stage chưa kết thúc; DQ17 publication gate **BLOCKED**. Không báo DQ01–18 all PASS. DQ19 DEFER.

DQ16 có 18 actual NOOP readbacks (monthly được kiểm ở hai source stages), phủ **17/17 tables**. Independent PyIceberg capture so với original committed candidate xác nhận cùng **UUID, snapshot ID, metadata location, row counts và exact data-file sets** cho đủ 17 tables. Có bằng chứng physical NOOP thực, không chỉ mock/count. Tuy nhiên formal replay cùng code/refs **sau successful publication** chưa chạy; code fingerprint của initial failed attempt khác recovery. Không đánh dấu final replay acceptance PASS.

Published pointer và immutable final publication manifest **chưa được tạo**; original failed state và recovery state còn trong MinIO. Consumer fail-closed thật đã xác minh cả khi writer active và sau khi batch failed. Valid published consumer query/readback và Gold DagRun SUCCESS: **NOT_VERIFIED / BLOCKED**.

## Resources và operational limits

Runtime Spark local[2], driver 768 MiB, shuffle=2, runner cap 3 GiB; writers serial. Initial full validation đạt runner cgroup peak **3,160,829,952 bytes (~2.94 GiB)**; recovery peak **3,100,647,424 bytes (~2.89 GiB)**, memory.events `oom=0`, `oom_kill=0`, `max=0`. Trino có hai **JVM heap OOM** dù không kernel OOM kill. Trade task đầu 882.8 s, recovery 805.4 s. CPU gần 2 cores; spill filesystem ~942 GiB available. Không tăng RAM/caps hoặc cache toàn dataset để vượt lỗi.

**Blocker tiếp theo cần xử lý bằng task riêng:** profile/reduce full trade semantic validation planning/history/memory trên đúng Trino 448 heap hiện hữu, giữ exact coverage/reconciliation; chưa đủ evidence để chọn một nguyên nhân retention cụ thể. Không restart/rerun vòng lặp để che failure. Khi có fix được test, dùng explicit recovery checks và mới chạy lại full acceptance/publication/replay. Hiện đã dừng phần runtime phụ thuộc theo owner constraint.

Official geo/transition, cross-provider equivalence, FX/global shares/ML/DQ19 vẫn deferred. [Workflow/demo/recovery/commands](IMPLEMENTATION.md). Gold completion chỉ sau actual 17-table reconciliation, four semantic query validations, published consumer, physical NOOP và Airflow SUCCESS.

## Final safety/static readback

- Seven existing services restored to running/healthy where healthchecks exist; zero active Airflow tasks/Spark writers. No further Gold runtime retries after the second failure.
- Fresh Silver success proof vẫn đúng 9 UUID/snapshot IDs ở bảng trên; source business logic/contracts/DBML table bodies/canonical mapping/historical artifacts không đổi. Không commit/push.
- Actual `jcmd 1 VM.flags` của Trino xác nhận `MaxHeapSize=536870912` bytes (512 MiB); không chỉ suy từ config file. Không thay heap hoặc infrastructure.
- Final 101 unit tests PASS, Gold mapping validator PASS, 9/9 Silver contract validator PASS, 32 Python AST PASS, 272 relative Markdown paths hợp lệ, evidence JSON parse được và đủ 17 tables. Official @dbml/core 10.3.1 cả dbml/dbmlv2 parse 17 tables/262 columns/40 refs. `git diff --check` sạch, chỉ có Git LF→CRLF informational warnings trên handoff.
