# NSO V2 recovery reconciliation — 16 October 2026

Final status: **SHARED_SAFETY_BLOCKED**. This task performed read-only diagnostics
and evidence collection only. No test writer, service restart, reset or deployment
was executed. NSO V2 remains DRAFT. Previous reports are preserved unchanged.

Authoritative machine-readable results: [report.json](report.json).

## Engine and incident evidence

Docker responds: Engine 28.5.1, Desktop 4.50.0, Linux WSL2 backend. At collection,
the Windows host had 15.38 GiB RAM, approximately 0.61 GiB available (96% load),
and 70.66 GiB free on C:. Docker reported 13.93 GB images, 1.409 GB container
layers and 1.868 GB local volumes. WSL had approximately 7.75 GiB RAM and 4.71 GiB
MemAvailable at its later sample; swap remained zero. No setting was changed.
Host and WSL samples have different timestamps and are not interchangeable.

[Kernel/daemon evidence](incident-events.json) confirms global OOM killed the
historical shared Trino process/container `86c53ec37810…` at **22:27:18 on
15 October, Asia/Bangkok**. This is evidence of memory exhaustion. It does not
prove the exact cause of earlier Docker API 500/timeouts, or that the REST
runner itself was OOM-killed. Its OOMKilled flag is false.

Logs also show graceful Docker VM/daemon shutdown starting 22:33:28. The initiator
is unknown. REST exit 137 at 22:33:29 is confirmed termination, without proof of
an OOM kill of that container. Retained Docker events cover only the newer event
window; they cannot reconstruct the original incident on their own.

## Suite outcomes

| Suite | Started | Exit | Classification | Last confirmed outcome |
|---|---|---:|---|---|
| Native read-context + NSO isolated | Yes | 0 | PASS_CONFIRMED | 8 passed in 642.74 seconds |
| Pure semantics cases in REST runner | Yes | Combined process 137 | PASS_CONFIRMED | Six individual cases passed before REST began; no separate unit-process exit |
| REST E2E acceptance | Yes | 137 | OUTCOME_UNKNOWN | Ingestion SUCCESS 833; three catalog metadata pointers committed; final acceptance missing |
| Any retry/new project | No | — | NOT_RUN | Not authorized or executed |

Both writer containers are stopped; neither has OOMKilled=true. Raw recovered
logs are compressed in [native-tests.log.gz](native-tests.log.gz) and
[rest-tests.log.gz](rest-tests.log.gz). The complete native pytest summary, not
partial progress, confirms PASS. Expected negative-case exceptions and Spark
hostname warnings in its log do not override that successful exit/summary.

## Native committed state

The native Hadoop warehouse was copied from the stopped container without
starting it. An initial parent-directory copy hit a Windows pytest symlink
privilege error; targeted copies of the two actual warehouse directories
succeeded. Original artifacts were retained. No symlink privilege change was
needed. Full data files remain in ignored local recovery cache, not this evidence
directory.

| Native table | UUID | Current snapshot | Rows |
|---|---|---|---:|
| bronze.all_raw | `dbcb20dd-bf0d-4cc1-901a-e0108580b030` | `2863067317283698443` | 833 |
| silver.all_raw | `b635f0eb-00eb-491e-b5fd-3a2fb0d30171` | `271218712242393542` | 23,060 |
| silver.all_raw_dead | `986b6f42-00e0-4dc6-ba9b-3682d5dd7ae6` | `864175850825503501` | 13 exclusions |

Physical readback of 35 native Parquet files independently confirms 22,114
numeric, 946 missing, five zeros, 766 provisional observations and zero canonical
conversion mismatches. All observations carry the actual pinned Bronze read
snapshot `2863067317283698443`; `_source_snapshot_id` remains independently zero.
Geography codes and boundary versions are NULL throughout. The V06.12 count is
560; each G71 matrix has 2,130 observations and each G37 matrix 1,110.

There are 344 observations with diagnostic evidence, and 22,534 observations
with any DQ warning, including geography review. Rule/member counts are 12
cross-table, 309 area/yield/production and 28 season-total warnings; overlap means
their sum is not a distinct-observation count. Production-input quarantine is
zero; 13 structural source records are separate exclusion dead letters.

The completed native suite proves full replay, incremental replay using selected
source identities, repeated incremental replay and empty-delta replay preserve
Silver snapshot/schema/content checksum/processing timestamp. Exclusion audit
metadata is allowed to change separately. Native duplicate/conflict and DRAFT
execution gates also passed. See [physical readback](native-physical-readback.json)
and [native metadata](native-catalog-recovered.json).

## REST state and preservation limit

The historical real ingestion run was
`nso_vietnam_20261015152722_cc6e33e9`, logged SUCCESS with 833 extracted records.
The stopped REST server's SQLite catalog was copied and read with mode=ro.
[Catalog pointers](rest-catalog-pointers.json) show:

- Bronze `nso_vietnam`: metadata version `00013`, previous `00012`.
- Silver `dead_letters`: metadata version `00001`, previous `00000`.
- Silver `nso_rice_statistics_v2`: metadata version `00001`, previous `00000`.

These establish catalog metadata commits, including advancement of the Silver
target. They do not establish its current row count, UUID or snapshot ID without
the actual metadata objects. Directory UUIDs must not be used as table UUIDs.

The MinIO `/data` and PostgreSQL `/var/lib/postgresql/data` were tmpfs mounts.
Both services are stopped. Read-only copies of those paths recovered zero files.
Consequently S3 metadata/data objects, PostgreSQL rows/checkpoints and a fresh
Trino readback cannot be recovered through those stopped services. Their original
containers, catalog and available artifacts were neither deleted nor reset.

`/tmp/nso-v2-rest-acceptance.json` is absent. No final pytest summary exists for
REST. Do not infer absence of commits or declare REST/Trino/NOOP PASS. Current
REST UUID/snapshot/row count remain UNKNOWN; expected Silver rows are 23,060.

## Shared safety

[REST identity verification](shared-safety.json) and
[Trino SELECT readback](shared-trino-readback.json) confirm:

- All 29 table UUID/snapshot/schema/location/metadata identities match the latest
  operational baseline; the full shared inventory also contains exactly 29 tables.
- Eight Silver targets contain 1,391,467 rows.
- Quarantine has four logical records, snapshot `7788475764751255879`, matching
  logical hash `4e3f8cafcc1a2262c0abc5981e0fbaca620c3283497e85c8806b1976b7a1865a`.
- All five PostgreSQL ingestion metadata counts/hashes match, including 29 runs.
- Both DAGs are paused, serialized schedule=None; active runs/tasks and advisory
  locks are zero. Shared Spark has only its API Python process, with no writer JVM.

Accepted Spark row checksums remain associated with exactly the same immutable
table identities. They were **not freshly recomputed with a Spark job** during
this blocked investigation. Fresh Trino counts and quarantine logical hashing
were executed. No shared Spark pipeline was run as a diagnostic.

The overall gate is BLOCKED by runtime drift: **all seven shared containers were
recreated at 14:56:39 on 16 October, Asia/Bangkok**. Compose labels reference only
the base `docker-compose.yml`. Spark currently uses `tabulario/spark-iceberg:latest`;
both Airflow services use `apache/airflow:2.10.4-python3.11`. They are not the
accepted execution images. All four required `SILVER_*` environment keys are
absent from both Airflow containers. Absence is not evidence of production=1,
but it does not satisfy the approved manual-only configuration.

Accepted image references to restore only with separate owner deployment approval:

```text
tlcn/silver-spark@sha256:79723a677138a36a03d195d82564c5e9039abeb0a2e149b7dd89d8b289403843
tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96
```

Required flags: accepted release `silver_v2_release_20261013`, production `0`,
canary `1`, Bronze cron empty; retain both DAGs paused/schedule=None and restore
approved readonly registry/dependency/secret configuration. Do not touch storage
services, data volumes or Airflow run history to accomplish this.

## Minimal resumption proposal — not executed

Prefer **B**, a new REST-only isolated project after shared runtime passes a fresh
safety gate. The old REST catalog references unavailable volatile objects, so
resuming its writer cannot safely establish acceptance. Preserve native eight-case
and pure-unit six-case PASS; do not rerun the eight CLOSED sources.

There is also a static test-harness issue to settle before any approved REST
rerun: the test revises `V06.13.csv` under the same filename, while the existing
FullLoader can skip already-ingested filenames. Approve an NSO-only test change
that stages the unapproved publication through a separate isolated Bronze
namespace/read identity, then proves Silver blocks it while retaining the
accepted target. Do not change ingestion business semantics or common framework.
This code review finding was not exercised by a writer in this investigation.

Run only one Spark runner at a time, local[2], driver 768 MiB, runner cap 3 GiB,
using the existing pinned offline test dependencies. Check host and WSL available
memory before starting. Do not change resource settings or dependency versions
implicitly. New project storage must be on its own internal network, PostgreSQL
instance and MinIO warehouse; current test database/bucket names may repeat only
on those separate instances, with an NSO-specific test namespace/READY overlay.

Proposed commands below require owner approval and the reviewed test-only staging
change first. They have **not** been run. Inspect cached image IDs and ensure the
new project name is unused before applying. No shared services are included.

```powershell
$env:SILVER_TEST_RAW_ROOT = (Resolve-Path 'data/raw').Path
docker compose -p tlcn-nso-v2-rest-recovery-20261016 -f compose.silver-test.yml up -d --no-recreate --pull never minio-test rest-test postgres-test trino-test

$nsoTestCommand = 'set -e; python3 -m pip install --quiet --no-index --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/nso-deps pytest==8.4.2 psycopg2-binary==2.9.10 boto3==1.35.99; export PYTHONPATH=/tmp/nso-deps:/workspace/src:/workspace:/workspace/jobs/silver:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip; python3 -m pytest -q -s -p no:cacheprovider tests/integration/test_nso_v2_rest_e2e.py --tb=short'
docker compose -p tlcn-nso-v2-rest-recovery-20261016 -f compose.silver-test.yml run --no-deps --pull never -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 768m --conf spark.sql.defaultCatalog=spark_catalog pyspark-shell' runner $nsoTestCommand
```

Expected order: fresh gate → isolated ingestion SUCCESS/833 → Silver 23,060,
22,114 numeric/946 missing/five zero, quarantine zero/13 exclusions → native
snapshot/lineage/value checks → PyIceberg/Trino readback → full/incremental NOOP
with actual run identity → separately staged unapproved revision BLOCKED with
accepted Silver unchanged. Save actual identities/counts, not forced baseline PASS.

Next approval must explicitly cover: restoring the three execution services to
accepted manual-only configuration, the NSO-only revision test staging adjustment,
and the new REST project/writer test after fresh gates. It must not enable shared
writers, scheduling, Airflow triggers, Gold or consumer cutover. No Docker Desktop
restart is currently necessary or requested, because the engine is already available.

## Code/evidence integrity

HEAD remains `5c571693f90101c55d00ff84ceb5138092a1263c`. The four previously
fingerprinted NSO/framework artifacts match `implementation_progress.json`.
Historical release evidence hashes match closure. No production code, contracts,
test code or old evidence was edited by this recovery task; only this new evidence
directory was added. No tests were rerun, including pure unit tests.
