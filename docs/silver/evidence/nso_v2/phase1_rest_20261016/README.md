# NSO V2 Phase 1 — local REST E2E

Status: **NSO_V2_ISOLATED_VALIDATED_PENDING_OWNER**. Production contract remains
**DRAFT**. This is isolated technical validation, not shared acceptance or Airflow
integration. HEAD remains `5c571693f90101c55d00ff84ceb5138092a1263c`.

The previous [recovery report](../recovery_20261016/README.md), native PASS
artifacts, interrupted REST project and eight-source release evidence are intact.
The historical REST outcome remains UNKNOWN; this result belongs to a new project.

## Execution and observed results

| Test | Actual outcome | Exit |
|---|---|---:|
| Same-filename revision staging, real adapter/PG/S3/REST | 1 passed in 42.76s | 0 |
| NSO Raw → Bronze → Silver → PyIceberg/Trino, NOOP and revision guard | 1 passed in 916.39s | 0 |
| Existing native/read-context suite | Previously confirmed 8/8 PASS; not rerun | Previous exit 0 |
| Existing pure semantics cases | Previously confirmed six PASS; not rerun | See recovery report |

The full runner ran from `2026-10-16T09:08:53.623334166Z` to
`2026-10-16T09:24:18.185981774Z`, without OOM. Exact process IDs, timestamps,
exit codes and sanitized logs are in [rest-process.json](rest-process.json),
[rest-tests.log.gz](rest-tests.log.gz), [staging-process.json](staging-process.json)
and [staging-tests.log.gz](staging-tests.log.gz).

Authoritative test result: [rest-acceptance.json](rest-acceptance.json).
Independent post-exit REST/Trino verification: [rest-final-readback.json](rest-final-readback.json).

| Observation | Actual count |
|---|---:|
| Pinned source CSVs | 13 / 137,301 bytes |
| Main Bronze records | 833 |
| Source records, excluding structural footers | 820 |
| Structural excluded records | 13 |
| Structural excluded observation slots | 368 |
| Silver / unique business keys | 23,060 / 23,060 |
| Numeric / marker-missing / numeric zero | 22,114 / 946 / 5 |
| V06.12 / provisional | 560 / 766 |
| Production-input quarantine / duplicate observations | 0 / 0 |
| Observations with any DQ warning / diagnostic evidence | 22,534 / 344 |

`dead_letters` contains exactly 13 `nso_vietnam_v2_exclusions` records, not
production quarantine. Diagnostic rule/member counts are cross-table 12,
area/yield/production 309 and season-total 28; members overlap.

| Main table | UUID | Final snapshot |
|---|---|---|
| `rebuild_test.bronze.nso_vietnam` | `3e00b19f-0c38-4f6d-9eac-90c4ca51dd08` | `8786414080148355674` |
| `rebuild_test.silver.nso_rice_statistics_v2` | `4ddaaa80-c114-4024-bfa1-27a5be3129d1` | `8595382440216504953` |
| `rebuild_test.silver.dead_letters` | `5ef06f78-8ec1-4228-8e38-2842699bc6cf` | `7208616430311507188` |

Silver schema ID is 0. Every observation, including all 946 missing observations,
retains the actual integer-64 Bronze read snapshot `8786414080148355674` and all
eight original Bronze audit fields. `_source_id` remains `nso_vietnam`;
the Silver dataset is `nso_vietnam_v2`; `_source_snapshot_id` remains independently
zero. Trino lineage EXCEPT returns zero mismatches.

Original ingestion run: `nso_vietnam_20261016090907_24ab5e6d`.
Full and incremental replay used this real source identity and preserved Silver
snapshot, schema and complete row checksum, including processing timestamps:
`a4b5b8742cfc65c70ceba1b285b55f53758eebd1564776b2607bc4b941641664`.
Bronze checksum replay was SKIPPED and preserved its original identity/content.
Exclusion audit metadata may change during replay; revision rejection preserves
the latest exclusion snapshot/content too.

All observations were compared with the independent Raw oracle, then read through
PyIceberg and Trino. Checks cover DECIMAL(28,8) values, source/raw labels, all
contract fields, metadata/unit/geography versions, five distinct seasons,
development indices, provisional/missing semantics and diagnostic warnings.
Unit counts: hectare 7,640; tonne 7,640; kg/hectare 7,500; percent 280.
Geography counts: 22,500 candidate-review and 560 national-scope observations.
All geography codes/boundary versions remain NULL, including historical Hà Tây.
Existing native duplicate/conflict and concurrent pinned-snapshot regressions
remain the supporting policy/concurrency evidence; they were not rerun here.

## Revision staging and isolation

The test-only helper switches the existing ingestion engine to a separate
`BronzeIcebergWriter` namespace. It never changes FullLoader, adapter, production
framework, source policy or the original publication bytes. The focused test uses
a separate PostgreSQL database and retains both 833-row versions.

In the full E2E, `V06.13.csv` changes 7192.5 → 7192.6 in a temporary Raw copy.
The revised checksum is actually present on 72 staged Bronze records, and the
revised value is physically read back. The accepted CSV copy is retained.

- Original SHA-256: `4aeba037c50f5ff2ff091f4b8804e3a39753ef008da9b83da52036c5e0ea7110`.
- Revised SHA-256: `78889c8f792f772474d2485320bc76055a5b9424f4cd458802c449e511d648cb`.
- Revision table: `rebuild_test.nso_revision_unapproved.nso_vietnam`.
- Revision run: `nso_vietnam_20261016092355_2f314ca9`.
- Revision UUID: `2c49a46e-2227-4a9c-a1ed-96c4c007b821`.
- Revision snapshot: `7395219837396472514`.

Only a temporary READY overlay reads this new snapshot. The checksum/schema guard
rejects the publication at transformation, with quarantine/exclusion commit flags
false. Accepted Bronze and Silver UUID/snapshot/schema/content remain unchanged.
See [staging-regression.json](staging-regression.json) and the revision section
of the acceptance report. No unknown writer was retried.

Project: `tlcn-nso-v2-rest-recovery-20261016`, confirmed unused before creation.
Its internal network has no published ports; PG, MinIO and SQLite REST catalog
have three project-scoped named volumes. Resolved Compose and actual HostConfig
both have no tmpfs. Raw and workspace mounts are readonly. Images are pinned to
the existing local digests; dependencies installed only from existing offline
wheels, with no pull, rebuild or upgrade.

The new REST service initially failed to open its empty SQLite volume due to
permissions. Read-only reconciliation confirmed no catalog file, no warehouse
bucket and no ingestion tables/writer. Only that new isolated REST service was
recreated with user=root, retaining its volume. Startup failure evidence is kept
in [startup-reconciliation.json](startup-reconciliation.json). This was not a
retry of an unknown writer or of the historical interrupted REST project.

## Resources, preservation and final state

Host: 15.38 GiB RAM, 12 Docker CPUs, approximately 64.88 GiB free on C: before
testing. WSL configuration was unchanged: memory=8GB, swap=0. Before stopping
shared services, Windows available RAM was 1.92 GiB; WSL MemAvailable was 4.47 GiB.
After stopping shared services WSL MemAvailable was 6.97 GiB. Windows availability
later fell to approximately 0.36 GiB; the test remained bounded and completed.
Measured running samples and exact timestamps are retained, not presented as
continuous peak telemetry.

One Spark runner only: local[2], actual JVM `-Xmx768m`, container cap 3 GiB/2 CPUs.
REST cap 448 MiB/heap256 MiB; PostgreSQL cap256 MiB; MinIO cap1 GiB; Trino cap1 GiB,
heap512 MiB/2 CPUs. No concurrent native suite. See [runtime-packages.json](runtime-packages.json)
and [resource-samples.jsonl](resource-samples.jsonl). Repeated YAML parsing in the
existing per-row test comparisons explains much of the 15-minute test duration;
no production performance change was made to address test-harness overhead.

Fresh shared verification before the owner-authorized graceful stop matched all
29 catalog identities, all five PostgreSQL metadata counts/hashes (29 ingestion
runs), 1,391,467 Silver rows and four logical quarantine records. Both DAGs were
paused/schedule=None, with no active/queued tasks/runs, writer JVM or advisory lock.
See [shared-pre-stop.json](shared-pre-stop.json).

All seven shared containers remain **STOPPED**, with the same IDs/images/mounts
and unchanged stop timestamps. Spark's idle API required docker stop's timeout
and exited137, OOMKilled=false; storage services exited0. No shared writer existed
at stop. Shared SQL/catalog was freshly checked before stop, not queried again
after stop; no service was restarted for verification. The isolated project
cannot access shared storage through its configured endpoints/mounts.

After final readback, both successful test runners remained exited0. The four
isolated infrastructure services were gracefully stopped too; all containers,
three named volumes and network remain. PG/MinIO exited0, REST/Trino143 after
intentional SIGTERM, all OOMKilled=false. No Docker cleanup/reset was performed.

The isolated main DB has three ingestion runs: two SUCCESS/833 (original and
deliberately unapproved publication), one checksum SKIPPED/0; no source locks.
Focused staging has its own retained DB. These are not shared ingestion runs.

Files changed this phase: `tests/integration/test_nso_v2_rest_e2e.py`, new
`compose.nso-rest-test.yml`, current status/link in `docs/silver/nso_v2.md` and this
new evidence directory. Ignored local inspection helpers/caches are not runtime
code or release artifacts. Four pinned production NSO/framework artifact hashes
are unchanged. No eight-source, legacy, business-rule or historical report edits;
no Airflow, Gold, shared writes, Git commit or push.

Remaining: owner review of Phase 1; NSO contract stays DRAFT. Nine-source Airflow
integration and portable data/dependency packaging belong to later separately
approved phases. The current local test still needs the existing offline wheel
cache and Raw bundle. This report does not claim those later phases are complete.

Machine-readable closure and artifact hashes: [report.json](report.json).
