# Phase 2 ? SIGINT, isolated restore and Airflow execution

**Final status: `AIRFLOW_RUNTIME_FAILED` ? Trino acceptance blocked after successful Airflow execution.**

This record supersedes the earlier blocked resource/stop attempts for current state only. Historical evidence remains unchanged. No restart or retry followed the new Trino OOM.

## Controlled stop and restore

- Exactly one approved SIGINT delivered to shared Spark `b5774d0f5aed5869cf1d1dcea8b3d63d2628890abca5c8459dd7ec7bb1d2dcbd`, at `2026-10-16T14:15:48.515757Z`; exited `130` at `14:15:48.536800817Z`.
- Fresh and post-SIGINT read-only reconciliation: 29 shared catalog identities and five ingestion metadata hashes unchanged; no active writer, query, lock or ingestion IN_PROGRESS; historical scheduled publish_silver_signal preserved.
- Remaining shared services exited after SIGTERM: Trino 143, REST 143, MinIO 0, PostgreSQL 0. Both shared Airflow containers had already stopped with exit 0. All seven shared containers remain stopped; volumes preserved.
- Existing isolated containers restored in storage/catalog/execution/scheduler order. No container recreate, initializer rerun, reset, forced kill or new image.

## Admission and execution

Three pre-trigger observations 45 seconds apart passed the approved minimum resource screen:

| UTC time | Windows available GiB | WSL MemAvailable GiB |
|---|---:|---:|
| 14:20:13 | 3.486 | 5.359 |
| 14:20:58 | 3.694 | 5.350 |
| 14:21:43 | 3.981 | 5.345 |

All 21 Raw artifacts and pinned configuration/source hashes passed before trigger. Runner cap 3 GiB, driver 768 MiB, local[2], sequential Spark jobs; webserver cap 1 GiB with one worker. Admission was not a guarantee against later Trino working-memory growth.

- Bronze manual run: `local_nine_phase2__20261016T142353Z`; SUCCESS, nine ingestion SUCCESS, batch gate PASS.
- Exactly one automatic Silver run: `local_nine_bronze__8492937f141452e5604f5ba187ed56281d817c58d2230bc3a1a967c1731e22cb`; SUCCESS, nine transformations and nine persisted Spark audits PASS.
- All 28 task instances SUCCESS, one attempt each, zero retries/failures. Bronze waited for Silver and succeeded.
- Both DAGs paused after completion, schedule=None, retries=0; no additional DagRun created. Signal DQ_PASSED only; no READY_FOR_GOLD.

## Actual committed results

Every target below is in `iceberg.silver_local`. Counts are actual physical PyIceberg readback, matched to framework commit verification and Spark audits. All schema IDs are 0.

| Target | Silver rows | UUID | Snapshot ID |
|---|---:|---|---|
| faostat_production | 22,738 | `3103676e-f1b8-4395-b255-0223fccb8def` | `8931802191858281040` |
| faostat_monthly_price | 15,334 | `7ea79c59-5d3b-4a10-853e-ea4981af17d9` | `674571528901073625` |
| faostat_supply_utilization | 36,380 | `9743209b-06b0-488c-94aa-afd92edef78f` | `1814979402872267535` |
| faostat_trade | 1,226,470 | `7c441846-2950-4f28-9b2d-0c7dfec87f50` | `9100227375555158764` |
| usda_rice_psd | 990 | `ea52de14-4fba-4ca5-8339-e43cdaf41c00` | `2377166929586296572` |
| usda_rice_yearbook | 12,936 | `9425fa9b-89d8-4c4f-9408-8be9540494ad` | `1608990784470048556` |
| worldbank_commodity_monthly | 56,232 | `60bc8cbd-1af1-4e37-bda5-900754db30d3` | `4658508361764596994` |
| thitruongnongsan | 20,387 | `0d18a8e2-2670-499e-84fb-6bf64437c2b7` | `5808898081118807581` |
| nso_rice_statistics_v2 | 23,060 | `63efb530-3e0c-4d96-aa94-7ed2c24ba9ca` | `5811414319209912997` |

**Total: 1,414,527 rows** = 1,391,467 from eight sources + 23,060 NSO.

Dead letters native readback: 17 distinct records = four logical market-price quarantine records + 13 NSO structural exclusions. NSO metrics exclude 368 observation slots represented by those 13 structural rows; these measures are different. NSO missing observations remain in Silver; its pinned snapshot is `8008744977791679084`. Eight Bronze lineage fields and the separate pinned read snapshot are retained.

Spark audits verified published schema, required fields, unique business keys and blocking DQ for all nine. Native PyIceberg verified physical row counts, schema identities, units/value distributions, flags, Bronze row/source identity and pins. Complete independent Trino lineage/value/checksum acceptance remains unfinished.

## Trino OOM ? stop condition

- Isolated Trino container `5fe24960fb14d5c4c11481998d8795fff9415aa3dac7459f0e772accce7622a8` exited 137, OOMKilled=true, at `2026-10-16T14:41:56.495374552Z` (21:41:56 Asia/Bangkok).
- Kernel explicitly records CONSTRAINT_MEMCG for that container and kills trino-server. This establishes container-limit OOM; a new global OOM is not inferred. Limit: 1 GiB.
- Failure occurred during read-only Trade all-column checksum readback. No Spark writer or active Airflow task was involved. Full Trino acceptance/checksums were not produced.
- After OOM, all 19 isolated table UUIDs/snapshots/schema IDs/metadata locations still match native readback; source locks=0 and no Spark JVM remains. There is no unknown write outcome.
- Trino remains stopped. No restart, service recreate, writer retry, data rollback or reset performed. Full/incremental NOOP NOT_RUN because the resource/runtime gate failed.

## Observed resources and final state

Windows available RAM minimum during the observed run: 0.294 GiB. Headroom dropped below admission level; no extra writer/replay workload was launched.

| Service | Cap MiB | Observed cgroup peak MiB |
|---|---:|---:|
| airflow-webserver | 1024 | 918.0 |
| airflow-scheduler | 1536 | 1053.1 |
| runner | 3072 | 1743.9 |
| rest-test | 448 | 192.6 |
| minio-test | 512 | 512.2 |
| postgres-test | 256 | 60.4 |

Trino cgroup peak was unavailable after exit; kernel RSS at OOM was 1,019,876 KiB anonymous + 24,260 KiB file, not a cgroup peak. Other six running services had oom=0 and oom_kill=0 in captured counters.

- Shared seven containers remain stopped, identities preserved; no shared namespace writes. Final shared catalog verification is the last live exact comparison before graceful stop, plus unchanged stopped containers/volumes, not a new query against stopped services.
- Isolated nine Bronze ingestion runs SUCCESS; exactly two Airflow DagRuns SUCCESS. Both DAGs remain paused/manual-only.
- NSO production contract DRAFT unchanged. No Gold, cron, Phase3-5, Git commit or push.
- Git HEAD `82bfec47988124cfcd73d53f14604714da6c2108`; no tracked source/config changes. New evidence only; ignored operator helpers are not required runtime code.

## Evidence

- [Report](report.json): nine target/source pins, metrics, runtime and final state.
- [Airflow XCom/contracts](airflow-results-contracts.json), [task states](final-paused-state.json), [sanitized task logs](airflow-task-logs.sanitized.json.gz).
- [Native readback](pyiceberg-native-readback.json), [post-OOM catalog reconciliation](post-oom-catalog-reconciliation.json), [OOM state](trino-readback-interruption.json), [Trino logs](trino-oom.log.gz).
- [Fresh safety gate](fresh-sigint-gate.json), [SIGINT reconciliation](post-sigint-reconciliation.json), [resource admission](resource-admission.json), [monitoring](monitor.jsonl).

## Remaining owner decision

Approve a narrowly scoped isolated Trino recovery and bounded Trade read-only validation plan. Reuse the successful runs and committed snapshots; do not repeat Bronze/Silver writers or the manual trigger. Resource/query changes and service restart have not been applied. Full Phase2 PASS requires the remaining Trino acceptance. Stop here for owner review.
