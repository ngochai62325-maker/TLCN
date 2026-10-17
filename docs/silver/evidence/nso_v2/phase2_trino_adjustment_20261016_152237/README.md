# Phase 2 ? final isolated Trino resource adjustment

**Status: `LOCAL_NINE_AIRFLOW_E2E_PASS_PENDING_OWNER`.**

Initial nine-source Airflow E2E: **PASS**. Nine-source Trino full-content validation: **PASS**. Separate nine-source Airflow full/incremental replay: **NOT_RUN**. Owner acceptance is pending; Phase3-5 have not started.

## Approved change and safety

- Fresh read-only preflight PASS: exact project/container/image/mounts/settings, all19 catalog identities, five ingestion metadata hashes, successful runs/tasks, paused DAGs, shared seven stopped, no active query/writer/lock. Previous readback evidence hashes verified.
- Applied exactly once:

```text
docker update --memory 1536m 5fe24960fb14d5c4c11481998d8795fff9415aa3dac7459f0e772accce7622a8
```

- Existing container stayed running: no restart/recreate, same StartedAt and RestartCount. Docker HostConfig changed only Memory from1,073,741,824 to1,610,612,736 bytes; cgroup memory.max verified1536 MiB. MemorySwap total remains2,147,483,648 bytes.
- Image unchanged: `trinodb/trino@sha256:94703aecb56dd0ee138aa45e49efa9a9f23e4af54134c83a6631aa629fea16f3`. JVM remains `-Xmx512M`.
- Only tracked change: `compose.local-nine-test.yml`, adding `services.trino-test.mem_limit: 1536m`. Resolved Compose comparison proves every other configuration value unchanged. No compose up/down executed.

## Resources

Before adjustment: Windows available 3.491 GiB; WSL MemAvailable approximately5.905 GiB. Windows minimum during readback 2.897 GiB.

Trino observed read cgroup peak 959.0 MiB; final cgroup peak 959.2 MiB. Guard1280 MiB was not reached; cap1536 MiB leaves approximately256 MiB beyond the guard. Final JVM RSS 810.5 MiB. Heap max512 MiB, final committed317,440 KiB and used212,982 KiB. New oom=0, oom_kill=0 and cgroup max events=0.

Actual Windows/WSL/swap, Docker/cgroup current/peak, CPU/memory pressure, JVM and query memory statistics are retained. The old1 GiB OOM remains historical evidence; no OOM counters were reset to manufacture a pass.

## Full-content method and evidence reuse

- Verified prior evidence hashes and unchanged UUID/snapshot/schema/metadata identities before reusing three complete tables and Trade years1986-1993 (81,824 rows). They were not recomputed.
- Validated five smaller sources, then dead_letters, then remaining Trade years1994-2024. Exactly31 new Trade partitions /1,144,646 rows.
- One query at a time, task_concurrency=1, Parquet blocks1 MiB/512 rows, small-file buffering0B, client processing batches<=1,024 rows; EXPLAIN before large reads.
- Native PyIceberg scans use the accepted pinned target snapshots. Trino uses FOR VERSION AS OF the same snapshots. Exact canonical business keys and full-row SHA256 are matched on disk with SQLite, not a complete Python list or global Trino sort.
- Every field participates: raw and normalized values, NULL/DECIMAL/string semantics, microsecond timestamps via PARAMETRIC_DATETIME, nested warnings, processing timestamps and eight lineage fields. Each Trino lineage tuple belongs to the pinned native Bronze source.
- Independent native/Trino digests match for every partition and table content root. These SHA256 digests are a new comparison method; they are not claimed to reproduce historical Trino checksum() bytes.
- No original unbounded global Trade checksum, large DISTINCT/GROUP BY or writer rerun. Fifty-two SQL queries completed in this resume; actual SQL, query IDs, plans, page/batch sizes and checksums retained.

## Nine actual Silver outcomes

Namespace: `iceberg.silver_local`. Unique business keys equal rows for every table. All results include full-content, schema/types, required fields, units/values/flags/missing semantics and lineage acceptance.

| Target | Rows = unique keys | Trino full content | Evidence |
|---|---:|---|---|
| faostat_production | 22,738 | PASS | prior hash-verified result |
| faostat_monthly_price | 15,334 | PASS | prior hash-verified result |
| faostat_supply_utilization | 36,380 | PASS | prior hash-verified result |
| usda_rice_psd | 990 | PASS | resumed validation |
| usda_rice_yearbook | 12,936 | PASS | resumed validation |
| worldbank_commodity_monthly | 56,232 | PASS | resumed validation |
| thitruongnongsan | 20,387 | PASS | resumed validation |
| nso_rice_statistics_v2 | 23,060 | PASS | resumed validation |
| faostat_trade | 1,226,470 | PASS | resumed validation |

**Total: 1,414,527 Silver observations.** Actual count/keys/checksums reconcile to persisted Spark/PyIceberg evidence; no expected constant was used to replace a measurement.

## Trade100% coverage

- Actual native and Trino year inventories match exactly:39 distinct years,1986-2024 inclusive; NULL-year rows0, unexpected year differences0.
- Every year appears exactly once; year is in the business key. The8 reused and31 newly read partitions do not overlap.
- Completed partition rows sum to1,226,470; each has equal native/Trino row counts, unique keys and all-column SHA256. No skipped partition or sample shortcut.
- [Coverage proof](trade-coverage.json) includes actual inventories, every partition SQL/row count/hash, and independent full-table content roots.

## NSO, DQ and dead letters

- NSO23,060 observations:22,114 numeric,946 missing, five numeric zeros,766 provisional and344 diagnostic observations. Missing marker/value_source/value semantics and DECIMAL conversions checked on every row.
- Units: hectare7,640; tonne7,640; kg/hectare7,500; percent280. Historical geography keeps codes/boundary version NULL; candidate_review22,500 and national_scope_metadata560.
- Seasons: annual6,530; main_season6,530; winter_spring6,530; summer_autumn140; summer_autumn_autumn_winter3,330.
- Every NSO observation, including missing observations, retains pinned Bronze read snapshot `8008744977791679084`, independently of _source_snapshot_id.
- Dead letters separately verified across all17 full records: four logical market-price quarantine records and13 NSO structural exclusions. Structural exclusions are not mislabeled as quarantine.
- Warnings remain approved observations: SUA236, market prices29, NSO22,534. No business-rule, contract, transformer or NSO DRAFT change.

## Final invariance and runtime

- All19 UUIDs/snapshots/schema IDs/metadata locations unchanged. Five ingestion metadata hashes unchanged; nine ingestion runs remain SUCCESS. No additional writer commit or unknown outcome.
- Bronze run `local_nine_phase2__20261016T142353Z` and automatic Silver run `local_nine_bronze__8492937f141452e5604f5ba187ed56281d817c58d2230bc3a1a967c1731e22cb` remain SUCCESS.28/28 tasks SUCCESS, one attempt each, zero retries; no extra DagRun.
- Both DAGs remain paused; actual serialized schedule_interval=None confirmed in PostgreSQL. No active Trino query, Spark JVM or source lock.
- Isolated running: PostgreSQL, MinIO, REST, idle Spark API/runner and Trino. Scheduler/webserver remain stopped exit0; initializer untouched/stopped. Shared seven remain stopped.
- No replay, unpause/trigger, shared data write, reset, Gold, cron, NSO DRAFT promotion, commit/push or Phase3-5. Unchanged snapshots are invariance proof, not replay proof.
- Git HEAD `82bfec47988124cfcd73d53f14604714da6c2108`; source policies unchanged. Historical reports/manifests remain hash-identical.

## Evidence and next decision

- [Final report](report.json), [combined full-content results](trino-full-content-readback.json), [Trade coverage](trade-coverage.json).
- [Actual SQL/query IDs/statistics](query-events.jsonl), [EXPLAIN](explain-plans.json), [validation client](readback_client.py), [reuse provenance](reuse-evidence.json).
- [Preflight](preflight.json), [memory adjustment](memory-adjustment.json), [Compose-only change](compose-change.json), [resource monitoring](readback-resources.jsonl).
- [Final reconciliation](post-validation-reconciliation.json), [serialized DAGs](serialized-dags.json), [sanitized logs](trino-recovery.sanitized.log.gz).

No mandatory Phase2 technical blocker remains. Owner review/acceptance and separate approval are required before Phase3. Nine-source replay remains NOT_RUN, as instructed; any later replay requirement needs its own controlled approval. Stop here.
