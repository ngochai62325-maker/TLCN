# Phase 2 ? controlled Trino-only recovery

**Final status: `TRINO_RESOURCE_BLOCKED`. Mandatory nine-source Trino acceptance is incomplete.**

Airflow ingestion/transformation succeeded in the previous run. This recovery did not rerun any writer, task or DagRun. Historical evidence is unchanged.

## Runtime and safety

- Fresh preflight PASS: exact images/mounts/caps, 19 table identities, successful run/task state, five ingestion-table hashes, zero source locks/writers, both DAGs paused, shared seven containers stopped.
- Isolated scheduler and webserver stopped gracefully using SIGTERM, both exit0; metadata/log volumes preserved.
- Existing Trino `5fe24960fb14d5c4c11481998d8795fff9415aa3dac7459f0e772accce7622a8` started exactly once. No recreate, pull, rebuild, initializer or config reset.
- Trino image remains `trinodb/trino@sha256:94703aecb56dd0ee138aa45e49efa9a9f23e4af54134c83a6631aa629fea16f3`; cap1 GiB / max heap512 MiB unchanged.

## Query design

The original global all-column checksum query `20261016_144152_00205_zkj49` scanned 39 Trade files without a predicate and ended in CONSTRAINT_MEMCG OOM, exit137. That query was not rerun. The reconstructed SQL and preserved client establish the query shape; no ingestion failure is inferred.

Replacement: one SQL query at a time, task_concurrency=1, Parquet blocks1 MiB/512 rows, small-file buffering0B, EXPLAIN before large reads. These are validation-session settings; server config remains unchanged. Trade uses actual year equality predicates. GROUP BY year enumeration passed a total-count assertion against native rows before partition reads; its full inventory was not emitted by the interrupted client, so complete table coverage is not claimed.

PyIceberg scans pinned native snapshots in batches. Disk-backed SQLite stores exact canonical business keys and SHA256 for all columns; Trino streaming pages are processed in batches<=1,024 rows and must match every key/hash exactly once. Independent native/Trino key-ordered SHA256 partition digests match. SQLite sorting is on disk in the client, not a global Trino sort. No full-table Python list/DataFrame, high-cardinality Trino aggregation, global ORDER BY or sample-based full-pass claim.

Canonicalization preserves NULL/empty/string distinctions, exact DECIMAL scale, 64-bit integers, raw UTF-8/whitespace, UTC timestamps with six fractional digits, schema-ordered structs and list order. All columns include processing timestamps, warnings and eight lineage fields. This is a new full-content comparison algorithm, not reproduction of the old Trino checksum bytes.

A bounded diagnostic proved default Trino wire timestamps round microseconds; PARAMETRIC_DATETIME preserves them. The initial diagnostic attempt is retained separately. No source data, contract or transformer was changed.

## Resource observations

| UTC | Windows available GiB | Trino current MiB |
|---|---:|---:|
| 2026-10-16T14:58:48.385046+00:00 | 3.366 | 764.4 |
| 2026-10-16T14:59:31.332708+00:00 | 3.371 | 764.4 |
| 2026-10-16T15:00:14.404818+00:00 | 3.422 | 765.7 |

WSL MemAvailable approximately6 GiB; swap and pressure captured in samples. An exploratory700 MiB idle screen was exceeded without OOM; measured heap/memory.stat informed the subsequent800 MiB idle budget and900 MiB readback stop threshold. No actual runtime limit changed.

Readback Windows minimum: 2.860 GiB. Observed Trino readback peak: 913.8 MiB. Highest completed-query tracked memory: 2.11 MiB. Query memory differs from full JVM/cgroup usage.

Guard stopped reads when current Trino memory exceeded900 MiB; query `20261016_150517_00026_5z565` for1994 was canceled. No new OOM: oom=0, oom_kill=0, max events=0. Final cgroup current 908.7 MiB, peak 932.4 MiB (includes telemetry/jcmd overhead); JVM RSS 764.4 MiB. Heap committed295,936 KiB, used251,265 KiB; max524,288 KiB.

Largest HTTP page: 1,276,732 bytes / 3,072 rows, subdivided into processing batches<=1,024. Twenty SQL queries completed; exact SQL/query IDs/stats/plans and result digests are retained.

## Exact readback status

Persisted rows are the prior Spark/PyIceberg baseline. Only explicit PASS entries have complete current Trino full-content acceptance.

| Table in iceberg.silver_local | Persisted rows | Current Trino full content |
|---|---:|---|
| faostat_production | 22,738 | PASS: all rows/all columns |
| faostat_monthly_price | 15,334 | PASS: all rows/all columns |
| faostat_supply_utilization | 36,380 | PASS: all rows/all columns |
| faostat_trade | 1,226,470 | PARTIAL: 81,824 rows |
| usda_rice_psd | 990 | NOT_RUN: resource guard |
| usda_rice_yearbook | 12,936 | NOT_RUN: resource guard |
| worldbank_commodity_monthly | 56,232 | NOT_RUN: resource guard |
| thitruongnongsan | 20,387 | NOT_RUN: resource guard |
| nso_rice_statistics_v2 | 23,060 | NOT_RUN: resource guard |

The three complete tables passed schema/types, required fields, unique exact keys, values/units/flags, missing semantics, pinned-Bronze eight-field lineage and all-column hashes. No data discrepancies found.

Trade verified **81,824/1,226,470 rows**; **1,144,646 remain**. Completed partitions do not overlap; full-table coverage is NOT claimed.

| Trade year | Verified rows | Native/Trino SHA256 |
|---:|---:|---|
| 1987 | 8,156 | `fa883761a4a42d15817fc76b2c7170977d8fee069e19fc9f587f137611c447cf` |
| 1986 | 8,304 | `f230c6ae497f4ed0f626691f033ecb94ea520f68cdb4a030a11fe53627bc5dbc` |
| 1988 | 8,728 | `38f880096b0579a9c091d287279925ad7d78569612f1eb06b25adaaa728802df` |
| 1989 | 9,454 | `2e70d7a1856684f6f4ebde8e2b55a643a4b70a140c988dd4846cfc9e21f0a44c` |
| 1990 | 9,512 | `1ccf2182d0496d956c6c9bd6b1f43f38f0077f400c679af1a875fb4352d7e9a3` |
| 1991 | 11,522 | `45f9988ad91a8fb4ef0c3515bca2140d42f1012a44c293be3d9431567161170b` |
| 1992 | 12,850 | `7b8be83915f24418486a2ebfbb3a0f3cd722bf44e727d537f7f4fb74cb52a544` |
| 1993 | 13,298 | `86f6e860a505cdbc327f176ef65186ebfc7298632d5fa71c773a73c192b966dc` |

Not reached: complete Trade comparison; USDA PSD/yearbook, World Bank, market prices, NSO Trino acceptance; NSO snapshot/units/geography/missing checks; Trino dead-letter comparison. Prior Spark/native evidence remains preserved (1,414,527 Silver rows, 23,060 NSO, four market quarantine and13 NSO structural exclusions), without substituting for unfinished Trino checks.

## Final state

- Post-validation all19 catalog UUIDs/snapshots/schema IDs/metadata locations and all five ingestion metadata hashes unchanged. Nine ingestion runs SUCCESS.
- Bronze `local_nine_phase2__20261016T142353Z` and automatic Silver `local_nine_bronze__8492937f141452e5604f5ba187ed56281d817c58d2230bc3a1a967c1731e22cb` remain SUCCESS; all28 tasks SUCCESS, one attempt, zero retries. No extra DagRun.
- Both DAGs paused, serialized schedule=None. No active Trino query, Spark JVM, source lock or unknown commit.
- Isolated PostgreSQL/MinIO/REST/idle runner/Trino running. Isolated scheduler/webserver stopped exit0; initializer untouched/stopped. Shared seven containers still stopped.
- Nine-source Airflow full/incremental replay NOT_RUN. Unchanged snapshots are not replay evidence.
- NSO production DRAFT unchanged. No shared writes, cron, Gold, Phase3-5, reset, commit or push.
- Git HEAD82bfec47988124cfcd73d53f14604714da6c2108, no tracked code/config changes. Historical evidence SHA256 manifest matches.

## Evidence and owner decision

- [Report](report.json), [SQL/query IDs/results/checksums](query-events.jsonl), [validation client](readback_client.py), [EXPLAIN plans](explain-plans.json).
- [Preflight](preflight.json), [actions](actions.json), [resource samples](resource-samples.json), [monitoring](readback-resources.jsonl), [resource stop](resource-guard-stop.json).
- [Final reconciliation](post-validation-reconciliation.json), [Trino logs](trino-recovery.sanitized.log.gz), [timestamp diagnostic](timestamp-wire-precision-diagnostic.json).

Further isolated Trino memory/query adjustment needs owner approval before continuing unfinished readback. Total cgroup usage remains the blocker despite low tracked query memory. Do not rerun successful writers, DagRuns or completed comparisons. No additional adjustment has been made.

**Stop for owner review. Phase2 is not marked PASS.**
