# C2 Production canary acceptance - BLOCKED

Owner: Vũ Đức Hoàng. Result: **BLOCKED at strict preflight; zero shared writes**. Native preflight passed, but Spark service exited before API/preview became available. Full load, acceptance validation and full/incremental NOOP tests are NOT_EXECUTED. No READY_FOR_GOLD signal is issued. [Machine acceptance evidence](c2_canary_acceptance_evidence.json).

## Conditional approval and exact scope

The current user approval supersedes the broader [original shared execution plan](silver_shared_execution_plan.md). Allowed: start only spark-iceberg from current Compose; after a full preflight PASS, create iceberg.silver and iceberg.silver.faostat_production, use checked common dead_letters only if required, and run Production full/full-rerun/real-run incremental NOOP. Not approved: seven other business writes, NSO Silver, Bronze replay/mutation, validation fixtures/tables/corrections, Airflow triggers/unpause, PostgreSQL ingestion updates, schema/policy/Gold changes, or deletion.

Executed scope: current Compose `up -d --no-deps spark-iceberg`. Compose recreated **only Spark** to apply current mounts including read-only contracts. Spark ID dda81f6af606 -> bb3dfbb09844; other service IDs/states unchanged. Spark then exited 0, OOMKilled=false. No alternate startup, configuration repair, service restart, Silver namespace, target, DLQ or shared writer was executed after the incompatibility was found. Existing local changes were preserved.

## Preflight result

| Check | Observed result |
|---|---|
| Git working tree | Existing dirty changes preserved; before/after status saved |
| REST catalog / MinIO raw read / Trino query | PASS |
| Bronze pinned snapshot | PASS: 3250087836768729857 |
| Bronze schema | PASS: 23 fields; field IDs/types/required flags/schema ID identical |
| Schema representation | Old REST evidence omitted optional identifier-field-ids; canonical empty default [] matches PyIceberg; no real schema drift |
| Raw rows / file-checksum-run identity | PASS: 22,738 rows, one file/checksum/run |
| Raw manifest/artifact checksum | PASS: exact baseline manifest and artifact hash |
| Approved Production contract and DQ definition | PASS: READY; dataset/key/schema/DQ projection identical |
| Eight raw lineage fields | PASS: no NULLs; original source_snapshot_id=0 preserved |
| Both DAGs paused / active task or ingestion writers | PASS: paused, no active tasks/runs/known source advisory locks |
| PostgreSQL source-lock infrastructure | PASS: Production lock held by one backend; contender denied; lock released after probe |
| Host resource capacity | 8,326,950,912 bytes Docker RAM, 12 CPUs; capacity supports planned envelope |
| Spark service and scheduler-to-API connectivity | **BLOCKED**: container exited; API DNS unavailable |
| Actual local[2] / 2g / shuffle=4 runtime | **NOT VALIDATED**; submit could not start |
| Fresh source+contract DQ and approved 22,738-row output/schema preview | **NOT EXECUTED**; stopped before writer |
| Existing Silver target | Absent before and after; no overwrite/evolution attempted |

[Native preflight JSON](c2_canary_native_preflight.json), [native log](c2_canary_native_preflight.log), [failed Spark preview log](c2_canary_spark_preflight.log), [startup log](c2_canary_spark_start.log).

## Root cause and reviewable next action

[Actual image entrypoint](c2_spark_entrypoint_evidence.txt) evaluates only its first argument. Compose passes three argv elements: bash, -c, and the API/notebook script. The entrypoint evaluates bash only and exits before that script runs. [Failure evidence](c2_canary_spark_failure_evidence.json) confirms exited/exit0/no OOM; [failure log](c2_canary_spark_failure.log) separately reports missing file:/tmp/spark-events for HistoryServer.

A concrete [startup fix proposal](c2_canary_spark_startup_fix_proposal.md) is prepared for review, **not applied or executed**. Changing the Compose startup exceeds the instruction to use the existing configuration, so this run stops at BLOCKED. Approve/test that startup correction, start only Spark, then repeat native and actual Spark preflight. No shared write is justified by the native-only PASS.

## Table, snapshot and execution evidence

| Item | Before | After |
|---|---|---|
| Namespaces | bronze only | bronze only |
| Production Silver target | absent | absent |
| Production Silver snapshot | none | none |
| Production Silver actual rows | N/A | N/A |
| Production Bronze snapshot | 3250087836768729857 | 3250087836768729857 |
| NSO Bronze snapshot | 3499764399995403202 | 3499764399995403202 |
| New registered shared tables/data files | 0 | 0 |

No pipeline run ID or write timestamp was issued because the writer did not run. Expected output 22,738, quarantine 0 and 229 nullable values remain the approved earlier preview; these are **not actual canary Silver/DQ results**. Publication state NOT_CREATED; no partly created table requires a NOT_PUBLISHED marker.

Verified real incremental source run ID: `faostat_production_20261004145822_c4ff8851`. Source file `production_world.csv`, checksum `6a23d820d61e990d1c108ffe3468e7f39a5beb57e376d5174e332dfc9e1200cc`. Full load, full rerun and incremental rerun: NOT_EXECUTED. No NOOP/idempotency PASS is claimed for this shared canary. Source-lock probe establishes infrastructure only; lock held during a real writer remains a future acceptance check.

## Trino SQL and results actually obtained

```sql
SELECT count(*) AS rows, count(DISTINCT _source_checksum) AS checksums FROM iceberg.bronze.faostat_production;
```

Result columns rows/checksums: `[[22738, 1]]`. This is a Bronze read probe. Target schema/count/unique keys/mandatory fields/values/units/flags/full lineage/DQ/quarantine validation and end-to-end reconciliation are pending because the target does not exist. No synthetic correction or validation namespace was used. Trino's previously unhealthy Compose healthcheck remained unchanged; the SQL probe worked, so that pre-existing health flag is recorded separately from the Spark blocker.

## Protected state and unexpected changes

Post-stop [native audit](c2_canary_after_blocked.json) compared against pre-start evidence: entire catalog inventory/namespaces, Production metadata location/schema/snapshot/data files, source and lineage identities, all raw objects/manifests/artifact hash, ingestion runs/snapshots/watermarks/checkpoints/dead letters, DAG pause states and active task state are identical. All 20 preservation comparisons PASS. The 11 approved contract/transformer/common engines/API/job/lock files retain their before hashes. Compose was not edited during C2. Other service IDs/states unchanged; allowed Spark recreation and its failed startup are the operational changes. Unexpected shared data changes: **none**. No commit, push or merge.

## Rollback readiness and acceptance gate

No shared Silver transaction or ambiguous commit occurred. No rollback is needed or executed; no table/file/snapshot/volume is deleted. Production and NSO Bronze snapshots and complete catalog history remain unchanged. For a future newly created target that fails, retain NOT_PUBLISHED and evidence. For an existing approved baseline, snapshot rollback requires expected-current guard and no concurrent writer; never automatic deletion/rollback.

Acceptance is **BLOCKED**: target absent, live Spark DQ/reconciliation not run, shared full/incremental NOOP not tested. Do not expand C2. After the startup fix and all canary checks PASS, stop shared writes and seek a separate confirmation for the seven remaining sources. NSO stays DRAFT/NEEDS_APPROVAL in its independent business-contract review track; no NSO mapping or writer approval is inferred from C2.
