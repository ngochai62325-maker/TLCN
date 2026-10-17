# Validation — local nine-source Silver

Current fresh demo: **runtime smoke PASS**,10 config/safety tests PASS and actual
stop/start retained8 container IDs/4 volumes. Both DAGs paused,0 DagRuns; its
Bronze/Silver E2E is **NOT_RUN** until owner trigger. See
[Phase B](evidence/demo_phase_b_20261017_131025/README.md).
The acceptance and206-test results below describe the retired clean-room run;
its runtime data was subsequently deleted under explicit owner approval.

Data acceptance: **PASS**. Full and incremental strict physical NOOP: **PASS**.
Final cleanup regression: **206 PASS,1 opt-in SKIP**. Git secret/style review PASS; final local commit is discoverable from the report history.
NSO production contract remains DRAFT. [Evidence index](evidence/index.md).

## Actual Airflow history

| Run | Actual ID | Persisted state |
|---|---|---|
| Initial Bronze | `local_nine_cleanroom_initial__20261016T155210Z` | SUCCESS |
| Initial automatic Silver | `local_nine_bronze__6d5240a929c5e113bad77a1305cf2efa1051a613ebf3a436b328dfe88601dcc1` | SUCCESS |
| Unchanged Bronze replay | `local_nine_cleanroom_noop_full__20261017T031611Z` | SUCCESS;9 ingestion outcomes SKIPPED |
| Same automatic full replay child | `local_nine_bronze__92c1b8621646e3e1613e9a365270df8ab60b33280d7dce5e1fc047a0c897a9e5` | SUCCESS |

The initial clean-room had a known no-commit Bronze startup failure and targeted
NSO job24?25 recovery. The full replay was later interrupted by Docker Desktop
shutdown. Persisted Trade and six other transformers were already SUCCESS;
Market job52 was stale RUNNING with no new commit. Independent19-table/file
reconciliation preceded the owner-approved clear of only Market, preserving
history, then execution of Market and never-run NSO/audit/terminal tasks.
Child/parent terminal bookkeeping used supported Airflow computation and a
previewed state-only parent trigger PATCH. No trigger operator rerun, duplicate
child, snapshot rollback or initial pipeline rerun occurred. This is a recovered
execution, not an uninterrupted zero-error/zero-retry claim.

## Strict physical NOOP

Both checks compare against the immutable
[pre-replay baseline](evidence/nso_v2/phase3_final_20261017_024536/physical-before-replay.json):

- All19 table UUIDs, snapshots, schema IDs/definitions and metadata locations.
- Entire active-file inventory and SHA-256 of all413 files, including timestamps
  and lineage stored in rows. No new file or unnecessary snapshot.
- Dead-letter17 IDs/payloads/errors plus original pipeline_run_id/detected_at.
  Both the data snapshot and audit content remain unchanged.
- Full9-source counts1,414,527; NSO23,060 including946 missing and5 genuine zeros.

[Full proof](evidence/noop_restart_20261017_063554/physical-noop-after-full.json)
and [incremental proof](evidence/noop_restart_20261017_063554/physical-noop-after-incremental.json).
The34 historical unreferenced NSO objects were preserved, not adopted or deleted.

Incremental execution ID: `local_nine_incremental_noop__20261017T070757Z`. There is no incremental DagRun: the
existing Bronze DAG automatically creates full-mode children only. Nine explicit
calls used the supported Spark API/common runner, mode incremental, each source's
real proved_run_id and verified Bronze snapshot. Batch proof was revalidated;
all source rows from that existing ingestion run were actually transformed and
compared by MERGE. This is an incremental replay of existing data, not a claim
of new-Raw ingestion or a newly inserted observation.

PostgreSQL records19 ingestion attempts: original9 SUCCESS, one preserved startup
FAILED, and9 legitimate checksum-SKIPPED attempts. Incremental does not ingest
Bronze or create extra DagRuns. Source snapshots/checkpoints remain unchanged.

## Independent readback

Initial validation checked Spark/PyIceberg/Trino schemas, business-key uniqueness,
values/units/flags, missing reasons and eight Bronze lineage fields. Current
independent PyArrow reads verified every active Parquet file. Final Trino queries
rechecked all9 actual counts, canonical-unit value sums, NSO missing/zero/pin
semantics and17 unique dead letters. Content invariance preserves initial key and
lineage proof. [Final SQL evidence](evidence/noop_restart_20261017_063554/final-trino-readback.json).

## Repeatable regression

Build the local Spark image as in [runbook](local_runbook.md). Tests additionally
need host Python/pip to download temporary test wheels; these are regenerated,
not shipped or needed by runtime startup.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1
# Same downloaded test dependencies, network-disabled test container:
powershell -ExecutionPolicy Bypass -File scripts/test_silver_isolated.ps1 -SkipDownload
```

The test runner uses a read-only repository mount, network=none, fresh /tmp Hadoop
warehouses and a3GiB cap/768MiB driver. It cannot reach accepted REST/MinIO data.
Critical coverage includes quarantine physical NOOP/new/mixed/error/payload paths,
MERGE freshness/dedup, eight CLOSED source outputs, NSO semantics and native pinned
snapshot/concurrent Bronze append proofs. The older FAOSTAT test command remains
a compatibility wrapper over the same maintained runner.

Airflow parse/routing/gate/lock tests run separately in the Airflow environment;
fixture flags explicitly test both base/eight-source compatibility and local9
scope. They must never trigger the real DAGs. Final test results and smoke checks
are recorded in the [current completion report](evidence/noop_restart_20261017_063554/report.json).

## Final regression result

139 native cases PASS and1 opt-in new-Raw/REST case SKIP, plus67 Airflow cases PASS.
The initial broad run had127 PASS/12 failures/1 SKIP. Eleven obsolete audit-refresh
expectations were strengthened to assert unchanged snapshot/full audit content.
The discovery test now reads a real pinned native Bronze table; its spy captures
three output columns within the cached merge stage, resolving a temporary test
JVM heap error without increasing memory. Only affected cases were rerun.
Accepted runtime cgroup OOM counters remain0; failed runs/logs are preserved.
Final smoke:213 local documentation links, real nine-source DAG parsing/retries,
Raw21+support hashes, Compose resolution and19 unchanged identities PASS.
