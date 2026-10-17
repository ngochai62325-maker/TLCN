# Controlled shared stop — stopped at Spark SIGTERM timeout

Status: **AIRFLOW_RUNTIME_FAILED**. Shared stop is partial; isolated restore
and Airflow E2E did not start. The owner's explicit SIGTERM timeout stop rule
was followed. No alternative signal, SIGKILL, recreation or reset was used.

Git branch `handoff/local-nine-phase2`, HEAD
`82bfec47988124cfcd73d53f14604714da6c2108`. Existing untracked logs and prior
evidence were preserved. Only this new evidence directory was added.

## Fresh pre-stop safety gate

All seven exact shared IDs matched the approved manifest. Both DAGs were paused;
no running/queued DagRuns, running/queued/retrying writer tasks, source locks,
ingestion IN_PROGRESS, Spark writer process or active Trino query existed.
The historical scheduled `publish_silver_signal` task under its FAILED parent
remained unchanged.29 catalog identities and hashes of all5 ingestion metadata
tables matched the latest pre-stop baseline. No new persisted commit drift.

The approval document SHA-256 and exact pre-stop identities are in
[shared-pre-stop-gate.json](shared-pre-stop-gate.json). No pipeline was run.

## Actual stop outcome

| Shared service | Exact container ID | Action / result |
|---|---|---|
| airflow-scheduler | `e97397848ca4d2845efe41df6b0d439568a7995b24a09bcac20064ebcd0038b6` | SIGTERM; exited0 at21:01:31 local |
| airflow-webserver | `085ad49facb00ec17ddf269545182f83c5bfd9bd98f28cdc399f55549e2ab665` | SIGTERM; exited0 at21:01:37 local |
| spark-iceberg | `b5774d0f5aed5869cf1d1dcea8b3d63d2628890abca5c8459dd7ec7bb1d2dcbd` | SIGTERM once; still running after60s; STOP |
| trino | `dde50d530f2a98d060ab7545048fd95eb05c94a5875d939a690f720be2331b0c` | Running; no stop signal sent |
| iceberg-rest | `9de335c9cfda42a3c1709df0761ab810f9956d50a1192085a1829ad23d57ef7a` | Running; no stop signal sent |
| minio | `ed96e8d61e8fad38f9615d2dbe91b89e4d9cb409a2817bb321a4179db217ddd2` | Running; no stop signal sent |
| postgres | `aca5ddca120c3ca24c8d45202fbec043c3661a03ee67958220ea4535023ba4e7` | Running; no stop signal sent |

Times are16 October2026, Asia/Bangkok (UTC+7). `docker kill --signal=SIGTERM`
was used only as an explicit signal delivery mechanism. There was no
`docker stop --timeout` or forced escalation. Polling reached the approved60s
limit for Spark; no subsequent mutation was performed.

See [actions.json](actions.json) for each signal/exit/timeout timestamp and
native exit state. An initial read-only comparison mixed Docker short/full IDs;
it was corrected to `--no-trunc` before any signal, then the full gate passed.
This was an audit helper correction, not a production code/config change.

## Read-only reconciliation after timeout

- Shared Spark PID1 is `python3`, sleeping, one thread; API `/health` remains OK.
  Actual signal-caught mask does not include SIGTERM; it does include SIGINT.
  No child JVM/SparkSubmit writer exists. This supports a narrow alternative
  proposal; it is not evidence that another signal has already worked.
- Both shared DAGs remain paused, no active/queued runs/writers or source locks,
  no ingestion IN_PROGRESS and no active Trino query. The historical scheduled
  non-writer signal task was preserved, not cleared/retried.
- All29 catalog UUID/snapshot/schema/metadata-location/table-location identities
  and hashes of all5 ingestion metadata tables still match baseline exactly.
  Ingestion runs remain29. No unresolved/new persisted commit drift detected.
- Shared named/anonymous volume inspection equals the pre-stop volume records.
  Containers, images, volume data and Airflow history were not deleted/reset.
- All seven operational isolated containers remain stopped with their original
  IDs. No initializer, start, recreation, image pull/build or config resize.

Evidence: [timeout-readonly-reconciliation.json](timeout-readonly-reconciliation.json)
and [shared-commit-and-writer-readonly.json](shared-commit-and-writer-readonly.json).

## Phase 2 execution status

Isolated storage safety gate, runtime health and three fresh post-restore RAM
samples: **NOT_RUN** because stageA did not complete. Last known isolated DAG
state was paused/manual-only; its stopped PostgreSQL was not queried as a live
database in this attempt. Shared DAG paused state was verified in live SQL.

Manual Bronze run ID: **NOT_CREATED_BY_THIS_ATTEMPT**. Automatic Silver run ID:
**NOT_CREATED_BY_THIS_ATTEMPT**. All nine source task outcomes, local target
rows/UUIDs/snapshots/keys/DQ/lineage/Trino and replay/NOOP: **NOT_RUN**. No Gold,
cron, NSO DRAFT change, business-rule change, shared data writer or Git mutation.
Existing passing regressions were not rerun; only runtime safety checks ran.

## Proposed next action — separate approval needed

Send **SIGINT once** to the same idle shared Spark PID1, after a fresh
quiescence/identity/commit gate, then poll up to60s without SIGKILL. If it exits,
resume the already-approved remaining SIGTERM groups and staged isolated
restore. On another timeout, stop/report without fallback.

The exact reviewable proposal is
[alternative-signal-proposal.json](alternative-signal-proposal.json).
**It was not executed.** Current approval required SIGTERM and stopping on a
timeout, so it does not authorize silently substituting SIGINT or changing API
code/images. No framework or signal-handler refactor is proposed in this task.
