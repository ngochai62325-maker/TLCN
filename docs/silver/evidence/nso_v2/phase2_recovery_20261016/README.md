# Phase 2 recovery — resource gate blocked before runtime changes

Status: **BLOCKED_RESOURCE_HEADROOM**. No container was recreated, no DAG was
unpaused/triggered and no data writer started. This is not an Airflow E2E PASS.

Fresh read-only checks on 2026-10-16 10:29–10:32 UTC matched the previous Phase 2
state. Project: `tlcn-local-nine-airflow-20261016`. Git HEAD:
`5c571693f90101c55d00ff84ceb5138092a1263c`.

## Verified state

- All eight isolated container IDs, image references, limits, mounts and network
  match the current resolved Compose configuration and previous failed state.
  Named volumes remain isolated; workspace and Raw bind mounts are readonly.
- Failed webserver `d2b68f6bcdd28d1fd1e3d82b507cacf251a42b376d5ce1740073a1c9aedc69c6`:
  OOMKilled=true, exit=1, cap=512 MiB, restart count=0. Kernel evidence identifies
  its memory cgroup. Failed logs were copied before any possible recreation.
  Gunicorn workers are already configured as **1**; lowering worker count is
  therefore not an additional remedy. The removed cgroup has no readable peak
  telemetry; do not substitute another container's peak.
- Scheduler heartbeat is alive; Spark API health returns `status=ok` and
  `local_profile=local_nine`. API `silver_release` is a capability field, not an
  enabled accepted-release route. Actual accepted-release environment is empty.
- Both serialized DAGs remain paused, schedule=None, max_active_runs=1,
  max_active_tasks=1 and all task retries=0. Bronze has nine ingestion tasks;
  Silver has nine transformation tasks. No import errors, DagRuns, task instances,
  ingestion tables, catalog tables or advisory locks exist. Runner has only its
  idle Python API process, with no Spark JVM.
- All 21 Raw files, **183,728,724 bytes**, match the prior inventory, including
  full Trade at 167,990,400 bytes and its approved SHA-256. Phase 1 evidence,
  previous Phase 2 evidence, code hashes and NSO transformer/contract/metadata
  pins were verified unchanged. Production NSO contract remains DRAFT.
- Seven shared containers retain their exact IDs and stopped timestamps. Shared
  data services were not started or queried; their frozen data baseline was not
  remeasured by restarting them.

## Resource decision

| Measurement / budget | GiB |
|---|---:|
| WSL MemTotal | 7.755 |
| WSL MemAvailable before recovery | 5.935 |
| Windows available before recovery | 2.882 |
| Windows available at final gate | 2.840 |
| Proposed steady service caps, runner kept at 3 GiB and web at 1 GiB | 7.688 |
| Conservative WSL/Docker reserve used for admission | 0.750 |
| Conservative whole-workload budget | 8.438 |
| Budget above WSL total | 0.682 |

WSL swap is zero. The 768 MiB reserve is an explicit conservative planning
assumption, not a measured peak or an owner-required minimum. Simultaneous caps
are an upper-bound admission model, not evidence that this sequential workload
must OOM. Current free memory and idle-container peaks do not prove peak memory
of a new all-nine-source run. No passing bounded peak budget was established.

Available cgroup high-water marks for the **existing idle/startup period**:
scheduler 704.1 MiB, runner 207.1 MiB, Trino 778.1 MiB, REST 120.9 MiB,
MinIO 193.1 MiB, PostgreSQL 139.0 MiB. Their `memory.events` counters show no
OOM kills. These are not ingestion/Silver peaks. The previous webserver OOM
occurred before any pipeline writer.

Runner cap **2.5 GiB is NOT_PROVEN** for the Trade/nine-source execution.
The Phase 1 NSO-only sample does not bound that workload. Its existing 3 GiB cap,
Spark driver768 MiB and local[2] were preserved. Webserver cap1 GiB was approved
but **not applied**, because the whole-workload admission gate was not met.

This stop follows the approval's resource-gate requirement. It is not an
assertion that 2.5 GiB is known to be insufficient, nor a justification to shrink
the runner, repeatedly try writers, raise memory, restart shared or reset data.

## Execution and tests

Manual Bronze run ID: **NOT_CREATED**. Nine ingestion outcomes: **NOT_RUN**.
Automatic Silver run ID: **NOT_CREATED**. Nine transformation outcomes:
**NOT_RUN**. Target rows/snapshots/checksums, Trino nine-source validation,
four logical quarantine records and thirteen NSO exclusions: **NOT_RUN** in
this project. The empty catalog is an observed state, not an acceptance result.

Previous passing suites are preserved: 67, 6, 24, 18 and 6 tests in overlapping
focused suites, with the original collection-dependency failure also retained.
They were not rerun; no production code/config/test changed in this recovery.
Fresh checks cover actual identities, readonly DB/catalog state, serialized
DAG state, scheduler/API health, checksums, mounts and resource telemetry.

## Evidence and next condition

- [report.json](report.json): recovery decision, outcomes and evidence hashes.
- [recovery-before.json](recovery-before.json): exact container identities,
  cgroup telemetry, matching kernel OOM, Raw/code/evidence hashes and DB state.
- [fresh-gate.json](fresh-gate.json): resolved Compose comparison, Raw inventory,
  fresh health/flags/DAG state and explicit memory-budget assumptions.
- `failed-webserver.log.gz`: retained sanitized failed-container logs.

Only this new evidence directory and a status note in `docs/silver/local_nine.md`
were added/updated. Historical Phase 1/Phase 2 evidence was not rewritten.
No config adjustment, recreation, writer retry, DAG clear, Git commit or push.

Before continuing Phase 2, establish a credible all-nine peak budget with host
and WSL headroom: either a supported measured basis for the approved runner cap
or an owner-approved resource arrangement. Do not treat increasing Docker's
limit alone as a fix when Windows available RAM is also limited. Recheck the
fresh safety gate before the still-authorized single Bronze-triggered Silver
run. Phase 3 remains blocked on actual Phase 2 E2E acceptance.
