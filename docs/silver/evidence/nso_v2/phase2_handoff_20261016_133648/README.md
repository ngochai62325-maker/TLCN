# Phase 2 after Git handoff — runtime gate failed

Status: **AIRFLOW_RUNTIME_FAILED**. Read-only preflight found a different
runtime arrangement from the handoff baseline. No service start/stop/recreation,
data writer, DAG trigger/unpause, initializer, reset or Git mutation was performed
by this preflight. No regression suite was rerun.

## Git and source integrity

- Branch: `handoff/local-nine-phase2`.
- HEAD: `82bfec47988124cfcd73d53f14604714da6c2108`.
- Local tracking ref `origin/handoff/local-nine-phase2` matches HEAD; no network
  fetch/push was performed. The eight-source CLOSED commit is an ancestor,
  not the current HEAD. Historical reports retain their original commit fields.
- No tracked changes at entry. Three existing untracked logs were preserved:
  `compat-regression.log`, `hook-regression-initial.log` and
  `phase1_rest_20261016/rest-startup-failure.log`. These files exist locally but
  are not backed up by the branch's tracked file set.
- All21 pinned Raw files/183,728,724 bytes and the exact13 NSO CSV set PASS.
  Full Trade remains167,990,400 bytes with SHA-256
  `697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
- Registry, NSO transformer/contract/metadata pins and previously validated
  routing/framework code hashes match.77 historical evidence checksum checks
  passed. NSO production remains DRAFT; no overlay or routing was rewritten.

## Actual runtime drift

Intended isolated project: `tlcn-local-nine-airflow-20261016`.
All seven operational services are **stopped**, with the same container IDs as
the last isolated verification. They finished near **18:31:12–13 on16 October
2026, Asia/Bangkok (UTC+7)**. The initializer remains exited0 and was not rerun.

| Isolated service | Exit | OOMKilled |
|---|---:|---|
| airflow-webserver | 137 | false |
| airflow-scheduler | 137 | false |
| runner | 137 | false |
| trino-test | 143 | false |
| rest-test | 143 | false |
| minio-test | 0 | false |
| postgres-test | 0 | false |

Exit137 alone is not proof of OOM. Actor/cause of the stop is unconfirmed.
The four isolated named volumes still exist; their existence does not prove
current catalog/database contents. Safe log tails were retained separately.

Project **`tlcn` is currently running seven replacement services**, with IDs
different from the previously frozen shared baseline. They started around
**20:29:06–32, Asia/Bangkok**. Docker labels reference this workspace's base
`docker-compose.yml`. Two initialization containers also exist and are exited;
their presence is recorded, without attributing who launched them.

| Running shared service | Current ID prefix |
|---|---|
| spark-iceberg | b5774d0f5aed |
| trino | dde50d530f2a |
| airflow-webserver | 085ad49facb0 |
| airflow-scheduler | e97397848ca4 |
| iceberg-rest | 9de335c9cfda |
| postgres | aca5ddca120c |
| minio | ed96e8d61e8f |

Full IDs, images, start times, flags, Compose labels and mounts are in
[preflight.json](preflight.json). The assumption that the seven historical shared
services remain stopped is **false at this observation**. The owner's subsequent
request authorized further read-only shared writer/commit investigation; results
are recorded below, separately from the initial preflight.

### Owner clarification and subsequent read-only checks

The owner reports restarting Windows and opening Docker Desktop, without
intentionally starting the shared Compose project. Current containers were
**created** at20:29:05, have restart_policy=`no` and restart_count=0. The current
Docker context is `desktop-linux`; labels identify base `docker-compose.yml`.
The retained Docker event query for20:28–20:31 returned no records, so the
API caller/actor is **undetermined**. No inference that restart policy created
the replacement containers is supported. Absence of retained events does not
prove no creation event occurred.

Running Spark/Airflow use base image references
`tabulario/spark-iceberg:latest` and `apache/airflow:2.10.4-python3.11`; their
actual IDs differ from the accepted immutable isolated execution images. Shared
memory limits are unset, and local_nine flags are absent. Named volumes
`tlcn_catalog_data`, `tlcn_minio_data`, `tlcn_postgres_data` and one anonymous
volume exist and are recorded; no volume operation was performed.

Actual shared SQL was executed inside **BEGIN READ ONLY**:

- 0 DagRuns running/queued,0 running/queued writer tasks,0 advisory locks and
  0 ingestion IN_PROGRESS records; both DAGs paused.
- 1 historical `publish_silver_signal` task is `scheduled`, with no PID, under
  the FAILED `manual__2026-10-15T14:02:14+00:00` parent. This non-writer task
  matches previous reconciliation evidence; it was not cleared/changed.
- 29 ingestion runs:10 SUCCESS,16 SKIPPED,3 historical NSO FAILED. Hashes of
  all5 ingestion metadata tables match the latest pre-stop baseline exactly.
- REST readback of all29 catalog identities matches UUID/snapshot/schema/
  metadata-location/table-location baseline exactly. No new persisted commit
  drift was detected; this does not assert absence of unreferenced object files.
- Spark process readback contains only its idle Python API, with no Java/
  SparkSubmit writer; API health PASS. Active Trino queries=0.

See [runtime-provenance.json](runtime-provenance.json),
[shared-postgresql-readonly.json](shared-postgresql-readonly.json) and
[shared-commit-and-writer-readonly.json](shared-commit-and-writer-readonly.json).
These establish observed quiescence, not permission to stop services. No shared
pipeline, transformer, writer or ingestion job was run by this investigation.

## Resource samples

Measured45 seconds apart, with the **actual shared stack running and isolated
stack stopped**:

| Local time,16 October2026 (UTC+7) | Windows available GiB | WSL available GiB |
|---|---:|---:|
| 20:36:50 | 2.675 | 4.938 |
| 20:37:35 | 2.609 | 4.936 |
| 20:38:20 | 2.611 | 4.925 |

Windows now exceeds the approximately2 GiB minimum in all samples. WSL has
zero swap and exceeds the nominal runner3 GiB + reserve1 GiB minimum. However,
these measurements **do not admit the intended E2E**: its isolated infrastructure
is not healthy/running, and a different stack is consuming resources. Do not
start a second stack based only on these figures. Configured caps, actual Docker
stats, recorded running-service cgroup peaks/events, WSL CPU/memory/I/O pressure
and disk free are stored separately in the preflight record. Isolated cgroup
counters are unavailable after its services stopped; historical peaks were not
relabeled as new measurements.

## Airflow and data results

- No Bronze or Silver run was created by this session; their run IDs are null.
- Live isolated DAG paused/schedule/retries, DagRuns/tasks/locks, PostgreSQL
  ingestion metadata and Iceberg catalog contents are **not reverified** because
  their services are stopped. Last recorded paused/None/0 and0 runs/tables are
  historical observations, not current SQL results.
- Nine-source task outcomes, actual rows/UUIDs/snapshots, Spark/PyIceberg/Trino
  readback, DQ/lineage/NSO semantics and full/incremental NOOP are **NOT_RUN**
  by this session. Expected counts were not used to manufacture acceptance.
- No transformer, contract, runtime configuration, historical evidence, Gold,
  shared namespace or consumer routing was changed. Only this new evidence
  directory was created after the read-only checks.

## Narrowest proposed recovery — not executed

1. Owner confirms the context of the shared startup/replacement. Before any
   stop, read-only checks must establish no shared active/queued runs, source
   locks, writer jobs or unresolved commits; do not interrupt an active writer.
2. With owner approval and safe quiescence, stop only the seven currently
   running shared services. Preserve all volumes/data and initialization history.
3. Start existing isolated storage/catalog containers with their current IDs
   and volumes, without Compose recreation, initializer, reset or image pull.
4. Verify isolated PostgreSQL/catalog state before starting its scheduler. If
   unknown runs/commits or an unexpected unpaused state exist, stop recovery
   and reconcile rather than permitting queued writers to execute.
5. Restore only the remaining existing isolated execution services, verify
   both DAGs paused/manual-only and exact local_nine scope, then perform fresh
   three-sample admission with the intended stack healthy.
6. Only after all gates PASS, execute the previously approved single manual
   Bronze→automatic Silver run and actual nine-target acceptance. Do not trigger
   Silver independently.

No blanket Docker stop/down/prune, snapshot rollback, storage reset or automatic
recreation is proposed. No Phase3–5 work was started. Resource figures alone
do not justify replacing this runtime recovery gate.
