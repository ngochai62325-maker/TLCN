# Phase 2 — targeted webserver recovery PASS; host admission BLOCKED

Final status: **BLOCKED_RESOURCE_HEADROOM**. Webserver recovery succeeded.
The approved actual Airflow E2E was **not started**, because Windows available
RAM failed the owner's post-infrastructure screening threshold. No DAG was
unpaused, no run/task was created and no writer started.

## Exact runtime change

Project: `tlcn-local-nine-airflow-20261016`. Only `airflow-webserver` was recreated
using the existing two-file Compose configuration, `--no-deps --pull never`.
Resolved Compose comparison proved that only its `mem_limit` changed:
512 MiB → **1,024 MiB**. Gunicorn remains one worker, verified in the process tree.

- Old failed ID: `d2b68f6bcdd28d1fd1e3d82b507cacf251a42b376d5ce1740073a1c9aedc69c6`.
- New ID: `95736198e85ed0e490aaf6eac0d094f0e405e634313e7d072e73dfe43d152381`.
- Image reference and actual image ID unchanged:
  `tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96`.
- New container running, OOMKilled=false, restart count=0, memory.events
  oom/oom_kill/max=0. Three actual health checks passed between approximately
  10:50 and 10:53 UTC on 2026-10-16. No Docker healthcheck is defined for this
  service; PASS refers to actual HTTP/application checks, not a fabricated
  Docker Health.Status.
- `/health`, UI login and authenticated API access passed using the existing
  session authentication backend. Scheduler heartbeat and Spark API are healthy.
- Every other isolated container retains its exact ID and start time. Runner
  remains at **3 GiB**, driver768 MiB/local[2], API idle and no Spark JVM.
- No initializer, pull/build, scheduler/storage/catalog/Trino recreation, volume
  reset or shared service operation occurred. Failed logs were saved before
  recreation; post-recovery logs were separately sanitized and retained.

## Measured resources and admission

| Post-infrastructure sample, UTC | Windows available GiB | WSL available GiB |
|---|---:|---:|
| 10:50:12 | 0.575 | 5.076 |
| 10:51:15 | 0.607 | 4.956 |
| 10:52:37 | 0.714 | 5.081 |

WSL MemTotal=7.755 GiB; swap=0. Disk free at the last sample: 62.26 GiB.
CPU/memory pressure and per-service Docker stats are included in each health
record. Resource samples are short sequential probes, not atomic measurements.

| Service | Configured cap MiB | Last cgroup current MiB | Recorded cgroup peak MiB |
|---|---:|---:|---:|
| Webserver | 1,024 | 908.1 | 910.6 |
| Scheduler | 1,536 | 508.1 | 734.2 |
| Runner | 3,072 | 14.8 | 207.1 |
| Trino | 1,024 | 778.2 | 786.4 |
| REST | 448 | 119.4 | 121.6 |
| MinIO | 512 | 193.7 | 196.3 |
| PostgreSQL | 256 | 147.9 | 151.9 |

Cgroup memory includes charged cache/shared pages and is distinct from Docker
stats working memory and individual process RSS. Raw Docker stats and RSS are
stored separately; summing RSS would double-count shared pages. Peaks are actual
container-lifetime counters, covering startup/probes only, **not nine-source
ingestion or Spark execution**. None of the running services recorded an OOM.

Trino configured heap: 512 MiB. Actual `jcmd 1 GC.heap_info` readback succeeded:
used190,318 KiB, committed227,328 KiB, reserved524,288 KiB; metaspace
used144,276 KiB. JVM RSS was625,592 KiB. Heap and RSS are different measures.
The HTTP JMX endpoint was unavailable; the direct JVM diagnostic supplied the
heap evidence without modifying Trino configuration.

Admission decision: **STOP BEFORE WRITER**.

- WSL passes the **minimum** runner cap3 GiB + reserve1 GiB screen, leaving
  approximately0.96–1.08 GiB for further infrastructure growth. This is not a
  guarantee of sufficient concurrent peak memory. One writer/Spark workload is
  configured; no native/REST tests run concurrently.
- Estimated additional runner demand is conservatively budgeted up to the
  existing3 GiB cap minus its idle charge. It is an estimate, not an observed
  Trade/Silver peak. Phase 1's NSO-only samples do not justify reducing the cap
  to2.5 GiB. No cap reduction was made.
- **Windows fails independently:** all three readings are below the owner's
  approximately2 GiB available requirement. The last shortfall is1.286 GiB;
  lower samples have a larger shortfall. This decision does not depend solely
  on the sum of Docker caps or the previous conservative-cap model.
- Webserver itself has only about113 MiB below its cap at the recorded peak;
  these healthy samples do not establish its peak under an actual pipeline.

The narrowest next resource action is for the owner to reduce host memory load
until Windows has sustained available RAM of at least2 GiB, preferably extra
margin, then repeat fresh admission including WSL/infrastructure growth. Do not
increase WSL's limit as a substitute for host headroom. No automatic application
termination, WSL/swap edit, service tuning, repeated resizing or writer trial.

## Source and orchestration checks

- Exactly21 Raw files,183,728,724 bytes, match their checksums. Trade is the full
  167,990,400-byte input, SHA-256
  `697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
- Actual local profile flags, registry checksum and nine explicit sources passed.
  All nine in-memory contracts route to `iceberg.silver_local`; the exact NSO
  DRAFT contract receives only its approved local READY overlay. On-disk NSO
  contract/transformer/metadata pins remain unchanged.
- Both serialized DAGs paused, schedule=None, max_active_runs=1,
  max_active_tasks=1, all retries=0, no import errors. Silver remains
  parent-gated/trigger-only. No NSO legacy or Trade fixture in the local allowlist.
- Local catalog0 tables, DagRuns0, task instances0, ingestion tables0,
  advisory locks0; no unknown commit. No physical Bronze or Silver work started.
- Manual Bronze run ID: **NOT_CREATED**. Automatic Silver run ID:
  **NOT_CREATED**. All nine ingestion and transformation outcomes: **NOT_RUN**.
  Target rows/UUIDs/snapshots/checksums, Spark/PyIceberg/Trino nine-source
  reconciliation, DQ/quarantine/exclusions and full/incremental replay:
  **NOT_RUN**. Expected1,414,527 rows /4 quarantine /13 exclusions are not
  measured results in this project.
- Seven shared container IDs/stopped timestamps match the frozen baseline;
  shared services were not started, queried or written. Shared catalog data was
  not remeasured by restarting services.

## Files and validation

Changed this turn: `compose.local-nine-test.yml` (one memory setting), current
status in `docs/silver/local_nine.md`, and this new evidence directory.
Production code, business rules and tests were not changed. Historical evidence
was not rewritten:57 files across Phase1, initial Phase2 and previous recovery
match the hashes captured before this recovery.

Existing overlapping regression suites (67/6/24/18/6 passing tests) remain
preserved and were not rerun for this single Compose memory change. New checks
cover the exact Compose delta, actual images/identities/mounts, three application
health/authenticated API checks, serialized DAG scope, source integrity, resource
telemetry and unchanged frozen shared state. `git diff --check` and the new
evidence credential scan pass.

Evidence: [report.json](report.json), [before.json](before.json),
[compose-plan.json](compose-plan.json), [apply.json](apply.json),
[input-integrity.json](input-integrity.json), three `health-*.json` records,
[processes-and-trino.json](processes-and-trino.json), [trino-heap.json](trino-heap.json)
and sanitized compressed logs. Git HEAD remains
`5c571693f90101c55d00ff84ceb5138092a1263c`; no commit/push.

Phase3 remains blocked on actual Phase2 Bronze→automatic Silver E2E acceptance.
No Phase3–5, Gold, scheduling or production work was performed.
