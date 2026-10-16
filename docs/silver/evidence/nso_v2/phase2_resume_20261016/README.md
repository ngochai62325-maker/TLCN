# Phase 2 resume after host recovery — admission still blocked

Status: **BLOCKED_RESOURCE_HEADROOM**. No services restarted/recreated; no DAG
unpaused/triggered and no data writer started. Existing successful webserver
recovery and passing regression suites were preserved, not repeated.

## Fresh admission

Three resource measurements on 2026-10-16, with intervals52.2s and46.5s:

| UTC measurement | Windows available GiB | WSL available GiB | WSL remaining after runner3 GiB + reserve1 GiB |
|---|---:|---:|---:|
| 11:08:39.906 | 1.783 | 5.061 | 1.061 GiB |
| 11:09:32.124 | 1.438 | 4.928 | 0.928 GiB |
| 11:10:18.589 | 1.578 | 4.911 | 0.911 GiB |

**Windows screen FAIL in all three samples:** available RAM did not sustain the
owner's approximately2 GiB minimum. The preferred2.5–3 GiB was not promoted to
an additional hard requirement. The latest shortfall to2 GiB is about432.5 MiB;
the lowest sample's shortfall is about575.1 MiB. Host memory recovery improved
the previous0.575–0.714 GiB readings, but admission remains below the approved
screen. No cause was assigned to unobserved Windows processes.

WSL MemTotal=7.755 GiB, swap=0. WSL passes the minimum3+1 GiB screen alone;
the remaining0.91–1.06 GiB must also accommodate infrastructure growth. This
does not establish concurrent-workload peak safety. Sum of configured Docker
caps was not used as the sole admission criterion.

Actual per-service Docker stats, cgroup current/peak/events, CPU/memory pressure,
WSL CPU/memory/I/O pressure and disk availability are in the three
`admission-*.json` records. Measurements are sequential probes, not atomic.
Disk free at the last sample:62.26 GiB. Webserver cap1,024 MiB, last cgroup
current908.8 MiB and recorded peak910.8 MiB; oom/oom_kill/max remain0. These
container-lifetime counters cover startup/probes, not an unexecuted Spark job.
Runner remains3 GiB/driver768 MiB/local[2], idle with no Java process. Its future
growth up to the existing cap is budgeted as an estimate, not a measured Trade
peak; no evidence-based case for reducing it to2.5 GiB was introduced.

## Verified state

- Exact container IDs, immutable image identities, startup timestamps and
  restart counters match the latest verified baseline. No recreate, resize,
  initializer, build/pull or runtime configuration operation.
- Webserver `/health`, UI login/session API, scheduler heartbeat and Spark API
  health PASS. Trino `/v1/info` PASS with starting=false and actual runner
  process readback confirms idle. No new OOM events in the seven running
  isolated services.
- Both DAGs paused, schedule=None, max_active_runs=1, max_active_tasks=1,
  retries=0, no import errors. Exactly nine ingestion sources and nine Silver
  dataset mappings remain in scope. The existing deterministic trigger,
  persisted parent gate and sequential writers are unchanged.
- Registry/checksum, local flags, nine routed contracts and the exact NSO
  in-memory READY overlay PASS. Production NSO remains DRAFT. No fixture or
  legacy NSO Silver in the local profile; targets use `iceberg.silver_local`.
- Exactly21 Raw artifacts/183,728,724 bytes match their SHA-256 inventory.
  Trade is the full167,990,400-byte input, SHA-256
  `697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
  Registry, NSO metadata/contract/transformer pins and prior code hashes match.
- Local catalog0 tables, DagRuns0, task instances0, ingestion tables0,
  advisory locks0. No unknown commit and no concurrent writer.
- All seven shared containers retain their IDs and stopped timestamps. Shared
  services/data were not started, queried or written.

## Execution and acceptance

Manual Bronze run ID: **NOT_CREATED**. Automatic Silver run ID:
**NOT_CREATED**. All nine source ingestion/transformation task outcomes:
**NOT_RUN**. Actual target row counts, UUIDs/snapshots/checksums, business-key
uniqueness, Spark/PyIceberg/Trino nine-source readback, lineage/DQ/NSO semantics,
quarantine/exclusion counts and full/incremental replay: **NOT_RUN** in this
project. Existing Phase1 and eight-source evidence was not substituted for
actual Phase2 execution. Expected counts were not used to declare PASS.

Only this new evidence directory and the current-status note in
`docs/silver/local_nine.md` were changed. Source/config/test files remain
unchanged from the successful routing/recovery baseline; no passing regression
suite was rerun. Historical evidence hashes were preserved. `git diff --check`
and new evidence credential scanning PASS. No commit/push.

See [report.json](report.json), [inputs.json](inputs.json), the three admission
records and [trino-runner-health.json](trino-runner-health.json).

## Narrowest remaining action

The owner needs to reduce host memory load enough for Windows available RAM to
remain at least approximately2 GiB across fresh samples. Relative to the lowest
reading, at least about576 MiB more available RAM is needed, plus practical
margin for fluctuations. This is not a request to reduce the Spark cap or
increase WSL memory. Do not automatically close Windows applications, modify
swap, tune other services, retry writers or restart shared services.

After resource admission passes, the still-unexecuted single manual Bronze→
automatic Silver run and actual nine-target acceptance remain required.
Phase3–5 have not started.
