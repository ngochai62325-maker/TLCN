# Phase 3 interrupted-run recovery — blocked

Runtime inspection was read-only. No service was started, stopped or recreated.

- **BLOCKED_RESOURCE_HEADROOM**: Windows available RAM in three samples about 36 seconds apart: **1.227 / 1.327 / 1.320 GiB**. WSL MemAvailable was about 4.74–4.75 GiB with no swap. This is screening of the current shared runtime, not admission for a restored isolated stack.
- Shared `tlcn`: seven running replacement containers, created 2026-10-17 01:18:05–06 UTC (08:18:05–06 Asia/Bangkok), restart policy `no`, restart count 0. The initiating actor is unknown. Shared catalog 29 identities and five ingestion metadata hashes match the prior baseline; both shared DAGs paused; no running/queued runs, active writer tasks, Trino queries, advisory locks or ingestion IN_PROGRESS. The historical scheduled signal task on a failed parent is preserved.
- Phase 2: all eight containers stopped; four named volumes retained; copied SQLite integrity `ok`; all 19 table UUID/snapshot/schema/metadata identities match the Phase 3 preservation preflight. PostgreSQL hashes were not queried again while stopped.
- Clean-room: existing eight containers stopped, immutable image references/mounts/limits/flags retained. All 21 Raw hashes and five artifact pins match. Catalog copy integrity `ok`; 19 entries. No initializer or second initial run was executed.

## Commit reconciliation

Original Bronze: `local_nine_cleanroom_initial__20261016T155210Z`.

Automatic Silver: `local_nine_bronze__6d5240a929c5e113bad77a1305cf2efa1051a613ebf3a436b328dfe88601dcc1`.

These IDs are corroborated by persisted task logs and prior intent; **fresh PostgreSQL verification remains pending**.

Trade: **CONFIRMED_COMMITTED_AND_AUDITED** using its framework `commit_verified` response, task SUCCESS/exit 0 log, current persisted catalog pointer and matching inline Iceberg metadata. UUID `c1800fdd-6d37-4b05-a913-bc61fa922b00`, snapshot `4017675517054557099`, schema 0, 1,226,470 rows. Eight Silver sources completed with matching audit/metadata evidence, total 1,391,467 rows. This recovery did not perform native Parquet scans or Trino readback.

NSO: **PARTIAL_OR_UNKNOWN_COMMIT_OUTCOME** at pipeline level. The target has an **empty committed snapshot**, ID `6224708582587211745`, 0 records, 0 data files; UUID `585ec4d4-d44b-45ad-b6ff-dc55548ebdaf`. Its 13 structural records were independently appended to dead letters; current dead-letter snapshot `3806092623472970273` reports 17 total records. The task log records SIGTERM and PostgreSQL DNS failure at shutdown, with no completed result. This is not a no-write run and does not authorize retry.

Snapshot summary counts are metadata evidence, not fresh row scans or logical-key validation. PostgreSQL/XCom state, any staged orphan files and NSO recovery must be reconciled before any writer or scheduler resumes. The last clean-room live PG sample had both DAGs unpaused; current persisted pause state is not asserted. Scheduler remains stopped. Exit 137 with OOMKilled=false is not classified as an OOM by itself.

## Safe next operation, awaiting owner approval

`controlled-stop-proposal.json` names the seven **current** shared container IDs and ordered signals. It proposes one graceful stop per service with a 60-second ceiling, no forced escalation or deletion; Spark SIGINT once requires approval for its new ID. After memory recovery, only existing clean-room storage/catalog would be started for PG/catalog read-only reconciliation. Scheduler and writers remain stopped. Any task clear/state mutation or NSO rerun requires separate approval after that reconciliation.

Strict replay invariance still includes dead-letter audit metadata. No replay has occurred. The prior Bronze startup failure and same-DagRun manual recovery remain in the original evidence directory. Historical reports were not changed.
