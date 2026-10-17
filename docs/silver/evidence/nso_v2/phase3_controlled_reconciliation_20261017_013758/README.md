# Phase 3 — controlled stop and read-only reconciliation

**CONTROLLED_STOP_AND_READ_ONLY_RECONCILIATION_PASS**. Initial Phase 3 remains **INCOMPLETE / RECOVERY_PENDING_OWNER**. All times in evidence are UTC.

## Corrected service mapping and actual stop

The previous proposal's parallel arrays were not zipped. Each service/ID was independently verified from Docker Compose labels before signaling. The corrected execution plan uses explicit service-ID objects.

| Service | Exact container ID | Single signal | Actual exit |
|---|---|---|---:|
| airflow-scheduler | `b76394eb3290814d7ebc91c868ee5a762855ec4c2692fd21acb35c68ca19d8e0` | SIGTERM | 0 |
| airflow-webserver | `fbd0b78ac6d1fc1cd7f783f9c4c2a44f9f4e6050a6faff448153c41bc144a549` | SIGTERM | 0 |
| spark-iceberg | `ac65ab2849c9a9a6762c8bb86f0fd7f7df3c958089eb8e1d5650f83572ee39cd` | SIGINT | 130 |
| trino | `628a560441838fbaba19276bcc787f14a074c81a15656558a3ba67c2cc7b2f1c` | SIGTERM | 143 |
| iceberg-rest | `f9c08d4e677b677abef9d5ead6c5e98e648b166cc5de58cb4ad61ea796b65a0f` | SIGTERM | 143 |
| minio | `513e89e55d90014d495c980f8beefc6823671cf5e007eb909bb0499c98e74ca6` | SIGTERM | 0 |
| postgres | `dd6aa1baf544cfe516b983cd00ac9988e234478546dd376960a3fb00f6fe6b4c` | SIGTERM | 0 |

No timeout, second signal, forced escalation, recreate, removal or volume operation occurred. Spark PID1 was an idle Python API with SIGINT caught/not ignored; its actual KeyboardInterrupt shutdown is in the sanitized log. Exit 130 is not interpreted as data loss. Shared remains fully stopped; Phase 2 remains fully stopped.

## Storage-only restoration and RAM

Only existing clean-room PostgreSQL, MinIO and REST were started, with image/mount/volume/flag checks. PostgreSQL read-only transactions succeed; MinIO is healthy and authenticated read-only S3 GET/list works; REST `/v1/config` and actual 19-table load responses succeed through the existing PostgreSQL network. No port/network changes or new client service were needed. Airflow scheduler/webserver, runner and Trino remain stopped. Initializer was not run.

Windows available RAM after storage restore, samples about 36 seconds apart: **[4.164, 4.133, 4.345] GiB**. WSL MemAvailable: **[6.743, 6.757, 6.746] GiB**, no swap. This passes the minimum storage-only screen; it is **not** admission for restoring execution services or running a writer. Fresh full-runtime resource admission is still required for later recovery.

## Persisted Airflow and commits

- Bronze run: `local_nine_cleanroom_initial__20261016T155210Z`.
- Existing automatic Silver run: `local_nine_bronze__6d5240a929c5e113bad77a1305cf2efa1051a613ebf3a436b328dfe88601dcc1`.
- Both DagRuns remain persisted `running`; both DAGs are **unpaused**, schedule `None`. No state/paused flags were changed. Scheduler startup is **NOT SAFE**: NSO task and parent trigger/wait task are stale `running`, attempt 1/max_tries 0, with no live processes.
- Nine Bronze tasks and eight Silver transforms are SUCCESS. Current XCom commit proofs match live REST pointers. Eight Silver sources retain **1,391,467 rows** by verified commit evidence. Trade is fully committed/audited; it must not rerun.
- PostgreSQL contains 10 ingestion runs: nine SUCCESS plus the preserved historical startup FAILED run; nine source snapshots; no ingestion IN_PROGRESS or advisory locks. No PostgreSQL source metadata or Airflow state was edited.
- NSO has no completed result/XCom; downstream audit/signal/end were never run. The old SIGTERM/DNS failure is preserved in the earlier task log.

NSO publication is **confirmed partial execution**, with resolved current catalog pointers:

- UUID `585ec4d4-d44b-45ad-b6ff-dc55548ebdaf`, snapshot `6224708582587211745`, schema 0: **empty committed snapshot, 0 observations / 0 active data files**.
- **34 unreferenced staged Parquet files**, actually readable with **22,294 observations**, 916 missing, five zeros and 22,294 unique business keys. Their pinned Bronze read snapshot is `6239067651375331267` and ingestion run is `nso_vietnam_20261016160030_2c409d8a`. These files are not published and do not count as NSO success. No adoption or deletion was performed.
- No observation MERGE snapshot or newer NSO metadata JSON exists in the inspected target prefix. No unresolved catalog commit was detected. The remaining work is recovering the interrupted application/task execution, not inferring success from staged files.

## Actual dead-letter readback

Current snapshot `3806092623472970273`, UUID `7711cdc0-135f-491c-a153-09c5ff6c1689`. Current Avro manifest references resolve two active Parquet files, scanned using PyArrow with read-only S3 GET through existing storage networking: **17 records = 13 NSO structural exclusions + 4 market quote conflicts**. No dead-letter orphan Parquet files were found. S3 URL-encoded listing keys were decoded according to the response `EncodingType` before reference comparison.

All 17 quarantine IDs are unique and recomputed successfully from dataset/payload; business-key hashes also recompute correctly. Thirteen NSO source files/checksums/eight lineage fields and the exact child pipeline_run_id match. Two duplicated business-key hash groups (two rows each) are the approved competing-price variants, not duplicate quarantine records.

All 13 NSO exclusions have detected_at `2026-10-16 16:10:17.672858+00:00`; their append committed before the empty NSO target creation. Market records retain detected_at `2026-10-16 16:08:58.556304+00:00`. Complete records, hashes and snapshot timing are durable in the readback/analysis JSON. Dead-letter and Silver writes are separate transactions; an exclusion commit does not imply an observation commit.

## Proposed recovery — NOT executed

See `minimal-recovery-proposal.json`. The current quarantine upsert unconditionally refreshes `detected_at` on a same-record retry. Preserving all 17 current records/audit fields requires separate approval for a narrow real-delta guard in `route_quarantine`, with native snapshot-invariance tests; merely adding a SQL matched predicate may still permit an empty Iceberg commit. No code has been edited.

Then, with further runtime/task approval, keep scheduler stopped and recover only NSO through existing Airflow task execution/common runner in the **same** child DagRun, retaining its pinned Bronze snapshot. Execute only its never-run audit/signal/end tasks; compute terminal child state from actual task success/readback. The stale parent `trigger_silver_release` must **not** be cleared/rerun: the duplicate-run pre-execution guard will reject its already-existing child. After verified child success, an explicitly approved state-only finalization of that existing trigger/wait task and its end is needed. No new DagRun or Bronze rerun is proposed.

Strict NOOP still covers Bronze, all nine Silver targets and dead-letter snapshots/audit fields. No actual replay was performed and no NOOP PASS or empirical replay failure is claimed. Expected Phase 2 counts (23,060 NSO observations, 946 missing, five zeros) remain baselines, not actual published output here.

## Preservation and scope

All 29 shared identities were rechecked after stop using read-only copies of the stopped SQLite catalog and MinIO metadata; they match the pre-stop baseline. Five shared ingestion hashes matched at the fresh pre-stop SQL gate; shared PostgreSQL exited cleanly and was not restarted for an additional SQL query. The historical scheduled publish task is retained. Phase 2 containers/images/mounts/volumes and historical recovery evidence are unchanged. All 19 clean-room catalog pointers, eight successful Silver targets, nine Bronze snapshots and five clean-room ingestion-table hashes remain unchanged across reconciliation.

Git HEAD remains `82bfec47988124cfcd73d53f14604714da6c2108`. Existing `compose.local-nine-test.yml` memory-limit edit is preserved. This task added forensic evidence and private ignored operator helpers only; no production code, business rules, contracts, NSO DRAFT, data, task states or historical reports were changed. No scheduler/writer/Gold/replay, commit or push occurred.
