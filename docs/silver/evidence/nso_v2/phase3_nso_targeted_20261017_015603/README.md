# Phase 3 — NSO targeted recovery

**NSO_RECOVERY_VALIDATED_CHILD_TASKS_PASS_DAGRUN_FINALIZATION_BLOCKED**.

The NSO recovery and all14 child tasks passed, but the child DagRun remains `running`. Full Phase3 PASS is not claimed. Parent finalization and pipeline NOOP replay were not performed.

## Changes and tests

- Production change:17 lines in `src/silver/engine/quarantine.py`; ID derivation/schema/detection rules unchanged. Persisted ID/payload/errors are compared before MERGE; identical input returns without any Iceberg commit. Real error changes retain the previous update semantics; changed payload gets a new ID; same-ID payload corruption fails closed. Mixed batches retain old audit fields and insert every new record.
- Focused native tests in `tests/unit/test_silver_quarantine.py`: **7 PASS**, using temporary Hadoop Iceberg tables and independent PyIceberg metadata/Parquet reads. `quarantine-native-snapshot-proof.jsonl` records UUID/snapshot/schema/metadata/content evidence.
- Affected regressions: **52 distinct cases PASS** (50 passed in corrected batch; only the2 test-catalog configuration failures rerun and passed). Earlier dependency/collection/configuration failures are retained. Test-only existing wheels were used; no package installation or image/dependency mutation occurred. The previous Compose memory-limit edit is preserved.

## Real recovery and data

Existing child: `local_nine_bronze__6d5240a929c5e113bad77a1305cf2efa1051a613ebf3a436b328dfe88601dcc1`. Existing parent: `local_nine_cleanroom_initial__20261016T155210Z`.

Only interrupted NSO was cleared through supported `clear_task_instances(dag_run_state=False)` with scheduler stopped. Its original job24/attempt1 is preserved in Airflow history and log evidence. New real execution used job25 and Airflow CLI `try_number=1` (not artificially changed to2). The inherited counter is reported transparently; this was a manual recovery, not an uninterrupted run.

- NSO target: `iceberg.silver_local.nso_rice_statistics_v2`.
- UUID: `585ec4d4-d44b-45ad-b6ff-dc55548ebdaf`.
- Snapshot: `6224708582587211745` (empty) → **`833301860238618812`** (published), schema0,35 active data files.
- Actual rows: **23,060**, comprising22,114 numeric,946 missing and5 genuine zeros;23,060 unique business keys;22534 warning observations. Missing markers, units/conversion factors, geography policy and all eight lineage fields validated against the pinned Bronze read snapshot `6239067651375331267`.
- Units: hectare7640, tonne7640, kg/hectare7500, percent280. Seasons/geography flags are in the native report. Production contract remains **DRAFT**.
- Framework commit verification, native PyIceberg/Parquet readback and bounded Trino **all-row/all-column hashes** PASS for NSO and dead letters, including schema compatibility and exact microsecond timestamp handling.
- All34 old staged objects remain unchanged and unreferenced; no adoption/deletion. They do not count as published rows.

Dead letters: **17 records =13 NSO structural exclusions +4 market conflicts**,17 unique quarantine IDs. UUID `7711cdc0-135f-491c-a153-09c5ff6c1689`, snapshot **`3806092623472970273`**, schema0 and metadata location unchanged. Before/after full content+audit SHA256:

`1224240117484e1bc8dd020deed57f737dfc2097dd7a0146360e7ddae968f305`

No refresh of pipeline_run_id/detected_at and no dead-letter commit occurred.

## Child/parent and runtime

Child audit ran once after its initial CLI admission was rejected at `scheduled` state (no operator execution); the supported queue-state override retained upstream checks. All9 Spark audit results PASSED. Signal task returned **DQ_PASSED**, no READY_FOR_GOLD or Gold. Terminal `end` task really ran (job29, actual CLI try_number0). **14/14 child task instances are SUCCESS**, but the child DagRun is still **RUNNING**, since scheduler is stopped and no additional state reconciliation was invoked.

Both DAGs are paused, schedule=None. Scheduler remains stopped. Existing webserver/runner/Trino were started at unchanged images/limits; storage stayed healthy. Shared and Phase2 remain stopped. Resource admission samples: Windows3.612/3.618/3.462GiB, WSL5.620/5.611/5.612GiB, no OOM. Observed cgroup peaks are in report.json; sampled Windows minimum is not a claim of continuous monitoring.

All18 other catalog identities (nine Bronze, eight completed Silver, dead letters) and eight completed transform task instances are unchanged. Five ingestion metadata hashes are unchanged;10 runs remain (nine SUCCESS plus the historical failed startup run). No parent state changes or new DagRun. Current Silver metadata total: **1,414,527 rows**.

## Remaining blocker and approvals

Automatic approval review rejected a final-verification command containing a conditional `DagRun.update_state(execute_callbacks=False)` call. It interpreted terminal finalization as outside the approval. The command did not execute; its mutation branch was removed. Subsequent checks were strictly SELECT/GET-only. The child is not labeled SUCCESS.

`remaining-finalization-proposal.json` gives the exact **child-only** supported state-computation operation to approve, with fresh14/14 task-success/audit/identity preconditions; scheduler stays stopped, no direct set_state override, no parent mutation or writer/replay.

Only after child SUCCESS is actually persisted should the separately unapproved parent step be considered: supported REST PATCH on the exact existing trigger/wait task RUNNING→SUCCESS (dry-run preview first), then execute its end task and compute parent state from actual tasks. The inspected endpoint performs state-only bookkeeping; no trigger operator execution or duplicate child. No mutation request was sent.

No additional source writer, parent finalization, full/incremental pipeline replay, Phase4/5, production/Gold, image recreation, data reset/rollback/delete, Git commit/push was performed. Historical evidence is retained. Stop here for owner review.
