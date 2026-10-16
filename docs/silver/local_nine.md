# local_nine — manual-only nine-source Airflow profile

Phase 2 status: **BLOCKED_RESOURCE_HEADROOM — Windows admission threshold**.
Targeted recovery recreated only the isolated webserver at1 GiB with one worker;
three actual health/UI/authenticated API checks passed without OOM/restart.
Runner remains3 GiB. Windows available RAM measured0.575–0.714 GiB after startup,
below the approved approximately2 GiB screen; no DAG was unpaused/triggered.
[Latest recovery evidence](evidence/nso_v2/phase2_final_airflow_20261016/README.md)
records the exact container change, measured peaks and admission decision.
After owner host-memory recovery, the latest resume measured Windows
available1.783/1.438/1.578 GiB in three samples about30–60s apart. Services remain
healthy and unchanged, but all samples are below the approved approximately2 GiB
screen, so no DAG was unpaused/triggered. See the
[resume admission record](evidence/nso_v2/phase2_resume_20261016/README.md).
Code, focused DAG/metadata regressions and native NSO routing/readiness/NOOP
checks passed. The controlled Airflow E2E has **not run**. The initial webserver
was OOM-killed at512 MiB; that failure is preserved in the earlier reports.
No Bronze/Silver table exists
in this new test catalog. Do not treat this as Phase 2 acceptance or Phase 3
clean-room evidence. [Actual evidence](evidence/nso_v2/phase2_local_nine_20261016/README.md).

The profile extends the two existing DAGs; it does not introduce another engine
or DAG framework. Eight CLOSED transformers/contracts, NSO legacy logic, the
production NSO DRAFT contract and historical reports remain unchanged.

## Scope and activation

| Bronze source | Silver dataset | Target under `iceberg.silver_local` |
|---|---|---|
| faostat_production | faostat_production | faostat_production |
| faostat_monthly_price | faostat_monthly_price | faostat_monthly_price |
| faostat_supply_utilization | faostat_supply_utilization | faostat_supply_utilization |
| faostat_trade | faostat_trade | faostat_trade |
| usda_psd | usda_psd | usda_rice_psd |
| usda_rice_yearbook | usda_rice_yearbook | usda_rice_yearbook |
| worldbank_pinksheet | worldbank_pinksheet | worldbank_commodity_monthly |
| thitruongnongsan | thitruongnongsan | thitruongnongsan |
| nso_vietnam | nso_vietnam_v2 | nso_rice_statistics_v2 |

`src/silver/local_profile.py` is the explicit source/dataset/target allowlist.
It never creates a tenth ingestion source `nso_vietnam_v2`. Enabled fixture and
NSO legacy Silver entries are excluded. The unchanged registry checksum is pinned.

Activation requires these independent local flags:

```text
SILVER_LOCAL_PROFILE=local_nine
SILVER_LOCAL_ENABLED=1
SILVER_ACCEPTED_RELEASE=
SILVER_PRODUCTION_ENABLED=0
SILVER_CANARY_ONLY=0
SILVER_BRONZE_SCHEDULE=
```

Nonempty release, production/canary activation, cron, an unknown profile, or
missing local enablement fails closed. An activated local runtime refuses an
implicit/default runner/API selection. The existing accepted-release flags and
eight-source routing continue to work independently outside this profile.
The API health `silver_release` field advertises existing release capability;
it is not an activation flag. Actual local routing requires its `local_profile`
capability and an explicit profile on every request.

The exact Phase 1 NSO DRAFT YAML hash is required before making a READY contract
copy **in memory**. Only its output namespace and execution status change. The
production YAML, business keys, units, DQ rules and transformations are untouched.
Dead letters always route to `iceberg.silver_local.dead_letters`.
`thitruongnongsan` quarantine and `nso_vietnam_v2_exclusions` remain separate
source_dataset identities and metrics; 13 structural exclusions are not quarantine.

## Workflow and safety

Both DAGs have schedule=None, catchup=False, max_active_runs=1 and writers
retries=0. Local source tasks are chained, with max_active_tasks=1, so an earlier
failed/unknown writer blocks later writers. These extra chains affect local only.

1. A single manual `rice_lakehouse_ingestion` run receives
   `{"force_reprocess":false}`. Local forced reprocessing is rejected.
2. Each of nine tasks uses the existing ingestion engine. SUCCESS must match its
   persisted PostgreSQL result and a physical Iceberg run readback. Checksum
   SKIPPED must also have a prior successful checksum/run proof. NOT_READY,
   FAILED, missing results and unknown outcomes block the batch.
3. `verify_bronze_batch` rechecks exactly nine results, registry identity and
   Bronze UUID/snapshot/schema/metadata location/row counts under source locks.
4. The batch gate admits only the unpaused, correctly serialized local Silver
   DAG, without another queued/running or duplicate run. Its run ID is
   `local_nine_bronze__` + SHA-256(parent Bronze run ID). The existing trigger
   operator uses reset_dag_run=False, wait_for_completion=True and retries=0.
5. Silver checks the actual successful parent gate/XCom and deterministic run ID.
   Independent manual Silver invocation is blocked. Before writes, all nine
   readiness checks run; NSO uses a real pinned read context rather than the old
   context-free empty-frame preprocessor.
6. Each writer receives its verified Bronze snapshot as an integer-64 identity.
   NSO locks the Bronze source `nso_vietnam`, not its different Silver dataset ID.
   The existing runner holds the lock through commit/catalog outcome readback;
   unexpected source/target schemas and unknown commits fail closed, without
   automatic ALTER, overwrite or retry.
7. Existing Silver audit reconciles valid/quarantine/dedup/exclusion counts and
   persisted keys/types/blocking DQ. The final signal is DQ_PASSED; no Gold task
   or READY_FOR_GOLD event is added.

The local profile does not replace `bronze_read_snapshot_id` with
`_source_snapshot_id`. All eight original Bronze audit fields are retained.
Full/incremental replay preserves the original snapshot/content/processing time;
the new native profile regression proves this on a small controlled fixture.
Full nine-source Airflow replay/readback remains untested because resource
admission stopped execution before any writer.

## Isolated execution record and next approval

The test configuration combines `compose.silver-test.yml` and
`compose.local-nine-test.yml` with project `tlcn-local-nine-airflow-20261016`.
It has its own internal network, persistent PostgreSQL/MinIO/catalog/log volumes,
readonly Raw/workspace mounts, existing pinned execution images and no startup
pip install. It never uses `docker-compose.yml` or shared storage.
Local-only ports are 127.0.0.1:18085 for Airflow and 127.0.0.1:18080 for Trino.
Airflow UI and authenticated API passed after the isolated webserver recovery;
both DAGs remain paused. Availability alone does not authorize bypassing RAM gates.

All 21 Raw artifacts (183,728,724 bytes) matched the pinned input inventory.
Trade is the full 167,990,400-byte file with SHA-256
`697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
For those inputs, **1,414,527** Silver rows is only a future reconciliation
expectation, not a pipeline constant or an observed local result.

The initial settings are retained as historical failure evidence: webserver cap512 MiB,
Spark cap3 GiB/driver768 MiB/local[2], scheduler1.5 GiB, Trino1 GiB/heap512 MiB,
MinIO512 MiB, REST448 MiB/heap256 MiB and PostgreSQL256 MiB.
The webserver limit proved insufficient; do not reuse this configuration as a
passing startup recipe. Docker/kernel evidence identifies CONSTRAINT_MEMCG for
that container; it does not indicate a new global WSL OOM.

Current approved adjustment, **applied to webserver only**:

```yaml
services:
  airflow-webserver:
    mem_limit: 1024m
  runner:
    mem_limit: 3072m
```

Runner cap3 GiB, driver768 MiB and one Spark workload remain unchanged. The
earlier2.5 GiB suggestion was not applied; its full workload headroom is unproven.
Do not use summed caps alone for admission. Latest evidence separates caps,
observed working memory, actual cgroup peaks and estimated runner growth.

Before continuing the still-unexecuted single Bronze manual run, host RAM must
meet the approved screen and a fresh RAM/identity/empty-catalog/DAG gate must pass.
Only webserver was recreated; runner and every other service retain their IDs.
Failed webserver logs/evidence were preserved first. Do not rerun init,
reset metadata, start shared, clear tasks, or retry any unknown writer. Unpause
Silver before Bronze only after those gates pass; pause both after the E2E.

After a successful run, verify exactly one automatic Silver run, all nine target
tasks, physical PyIceberg/Trino readback, key/DQ/lineage reconciliation and
separate 4-quarantine/13-exclusion counts. Perform sequential replay only when no
writer is active. No cron, production activation, NSO READY promotion, shared
write, Phase 3 clean rebuild, cleanup, commit or push is authorized by this record.
