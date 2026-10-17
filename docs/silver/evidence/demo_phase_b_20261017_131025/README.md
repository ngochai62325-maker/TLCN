# Final local demo ? runtime ready

**LOCAL_DEMO_RUNTIME_READY_PENDING_OWNER_PIPELINE**.
Airflow is available at **http://localhost:8085**; login credentials are the
public local-demo account in the [demo guide](../../demo_guide.md).

- [Report](report.json), [actual runtime/ports/DB/catalog/packages](final-runtime-readback.json).
- [Cleanup result](cleanup-result.json):64 stopped owned containers,7 eligible
  networks,15 owned volumes removed without force. Volume backups waived by owner.
  All original images and every Windows bind directory retained. Five excluded
  containers,19 volumes (including unproven62df...) and their dependency network
  were preserved. Only tlcn-local has running services.
- [Preflight/Raw/ownership](safety-preflight.json), [effective narrowed scope](effective-delete-plan.json),
  [signal outcomes](controlled-stop.json), [action journal](actions.json).
- [RAM admission](resources.json): Windows5.542/5.621/5.597GiB and
  WSL6.849/6.840/6.841GiB before build. No memory limits increased.
- [Tests](tests.json):10 focused config/safety tests PASS; actual start/status,
  graceful stop/restart and reset PLAN_ONLY PASS.8container IDs and4volumes retained.
- [Helper fixes](helper-corrections.json): original initial errors preserved,
  corrected under the approved scripts scope; no pipeline retry or data reset.
- [Repository cleanup plan](repository-cleanup-plan.md): owner approval required;
  no documentation/evidence removal or Git commit/push occurred.

Both DAGs paused, schedule=None, retries0. Nine Bronze definitions and14 Silver
DAG tasks include transform_nso_vietnam_v2; actual serialized DAGs were checked.
Zero DagRuns/task instances and no loaded Bronze/Silver tables. NSO production
contract remains DRAFT; local-only in-memory overlay is active. Trino SELECT1,
REST/PyIceberg connectivity, Windows HTTP and demo login checks PASS.

Fresh Bronze/Silver E2E is **NOT_RUN**. Owner unpauses Silver then Bronze and
triggers only rice_lakehouse_ingestion with force_reprocess=false. No automatic
run, Gold, production scheduling or consumer change was performed.
