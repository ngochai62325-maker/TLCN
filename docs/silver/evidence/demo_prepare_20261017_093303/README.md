# Thesis demo consolidation ? PREPARE only

Status: **PREPARE_COMPLETE_EXECUTION_NOT_APPROVED**. No Docker stop/start/recreate,
deletion, image build or pipeline trigger was performed. HEAD remains
`e4cd47db6654a8aa0494b03e6f0a09e845a7b58d`; no commit/push in this preparation.

- [Preparation report](report.json), [focused tests](tests.json).
- [Exact deletion manifest](deletion-manifest.json):64 verified containers,
  8 project networks,15 owned volumes. No image deletion. Five standalone
  containers and18 unattributed/non-TLCN volumes are excluded. Bind directories
  (especially Windows Raw/source) and all Git evidence are retained.
- [Inventory/ownership](docker-inventory.json), [Raw pins](raw-verification.json).
- [Backup plan](backup-plan.json), [verified Git archive](git-and-backup.json).
  Volume exports are **NOT_RUN**; the Git archive is not a substitute for data.
- [Port investigation](port-investigation.json): actual Internal=true bridge;
  namespace sharing absent; container-local HTTP healthy but requested8085
  mapping unpublished. Canonical bridge fix is prepared, **not live-validated**.
- [Read-only runtime](runtime-readonly.json), [historical comparison](catalog-baseline-comparison.json).
  Clean-room19 identities match the full physical baseline. Shared has29 tables
  but6 committed snapshot/metadata changes since the earlier accepted baseline,
  and current PostgreSQL has0 ingestion runs. Cause/actor not established.
  No attempt was made to reconstruct history or undo those changes.
- [Resource samples](resources.json): Windows2.641/2.164/1.898GiB, WSL3.189/3.200/3.184GiB.
  Admission for build/start **BLOCKED**; remeasure after approved controlled stop.
- [Demo guide](../../demo_guide.md): one canonical `tlcn-local` project,
  localhost8085, nine sources/14 Silver tasks, manual-only/paused.

Eight new config/safety tests and two existing DAG regression tests PASS.
Existing clean-room readiness/PowerShell probes PASS. Historical206-test
acceptance is preserved, not rerun or claimed as fresh-demo acceptance.
Preparation fixed a check-only parser assumption (Compose omits false internal),
and supplied Trino's required user header. The first catalog comparison's false
location warnings are preserved separately; the full physical baseline corrected
them. The six shared changes are real and remain disclosed.

Owner approval must cover the exact manifest, backup plan, retained exceptions
and known shared drift. Then fresh quiescence/ID/backup/resource gates precede
any removal or fresh deployment. No retry/reset to hide failure, no forced kill,
no global prune. The owner alone triggers the fresh Bronze pipeline after smoke.
