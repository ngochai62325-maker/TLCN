# Repository cleanup proposal ? no deletion performed

Runtime smoke PASS; repository cleanup still needs separate owner approval.
Current supported navigation: root README, docs/silver README, demo_guide,
local_runbook, architecture, data_catalog, nso_v2, validation, evidence/index.

| Candidate | Evidence / decision |
|---|---|
| `compose.cleanroom-ui-8085.yml` | User-owned untracked retired overlay. Current8 demo containers reference only the3 root canonical Compose files. rg finds this filename in historical preparation inventories/reports, with no script/test execution reference. Candidate to archive after owner approval; it is NOT in e4cd47d. Keep now. |
| Old progress/log directories under `docs/silver/evidence/nso_v2/` | Many are committed at e4cd47d, but accepted proofs, MD links, unit/integration fixtures and historical overlays must be checked file by file. Archive duplicate progress only after a reference manifest/link update and explicit approval. No bulk deletion proposal. |
| `compose.silver-canary.yml`, `compose.silver-production.yml`, legacy `docker-compose.yml` | Not current demo inputs; historical release docs still reference them. Keep until reference/test audit and owner decision. Fresh startup remains start.ps1, not the legacy base. |
| Regenerable wheel/bytecode/test caches | Runtime Dockerfiles/commands/PYTHONPATH do not depend on .pytest_cache. Isolated test runners regenerate wheels. Remove only proven generated subdirectories after approval; do not blanket-delete private backups or interrupted-operation journals. |
| Raw/Trade auxiliary files already tracked | Git currently tracks26 data/raw files, including small source/lookup files; full167990400-byte Trade input remains outside Git. Do not delete or untrack blindly. Keep manifest + complete21-file bundle/support metadata; any Git-only untracking needs explicit review and must retain Windows files. |

Always keep source,9-source profile/registry/contracts (NSO DRAFT), pinned
Dockerfiles/lockfiles, canonical3 Compose inputs, local_demo scripts and critical
framework/DQ/lineage/NSO/orchestration/NOOP tests. Older isolated Compose files
also support tests; being unused by the live demo does not prove obsolete.

Git e4cd47db6654a8aa0494b03e6f0a09e845a7b58d is reachable and contains the
accepted clean-room proof. The new demo_prepare/demo_execute/demo_phase_b
folders, demo guide and local scripts are currently UNCOMMITTED; they cannot be
recovered from that old commit. Preserve/review them before any later cleanup.
No commit, push, document deletion or evidence removal was performed in Phase B.
