# NSO Silver shared write plan ? NOT EXECUTED

E1 authorization covers implementation, local evidence and isolated tests only. Contract remains DRAFT with critical blockers; `SilverContract.require_ready()` refuses individual and all-source writers before read/write. No shared Bronze/Silver mutation, Airflow writer, DAG unpause, Gold change, commit, push or merge is authorized or executed here.

## Prerequisites

1. Approve the proposed `iceberg.silver.nso_rice_statistics` target, grain, 47 contract fields plus three shared diagnostic fields (50 physical fields), and source identity preservation.
2. Approve exact V06 matrix units for each series and validity period; provide an official export reference if available.
3. Approve historical geography aliases/codes/schemes/parents/full-year intervals and transition-year reporting boundaries. Unresolved labels must remain quarantined. Do not set all mappings approved merely to unblock a writer.
4. Approve the missing/provisional/development-index policy, all-source-NULL exclusion with 13 audit records, DQ actions and the two NSO partitions in common dead_letters.
5. Decide lineage reference=0 warning policy and quarantine reprocessing/promotion strategy. Apply versioned reviewed mappings and rerun a complete read-only preview; all remaining critical mappings must be resolved before READY, or scope an explicit revised contract for a reviewed subset.
6. Receive separate explicit authorization for shared NSO Silver and the affected DLQ partitions. B4 authorization applied only to Bronze and cannot authorize this Silver write.

## Concrete execution proposal after approval

Re-read current Bronze snapshot/schema and raw ZIP plus all 13 checksums; compare to reviewed preview. Any drift means stop and create new evidence. Pin the exact approved Bronze snapshot. Inventory Silver target/DLQ snapshots and existing namespace, check concurrent NSO ingestion/Silver tasks and acquire the NSO source lock; keep Airflow paused. Reuse `SilverTransformationFramework.run`, existing source transformer, loader, common DQ/quarantine/dedup and Iceberg MERGE; bounded Spark resources and explicit NSO dataset only. No all-source run. No append-only Silver load, table truncation, snapshot expiration or source deletion.

Proposed full command after implementation of approved mapping versions and operational lock integration: `python jobs/silver/job_nso_vietnam.py --dataset nso_vietnam --mode full --pipeline-run-id <stable-approved-id> --output <local-evidence.json>`. This is a review command, not run in E1. The existing wrapper/runner requires READY; operational source-lock acquisition/timeout identity checks across Silver and DLQ still need an execution wrapper before shared use. Preview command can use `run_all_silver.py --dataset nso_vietnam --preview` without writes.

The pipeline commits quarantine, exclusion audit, and valid table separately; Iceberg does not provide a multi-table transaction. Stop downstream on any partial failure, preserve per-target snapshots and deterministic IDs, inspect commit outcome before retry. Never blindly rollback. A commit timeout requires inspection of target snapshots/keys/payload and DLQ repair identity. No generic automatic rollback or cleanup is proposed. Retrying verified unchanged commits is idempotent. Policy changes require full quarantine reconciliation, not only ingestion watermark replay.

## Validation and rollback readiness

Compare approved preview to PyIceberg and Trino: table schema/types, accepted business keys, values and missing/provisional statuses, lineage, DLQ payload/reasons and exclusion IDs, partition counts, snapshot before/after and written files. Assert all reconciliation identities and no unmatched geography in any Gold-eligible subset. Rerun with the same inputs and stable run identity; require NOOP with unchanged valid/DLQ snapshots. Check other source tables and DAG states unchanged. No Gold-ready output is declared merely because a MERGE succeeds.

Capture each target's metadata location, snapshot/history and object list before any write; check previous files remain available. A new target has no prior baseline snapshot, so rollback requires a separately approved recovery plan rather than automatic DROP. For existing targets, retain old metadata/snapshots for a reviewed per-table rollback. No NSO Silver rollback is executed or needed in E1.

The 560/22,500 valid/quarantine conservative preview expected split is not a final approved shared-write target. Approving units/geography will change it; regenerate the pinned preview and seek approval for concrete counts, target changes and mapping versions before execution.
