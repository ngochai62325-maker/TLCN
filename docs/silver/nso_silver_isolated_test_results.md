# NSO Silver E1 isolated test results

Result: **PASS for the implemented draft**, **NEEDS_APPROVAL for shared execution**. NSO contract remains DRAFT and `gold_ready=false`. No shared Silver/Quarantine target is created or written.

## Deliverables

1. [Contract proposal](nso_silver_contract_proposal.md): target, 47 business/lineage/control fields plus three shared diagnostic fields (50 physical fields), grain/key, full source mapping, processing and approval gates.
2. [Unit and season mapping](nso_unit_season_mapping.md): exact V06 identities, five distinct seasons, official NSO page references, active national conversions and inactive matrix proposals.
3. [Historical geography mapping](nso_historical_geography_mapping.md): all 71 raw labels, 750 source/label proposals with candidate names, parents, NULL unverified codes/dates, confidence, references and status. No fuzzy repair or current province code assigned.
4. [DQ policy](nso_silver_dq_policy.md): SQL rules/actions, shared quarantine schema, missing/footer semantics and reconciliation ledger.
5. This test report plus [machine evidence](nso_e1_test_evidence.json).
6. [Shared write plan](nso_silver_shared_write_plan.md): prerequisites and concrete proposed target/runner, partial-commit/timeout precautions and rollback readiness. NOT EXECUTED.

## Observed read-only preview

Native PyIceberg scan pinned the real recovered Bronze snapshot `3499764399995403202`, verified raw ZIP SHA and all 13 CSV checksums/headers/cell multisets, and exported 833 rows/49 fields locally. The Spark preview reads the whole Parquet export only after its SHA-256 matches [profile evidence](nso_e1_bronze_profile.json). Spark runs with `--network none`, repository mounted read-only, and only the local evidence directory writable. This is a preview of the real snapshot export, not a live shared Spark REST scan or shared writer run.

[Final preview JSON](nso_e1_preview_evidence.json) includes the actual input identity, implementation/config hashes, full physical schema, per-source/statistic/season/measure breakdown, DQ reason counts, missing/provisional/duplicate/checksum counts and 13 complete record-level exclusion audits.

| Reconciliation category | Observed count |
|---|---:|
| Bronze physical source rows | 833 |
| Nonempty source rows | 820 |
| All-source-NULL rows audited separately | 13 |
| Candidate observation slots before structural exclusion | 23,428 |
| Eligible observations generated | 23,060 |
| Valid before dedup / accepted survivors | 560 / 560 |
| Quarantined observations | 22,500 |
| Excluded candidate slots (not observations generated) | 368 |
| Excluded matrix NULL cells / national empty candidates | 360 / 8 |
| Matrix numeric cells preserved | 21,554 |
| Matrix missing markers preserved | 946 |
| National numeric observations: quantity / development index | 280 / 280 |
| Actual NULL observations in nonempty rows in this snapshot | 0 |
| Provisional observations | 766 |
| Duplicate business keys | 0 |
| Distinct source checksums | 13 |
| Valid observations with lineage-reference warning | 560 |
| Gold-ready observations | 0 |

Identity: `23,428 = 560 + 22,500 + 368`; `23,060 = 560 + 22,500`. Quarantine includes all unresolved matrix units and 22,140 non-national observations needing historical geography review. Error counts overlap: NSO_UNIT=22,500 and NSO_GEOGRAPHY=22,140; no other error rule fails in the final preview. The 560 accepted observations are national V06.12, including index values; accepted does not mean Gold-ready or shared-write approved.

## Tests actually run

| Suite | Result | Evidence |
|---|---|---|
| Final E1 tests | **14 passed**, 94.16 seconds, no skips/failures | [Final validation log](nso_e1_final_validation.log) |
| Shared framework and source regression | **78 passed**, 238.27 seconds, no skips/failures | [Regression log](nso_e1_regression.log) |
| Focused NSO grain/duplicate assertions | **1 passed**, 15.80 seconds | [Grain/duplicate log](nso_e1_grain_duplicate.log) |
| YAML contract validation | **9 passed** | [Contract validation log](nso_e1_contract_validation.log) |
| Python source syntax and diff whitespace checks | PASS | Syntax compiled in memory; `git diff --check` |
| Shared state preservation | PASS, all 11 comparisons true | [Safety evidence](nso_e1_safety_evidence.json), [PyIceberg/Trino audit](nso_e1_shared_controls.json) |

Regression suite contains 12 E1 tests from before the final unknown-statistic and decimal-overflow guards; the final 14-test E1 suite includes those guards. Suites overlap, so counts are not additive. After expanding the existing NSO grain test, a focused rerun verified 32 repeated observations deduplicate to 16 survivors and six national/region/province candidate observations retain six distinct keys; no totals are summed. Actual Iceberg tests ran using a fresh Hadoop warehouse and `HOANG_ISOLATED_ICEBERG_TESTS=1`, with no shared network route. A test-local READY contract override exists only in memory to exercise isolated writes; it never modifies the production DRAFT contract or approved mappings.

E1 tests cover declared schema/types, verified and unapproved units, quantity versus development-index conversion, all five seasons, provisional years, exact historical intervals/gaps/2008 transitions, overlap rejection, unapproved entries, genuine NULL/blank/marker/zero, per-file absent-column exclusion, all-source-NULL audits, grain and duplicates, all eight original lineage fields and the actual read snapshot, the complete real-snapshot observation ledger, and writer gating before any read/write.

Actual isolated Iceberg pipeline full/incremental test uses representative real source rows: 16 accepted observations + 30 quarantined observations + 38 excluded slots, two physical-record audits, 16 Silver rows and 32 DLQ rows. Exact retry is NOOP and creates no new Silver/DLQ snapshots; empty run is NO_INPUT; a controlled newer correction updates eight observations without increasing accepted key count; stale original input cannot undo it. All 833 real rows are separately tested through common prepare/DQ/dedup/reconciliation. This is not a claim that all 23,060 observations were physically loaded into shared Iceberg.

## Code changes made in E1

- Existing `src/silver/transformers/nso_vietnam.py`: source-specific unpivot, transparent exclusion audit, exact statistic aliases, decimal parsing/normalization with overflow quarantine, provisional/missing flags, five seasons, unit approval controls, original lineage and no Gold readiness.
- `config/silver/nso_source_tables.json`: all 13 exact header inventories/checksums; 12 matrix unit proposals remain NEEDS_APPROVAL.
- `config/silver/nso_geography_candidates.json`: 71 exact candidate names/levels for review only. `nso_geography_reviewed.json`: empty approved runtime mapping.
- `src/silver/mappings/nso_geography.py`: source-specific exact dated lookup supporting the existing transformer; no replacement pipeline or DQ framework.
- Existing loader/framework/runner: explicit READY gate before writes, opt-in draft preview, source observation-audit hook, and record audit via the common quarantine manager when a future approved pipeline runs. No other source business policy changed.
- Read-only profile/preview jobs, isolated test script, two E1 test modules, and one existing partial-year NSO fixture updated to declare its actual one-year source inventory.

## Verified rules and approval decisions

Verified: exact source/header identity, raw checksum/cell reconciliation, national quantity units from headers, development-index percent with factor 1, exact raw statistic/year/provisional tokens, distinct season titles, missing preservation, exclusion counts with original-record audits, and implementation mechanics. Official NSO yearbook page evidence corroborates the proposed three matrix unit factors; it does not approve their exact V06 series/period applicability. A proposed structural exclusion is implemented for preview and tests while production remains gated.

Still requires your review: target/grain/physical schema; matrix unit applicability for each exact series/year span; geography alias/code/scheme/parent/validity and transition-year boundary notes; retaining missing/index/provisional observations and auditing all-source-NULL exclusions; DQ actions and the two DLQ partitions; ingestion snapshot-reference=0 warning policy; final-versus-provisional release survivorship and mapping/quarantine promotion on a full rerun. Shared execution additionally requires separate authorization and an operational source-lock/commit-outcome wrapper. These choices are recorded in the contract and shared write plan; no unresolved mapping is marked approved.

## Shared safety result

Snapshot before/after E1: **3499764399995403202 ? 3499764399995403202**. Catalog metadata/registered data files, all other tables, raw objects, ingestion history, source snapshots, watermarks, checkpoints, dead letters and DAG pause states exactly match the completed B4 baseline. New registered shared files since B4: **0**. No shared Bronze/Silver writes, Airflow writer/unpause, Gold changes, data deletion, commit, push or merge. The reused B4 audit's `written_data_files` field refers to the B4 recovery relative to its original pre-B4 baseline; [E1 safety evidence](nso_e1_safety_evidence.json) explicitly compares against post-B4 and confirms zero E1 files.

Reproduce E1 tests with `powershell -NoProfile -File scripts/test_nso_silver_isolated.ps1 -SkipDownload` after generating/copying the pinned native export and required cached pytest wheels. The export stays in `.pytest_cache`, is not committed, and is always hash-checked against saved profile evidence. Run `jobs/silver/profile_nso_contract.py` in the existing native read-only environment to recreate it; the job refuses a shared snapshot drift from B4.
