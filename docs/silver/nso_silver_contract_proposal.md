# NSO Silver data contract proposal ? E1

Status: **DRAFT / NEEDS_APPROVAL**. This is a reviewable implementation and read-only preview, not shared-write authorization. Critical mappings are intentionally unresolved; Gold eligibility remains false.

## Source and target

Input `iceberg.bronze.nso_vietnam`, pinned B4 snapshot `3499764399995403202`, 833 physical rows, 49 fields, 13 CSVs V06.12?V06.24. Raw ZIP SHA-256 `eb231503f3a1051d226100c699983c7f1b420f11e205cc1e419a90373f5d700d`. `nso_e1_bronze_profile.json` records all 13 checksums, original headers, exact source-column inventories, and the checksummed local Arrow export. Local metadata.txt supplies titles, not units or historical geography codes.

Proposed target: **`iceberg.silver.nso_rice_statistics`**; shared quarantine `iceberg.silver.dead_letters`, source partitions `nso_vietnam` and `nso_vietnam_exclusions`. Neither target is created in E1. No sum across geography levels, overlapping sources, annual/season totals, or quantity/index kinds.

## Grain and key

One observation per source table, exact raw geography label and level, reference year, crop season, measure, statistic kind. Business key: `source_table, geography_name_raw, geography_level, year, season, measure, statistic_kind`. National V06.12 and matrix national totals remain different source series. A country, region and province never share a key. Retain source-specific raw identity even after canonical code mapping; aliases cannot silently collapse observations. Before READY, approve a fixed level classification and mapping version; a later classification change requires a reconciliation migration, not a blind incremental replay.

## Source-to-target mapping

| Source | Target | Rule |
|---|---|---|
| `_source_file` exact V06 filename | `source_table` | Reject unprofiled filename; preserve full original path separately |
| V06.12 national columns | `source_column`, `measure`, `season` | Eight exact header mappings in existing transformer; four area and four production series |
| V06.13?24 source header inventory | `source_column`, `period_raw` | Unpivot only columns present in that individual file; no union-schema padding observations |
| V06.12 `nam` | `period_raw`, `year`, `is_provisional` | Exact year / `So b? YYYY` token; matrix `col_YYYY` / `so_b_YYYY` |
| V06.12 statistic label | `statistic_raw`, `statistic_kind` | Exact snapshot labels for quantity and previous-year=100 development index; unknown quarantined |
| Matrix geography label | `geography_name_raw` | Preserve unchanged, including damaged characters; no trim/fuzzy repair |
| Exact national alias | name/code/level | Vi?t Nam / 704 / national; UN M49 country identity |
| Historical reviewed entries | canonical geography, code/scheme/parent/dates/reference/status | Exact source-table + raw-label + interval containing the whole reference year; overlap rejected |
| Unapproved region/province | canonical geography fields | NULL, `mapping_status=needs_review`; candidate spelling exists only in separate review artifact |
| Original measure string | `value_raw`, `value_source` | Preserve raw; parse decimal only for nonmissing tokens |
| Unit inventory | source/canonical unit/factor, `value` | Verified national quantity ?1000; development index percent ?1; matrix proposals remain separate and conversion inactive |
| NULL / blank / `..` / number | missing flag/kind | Distinct `source_null`, `source_blank`, `source_marker`, `present`; missing never zero |
| Original eight lineage fields | same columns | Exact values retained (timestamp UTC); `_source_snapshot_id=0` preserved with warning |
| Iceberg scan identity | `_bronze_iceberg_snapshot_id` | Actual read snapshot; never confuse with ingestion snapshot reference; excluded from payload/change identity |
| Source row | `_source_payload` | Stable full raw row JSON, including NULLs and source metadata; record provenance for unpivoted cells |
| Current approval state | `gold_ready` | false for every E1 observation |

National quantity headers independently establish thousand hectares / thousand tonnes. Matrix unit formulas are corroborated by official NSO tables but exact applicability remains NEEDS_APPROVAL; see `nso_unit_season_mapping.md`. Country identity uses [UN M49](https://unstats.un.org/unsd/methodology/m49/).

## Output schema

47 contract fields; original Bronze lineage types are explicitly cast and validated by the existing contract loader/framework. Normalized nullable fields must remain nullable because unresolved values are held in quarantine. Missing is a permitted state and does not imply zero.

| Column | Spark/Iceberg type | Nullable |
|---|---|---|
| `source_table` | `string` | no |
| `source_column` | `string` | no |
| `geography_name_raw` | `string` | no |
| `geography_level` | `string` | no |
| `geography_name` | `string` | yes |
| `geography_code` | `string` | yes |
| `geography_code_scheme` | `string` | yes |
| `parent_geography` | `string` | yes |
| `geography_valid_from` | `date` | yes |
| `geography_valid_to` | `date` | yes |
| `geography_source_reference` | `string` | yes |
| `mapping_status` | `string` | no |
| `country_code` | `string` | no |
| `province_code` | `string` | yes |
| `commodity_name` | `string` | no |
| `period_raw` | `string` | no |
| `year` | `int` | no |
| `is_provisional` | `boolean` | yes |
| `season` | `string` | no |
| `measure` | `string` | no |
| `statistic_raw` | `string` | yes |
| `statistic_kind` | `string` | no |
| `source_unit` | `string` | yes |
| `unit` | `string` | yes |
| `conversion_factor` | `decimal(18,6)` | yes |
| `unit_status` | `string` | no |
| `unit_source_reference` | `string` | yes |
| `proposed_source_unit` | `string` | yes |
| `proposed_unit` | `string` | yes |
| `proposed_factor` | `int` | yes |
| `value_raw` | `string` | yes |
| `value_source` | `decimal(28,8)` | yes |
| `value` | `decimal(28,8)` | yes |
| `_missing_value` | `boolean` | no |
| `missing_kind` | `string` | no |
| `_column_present` | `boolean` | no |
| `gold_ready` | `boolean` | no |
| `_source_payload` | `string` | no |
| `_bronze_iceberg_snapshot_id` | `bigint` | yes |
| `_ingestion_run_id` | `string` | yes |
| `_ingestion_batch_id` | `string` | yes |
| `_ingestion_chunk_id` | `bigint` | yes |
| `_ingestion_timestamp` | `timestamp` | yes |
| `_source_id` | `string` | yes |
| `_source_file` | `string` | yes |
| `_source_checksum` | `string` | yes |
| `_source_snapshot_id` | `bigint` | yes |

The existing framework also retains three diagnostic fields in the physical valid-table schema: `_period_valid boolean`, `_contract_cast_error boolean`, and `dq_warnings array<struct<rule_id:string,error_message:string,failed_column:string>>`. Therefore the proposed physical Silver schema has 50 fields. Quarantine payload additionally includes `dq_errors` of the same array type. These fields are not separate business measures. No shared schema alteration is performed in E1.

## NULL/footer and reconciliation

All 13 files contain one record with every actual source field NULL. Preserve each physical source row in Bronze and a deterministic record-level audit; proposal excludes these records before unpivot. Twelve matrix records account for 360 actual NULL cells; the national empty record accounts for eight potential fabricated observations. Actual NULL cells in any nonempty row remain observations. Columns absent from a source header generate no observation, even when present in the union table.

Expected candidates = `762?30 + 71?8 = 23,428`; excluded candidates = `12?30 + 1?8 = 368`, with 13 record audits. Eligible = `750?30 + 70?8 = 23,060`. Matrix includes 21,554 numeric cells and 946 missing markers; national includes 560 observations (280 quantity / 280 index). Reconcile candidates = valid + quarantine + excluded, valid = deduplicated survivors + duplicate removals. File/period/season/measure/statistic/geography/missing/lineage breakdowns are recorded in `nso_e1_preview_evidence.json`. DQ errors overlap; their summed counts are not a quarantine count.

## DQ, deduplication and processing

See `nso_silver_dq_policy.md`. Shared `SilverQualityEngine` enforces SQL rules and lineage; shared `IcebergMergeEngine` selects latest ingestion timestamp, descending file name, then deterministic full payload hash, rejects NULL keys and schema drift, ignores unchanged/stale input. Tie handling is deterministic technical survivorship, not proof that a conflicting source value is correct. Provisional status is an attribute, not a second business key: a documented later release should revise the same year/series. Owner must approve whether final publication outranks provisional regardless of ingestion order; the current shared framework uses ingestion freshness and cannot infer official release priority. An explicit release identity/priority is required for any stronger semantic rule. Report duplicate counts and escalate conflicting official revisions for review.

Full mode pins one Bronze snapshot and reconciles all source observations. Incremental mode intersects an ingestion timestamp watermark and/or run ID, then uses the same transform/DQ/dedup/MERGE path. Empty input is NO_INPUT; identical retry is NOOP with unchanged snapshots. A future Bronze append changes the read snapshot but must not change unchanged source payload identities. Unit/geography/missing policy version changes require full re-evaluation of prior quarantine and an approved promotion/reconciliation plan; an ingestion watermark alone cannot process them. No deletion-based synchronization is authorized.

## Decisions before READY

Approve target/grain/50-field physical schema and raw identity preservation; approve exact matrix series unit applicability; review geography codes, whole-year validity and transition-year boundaries (especially H? T?y/H? N?i 2008); approve missing retention, all-source-NULL record exclusion with audit, provisional visibility and national index separation; approve DQ severities and DLQ partitions; resolve ingestion snapshot reference warning policy; obtain separate shared Silver execution approval. Until then `require_ready()` blocks individual and all-source writers before any input read or mutation. Explicit draft preview is read-only.
