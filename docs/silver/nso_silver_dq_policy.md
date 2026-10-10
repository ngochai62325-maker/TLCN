# NSO Silver DQ and reconciliation policy proposal ? E1

Status **NEEDS_APPROVAL**. NSO-specific SQL rules run through the existing common DQ engine; no business rule of another source is changed. ERROR means quarantine; WARNING/LOG is retained as a warning. Operational schema/source drift raises a failure before writes.

| Rule | Condition | Severity/action |
|---|---|---|
| NSO_SOURCE_COLUMN | Actual per-file source header column only | ERROR / QUARANTINE |
| NSO_DATE | Exact period token; valid reference year 1960 through current year+1 | ERROR / QUARANTINE |
| NSO_GEOGRAPHY | Verified country identity or exact dated approved historical mapping | ERROR / QUARANTINE |
| NSO_UNIT | Explicit canonical unit/factor/statistic kind | ERROR / QUARANTINE |
| NSO_VALUE | Missing permitted; otherwise decimal parse succeeds, source value ?0, and verified conversion fits normalized decimal type | ERROR / QUARANTINE |
| CONTRACT_CAST | Every non-NULL declared value fits its type | ERROR / QUARANTINE |
| CONTRACT_REQUIRED | Required identity and control fields present | ERROR / QUARANTINE |
| LINEAGE_REQUIRED | All eight original ingestion/source metadata fields present | ERROR / QUARANTINE |
| LINEAGE_REFERENCE | Ingestion `_source_snapshot_id` >0 | WARNING / LOG; existing 0 retained |
| NSO_EMPTY_SOURCE_RECORD | Every actual source field NULL | Record exclusion audit; 13 source records, 368 candidate slots |

Missing marker `..`, genuine NULL and blank are distinguished; none is filled with zero. Actual zero is retained as numeric 0. Missing is not a standalone DQ error. If unit/geography is unresolved, the missing observation is still quarantined for that unresolved rule. Unapproved matrix normalized value/unit/factor are NULL even when source numeric value is present; `value_source` and `value_raw` retain the evidence. No extreme-value cap, region-to-province distribution, residual balancing or index-to-quantity conversion is introduced. Preliminary years stay preliminary. `gold_ready=false` throughout E1.

The exclusion predicate examines the fields of that actual source file, excluding union padding and metadata. A source record with a label/year/statistic but NULL measures is not a footer and remains observations. A column absent from a source header is not an actual NULL cell. Full snapshot contains 360 actual matrix NULL cells in 12 all-source-NULL rows, plus one empty national row: no eight fake national observations. Keep record-level original row JSON and metadata, exclusion reason and stable ID. Exclusions do not mutate or delete Bronze.

Common quarantine schema is reused: `quarantine_record_id string`, `source_dataset string`, `pipeline_run_id string`, `detected_at timestamp`, `business_key_hash string`, `errors array<struct<rule_id:string,error_message:string,failed_column:string>>`, `payload string`. Proposed NSO DLQ partitions are `nso_vietnam` for rejected observations and `nso_vietnam_exclusions` for audited physical source records. Payload contains raw source and normalization/mapping status; observation keys prevent one raw row's cells from collapsing together. IDs are deterministic; exact retries and unchanged error sets create no new snapshots. Actual read Iceberg snapshot changes are excluded from identity.

Reconcile 833 physical rows = 820 nonempty + 13 excluded records. Candidate observations 23,428 = 23,060 eligible + 368 excluded. Eligible = valid + quarantine (before dedup); valid = output survivors + duplicates removed. Matrix 22,500 = 21,554 numeric + 946 markers; national 560 = 280 quantity + 280 index. Verify by file, year/provisional, measure, season, geography level, statistic kind, missing kind and all eight lineage fields. Reason counts can overlap; never sum them as record counts. Preview excludes no nonempty observation for business policy. Numeric area/production/yield across overlapping totals are not aggregated.

Critical unresolved geography and units block contract READY and shared writer. Recommended review: accept the conservative quarantine policy, approve footer exclusion with retained record audit, retain missing/index/provisional observations, decide whether source snapshot reference=0 may remain a warning in future shared Silver, and approve eventual quarantine promotion after mapping changes. Threshold/range rules or economic reconciliation beyond structural counts need separate evidence and approval.
