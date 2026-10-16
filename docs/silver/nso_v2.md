# NSO V2 — isolated implementation, production DRAFT

Status: **NSO_V2_ISOLATED_VALIDATED_PENDING_OWNER**. Production remains DRAFT.

Phase 1 local REST E2E passed on 16 October: focused staging regression 1 PASS,
full Raw/PG/MinIO/REST/Spark/PyIceberg/Trino acceptance 1 PASS, including full and
incremental NOOP and unapproved revision rejection. Latest results and final
stopped runtime state: [Phase 1 evidence](evidence/nso_v2/phase1_rest_20261016/README.md).
Earlier interruption/recovery reports are preserved as historical records.

Owner approved Phase 1 for local integration. Phase 2 `local_nine` routing and
focused/native regressions are prepared, but its Airflow E2E stopped before any
trigger because the isolated webserver hit its 512 MiB memory limit. Production
stays DRAFT. [Phase 2 local profile and blocker](local_nine.md).

The eight-source release remains CLOSED_MANUAL_ONLY. No NSO shared writer,
Airflow trigger/unpause, Git commit or push was performed. The new isolated test
project was created under Phase 1 approval; shared containers were only gracefully
stopped under its conditional resource approval and remain stopped.

## Reconciliation before implementation

Owner-approved manual run `rice_lakehouse_silver_pipeline` /
`manual__2026-10-15T14:02:14+00:00` was marked FAILED with
`DagRun.set_state(DagRunState.FAILED)` after all eight writer tasks were SUCCESS,
Spark API had no child writer/queued request, and active tasks/source locks were
zero. Task states and all other DagRun states were preserved. No history deleted.

Evidence: [manual_run_reconciliation.json](evidence/nso_v2/manual_run_reconciliation.json).
The new record preserves the pre-change state, task timestamps, catalog
identities, PostgreSQL hashes and post-resolution verification. Historical
acceptance/closure reports were not changed.

Quarantine snapshots `5171447547413260487` and `7788475764751255879` have the
same four logical records, including IDs, payloads, errors and embedded lineage.
Only `pipeline_run_id` and `detected_at` changed, identifying this manual run.
The latter snapshot is the latest verified operational baseline. Logical hash:
`4e3f8cafcc1a2262c0abc5981e0fbaca620c3283497e85c8806b1976b7a1865a`.

The post-resolution safety gate PASS verified 28 other catalog identities,
1,391,467 Silver rows, 29 ingestion runs and unchanged ingestion metadata.
Both DAGs remained paused, schedule=None, production=0, canary=1, cron empty.

## Read context API

The frozen `BronzeReadContext(bronze_input, bronze_read_snapshot_id)` is metadata
for the read already pinned by the runner. The optional typed interface is
`preprocess_with_read_context(df, context)`. Only opt-in transformers receive
the reserved `_bronze_read_snapshot_id` binding, stamped from that same runner
variable. NSO validates the binding/type/context and writes a Spark LONG / Iceberg
long / Trino BIGINT `bronze_read_snapshot_id`, including missing observations.
It never substitutes `_source_snapshot_id` or queries latest/current in the
transformer. Non-opt-in transformers retain `preprocess(df)` unchanged.

Native regression appended a new Bronze snapshot after the old snapshot was
pinned: the target contained only the eight old observations, with the old read
snapshot on every row, including `..`. Invalid, missing and mismatched context
was rejected. Eight CLOSED source pipelines passed existing native output,
schema and replay checks. An initial test environment omitted `jobs/silver`
from PYTHONPATH; rerunning only the eight affected cases with the correct path
passed. No CLOSED transformer or contract was edited.

## Source, units and geography

`config/silver/nso_v2_metadata.json` is independent of the legacy inventory.
It pins 13 CSV SHA-256 values, sizes, exact decoded headers, layouts and titles.
Metadata version: `nso-v2-20261015.1`; unit mapping: `nso-units-20261015.1`;
geography candidates: `nso-candidates-20261015.1`.

| Measure | Source reference scale | Canonical unit | DECIMAL factor |
|---|---|---|---:|
| planted area | 1000 hectare | hectare | 1000 |
| paddy production | 1000 tonne | tonne | 1000 |
| yield | quintal/hectare | kg/hectare | 100 |
| development index | base-100 percent | percent | 1 |

`value_source` and `value` are DECIMAL(28,8); factor is DECIMAL(8,0). No float
conversion or derivation of growth/milled rice occurs. Matrix `unit_raw` stays
NULL. `unit_reference`, `unit_evidence`, `unit_status` and mapping version preserve
the approved reference basis. The 2018 PDF is a reference, not a claim about the
publication that exported the 2024 CSV. Missing/tampered metadata fails closed.

Source keys use SHA-256 of the exact decoded raw geography label with a `raw:`
prefix. Candidate names never participate in the key. G71/G37 names and levels
remain candidates requiring review. Codes and boundary versions stay NULL;
there is no historic province backfill, hierarchy inference or synthetic G37
observation. Country and region rows remain observations. V06.12 has a technical
national-scope key, geography_raw=NULL and explicit scope evidence.

## Grain and cleaning

Business key:
`source_table, source_geography_key, year, measure, season, statistic_kind`.

Seasons remain distinct: annual, winter_spring, summer_autumn,
summer_autumn_autumn_winter, main_season. Quantity and development_index remain
independent. Year is the reporting year; no observation/crop date is fabricated.
Only source `So b?` evidence sets provisional; other years are unconfirmed.

`..` retains its raw text and maps to NULL with
`missing_reason=source_marker_unspecified`. Source NULL/blank have separate
reasons; zero remains zero. Malformed/negative/overflowing or precision-losing
numbers go through the existing DQ/quarantine mechanism. Structural NUL/blank
source records are exclusions, with separate exclusion dead letters.

Exact duplicates are deterministic. Conflicting observations in an approved
version block before writes and expose conflict-group evidence. Unapproved
source checksum/schema revisions also block before writes; ingestion timestamp
is not publication precedence. The previous valid target is preserved.

## Diagnostic warnings

Source tables remain separate. Cross-table annual national comparisons use
zero tolerance, including the 2021 difference of 0.1. Arithmetic diagnostics use
a half-step 0.05 envelope for one-decimal native measurements; this is an explicit
diagnostic convention, not an assertion of the producer's rounding method.
Season-total tolerance is the sum of the four half-steps (0.20). Area/yield/
production comparisons use interval arithmetic and DECIMAL, not a fitted cutoff.

Evidence identifies member observation keys and values. Warnings never repair,
select a winning source, or quarantine arithmetic discrepancies. They never sum
mixed geography levels or annual plus seasonal observations, or average yields.

Full/incremental diagnostics read the same pinned complete Bronze version to
keep warnings stable when an incremental filter selects fewer source tables.
Only compact distinct diagnostic fields are collected; a 100,000-observation
capacity guard fails closed. This bounded NSO diagnostic path is not a general
large-source aggregation mechanism.

## Exact schema

The authoritative schema is [nso_vietnam_v2.yaml](../../contracts/silver/nso_vietnam_v2.yaml),
which remains **DRAFT**. Its 34 columns are:

| Type | Columns |
|---|---|
| BIGINT, required | bronze_read_snapshot_id |
| INTEGER, required | year |
| DECIMAL(28,8), nullable | value_source, value |
| DECIMAL(8,0), required | conversion_factor |
| STRING, nullable | geography_raw, geography_name_candidate, geography_code, boundary_version, value_raw, unit_raw, missing_reason, review_reason |
| STRING, required | source_table, source_column, source_geography_key, geography_level, geography_status, geography_mapping_version, geography_evidence, period_raw, measure, season, statistic_kind, release_status, unit_source_reference, unit, unit_status, unit_reference, unit_evidence, unit_mapping_version, metadata_version, quality_status, diagnostic_evidence |

The framework adds the original eight Bronze lineage columns, `_dq_warnings`
and `_silver_processed_at`. No source row ordinal is fabricated. Partition: year.

## Isolated validation and current limitation

Tests:

- `tests/integration/test_nso_v2_read_context.py`: native pinned/concurrent read proof.
- `tests/integration/test_nso_v2_isolated.py`: all 13 CSVs, independent Raw oracle,
  full/incremental replay, completeness, DQ, conflicts and DRAFT write gate.
- `tests/unit/silver/test_nso_v2_semantics.py`: diagnostic boundaries, candidate-key
  independence and metadata evidence guards.
- `tests/integration/test_nso_v2_rest_e2e.py`: opt-in real adapter, PostgreSQL,
  MinIO, REST Catalog, Spark, PyIceberg and Trino using existing isolated Compose.

The Raw oracle expectations are 833 records, 13 footer exclusions, 820 source
records, 23,060 observations, 22,114 numeric, 946 missing and five zeros;
V06.12 has 560 observations. These are test expectations, not transformation
counts hardcoded to manufacture PASS.

The test infrastructure is Compose project `tlcn-nso-v2-20261015`, based on
`compose.silver-test.yml`: internal network, no host ports, no shared data volumes,
readonly workspace/Raw mounts. Native tests use separate network=none containers.
Production has not been recreated or activated.

During native/REST testing Docker stopped responding: both container inspection
and server `/version` returned HTTP 500. Cause is unconfirmed; OOM is not proven.
The final native suite and REST E2E outcomes are **UNKNOWN / NOT ACCEPTED**.
Their writers must not be rerun blindly. After Docker is available, inspect
container exits/OOM flags, `/tmp/nso-tests.log`, `/tmp/nso-rest-tests.log`, the
isolated catalog and PostgreSQL before deciding whether to resume or use a new
isolated project. Reverify shared identities against the new operational baseline.
Do not restart shared services, reset test/shared data, or automatically retry.

No claim of full NSO E2E PASS, NOOP PASS or Trino PASS is made. Remaining work:
complete native and REST acceptance, review actual counts/warnings/lineage and
revision behavior, save sanitized evidence, then ask owner before READY/shared use.
