# NSO V2 — business semantics và local acceptance

Production contract vẫn **DRAFT**. Profile `local_nine` dùng đúng approved READY
overlay trong bộ nhớ; không đổi YAML, keys hay business policies.

Initial local acceptance và full/incremental NOOP đã được kiểm chứng với23.060
observations,946 missing,5 số0 thật, pinned Bronze snapshot và13 structural
exclusions. Xem [catalog](data_catalog.md), [validation](validation.md) và
[evidence index](evidence/index.md) cho UUID/snapshot/run IDs và recovery history.
Native/REST interruption cũ được giữ nguyên trong evidence; không phải trạng thái
hiện tại và không được dùng để suy đoán một commit chưa được đối soát.

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


Current local counts and replay proof are indexed in [evidence/index.md](evidence/index.md).
