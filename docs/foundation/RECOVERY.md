# Foundation recovery and source-retention protocol

Baseline: `feat(gold)@4bccf05ef1ba91f58d94df141cbbe6b97a28a6f4`, 26/10/2026.
This matrix was recorded before H04 writer changes. No failures are injected into
the nine real sources or published Gold. Existing historical outcomes stay intact.

| Transition | Intended mutation / partial state | Evidence and retry action | Manual intervention |
|---|---|---|---|
| Validation | No Bronze writes; persist rejected input diagnostic | Durable dead-letter ID + readback; same diagnostic identity deduplicates | Unavailable diagnostic store: FAILED, retain Raw |
| Source lock | Exclusive session advisory lock | Fail fast if unavailable; release in finally | Non-cooperating external writers cannot be fenced by Python |
| Run start | Persist IN_PROGRESS before extraction | DB failure stops ingestion | Reconcile interrupted IN_PROGRESS before resuming |
| Checkpoint | SUCCESS only after exact physical chunk readback | Never trust checkpoint alone for admission | Changed source/checkpoint identity must fail closed |
| Bronze chunk | Previously DELETE then APPEND: interrupt could remove committed rows | Replace with immutable chunk admission and a single append commit; exact multiset readback | Conflicting/partial existing chunk: preserve it, block; do not auto-delete |
| Catalog commit | Reply can be lost after append succeeds | Fresh catalog readback of exact chunk identity/content; no fallback INSERT | Unreadable catalog or non-matching readback: UNKNOWN, no automatic mutation |
| Metadata persistence | Previously best-effort, could return SUCCESS without DB proof | Errors propagate to FAILED; retain committed snapshots | Repair unavailable metadata then replay same immutable identity |
| Commit readback | Match schema, source identity, rows, NULLs, values and multiplicity | Mismatch never records successful checkpoint | Partial historic DELETE/APPEND damage requires explicit recovery |
| Airflow proof | Match persisted run/checksum plus physical catalog evidence | All-success gate; never infer success from table existence | Historic metadata without proof cannot be upgraded by assertion |
| Silver admission | Pinned Bronze UUID/snapshot + successful parent gate | Existing source lock/catalog readback and parent-run checks | Inconsistent parent or changed pin blocks processing |
| Silver MERGE | Atomic Iceberg MERGE, separate quarantine writes | Existing exact replay check prevents empty commits; unknown commit blocks orchestration retry | No multi-table atomicity claimed |
| Retry/resume | Re-extract deterministic chunks; verify every retained chunk | Checkpoints accelerate diagnosis, not proof; stable persisted original run identity required | Source content/layout changes require a fresh version, never overwrite old chunks |
| Source withdrawal | New FULL Raw snapshot may omit an old key | Current Silver is freshness-aware upsert/history retention, NOT full replacement | Physical withdrawal/delete requires owner retention decision and Gold dependency review |

No automatic deletion of committed source rows is approved. A changed Raw checksum
creates a new source version; it does not authorize removing old Bronze/Silver keys.
Silver MERGE retains absent keys. Therefore “FULL extraction” is not “complete
replacement snapshot”. Gold still reads its approved, immutable Silver pins.
Changing retention would require a separately approved migration, reconciliation,
republishing Gold, and explicit consumer maintenance; this task does none of those.

`_source_snapshot_id` historically denotes a PostgreSQL `source_snapshots` record,
not an Iceberg snapshot ID. Historical zero is an unknown sentinel, never physical
commit proof. Actual catalog UUID/snapshot/metadata and exact readback are the
proof. New writes must use an honest NULL when no source-snapshot record is known.

## Implemented recovery (27/10/2026)

New run records declare protocol `foundation-20261026.1` and a durable
`physical_write_started=false`. Validation is before skip/write admission. An
invalid artifact can be corrected without pretending an interrupted write exists.
Before the first write, immutable input context and the write-started marker must
be persisted. A failed metadata operation cannot authorize a chunk commit.

A subsequent logical run may reuse the original physical run/batch identity only
when Raw checksum, adapter/chunk layout and persisted source identity agree. It
re-extracts **every** chunk, verifies exact contents, and appends only genuinely
uncommitted chunks. The old FAILED run stays FAILED. New SUCCESS/snapshot metadata
points to verified physical lineage, and the Bronze→Silver proof understands that
alias. Never skip chunks because a checkpoint says SUCCESS.

Unresolved `UNKNOWN_COMMIT`, missing legacy context, incompatible source versions
or conflicting partial chunks stop automatic replay. Operator readback/recovery
must establish exact physical evidence; do not edit statuses or delete rows just
to unblock. The process-crash/kill matrix is not exhaustively physically tested.
Actual selected REST/PG fixtures and mock coverage are listed in [ACCEPTANCE](ACCEPTANCE.md).
