# Load Strategy

## FULL vs INCREMENTAL

- **FULL (Snapshot)**: The engine extracts the entire dataset from the source and overwrites or appends it as a new distinct batch in the Bronze layer. 
- **INCREMENTAL**: The engine maintains a watermark (e.g., `last_updated_date`) and only requests records newer than the watermark.

## Why Current Sources are FULL

The primary data source (FAOSTAT Trade Matrix) is provided as a massive bulk Zip file updated periodically. The source does not provide APIs for retrieving only row-level changes, nor does it provide a transaction log. Therefore, we must download the entire archive.

## Snapshot Detection and Idempotency

To prevent downloading and processing gigabytes of identical data repeatedly, we implement **Snapshot Detection**:

1. **Pre-flight Check**: The `ReadinessChecker` makes a `HEAD` request to the endpoint.
2. **Metadata Extraction**: It captures the `ETag` and `Last-Modified` headers.
3. **State Comparison**: The engine queries the local PostgreSQL Metadata database. If the latest successful ingestion for this `source_id` has the exact same `ETag` or `Last-Modified` value, the ingestion is **SKIPPED**.

This provides true idempotency. Calling `Engine.run()` multiple times on unchanged data results in instant, successful no-ops.

## Watermark Semantics (For Future Incremental Sources)

When APIs (like future World Bank API connections) support incremental fetching:
1. Engine reads `watermark_after` from the last successful `IngestionResult`.
2. Adapter injects this watermark into the API request (e.g., `?updated_since=2023-01-01`).
3. Upon completion, engine updates the state with the new high-water mark.

## CDC Assessment

**Change Data Capture (CDC)** is typically used when connecting directly to operational databases (e.g., using Debezium on MySQL binlogs). 
Because we do not have direct database access to external providers like the FAO, CDC is fundamentally impossible for this architecture. We rely on API pagination and Bulk Snapshots.
