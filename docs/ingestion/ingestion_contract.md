# Ingestion Contract (Person 1 <-> Person 2)

This document defines the contract between the Ingestion Layer (Person 1) and the Transformation Layer (Person 2).

## The Core Contract: `IngestionResult`

Every ingestion run produces an `IngestionResult`. Person 2's Airflow tasks (or Spark jobs) should read this result (passed via XCom or read from the metadata DB) to know what to process.

```python
@dataclass
class IngestionResult:
    source_id: str
    run_id: str
    batch_id: str
    load_type: LoadStrategy # FULL or INCREMENTAL
    status: IngestionStatus
    artifact_uri: Optional[str] # e.g. s3://bronze/raw/faostat_trade/20231010_120000/
    manifest_uri: Optional[str] # e.g. s3://bronze/raw/faostat_trade/20231010_120000/_manifest.json
    data_format: ArtifactFormat # CSV, PARQUET, etc.
    records_extracted: int
    ...
```

## Consuming the Artifact (Person 2's Responsibility)

When Person 2's task starts, it should:
1. Verify the `status` of the result is `SUCCESS`.
2. Use the `artifact_uri` as the base path.
3. (Optional but recommended) Read the `_manifest.json` at `manifest_uri` to understand exactly which files were written and their row counts/checksums.
4. Point PySpark to the `artifact_uri`:
   ```python
   df = spark.read.format(result.data_format.value.lower()).load(result.artifact_uri)
   ```

## Manifest Structure

A JSON manifest is written alongside the raw data in Bronze.

```json
{
  "batch_id": "20231010_120000",
  "source_id": "faostat_trade",
  "generated_at": "2023-10-10T12:05:00Z",
  "files": [
    {
      "file_name": "chunk_0.csv",
      "record_count": 500000,
      "checksum": "abc123def456"
    }
  ],
  "total_records": 500000
}
```

## Batch ID Semantics

- **Batch ID** is a timestamp-based or UUID identifier for a specific successful extraction of data.
- It is deterministic within a single run.
- Data for a batch is isolated in its own directory: `s3://bronze/raw/{source_id}/{batch_id}/`.

## Status Codes

- `SUCCESS`: Data was successfully extracted, written to Bronze, and is ready for Silver.
- `FAILED`: Hard failure. Do not process.
- `NOT_READY`: Source was unavailable. DAG skips.
- `NO_NEW_DATA`: API indicated no new records.
- `SKIPPED`: Idempotency check matched; exact data was already ingested previously.
- `IN_PROGRESS`: In-flight.
- `QUARANTINED`: Data retrieved but failed initial strict formatting (rare for raw ingestion).

## Error Handling Expectations

- Person 1 ensures network retries, transient HTTP errors, and zip unzipping are handled.
- If data is fundamentally structurally broken (e.g., HTML instead of CSV), Person 1 will log `FAILED`.
- Person 2 is responsible for schema validation, data type casting, and semantic data quality checks (e.g., negative prices).

## Idempotency Guarantees

If Person 2 re-runs their Spark job for a specific `batch_id`, they will read the exact same immutable raw data.
If Person 1 re-runs the ingestion DAG for a day, and the source hasn't changed, Person 1 will return `SKIPPED`, and Person 2 can optionally use the previous day's `batch_id` or skip processing.
