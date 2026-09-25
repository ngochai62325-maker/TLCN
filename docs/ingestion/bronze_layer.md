# Bronze Layer Design

The Bronze Layer is the raw landing zone for the Lakehouse. 

## Immutable Raw Landing
All data written to Bronze is **immutable**. It represents the exact state of the source system at the time of extraction. We do not UPDATE or DELETE records here; we only APPEND new batches.

## Directory Structure
Data is organized in MinIO (S3) logically:
`s3://{bronze_bucket}/raw/{source_id}/{batch_id}/`

Example:
```
s3://rice-lakehouse-bronze/
└── raw/
    └── faostat_trade_matrix/
        ├── 20231015_060000/
        │   ├── _manifest.json
        │   ├── chunk_0000.csv
        │   ├── chunk_0001.csv
        │   └── chunk_0002.csv
        └── 20231115_060000/
            ├── _manifest.json
            └── ...
```

## Manifest Alongside Artifact
Every batch contains a `_manifest.json` written upon successful completion. Silver layer pipelines should look for this file as a marker that the batch is fully written and safe to read (preventing partial reads of in-progress batches).

## Quarantine Concept
If data is extracted but fails very basic sanity checks (e.g., unexpected encoding that breaks parsing), it may be written to a quarantine prefix:
`s3://rice-lakehouse-bronze/quarantine/{source_id}/{batch_id}/`
This allows developers to inspect the broken files without disrupting downstream automated pipelines.

## No Business Transformation in Bronze
- **NO** data type casting (e.g., String to Integer).
- **NO** renaming of columns (unless completely necessary due to illegal characters in source headers).
- **NO** joining datasets.
- **NO** dropping nulls.
All business logic is deferred to the Silver Layer (Person 2).

## Metadata Columns
The Ingestion Engine may optionally append technical metadata columns to the raw files during extraction (if formats like Parquet are used, or as extra CSV columns):
- `_ingest_run_id`
- `_ingest_timestamp`
- `_source_file_name`
This assists Person 2 in traceability.
