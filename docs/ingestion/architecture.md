# Ingestion Framework Architecture

## Overview
The Ingestion Engine for the Vietnam Rice Market Data Lakehouse acts as the foundational data acquisition layer. Its primary goal is to reliably, idempotently, and efficiently ingest raw data from diverse sources (APIs, HTTP endpoints, local files, zipped bulk files) into the Bronze layer (MinIO S3).

## Component Diagram

```mermaid
flowchart TD
    Airflow[Airflow DAG] -->|Triggers| Engine[Ingestion Engine]
    
    subgraph Data Ingestion Framework
        Engine --> Registry[Source Registry]
        Engine --> Checker[Readiness Checker]
        Engine --> Checkpoint[Checkpoint Manager]
        Engine --> MetaRepo[Metadata Repository]
        
        Engine --> Factory[Adapter Factory]
        Factory --> BaseAdapter[Base Adapter Interface]
        BaseAdapter <|-- APIAdapter
        BaseAdapter <|-- HTTPAdapter
        BaseAdapter <|-- ZipAdapter
    end
    
    subgraph External Sources
        API[REST APIs]
        HTTP[Static Files]
        ZIP[Bulk Zips - FAOSTAT]
    end
    
    subgraph Bronze Layer
        MinIO[(MinIO S3 - Bronze)]
        Postgres[(PostgreSQL - Meta)]
    end
    
    APIAdapter -.->|Pulls| API
    HTTPAdapter -.->|Pulls| HTTP
    ZipAdapter -.->|Pulls| ZIP
    
    BaseAdapter -->|Streams / Chunks| MinIO
    Engine -->|Logs status| Postgres
```

## Responsibility Boundaries

- **Person 1 (Data Engineer - Ingestion):** Responsible for building the engine, extracting data from external systems, handling network errors/retries, parsing complex formats (like zipped bulk CSVs), and saving the **raw**, unaltered data into the Bronze layer. Produces the `IngestionResult` contract.
- **Person 2 (Data Engineer - Transformation):** Consumes the `IngestionResult` (or polls MinIO), reads the data from Bronze layer using Spark, enforces data quality rules, applies schemas, and writes to the Silver layer (Apache Iceberg).
- **Person 3 (Data Analyst):** Consumes structured Iceberg tables from Silver/Gold via Trino for reporting and BI.

## Data Flow
1. **Trigger**: Airflow initiates the DAG.
2. **Readiness**: `ReadinessChecker` verifies if the external source is ready (HTTP 200, Content-Length, etc.).
3. **Idempotency**: Engine calculates upstream checksums (ETags/Last-Modified). If checksum matches the previous successful run, it skips extraction (`SKIPPED`).
4. **Extraction**: The specific adapter fetches data. For large sources (FAOSTAT), it chunks data into manageable pieces.
5. **Storage**: Data is written to MinIO (`s3://bronze/raw/{source_id}/{batch_id}/data.ext`).
6. **Manifest**: A manifest file is written detailing the batch.
7. **Metadata**: Run status and checkpoints are updated in PostgreSQL.

## Technology Stack
- **Python 3.10+**: Core engine logic (no PySpark in ingestion).
- **Requests / urllib3**: For HTTP and API interactions.
- **Minio SDK / boto3**: For S3-compatible storage interactions.
- **SQLAlchemy / psycopg2**: For PostgreSQL metadata and checkpoint management.
- **Airflow**: Orchestration.

## Design Patterns Used
1. **Factory Pattern**: Used in `AdapterFactory` to instantiate the correct adapter based on the YAML configuration.
2. **Strategy Pattern**: `BaseSourceAdapter` defines standard interfaces (`extract_full`, `extract_incremental`) while concrete adapters implement the specifics.
3. **Data Contracts**: Explicit `IngestionResult` and dataclass models strictly define boundaries between components.
4. **Idempotency Keys**: Run IDs, Batch IDs, and source checksums ensure that re-running pipelines does not produce duplicate side effects.

## Design Patterns NOT Used (and Why)
- **Change Data Capture (CDC)**: CDC is not used here because the external sources (FAOSTAT, World Bank, General Statistics Office) provide static files, bulk zips, or paginated APIs rather than database transaction logs. We must rely on FULL loads or API-based incremental watermarks.
- **Distributed Processing (PySpark)**: Not used in ingestion to keep the ingestion layer lightweight, network-resilient, and focused purely on IO operations, deferring heavy computation to the Silver layer.
