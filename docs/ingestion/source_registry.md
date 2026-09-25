# Source Registry

The Source Registry is a YAML-driven configuration system that defines all external data sources to be ingested.

## How to Add a New Source

1. Open `src/ingestion/config/sources.yaml`.
2. Add a new block under the `sources` list following the YAML schema.
3. The engine dynamically picks it up on the next run. No code changes are required unless a completely new extraction paradigm (Adapter) is needed.

## YAML Schema Explanation

```yaml
sources:
  - source_id: unique_identifier        # String ID used in logging/DB
    provider: source_organization       # E.g., "FAO", "GSO"
    dataset: dataset_name               # E.g., "Trade Matrix"
    source_type: HTTP_BULK_ZIP          # Must match enums.SourceType
    load_strategy: FULL                 # FULL or INCREMENTAL
    format: CSV                         # Expected output format
    endpoint: "https://url.to/data"     # URL to hit
    chunk_size: 500000                  # (Optional) For splitting large files
    encoding: "utf-8"                   # Encoding of the source data
    readiness:
      require_http_200: true
      require_content_length: true
    retry:
      max_attempts: 3
      exponential_backoff: true
    adapter_class: FaostatZipAdapter    # The Python class name in adapters module
```

## Current Sources

1. **`faostat_trade_matrix`**
   - **Type**: HTTP_BULK_ZIP
   - **Strategy**: FULL
   - **Adapter**: `FaostatZipAdapter`
   - **Description**: Massive zipped CSV containing global agricultural trade data. Requires chunking.

2. **`gso_rice_production`** (Planned)
   - **Type**: API
   - **Strategy**: FULL
   - **Adapter**: `GsoApiAdapter`

## Load Strategy in Registry
Defines how data is extracted. See `load_strategy.md` for detailed semantics. All current bulk files use `FULL`.
