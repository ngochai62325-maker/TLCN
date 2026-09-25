# Retry Policy

Handling unreliable external networks is a primary responsibility of the Ingestion layer. 

## Error Classification

Errors are strictly classified to determine retry behavior:

- **TRANSIENT**: Network timeouts, connection resets, HTTP 502/503/504. **(Retryable)**
- **PERMANENT**: HTTP 401 Unauthorized, HTTP 404 Not Found, DNS resolution failures for unknown hosts. **(Not Retryable)**
- **DATA_QUALITY/SCHEMA**: Missing required columns, completely wrong file formats (e.g., HTML instead of CSV). **(Not Retryable at ingestion time)**

## Exponential Backoff Algorithm

For Transient errors, the engine implements exponential backoff:
`Delay = initial_delay * (2 ^ attempt_number)`

## Jitter

To prevent the "thundering herd" problem (especially if we hit rate limits), random jitter is applied to the delay:
`Final Delay = Delay + Random(-jitter, +jitter)`

## Retry-After Header

If the external server responds with a `429 Too Many Requests` or `503 Service Unavailable` and includes a `Retry-After` header, the engine will respect this header, ignoring the standard backoff calculation.

## Application Retry vs Airflow Retry Boundary

- **Application (Engine) Retry**: Handled natively in Python using the `RetryConfig`. This is for immediate, short-lived network glitches (e.g., retrying a chunk download 3 times over 30 seconds).
- **Airflow Task Retry**: Configured on the DAG (`retries=1`). If the Application Retry exhausts all attempts, it throws an Exception. Airflow will catch this and can retry the *entire task* hours later. 

This dual-layer approach provides resilience against both millisecond blips and multi-hour API outages.

## Configuration Options

Configured in `sources.yaml` per source:
```yaml
retry:
  max_attempts: 5
  initial_delay_seconds: 5
  max_delay_seconds: 60
  exponential_backoff: true
  jitter: true
  respect_retry_after: true
```
