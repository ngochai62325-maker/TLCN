import os
import sys

sys.path.insert(0, "src")
os.environ["INGESTION_USE_LOCAL_FALLBACK"] = "1"

from ingestion.core.enums import IngestionStatus
from ingestion.core.ingestion_engine import IngestionEngine

print("Initializing IngestionEngine...")
engine = IngestionEngine.create_default()

# 1. Clean up existing table and metadata for a pristine Run 1
engine.bronze_writer.execute_query("DROP TABLE IF EXISTS iceberg.bronze.faostat_production")
with engine.metadata_repo._get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ingestion.source_snapshots WHERE source_id = 'faostat_production'")
        cur.execute("DELETE FROM ingestion.ingestion_runs WHERE source_id = 'faostat_production'")
    conn.commit()

print("Starting Run 1 (Fresh Ingestion)...")
res1 = engine.run("faostat_production")
print(f"Run 1 Status: {res1.status.value}")
print(f"Run 1 Records Extracted: {res1.records_extracted}")
print(f"Run 1 Checksum: {res1.checksum}")
print(f"Run 1 Artifact URI: {res1.artifact_uri}")
print(f"Run 1 Manifest URI: {res1.manifest_uri}")

cnt1 = engine.bronze_writer.get_row_count("faostat_production")
print(f"Iceberg row count after Run 1: {cnt1}")

print("\nStarting Run 2 (Idempotent Rerun)...")
res2 = engine.run("faostat_production")
print(f"Run 2 Status: {res2.status.value}")
print(f"Run 2 Skipped Reason: {(res2.source_metadata or {}).get('skipped_reason')}")
print(f"Run 2 Records Extracted: {res2.records_extracted}")

cnt2 = engine.bronze_writer.get_row_count("faostat_production")
print(f"Iceberg row count after Run 2: {cnt2}")

# Verify assertions
assert res1.status == IngestionStatus.SUCCESS, f"Run 1 failed: {res1.error_message}"
assert res1.records_extracted == 22738, f"Expected 22738 records, got {res1.records_extracted}"
assert cnt1 == 22738, f"Expected 22738 rows in table, got {cnt1}"

assert res2.status == IngestionStatus.SKIPPED, f"Run 2 was not skipped: {res2.status}"
assert (res2.source_metadata or {}).get("skipped_reason") == "unchanged_snapshot"
assert cnt2 == 22738, f"Row count changed after Run 2: {cnt2} != 22738"

print("\nALL E2E IDEMPOTENCY AND BRONZE INGESTION ASSERTIONS PASSED!")
