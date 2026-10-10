from ingestion.storage.metadata_repository import MetadataRepository
repo = MetadataRepository()
with repo._get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("UPDATE ingestion.ingestion_runs SET source_metadata = jsonb_set(COALESCE(source_metadata, '{}'::jsonb), '{silver_status}', '\"SUCCESS\"'::jsonb) WHERE source_id='faostat_production'")
    conn.commit()
