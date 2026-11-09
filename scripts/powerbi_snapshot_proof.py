"""Read-only preservation proof for the 36 inventoried lakehouse tables.

Run in the existing runner; emits metadata/file paths, never table data.
"""
import hashlib
import json
import os


def capture():
    from pyiceberg.catalog import load_catalog
    from gold.publication import Publication
    from ingestion.storage.metadata_repository import MetadataRepository
    cat=load_catalog('powerbi_preservation',**{'type':'rest','uri':os.environ['ICEBERG_REST_URI'],
        's3.endpoint':'http://'+os.environ['MINIO_ENDPOINT'].removeprefix('http://'),
        's3.access-key-id':os.environ['AWS_ACCESS_KEY_ID'],'s3.secret-access-key':os.environ['AWS_SECRET_ACCESS_KEY'],'s3.region':'us-east-1'})
    tables={}
    for ns in ('bronze','silver_local','gold_local'):
        for name in cat.list_tables(ns):
            table=cat.load_table(name);snap=table.current_snapshot()
            files=sorted(task.file.file_path for task in table.scan().plan_files()) if snap else []
            tables['.'.join(name)]={'uuid':str(table.metadata.table_uuid),'snapshot_id':str(snap.snapshot_id) if snap else None,
                'metadata_location':table.metadata_location,'files':files}
    store=Publication()
    pointer=store.get('published.json')
    manifest=store.get(pointer['batch']+'.json') if pointer else None
    repo=MetadataRepository()
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dag_id,run_id,state FROM dag_run WHERE dag_id IN ('rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline','rice_lakehouse_gold_pipeline') ORDER BY dag_id,run_id")
            runs=cur.fetchall()
    return {'tables':tables,'publication_pointer':pointer,'active':store.get('active.json'),
        'manifest_sha256':hashlib.sha256(json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'dag_runs':runs}


if __name__=='__main__':print(json.dumps(capture(),sort_keys=True))
