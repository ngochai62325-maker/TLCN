"""Local consumer read window: refuse incomplete batches, hold the writer lock.

Direct SQL clients can bypass this application protocol; the demo uses this
entrypoint and only opens raw Trino access after the Gold DAG succeeds.
"""
from contextlib import contextmanager
import os

from gold import MODEL_VERSION, OWNER
from gold.publication import Publication, verify_manifest, PREFIX
from gold.views import VIEW_NAMES, definitions, expected_schema, trino_result
from gold.spec import load_spec


@contextmanager
def read_window():
    from ingestion.storage.metadata_repository import MetadataRepository
    repo = MetadataRepository()
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock_shared(%s)", (repo._derive_lock_key("tlcn-local:gold_local"),))
            if not cur.fetchone()[0]:
                raise RuntimeError("Gold writer active; read window closed")
        try:
            store = Publication()
            pointer, active = store.get("published.json"), store.get("active.json")
            if not pointer or not active or active.get("status") != "PUBLISHED" or active["batch"] != pointer["batch"]:
                raise RuntimeError("Gold batch not published; read window closed")
            manifest = store.get(pointer["batch"] + ".json")
            if (not manifest or manifest.get("batch") != pointer["batch"] or pointer.get("owner") != OWNER
                    or pointer.get("manifest") != PREFIX + pointer["batch"] + ".json"):
                raise RuntimeError("Invalid Gold publication")
            verify_manifest(manifest)
            trade = manifest["semantic_queries"][VIEW_NAMES[3]]["validation"]
            progress = store.get(trade["progress_key"])
            if (not progress or progress.get("status") != "ACCEPT" or progress.get("scope") != trade["scope"]
                    or progress.get("identity", {}).get("context") != manifest["context"]
                    or progress.get("domain") != trade["domain"]
                    or [p["seal"] for p in progress.get("accepted", [])] != trade["checkpoint_seals"]):
                raise RuntimeError("Unverified Trade partition checkpoints")
            # Metadata readback via catalog (not upstream lineage fields).
            from pyiceberg.catalog import load_catalog
            catalog = load_catalog("gold_consumer", **{"type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse",
                "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"),
                "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1"})
            for name, output in manifest["outputs"].items():
                table = catalog.load_table("gold_local." + name)
                snapshot = table.current_snapshot()
                if str(table.metadata.table_uuid) != output["uuid"] or not snapshot or str(snapshot.snapshot_id) != output["snapshot_id"]:
                    raise RuntimeError(f"Unpublished Gold state at {name}")
                from gold.preflight import validate_gold_schema
                validate_gold_schema(table, load_spec()[name])
            yield manifest
        finally:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock_shared(%s)", (repo._derive_lock_key("tlcn-local:gold_local"),))


def query(view, year=None):
    if view not in VIEW_NAMES:
        raise ValueError("Choose one canonical Gold semantic query")
    with read_window() as manifest:
        sql = definitions(manifest["outputs"])[view]
        years = manifest["semantic_queries"][view]["validation"]["partition_years"]
        if year is not None and (type(year) is not int or year not in years):
            raise ValueError("Year outside validated published coverage")
        rows, schema, sampled = [], None, []
        sampled_partitions = []
        for selected in ([year] if year is not None else reversed(years)):
            predicates = [f"year_key={int(selected)}"]
            if view == VIEW_NAMES[3]:
                from gold.trade_validation import predicate
                parts = [p for p in manifest["semantic_queries"][view]["validation"]["domain"] if p["year"] == selected]
                predicates = [predicate(selected, p["bucket"], "q") for p in parts]
            for position, condition in enumerate(predicates):
                result = trino_result(f"SELECT * FROM ({sql}) q WHERE {condition} LIMIT {20-len(rows)}")
                if result["columns"] != expected_schema(view):
                    raise RuntimeError("Semantic output schema changed")
                rows.extend(result["rows"])
                schema = result["columns"]
                if view == VIEW_NAMES[3]:
                    sampled_partitions.append({"year": selected, "bucket": parts[position]["bucket"]})
                if len(rows) >= 20:
                    break
            sampled.append(selected)
            if len(rows) >= 20:
                break
        return {"batch": manifest["batch"], "view": view, "semantic_version": manifest["semantic_queries"][view]["version"], "sample_years": sampled,
                "sample_partitions": sampled_partitions, "columns": schema, "rows": rows}
