"""Every request enters the existing published Gold read window. No result cache."""
import contextlib
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from analytics import VERSION
from analytics.queries import validate, compile_query
from analytics.transport import execute, MAX_BYTES


@contextlib.contextmanager
def admission(ticket):
    if (type(ticket) is not dict or set(ticket) != {"windows_available_bytes", "measured_at"}
            or type(ticket["windows_available_bytes"]) is not int or ticket["windows_available_bytes"] < 2*1024**3
            or type(ticket["measured_at"]) not in (float, int) or not 0 <= time.time()-ticket["measured_at"] <= 30):
        raise RuntimeError("Missing/stale/insufficient Windows resource admission")
    from gold.admission import runtime_resources
    from ingestion.storage.metadata_repository import MetadataRepository
    from silver.local_profile import BRONZE_SOURCES
    runtime_resources()  # Existing Linux gate: WSL available >=3 GiB.
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            if b"org.apache.spark" in path.read_bytes():
                raise RuntimeError("Spark workload active in runner")
        except (PermissionError, FileNotFoundError):
            continue
    repo = MetadataRepository()
    with contextlib.closing(repo._get_connection()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dag_id FROM dag_run WHERE dag_id IN ('rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline','rice_lakehouse_gold_pipeline') AND state IN ('running','queued') LIMIT 1")
            if cur.fetchone():
                raise RuntimeError("Active pipeline DagRun; EDA admission closed")
            held = []
            try:
                serial = repo._derive_lock_key("tlcn-local:analytics_readonly")
                cur.execute("SELECT pg_try_advisory_lock(%s)", (serial,))
                if not cur.fetchone()[0]:
                    raise RuntimeError("Another analytical query active")
                held.append((serial, False))
                for source in sorted(BRONZE_SOURCES):
                    lock = repo._derive_lock_key(source)
                    cur.execute("SELECT pg_try_advisory_lock_shared(%s)", (lock,))
                    if not cur.fetchone()[0]:
                        raise RuntimeError("Bronze/Silver source writer active")
                    held.append((lock, True))
                yield
            finally:
                for lock, shared in reversed(held):
                    cur.execute("SELECT " + ("pg_advisory_unlock_shared" if shared else "pg_advisory_unlock") + "(%s)", (lock,))


def run_query(name, params, ticket, expected_batch=None):
    params = validate(name, params)
    from gold.consumer import read_window
    from gold.admission import code_fingerprint
    from gold.keys import fingerprint
    with admission(ticket), read_window() as manifest:
        if expected_batch is not None and expected_batch != manifest["batch"]:
            raise RuntimeError("Publication changed; restart notebook instead of mixing batches")
        if code_fingerprint() != manifest["context"]["code_fingerprint"]:
            raise RuntimeError("Published Gold code fingerprint differs")
        sql, definition_hash = compile_query(name, params, manifest)
        if sql is None:
            result = {"columns": [{"name": k} for k in ("table", "rows", "uuid", "snapshot_id")],
                      "rows": [[n, v["rows"], v["uuid"], v["snapshot_id"]] for n,v in sorted(manifest["outputs"].items())],
                      "query_id": None, "http_bytes": 0, "query_seconds": 0}
            # Descriptive metadata from the verified manifest, never a second
            # publication pointer or an invented publication timestamp.
            result["publication_info"] = {
                "status": manifest.get("publication_status"), "model": manifest.get("model"),
                "source_count": len(manifest["context"].get("inputs", {})),
                "dq": manifest.get("dq"),
            }
        else:
            result = execute(sql)
        result.update(query=name, parameters=params, version=VERSION, definition_sha256=definition_hash,
            compiled_sql_sha256=hashlib.sha256((sql or "").encode()).hexdigest(),
            batch=manifest["batch"], manifest_sha256=fingerprint(manifest), gold_code_fingerprint=manifest["context"]["code_fingerprint"],
            snapshots={n: {k:v[k] for k in ("uuid", "snapshot_id")} for n,v in manifest["outputs"].items()},
            measured_at=datetime.now(timezone.utc).isoformat(), windows_available_bytes=ticket["windows_available_bytes"], cache="DISABLED")
        result["row_count"] = len(result["rows"])
        result["response_bytes"] = 0
        for _ in range(3):
            result["response_bytes"] = len(json.dumps(result, ensure_ascii=False, default=str).encode()) + 1
        if result["response_bytes"] > MAX_BYTES:
            raise RuntimeError("Serialized analytical response exceeds 5 MiB")
        return result
