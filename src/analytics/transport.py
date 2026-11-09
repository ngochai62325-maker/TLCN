"""Bounded Trino HTTP protocol. Exact decimals, cancellation, finite fetch budgets."""
import json
import os
import time
from decimal import Decimal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

MAX_ROWS, MAX_BYTES, MAX_SECONDS = 5000, 5 * 1024 * 1024, 15


def execute(sql, opener=urlopen, clock=time.monotonic):
    base = "http://" + os.getenv("TRINO_HOST", "trino") + ":" + os.getenv("TRINO_PORT", "8080")
    started, consumed, rows, columns, next_uri = clock(), 0, [], [], None
    headers = {"X-Trino-User": "tlcn_eda", "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME",
               "X-Trino-Session": "query_max_memory=64MB,query_max_execution_time=15s,query_max_run_time=15s,query_max_planning_time=5s"}
    def checked(uri):
        a, b = urlparse(uri), urlparse(base)
        if (a.scheme, a.hostname, a.port) != (b.scheme, b.hostname, b.port):
            raise RuntimeError("Unexpected Trino polling destination")
        return uri
    request = Request(base + "/v1/statement", data=sql.encode(), headers=headers)
    try:
        while True:
            remaining = MAX_SECONDS - (clock()-started)
            if remaining <= 0:
                raise TimeoutError("EDA query time budget exceeded")
            with opener(request, timeout=min(5, remaining)) as response:
                raw = response.read(MAX_BYTES-consumed+1)
            consumed += len(raw)
            if consumed > MAX_BYTES:
                raise RuntimeError("EDA HTTP result byte budget exceeded")
            page = json.loads(raw, parse_float=Decimal)
            next_uri = checked(page["nextUri"]) if page.get("nextUri") else None
            if page.get("error"):
                raise RuntimeError(f"Trino {page.get('id')}: {page['error'].get('message')}")
            columns = page.get("columns", columns)
            rows.extend(page.get("data", []))
            if len(rows) > MAX_ROWS:
                raise RuntimeError("EDA output overflow; no partial result returned")
            if not next_uri:
                break
            request = Request(next_uri, headers={"X-Trino-User": "tlcn_eda"})
        if clock()-started > MAX_SECONDS:
            raise TimeoutError("EDA query time budget exceeded")
        return {"columns": columns, "rows": rows, "query_id": page.get("id"),
                "http_bytes": consumed, "query_seconds": clock()-started,
                "stats": {k: page.get("stats", {}).get(k) for k in ("elapsedTimeMillis", "processedBytes", "peakMemoryBytes", "spilledBytes")}}
    except Exception:
        if next_uri:
            try:
                with opener(Request(checked(next_uri), method="DELETE", headers={"X-Trino-User": "tlcn_eda"}), timeout=3):
                    pass
            except Exception:
                pass
        raise
