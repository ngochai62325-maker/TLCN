"""Analytics Client, Trino HTTP Transport, Access Gate & Notebook EDA Helpers.

Consolidates what used to live in `client.py`, `access.py`, `transport.py`,
`methods.py`, and `notebook.py` into a single easy-to-follow module.
"""
import contextlib
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from analytics import VERSION
from analytics.queries import validate, compile_query

MAX_ROWS, MAX_BYTES, MAX_SECONDS = 5000, 5 * 1024 * 1024, 15


def windows_available_bytes():
    if os.name != "nt":
        raise RuntimeError("This execution profile requires the Windows host resource gate")
    import ctypes
    class Memory(ctypes.Structure):
        _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [(n,ctypes.c_ulonglong) for n in
            ("total", "available", "page_total", "page_available", "virtual_total", "virtual_available", "extended")]
    info = Memory()
    info.length = ctypes.sizeof(info)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(info)):
        raise RuntimeError("Windows memory measurement failed")
    return int(info.available)


class EDAClient:
    def __init__(self):
        self.batch = None
        self.evidence = []
        self._worker = None

    def _get_worker(self):
        if self._worker is None or self._worker.poll() is not None:
            command = ["docker", "exec", "-i", "tlcn-local-runner-1", "python3", "/workspace/jobs/analytics/query_eda.py", "--worker"]
            self._worker = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, encoding="utf-8", bufsize=1,
            )
        return self._worker

    def close(self):
        if self._worker is not None:
            try:
                if self._worker.stdin:
                    self._worker.stdin.close()
                self._worker.wait(timeout=3)
            except Exception:
                try:
                    self._worker.kill()
                except Exception:
                    pass
            self._worker = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def query(self, name, **parameters):
        from analytics.queries import validate
        validate(name, parameters)
        memory = windows_available_bytes()
        if memory < 2*1024**3:
            raise RuntimeError(f"RESOURCE_BLOCKED: Windows available {memory/1024**3:.2f} GiB <2 GiB")
        request = {"query": name, "parameters": parameters, "admission": {"windows_available_bytes": memory,
                   "measured_at": time.time()}, "expected_batch": self.batch}
        started = time.monotonic()
        if getattr(subprocess.run, "__module__", "") == "subprocess" and getattr(subprocess.Popen, "__module__", "") == "subprocess":
            proc = self._get_worker()
            proc.stdin.write(json.dumps(request) + "\n")
            proc.stdin.flush()
            line = proc.stdout.readline()
            if not line:
                err = proc.stderr.read() if proc.stderr else ""
                self.close()
                raise RuntimeError(err[-2500:] or "Worker process terminated unexpectedly")
            if len(line.encode()) > 5*1024*1024:
                self.close()
                raise RuntimeError("Client response byte budget exceeded")
            response = json.loads(line)
        else:
            command = ["docker", "exec", "-i", "tlcn-local-runner-1", "python3", "/workspace/jobs/analytics/query_eda.py"]
            result = subprocess.run(command, input=json.dumps(request), capture_output=True, text=True, encoding="utf-8", timeout=40)
            if result.returncode:
                raise RuntimeError(result.stderr[-2500:])
            if len(result.stdout.encode()) > 5*1024*1024:
                raise RuntimeError("Client response byte budget exceeded")
            response = json.loads(result.stdout)
        response["client_seconds"] = time.monotonic()-started
        if self.batch is not None and response["batch"] != self.batch:
            raise RuntimeError("Mixed published batches")
        self.batch = response["batch"]
        self.evidence.append({k:v for k,v in response.items() if k not in ("rows", "columns", "snapshots")})
        import pandas as pd
        return pd.DataFrame(response["rows"], columns=[c["name"] for c in response["columns"]])


# ---------------------------------------------------------------------------
# 2. Trino HTTP Transport (formerly `transport.py`)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 3. Read Window Admission & Query Execution (formerly `access.py`)
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def admission(ticket):
    if (type(ticket) is not dict or set(ticket) != {"windows_available_bytes", "measured_at"}
            or type(ticket["windows_available_bytes"]) is not int or ticket["windows_available_bytes"] < 2*1024**3
            or type(ticket["measured_at"]) not in (float, int) or not -5.0 <= time.time()-ticket["measured_at"] <= 30.0):
        raise RuntimeError("Missing/stale/insufficient Windows resource admission")
    from gold.admission import runtime_resources
    from ingestion.storage.metadata_repository import MetadataRepository
    from silver.local_profile import BRONZE_SOURCES
    runtime_resources()
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


# ---------------------------------------------------------------------------
# 4. Analytical Methods & Notebook Plot Helpers (`methods.py`, `notebook.py`)
# ---------------------------------------------------------------------------

def source_labels(text):
    items = json.loads(text)
    return " | ".join(dict.fromkeys(str(x["label"]) for x in items if x.get("label") is not None))


def calendar_yoy(frame, group, year="year_key", value="value"):
    """Exact year Y-1 join, not previous available row. Zero denominator stays undefined."""
    import pandas as pd
    keys = list(group) + [year]
    if frame.duplicated(keys).any():
        raise ValueError("Ambiguous publication/grain; do not resolve with MAX/FIRST")
    out = frame.copy()
    out[value] = pd.to_numeric(out[value], errors="raise")
    previous = out[keys+[value]].rename(columns={value: "previous_value"})
    previous[year] = previous[year] + 1
    out = out.merge(previous, on=keys, how="left", validate="one_to_one")
    out["yoy_pct"] = (out[value] / out["previous_value"].where(out["previous_value"] > 0)-1)*100
    return out


def endpoint_change(frame, year="year_key", value="value"):
    valid = frame.dropna(subset=[value]).sort_values(year)
    if len(valid)<2:
        raise ValueError("Two numeric endpoints required")
    a,b=valid.iloc[0],valid.iloc[-1]
    lo,hi=float(a[value]),float(b[value])
    years=int(b[year])-int(a[year])
    return dict(first_year=int(a[year]),last_year=int(b[year]),first_value=lo,last_value=hi,
                change_pct=(hi/lo-1)*100 if lo>0 else None,
                cagr_pct=((hi/lo)**(1/years)-1)*100 if lo>0 and hi>0 and years>0 else None,
                numeric_periods=len(valid), calendar_span=years+1)


def monthly_calendar(frame, period="period", value="value"):
    """Reindex only for analysis; preserve absent-row vs present-NULL indicators."""
    import pandas as pd
    if frame.duplicated(period).any():
        raise ValueError("Select exactly one series/reference grain")
    df=frame.copy()
    df.index=pd.to_datetime(df[period].astype(str),format="%Y%m")
    df[value]=pd.to_numeric(df[value],errors="raise")
    df["observed_row"]=True
    df=df.reindex(pd.date_range(df.index.min(),df.index.max(),freq="MS"))
    df["observed_row"]=df["observed_row"].eq(True)
    df["change_pct"]=(df[value]/df[value].shift(1).where(df[value].shift(1)>0)-1)*100
    df["rolling_mean_12m"]=df[value].rolling(12,min_periods=12).mean()
    returns=df[value].where(df[value]>0).apply(lambda v: math.log(v) if pd.notna(v) else float("nan")).diff()
    df["volatility_12m"]=returns.rolling(12,min_periods=12).std(ddof=1)*100
    return df


def inspect_frame(frame):
    import pandas as pd
    return pd.DataFrame({"dtype":frame.dtypes.astype(str),"nulls":frame.isna().sum()})


def label(text):
    items=json.loads(text)
    return " | ".join(dict.fromkeys(str(x.get('label')) if isinstance(x,dict) else str(x) for x in items))


def resolve(frame, namespace, identity, key_name):
    matched=frame[(frame.identity_namespace==namespace)&(frame.source_identity==identity)]
    if len(matched)!=1:
        raise ValueError(f"Missing/ambiguous source identity: {namespace}/{identity}")
    return matched.iloc[0][key_name]


def annual_plot(frame,value,title,unit,year="year_key",group=None):
    import pandas as pd
    if frame.empty or not pd.to_numeric(frame[value],errors='raise').notna().any():
        raise ValueError("No numeric observations in this scope; refuse a blank analytical chart")
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(9,4))
    groups=frame.groupby(group,dropna=False) if group else [(None,frame)]
    for name,data in groups:
        data=data.sort_values(year)
        if data.duplicated(year).any():raise ValueError("Plot grain collision")
        x=data.set_index(year)[value]
        x=pd.to_numeric(x,errors='raise').reindex(range(int(x.index.min()),int(x.index.max())+1))
        ax.plot(x.index,x.values,label=str(name) if name is not None else None)
    ax.set(title=title,xlabel="Year / source reporting label",ylabel=unit)
    ax.grid(alpha=.2)
    if group:ax.legend(fontsize=8)
    fig.text(.01,.005,"Source: published TLCN Gold; scope and snapshot in notebook query evidence",fontsize=8)
    fig.tight_layout(rect=(0,.04,1,1));plt.show();plt.close(fig)


def save_evidence(root,slug,client,findings):
    folder=Path(root)/'docs/analytics/evidence'
    folder.mkdir(exist_ok=True)
    path=folder/(slug+'.json')
    payload={"notebook":slug,"batch":client.batch,"queries":client.evidence,"findings":findings}
    path.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return path.relative_to(root).as_posix()
