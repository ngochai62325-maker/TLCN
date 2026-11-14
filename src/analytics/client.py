"""Notebook client for the existing local runner. No credentials on the host."""
import json
import os
import subprocess
import time


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
