#!/usr/bin/env python3
"""Serial HTTP bridge executing only known Silver entry points."""
import json
import logging
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parent
ALLOWED_JOBS = {"run_all_silver.py", "job_faostat_trade.py", "job_usda_psd.py",
                "job_usda_export_price.py", "job_worldbank.py", "job_faostat_production.py",
                "job_faostat_monthly_price.py", "job_faostat_supply_utilization.py", "job_thitruongnongsan.py"}
logger = logging.getLogger("spark_api_server")


class SparkApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, data):
        encoded = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        self._send_json(200, {"status": "ok"}) if self.path == "/health" else self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/run":
            self._send_json(404, {"error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            if not 0 < length <= 65536:
                raise ValueError("Invalid request size")
            request = json.loads(self.rfile.read(length))
            job = request.get("job", "run_all_silver.py")
            mode = request.get("mode", "full")
            action = request.get("action", "run")
            if job not in ALLOWED_JOBS or mode not in {"full", "incremental"} or action not in {"run", "check-bronze", "audit"}:
                raise ValueError("Unknown job, mode or action")
            command = ["/opt/spark/bin/spark-submit", str(JOBS_DIR / job), "--mode", mode]
            for key, flag in [("run_id", "--run-id"), ("pipeline_run_id", "--pipeline-run-id"), ("dataset", "--dataset")]:
                if request.get(key):
                    command += [flag, str(request[key])]
            if action != "run":
                command += ["--" + action]
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, timeout=3300)
            result = None
            for line in proc.stdout.splitlines():
                if line.startswith("SILVER_RESULT="):
                    result = json.loads(line.split("=", 1)[1])
            success = proc.returncode == 0 and result is not None
            self._send_json(200 if success else 500,
                            {"status": "SUCCESS" if success else "FAILED", "exit_code": proc.returncode,
                             "result": result, "output": proc.stdout[-4000:]})
        except (ValueError, TypeError) as exc:
            self._send_json(400, {"error": str(exc)})
        except subprocess.TimeoutExpired:
            self._send_json(504, {"error": "Spark job timed out"})
        except Exception as exc:
            logger.exception("Spark job failed")
            self._send_json(500, {"error": str(exc)})


def main():
    logging.basicConfig(level=logging.INFO)
    HTTPServer(("0.0.0.0", 5005), SparkApiHandler).serve_forever()


if __name__ == "__main__":
    main()
