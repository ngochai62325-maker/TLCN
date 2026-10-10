#!/usr/bin/env python3
"""Lightweight HTTP REST API Server for triggering Spark Jobs from Airflow.

Zero-dependency implementation using Python standard library http.server.
Listens on port 5005 inside spark-iceberg container.
"""

import json
import logging
import os
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("spark_api_server")

HOST = "0.0.0.0"
PORT = 5005
JOBS_DIR = "/home/iceberg/jobs/silver"


class SparkApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "service": "spark_api_server"})
        else:
            self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        if self.path == "/run":
            try:
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len).decode("utf-8")
                req = json.loads(body) if body else {}

                job_file = req.get("job")
                mode = req.get("mode", "full")
                run_id = req.get("run_id")

                if not job_file:
                    self._send_json(400, {"error": "Missing 'job' parameter"})
                    return

                script_path = os.path.join(JOBS_DIR, job_file)
                if not os.path.exists(script_path):
                    self._send_json(404, {"error": f"Job script '{job_file}' not found in {JOBS_DIR}"})
                    return

                cmd = ["/opt/spark/bin/spark-submit", script_path, "--mode", mode]
                if run_id:
                    cmd.extend(["--run-id", str(run_id)])

                logger.info(f"Executing Spark Job: {' '.join(cmd)}")
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=req.get("timeout_seconds", 900),
                )

                status = "SUCCESS" if proc.returncode == 0 else "FAILED"
                logger.info(f"Job {job_file} finished with code {proc.returncode} ({status})")

                self._send_json(
                    200 if proc.returncode == 0 else 500,
                    {
                        "job": job_file,
                        "status": status,
                        "exit_code": proc.returncode,
                        "output": proc.stdout[-4000:],  # return tail of logs
                    },
                )

            except subprocess.TimeoutExpired:
                logger.error(f"Job {job_file} timed out")
                self._send_json(504, {"error": "Job execution timed out"})
            except Exception as e:
                logger.error(f"Error handling job execution: {e}", exc_info=True)
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        logger.info(f"{self.address_string()} - {format % args}")


def main():
    server = HTTPServer((HOST, PORT), SparkApiHandler)
    logger.info(f"Spark API Server started on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
