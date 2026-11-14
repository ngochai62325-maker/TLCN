#!/usr/bin/env python3
"""HTTP bridge with serial Spark jobs and a responsive health endpoint."""
import glob
import json
import logging
import os
import subprocess
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Timer
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR / "src"))
from silver.local_profile import active_profile, require_profile, DATASETS
from gold.admission import code_fingerprint
from gold.views import SEMANTIC_VERSION

GOLD_CODE_FINGERPRINT = code_fingerprint()

JOBS_DIR = Path(__file__).resolve().parent
ALLOWED_JOBS = {"run_all_silver.py", "job_faostat_trade.py", "job_usda_psd.py",
                "job_usda_export_price.py", "job_worldbank.py", "job_faostat_production.py",
                "job_faostat_monthly_price.py", "job_faostat_supply_utilization.py", "job_thitruongnongsan.py",
                "job_nso.py"}
logger = logging.getLogger("spark_api_server")
JOB_LOCK = Lock()
_WARM_SPARK = None
_IDLE_TIMER = None


def _stop_warm_spark():
    global _WARM_SPARK, _IDLE_TIMER
    if _IDLE_TIMER is not None:
        _IDLE_TIMER.cancel()
        _IDLE_TIMER = None
    if _WARM_SPARK is not None:
        try:
            _WARM_SPARK.stop()
        except Exception:
            pass
        _WARM_SPARK = None


def _schedule_idle_stop(seconds=60.0):
    global _IDLE_TIMER
    if _IDLE_TIMER is not None:
        _IDLE_TIMER.cancel()
    def _idle_cleanup():
        with JOB_LOCK:
            _stop_warm_spark()
    _IDLE_TIMER = Timer(seconds, _idle_cleanup)
    _IDLE_TIMER.daemon = True
    _IDLE_TIMER.start()


def _get_warm_spark(ansi=False):
    global _WARM_SPARK
    if _IDLE_TIMER is not None:
        _IDLE_TIMER.cancel()
    for p in [*glob.glob("/opt/spark/python/lib/py4j-*.zip"), "/opt/spark/python", str(ROOT_DIR)]:
        if p not in sys.path and os.path.exists(p):
            sys.path.insert(0, p)
    os.environ.setdefault("SPARK_HOME", "/opt/spark")
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--master local[2] --driver-memory 2g --conf spark.ui.enabled=false "
        "--conf spark.sql.execution.arrow.pyspark.enabled=true "
        "--conf spark.sql.shuffle.partitions=4 "
        "--conf spark.sql.defaultCatalog=spark_catalog pyspark-shell"
    )
    if _WARM_SPARK is None:
        from silver.core.spark_session import get_spark_session
        _WARM_SPARK = get_spark_session("RiceLakehouseRunner")
    _WARM_SPARK.catalog.clearCache()
    _WARM_SPARK.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
    _WARM_SPARK.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    _WARM_SPARK.conf.set("spark.sql.shuffle.partitions", "4")
    return _WARM_SPARK


def _use_in_process():
    return (getattr(subprocess.run, "__module__", "") == "subprocess"
            and Path("/opt/spark/python").exists()
            and os.environ.get("SPARK_API_IN_PROCESS", "1") == "1")


class SparkApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, data):
        encoded = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        health = {"status": "ok", "silver_release": "silver_v2_release_20261013"}
        health["gold_model"] = "gold-20261025.1"
        health["gold_semantic_version"] = SEMANTIC_VERSION
        health["gold_code_fingerprint"] = GOLD_CODE_FINGERPRINT
        if active_profile():
            health.update(local_profile=active_profile())
        self._send_json(200, health) if self.path == "/health" else self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/run":
            self._send_json(404, {"error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            if not 0 < length <= 65536:
                raise ValueError("Invalid request size")
            request = json.loads(self.rfile.read(length))
            if request.get("job") == "run_gold.py":
                self._run_gold(request)
                return
            job = request.get("job", "run_all_silver.py")
            mode = request.get("mode", "full")
            action = request.get("action", "run")
            profile = request.get("profile")
            if profile or active_profile():
                require_profile(profile, request.get("release"))
                if job != "run_all_silver.py" or request.get("dataset") not in DATASETS:
                    raise ValueError("local_nine API requires one explicit approved dataset")
                snapshot = request.get("bronze_snapshot_id")
                if action == "run" and (type(snapshot) not in (int, str) or not str(snapshot).isdigit()
                                        or not 0 < int(snapshot) <= 2**63-1):
                    raise ValueError("Missing/invalid local Bronze snapshot identity")
            if job not in ALLOWED_JOBS or mode not in {"full", "incremental"} or action not in {"run", "check-bronze", "audit"}:
                raise ValueError("Unknown job, mode or action")
            command = ["/opt/spark/bin/spark-submit"]
            if profile:
                command += ["--master", "local[2]", "--driver-memory", "2g", "--conf", "spark.ui.enabled=false",
                            "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
                            "--conf", "spark.sql.shuffle.partitions=4"]
            command += [str(JOBS_DIR / job), "--mode", mode]
            for key, flag in [("run_id", "--run-id"), ("pipeline_run_id", "--pipeline-run-id"),
                              ("dataset", "--dataset"), ("watermark", "--watermark"), ("release", "--release"),
                              ("profile", "--profile"), ("bronze_snapshot_id", "--bronze-snapshot-id")]:
                if request.get(key):
                    command += [flag, str(request[key])]
            if action != "run":
                command += ["--" + action]
            # Health requests remain responsive while writes stay serial.
            with JOB_LOCK:
                if _use_in_process() and job == "run_all_silver.py" and profile == "local_nine":
                    try:
                        spark = _get_warm_spark(ansi=False)
                        from jobs.silver.run_all_silver import datasets as load_datasets, run_pipeline, selected_sources
                        contract_dir = str(ROOT_DIR / "contracts/silver")
                        if action in ("check-bronze", "audit"):
                            from silver.audit import audit_tables, check_bronze
                            from silver.framework import SilverTransformationFramework
                            contracts = load_datasets(contract_dir)
                            framework = SilverTransformationFramework(spark, contract_dir, release=request.get("release"), profile=profile)
                            selected = [framework.contract(k) for k in selected_sources(request["dataset"], contracts, request.get("release"), profile, contract_dir)]
                            raw_res = check_bronze(spark, selected, profile=profile) if action == "check-bronze" else audit_tables(spark, selected)
                            _schedule_idle_stop()
                        else:
                            wm = datetime.fromisoformat(request["watermark"]) if request.get("watermark") else None
                            snap = int(request["bronze_snapshot_id"]) if request.get("bronze_snapshot_id") is not None else None
                            raw_res = run_pipeline(
                                dataset=request["dataset"], mode=mode, run_id=request.get("run_id"),
                                watermark=wm, pipeline_run_id=request.get("pipeline_run_id"),
                                contract_dir=contract_dir, spark=spark, release=request.get("release"),
                                profile=profile, bronze_snapshot_id=snap,
                            )
                            _schedule_idle_stop()
                        result = json.loads(json.dumps(raw_res, default=str))
                        self._send_json(200, {"status": "SUCCESS", "exit_code": 0, "result": result, "output": ""})
                        return
                    except Exception:
                        _stop_warm_spark()
                        self._send_json(500, {"status": "FAILED", "exit_code": 1, "result": None, "output": traceback.format_exc()[-4000:]})
                        return
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

    def _run_gold(self, request):
        from gold.api import command
        require_profile(request.get("profile"))
        args = command(request, JOBS_DIR.parents[1])
        # Same lock as Silver; a queue cannot launch overlapping Spark writers.
        with JOB_LOCK:
            if _use_in_process():
                try:
                    from gold.admission import runtime_resources, success_context
                    from gold.views import require_semantic_definitions
                    runtime_resources()
                    success_context(request.get("silver_run_id"))
                    require_semantic_definitions()
                    spark = _get_warm_spark(ansi=True)
                    from gold.pipeline import execute
                    result = execute(spark, request["action"], request["owner_run_id"], request.get("silver_run_id"), request.get("source"))
                    if request["action"] == "publish":
                        _stop_warm_spark()
                    else:
                        _schedule_idle_stop()
                    self._send_json(200, {"status": "SUCCESS", "exit_code": 0, "result": result, "output": ""})
                    return
                except Exception:
                    _stop_warm_spark()
                    self._send_json(500, {"status": "FAILED", "exit_code": 1, "result": None, "output": traceback.format_exc()[-6000:]})
                    return
            proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, timeout=3300)
        result = None
        for line in proc.stdout.splitlines():
            if line.startswith("GOLD_RESULT="):
                result = json.loads(line.split("=", 1)[1])
        success = proc.returncode == 0 and result is not None
        self._send_json(200 if success else 500,
                        {"status": "SUCCESS" if success else "FAILED", "exit_code": proc.returncode,
                         "result": result, "output": proc.stdout[-6000:]})


def main():
    active_profile()  # Reject conflicting runtime flags before opening the API socket.
    logging.basicConfig(level=logging.INFO)
    ThreadingHTTPServer(("0.0.0.0", 5005), SparkApiHandler).serve_forever()


if __name__ == "__main__":
    main()
