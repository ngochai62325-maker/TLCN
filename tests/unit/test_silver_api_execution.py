"""Validate the orchestration boundary without starting a Spark process or service."""
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


def api_module():
    spec = importlib.util.spec_from_file_location("silver_api", Path("jobs/silver/spark_api_server.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def handler(module, request):
    result = module.SparkApiHandler.__new__(module.SparkApiHandler)
    body = json.dumps(request).encode()
    result.path = "/run"
    result.headers = {"Content-Length": str(len(body))}
    result.rfile = io.BytesIO(body)
    result._send_json = MagicMock()
    return result


def test_api_uses_preview_limits_and_returns_validated_result(monkeypatch):
    module = api_module()
    for name in ("SILVER_SPARK_MASTER", "SILVER_DRIVER_MEMORY", "SILVER_SHUFFLE_PARTITIONS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(module.os.path, "exists", lambda _: True)
    submitted = MagicMock(return_value=SimpleNamespace(returncode=0,
        stdout='SILVER_RESULT_JSON={"faostat_production":{"status":"SUCCESS"}}\n'))
    monkeypatch.setattr(module.subprocess, "run", submitted)
    request = handler(module, {"job": "run_all_silver.py", "dataset": "faostat_production", "mode": "incremental",
                               "run_id": "bronze-run", "pipeline_run_id": "pipeline"})
    request.do_POST()
    command = submitted.call_args.args[0]
    assert command[command.index("--master") + 1] == "local[2]"
    assert command[command.index("--driver-memory") + 1] == "2g"
    assert "spark.sql.shuffle.partitions=4" in command
    assert command[command.index("--run-id") + 1] == "bronze-run"
    assert command[command.index("--pipeline-run-id") + 1] == "pipeline"
    assert request._send_json.call_args.args[1]["result"]["faostat_production"]["status"] == "SUCCESS"


def test_api_refuses_job_path_before_subprocess(monkeypatch):
    module = api_module()
    submitted = MagicMock()
    monkeypatch.setattr(module.subprocess, "run", submitted)
    request = handler(module, {"job": "../run_all_silver.py"})
    request.do_POST()
    assert request._send_json.call_args.args[0] == 400
    submitted.assert_not_called()
