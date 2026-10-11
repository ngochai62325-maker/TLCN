"""Validate the API boundary without starting jobs or network connections."""
import io
import json
from unittest.mock import MagicMock, patch

import pytest

from silver.orchestration import trigger_spark_job
from jobs.silver.spark_api_server import SparkApiHandler


def test_remote_job_requires_successful_structured_result():
    response = MagicMock()
    response.__enter__.return_value.read.return_value = json.dumps({"status": "SUCCESS", "result": {"usda_psd": {"status": "SUCCESS"}}}).encode()
    with patch("silver.orchestration.urllib.request.urlopen", return_value=response) as request:
        assert trigger_spark_job(dataset="usda_psd", mode="incremental", run_id="bronze-1")["usda_psd"]["status"] == "SUCCESS"
        payload = json.loads(request.call_args.args[0].data)
        assert payload["run_id"] == "bronze-1" and payload["dataset"] == "usda_psd"
    response.__enter__.return_value.read.return_value = b'{"status":"SUCCESS"}'
    with patch("silver.orchestration.urllib.request.urlopen", return_value=response), pytest.raises(RuntimeError, match="no successful result"):
        trigger_spark_job()


def handler(payload):
    body = json.dumps(payload).encode()
    instance = SparkApiHandler.__new__(SparkApiHandler)
    instance.path = "/run"
    instance.headers = {"Content-Length": str(len(body))}
    instance.rfile = io.BytesIO(body)
    instance._send_json = MagicMock()
    return instance


def test_api_rejects_unknown_script_without_subprocess():
    instance = handler({"job": "../../unapproved.py"})
    with patch("jobs.silver.spark_api_server.subprocess.run") as run:
        instance.do_POST()
    run.assert_not_called()
    assert instance._send_json.call_args.args[0] == 400


def test_api_passes_arguments_and_extracts_json():
    instance = handler({"job": "run_all_silver.py", "dataset": "usda_psd", "run_id": "bronze-1", "mode": "incremental"})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='log\nSILVER_RESULT={"usda_psd":{"status":"SUCCESS"}}\n')) as run:
        instance.do_POST()
    command = run.call_args.args[0]
    assert command[-2:] == ["--dataset", "usda_psd"]
    assert "--run-id" in command and "bronze-1" in command
    assert instance._send_json.call_args.args[0] == 200
    assert instance._send_json.call_args.args[1]["result"]["usda_psd"]["status"] == "SUCCESS"
