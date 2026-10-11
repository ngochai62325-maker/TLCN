"""Validate API boundaries and local HTTP behavior without starting Spark jobs."""
import io
import json
from unittest.mock import MagicMock, patch

import pytest

from silver.orchestration import trigger_spark_job
from jobs.silver.spark_api_server import SparkApiHandler, ThreadingHTTPServer


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


def test_api_forwards_incremental_watermark():
    instance = handler({"dataset": "nso_vietnam", "mode": "incremental", "watermark": "2026-10-01T00:00:00+00:00"})
    with patch("jobs.silver.spark_api_server.subprocess.run", return_value=MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')) as run:
        instance.do_POST()
    assert "--watermark" in run.call_args.args[0]
    assert "2026-10-01T00:00:00+00:00" in run.call_args.args[0]


def test_health_responds_during_job_and_jobs_remain_serial():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Thread
    from urllib.request import Request, urlopen

    started, release = Event(), Event()
    executions = []

    def fake_job(*args, **kwargs):
        executions.append(args[0])
        if len(executions) == 1:
            started.set()
            assert release.wait(5), "Test did not release the first job"
        return MagicMock(returncode=0, stdout='SILVER_RESULT={}\n')

    server = ThreadingHTTPServer(('127.0.0.1', 0), SparkApiHandler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f'http://127.0.0.1:{server.server_port}'

    def post():
        request = Request(url + '/run', data=b'{"dataset":"nso_vietnam"}',
                          headers={'Content-Type': 'application/json'})
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    try:
        with patch('jobs.silver.spark_api_server.subprocess.run', side_effect=fake_job):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(post)
                assert started.wait(5)
                second = executor.submit(post)
                try:
                    with urlopen(url + '/health', timeout=2) as response:
                        assert json.loads(response.read()) == {'status': 'ok'}
                    assert len(executions) == 1
                finally:
                    release.set()
                assert first.result(timeout=10)['status'] == 'SUCCESS'
                assert second.result(timeout=10)['status'] == 'SUCCESS'
                assert len(executions) == 2
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
