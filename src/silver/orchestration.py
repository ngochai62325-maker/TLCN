"""Standard-library bridge from Airflow to the Spark job container."""
import json
import os
import urllib.request
import urllib.error


def trigger_spark_job(job_file="run_all_silver.py", mode="full", run_id=None,
                      dataset="all", pipeline_run_id=None, action="run"):
    url = os.environ.get("SPARK_API_URL", "http://spark-iceberg:5005/run")
    payload = {"job": job_file, "mode": mode, "run_id": run_id,
               "dataset": dataset, "pipeline_run_id": pipeline_run_id, "action": action}
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3600) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Spark job failed HTTP {exc.code}: {exc.read().decode('utf-8')}") from exc
    if result.get("status") != "SUCCESS" or "result" not in result:
        raise RuntimeError(f"Spark job returned no successful result: {result}")
    return result["result"]
