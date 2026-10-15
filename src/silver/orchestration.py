"""Standard-library bridge from Airflow to the Spark job container."""
import json
import os
import urllib.request
import urllib.error


def trigger_spark_job(job_file="run_all_silver.py", mode="full", run_id=None,
                      dataset="all", pipeline_run_id=None, action="run", watermark=None, release=None):
    url = os.environ.get("SPARK_API_URL", "http://spark-iceberg:5005/run")
    if release:
        # An already-running old API would silently ignore the new request field.
        # Capability check must happen BEFORE POST, including before legacy quarantine.
        with urllib.request.urlopen(url.rsplit("/", 1)[0] + "/health", timeout=10) as response:
            health = json.loads(response.read().decode("utf-8"))
        if health.get("silver_release") != release:
            raise RuntimeError("Spark API lacks approved release routing; reload it at owner-approved cutover")
    payload = {"job": job_file, "mode": mode, "run_id": run_id,
               "dataset": dataset, "pipeline_run_id": pipeline_run_id, "action": action, "watermark": watermark,
               "release": release}
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
