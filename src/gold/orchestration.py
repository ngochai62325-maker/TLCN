"""Airflow-to-runner calls with capability check and explicit stage allowlist."""
import json
import os
import urllib.error
import urllib.request

from gold import MODEL_VERSION
from gold.admission import code_fingerprint
from gold.views import SEMANTIC_VERSION


def run_stage(action, owner_run_id, silver_run_id, source=None):
    from gold.api import command
    url = os.environ.get("SPARK_API_URL", "http://runner:5005/run")
    with urllib.request.urlopen(url.rsplit("/", 1)[0] + "/health", timeout=10) as response:
        health = json.load(response)
    if (health.get("gold_model") != MODEL_VERSION or health.get("local_profile") != "local_nine"
            or health.get("gold_semantic_version") != SEMANTIC_VERSION or health.get("gold_code_fingerprint") != code_fingerprint()):
        raise RuntimeError("Runner has not loaded the reviewed Gold allowlist; controlled reload required")
    payload = {"job": "run_gold.py", "profile": "local_nine", "action": action, "owner_run_id": owner_run_id, "silver_run_id": silver_run_id}
    if source is not None:
        payload["source"] = source
    command(payload, "/workspace")
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=3400) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Gold stage {action}/{source} failed: {exc.read().decode('utf-8')}") from exc
    if result.get("status") != "SUCCESS" or not result.get("result"):
        raise RuntimeError("Missing successful Gold stage result")
    return result["result"]
