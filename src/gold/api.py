"""Strict argument construction for the existing Spark API, never a shell endpoint."""
from pathlib import Path

from gold import ACTIONS, BUILD_ORDER


def command(request, root):
    allowed = {"job", "profile", "action", "source", "owner_run_id", "silver_run_id"}
    if set(request) - allowed or request.get("job") != "run_gold.py" or request.get("profile") != "local_nine":
        raise ValueError("Unapproved Gold request fields/profile/job")
    action = request.get("action")
    if action not in ACTIONS or (action == "fact" and request.get("source") not in BUILD_ORDER) or (action != "fact" and request.get("source") is not None):
        raise ValueError("Unapproved Gold action/source")
    for field in ("owner_run_id", "silver_run_id"):
        value = request.get(field)
        if value is not None and (not isinstance(value, str) or not 0 < len(value) <= 250 or any(ord(c) < 32 for c in value)):
            raise ValueError(f"Invalid {field}")
    if not request.get("owner_run_id"):
        raise ValueError("Gold requires explicit run ownership")
    args = ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "768m", "--conf", "spark.ui.enabled=false", str(Path(root) / "jobs/gold/run_gold.py"), "--action", action, "--owner-run-id", request["owner_run_id"]]
    for field in ("silver_run_id", "source"):
        if request.get(field) is not None:
            args += ["--" + field.replace("_", "-"), request[field]]
    return args
