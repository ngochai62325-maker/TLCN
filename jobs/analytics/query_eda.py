"""One read-only allowlisted Analytics request; JSON stdin/stdout. No sample KPIs."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from analytics.access import run_query


def main():
    if "--worker" in sys.argv:
        import os
        os.environ["EDA_WORKER_MODE"] = "1"
        for line in sys.stdin:
            if not line.strip():
                continue
            request = json.loads(line)
            if set(request) != {"query", "parameters", "admission", "expected_batch"}:
                raise ValueError("Invalid EDA request envelope")
            result = run_query(request["query"], request["parameters"], request["admission"], request["expected_batch"])
            print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
        return
    request = json.loads(sys.stdin.read(16385))
    if set(request) != {"query", "parameters", "admission", "expected_batch"}:
        raise ValueError("Invalid EDA request envelope")
    result = run_query(request["query"], request["parameters"], request["admission"], request["expected_batch"])
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
