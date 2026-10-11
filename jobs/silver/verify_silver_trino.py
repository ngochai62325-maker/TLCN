"""Read Silver through Trino and compare counts with Spark acceptance evidence."""
import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def query(url, sql, timeout=600):
    deadline = time.monotonic() + timeout
    request = urllib.request.Request(
        url.rstrip("/") + "/v1/statement", data=sql.encode("utf-8"),
        headers={"X-Trino-User": "silver-validation", "Content-Type": "text/plain"})
    rows, columns = [], []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Trino query exceeded acceptance timeout")
        with urllib.request.urlopen(request, timeout=min(30, remaining)) as response:
            page = json.loads(response.read())
        if "error" in page:
            raise RuntimeError(f"Trino {page.get('id')}: {page['error']['message']}")
        columns = page.get("columns", columns)
        rows.extend(page.get("data", []))
        if not page.get("nextUri"):
            return [dict(zip([c["name"] for c in columns], row)) for row in rows]
        request = urllib.request.Request(page["nextUri"], headers={"X-Trino-User": "silver-validation"})


def identifier(name):
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-report", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--url", default="http://trino:8080")
    args = parser.parse_args()
    evidence = json.loads(args.spark_report.read_text(encoding="utf-8"))
    if evidence["status"] != "PASSED":
        raise ValueError("A PASSED Spark report is required")
    report = {"status": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        statements = []
        for source, record in evidence["datasets"].items():
            if not record.get("metrics") or not record.get("replay_verified"):
                raise ValueError("Trino comparison requires a full/replay report, not audit-only")
            table = record["metrics"]["target_table"]
            source_literal = source.replace("'", "''")
            statements.append(f"SELECT '{source_literal}' AS dataset, count(*) AS rows FROM {identifier(table)}")
        report["datasets"] = query(args.url, " UNION ALL ".join(statements))
        for row in report["datasets"]:
            expected = evidence["datasets"][row["dataset"]]["silver"]["rows"]
            if row["rows"] != expected:
                raise ValueError(f"Spark/Trino row mismatch: {row['dataset']}: {row['rows']} != {expected}")
        if "nso_vietnam" in evidence["datasets"]:
            table = identifier(evidence["datasets"]["nso_vietnam"]["metrics"]["target_table"])
            report["nso"] = query(args.url, f"""SELECT count(*) AS rows,
                count_if(is_missing) AS missing_observations,
                count_if(mapping_status = 'needs_review') AS geography_review,
                count_if(geography_name IS NOT NULL) AS named_observations,
                count_if(strpos(geography_name, '?') > 0) AS damaged_candidate_names,
                count_if(mapping_status = 'needs_review' AND geography_code IS NOT NULL) AS invalid_review_code,
                count_if(is_missing AND value IS NOT NULL) AS invalid_missing_value
                FROM {table}""")[0]
            if (report["nso"]["invalid_review_code"] or report["nso"]["invalid_missing_value"]
                    or report["nso"]["damaged_candidate_names"]):
                raise ValueError("NSO nullable geography/missing policy violated through Trino")
        if "usda_psd" in evidence["datasets"]:
            table = identifier(evidence["datasets"]["usda_psd"]["metrics"]["target_table"])
            report["usda_milling_rate"] = query(args.url, f"""SELECT count(*) AS rows,
                min(value_source) AS min_source_value, max(value_source) AS max_source_value,
                min(value) AS min_fraction, max(value) AS max_fraction,
                count_if(unit <> 'fraction' OR conversion_factor <> DECIMAL '0.0001'
                    OR value NOT BETWEEN 0 AND 1
                    OR value <> cast(value_source * conversion_factor AS DECIMAL(18,4))) AS invalid_rates
                FROM {table} WHERE attribute = 'Milling Rate (.9999)'""")[0]
            if report["usda_milling_rate"]["invalid_rates"]:
                raise ValueError("USDA milling conversion violated through Trino")
            columns = {r["Column"]: r["Type"] for r in query(args.url, f"DESCRIBE {table}")}
            report["usda_value_type"] = columns["value"]
            if columns["value"] != "decimal(18,4)":
                raise ValueError("Trino cannot read USDA value at the contract precision")
        report["status"] = "PASSED"
    except Exception as exc:
        report.update(status="FAILED", error=str(exc))
        raise
    finally:
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("TRINO_RESULT=" + json.dumps(report))


if __name__ == "__main__":
    main()
