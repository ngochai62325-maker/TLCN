"""Read Silver through Trino and compare counts with Spark acceptance evidence."""
import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal


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


def verify_record(url, source, record):
    """Reconcile persisted release data; expectations come from this Spark run."""
    if not record.get("replay_verified") or not record.get("incremental_replay_verified"):
        raise ValueError("Full AND incremental replay evidence required")
    table = identifier(record["metrics"]["target_table"])
    types = {r["Column"]: r["Type"] for r in query(url, f"DESCRIBE {table}")}
    expected = {}
    for field in record["silver"]["schema"]["fields"]:
        dtype = field["type"]
        if isinstance(dtype, str):
            expected[field["name"]] = {"string": "varchar", "long": "bigint", "timestamp": "timestamp(6) with time zone", "timestamp_ntz": "timestamp(6)"}.get(dtype, dtype)
        elif field["name"] == "_dq_warnings":
            expected[field["name"]] = "array(row(rule_id varchar, error_message varchar, failed_column varchar))"
    if types != expected:
        raise ValueError(f"Trino/Spark schema mismatch: {source}")
    keys = ",".join(identifier(k) for k in record["business_keys"])
    value = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
    partitions = record.get("key_partitions", [])
    distinct = "" if partitions else f"count(DISTINCT ROW({keys})) AS unique_keys,"
    counts = query(url, f"SELECT count(*) AS rows, {distinct} count_if(cardinality(_dq_warnings)>0) AS warnings, count_if(missing_reason IS NOT NULL AND {value} IS NOT NULL) AS invalid_missing FROM {table}")[0]
    if partitions:
        if "year" not in record["business_keys"] or len({p["year"] for p in partitions}) != len(partitions):
            raise ValueError("Invalid key partitions")
        rows, unique = 0, 0
        for partition in partitions:
            checked = query(url, f"SELECT count(*) AS rows,count(DISTINCT ROW({keys})) AS unique_keys FROM {table} WHERE year={int(partition['year'])}")[0]
            if checked["rows"] != partition["rows"]:
                raise ValueError(f"Trino partition count mismatch: {source}")
            rows += checked["rows"]
            unique += checked["unique_keys"]
        if rows != counts["rows"]:
            raise ValueError(f"Trino key partition coverage mismatch: {source}")
        counts["unique_keys"] = unique
    if counts["rows"] != record["silver"]["rows"] or counts["unique_keys"] != counts["rows"] or counts["warnings"] != record["metrics"]["warning_count"] or counts["invalid_missing"]:
        raise ValueError(f"Trino count/key/missing/warnings mismatch: {source}")
    from silver.framework import AUDIT_COLUMNS
    bronze_name = record["bronze_table"]
    snapshot = int(record["bronze_snapshot_id"])
    # Compare identity sets, not every row with every other row sharing batch lineage.
    # EXCEPT also preserves NULL-safe tuple semantics without a correlated join.
    audit = ",".join(identifier(c) for c in AUDIT_COLUMNS)
    invalid = query(url, f"SELECT count(*) AS invalid FROM (SELECT {audit} FROM {table} EXCEPT SELECT {audit} FROM {identifier(bronze_name)} FOR VERSION AS OF {snapshot})")[0]["invalid"]
    if invalid:
        raise ValueError(f"Trino Bronze lineage mismatch: {source}")
    distribution = query(url, f"SELECT unit,count(*) AS rows,count({value}) AS present,cast(sum({value}) AS varchar) AS sum_value FROM {table} GROUP BY unit ORDER BY unit")
    normalize = lambda rows: sorted([(r["unit"], int(r["rows"]), int(r["present"]), Decimal(r["sum_value"]) if r["sum_value"] is not None else None) for r in rows], key=lambda r: str(r[0]))
    if normalize(distribution) != normalize(record["unit_distribution"]):
        raise ValueError(f"Trino values/units distribution mismatch: {source}")
    for column in record["flag_distributions"]:
        actual = query(url, f"SELECT {identifier(column)} AS flag,count(*) AS rows FROM {table} GROUP BY {identifier(column)}")
        normalized = lambda rows: sorted([(r["flag"], r["rows"]) for r in rows], key=lambda r: str(r[0]))
        if normalized(actual) != normalized(record["flag_distributions"][column]):
            raise ValueError(f"Trino flag distribution mismatch: {source}.{column}")
    return {"status": "PASSED", "counts": counts, "unit_distribution": distribution, "lineage": "PASSED", "schema": types}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-report", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--url", default="http://trino:8080")
    args = parser.parse_args()
    evidence = json.loads(args.spark_report.read_text(encoding="utf-8"))
    if evidence["status"] not in {"PASSED", "VALIDATED_PENDING_OWNER"}:
        raise ValueError("A PASSED Spark report is required")
    report = {"status": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        if evidence.get("release"):
            report["datasets"] = {s: verify_record(args.url, s, r) for s, r in evidence["datasets"].items()}
            report["status"] = "VALIDATED_PENDING_OWNER"
            return
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
