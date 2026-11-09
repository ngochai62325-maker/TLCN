"""Read Silver through Trino and compare counts with Spark acceptance evidence."""
import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
import re


def contract_expression_trino(expression):
    """Translate only the Spark constructs used by the nine pinned contracts."""
    expression = expression.replace("sha2(encode(geography_raw,'UTF-8'),256)", "lower(to_hex(sha256(to_utf8(geography_raw))))")
    expression = expression.replace("right(period_raw,4)", "substr(period_raw,-4)").replace("current_date()", "current_date")
    expression = expression.replace("try_multiply(value_source, conversion_factor)", "try(value_source * conversion_factor)")
    return re.sub(r"period_raw RLIKE ('[^']*')", r"regexp_like(period_raw,\1)", expression)


def verify_nso(url, table="iceberg.silver_local.nso_rice_statistics"):
    """Verify the current contract; unknown geography/review warnings are valid.

    Only retained Silver observations are examined. Structural exclusions and
    quarantined rows require the separate persisted Spark admission report;
    they must never be inferred from missing rows in this query.
    """
    import yaml
    contract = yaml.safe_load((Path(__file__).resolve().parents[2] / "contracts/silver/nso.yaml").read_text(encoding="utf-8"))
    target = identifier(table)
    actual = {r["Column"]: r["Type"] for r in query(url, f"DESCRIBE {target}")}
    for field in contract["schema"]["columns"]:
        expected = {"string": "varchar", "long": "bigint", "integer": "integer"}.get(field["data_type"], field["data_type"])
        if actual.get(field["name"]) != expected:
            raise ValueError(f"NSO contract schema mismatch: {field['name']}")
    expressions = []
    hard_rules = []
    for rule in contract["data_quality"]:
        expression = contract_expression_trino(rule["sql_expr"])
        # Treat SQL UNKNOWN as a failed predicate, exactly like framework DQ.
        alias = rule["rule_id"].lower()
        expressions.append(f"count_if(NOT coalesce(({expression}), FALSE)) AS {alias}")
        if rule.get("action", "QUARANTINE") != "LOG":
            hard_rules.append(alias)
    nonnull = [c["name"] for c in contract["schema"]["columns"] if not c["nullable"]]
    required = " OR ".join(identifier(c) + " IS NULL" for c in nonnull)
    keys = ",".join(identifier(c) for c in contract["grain"]["business_key"])
    sql = f"""SELECT count(*) AS rows, count(DISTINCT ROW({keys})) AS unique_keys,
        count_if({required}) AS invalid_required,
        count_if(geography_level='unknown') AS unknown_geography,
        count_if(quality_status='MISSING') AS missing_observations,
        count_if(value=0) AS genuine_zero,
        count_if(release_status='provisional') AS provisional,
        count_if(release_status='unconfirmed') AS unconfirmed,
        {', '.join(expressions)} FROM {target}"""
    counts = query(url, sql)[0]
    if counts["rows"] != counts["unique_keys"] or counts["invalid_required"] or any(counts[r] for r in hard_rules):
        raise ValueError(f"NSO contract/DQ violation: {counts}")
    return {"status": "PASSED", "target": table, "counts": counts,
            "scope": "retained Silver only; exclusions/quarantine need upstream admission evidence"}


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
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--spark-report", type=Path)
    group.add_argument("--nso-only", action="store_true", help="Read-only current NSO contract verification")
    parser.add_argument("--namespace", default="iceberg.silver_local")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--url", default="http://trino:8080")
    args = parser.parse_args()
    if args.nso_only:
        result = {"status": "RUNNING"}
        try:
            result = verify_nso(args.url, args.namespace + ".nso_rice_statistics")
        except Exception as exc:
            result = {"status": "FAILED", "error": str(exc)}
            raise
        finally:
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("TRINO_RESULT=" + json.dumps(result))
        return
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
        if "nso" in evidence["datasets"]:
            report["nso"] = verify_nso(args.url, evidence["datasets"]["nso"]["metrics"]["target_table"])
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
