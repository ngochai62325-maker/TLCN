"""Pinned nine-source Trino schema/key/DQ audit; no writes. Run in runner.

Uses the existing Gold bounded HTTP client (64 MB query-memory session budget).
Trade keys include year: exhaustive year domains contain whole keys, with a
global count/NULL check. Executes one partition at a time, never samples.
"""
import json
from pathlib import Path
from datetime import datetime, timezone

import yaml

from gold.admission import success_context
from gold.consumer import read_window
from gold.views import trino_result
from jobs.silver.verify_silver_trino import contract_expression_trino, identifier


ROOT = Path("/workspace")


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "sources": {}, "queries": []}
    def query(sql):
        result = trino_result(sql, max_rows=256, max_seconds=90)
        report["queries"].append({"id": result["query_id"], "seconds": result["client_elapsed_seconds"]})
        names = [c["name"] for c in result["columns"]]
        return [dict(zip(names, row)) for row in result["rows"]]
    with read_window() as manifest:
        proof = success_context()
        assert proof["pins"] == manifest["context"]["inputs"], "Persisted Silver input proof differs from published Gold"
        report["silver_run_id"] = proof["silver_run_id"]
        report["published_batch"] = manifest["batch"]
        for source, pin in proof["pins"].items():
            contract = yaml.safe_load((ROOT / "contracts/silver" / (source + ".yaml")).read_text(encoding="utf-8"))
            name = identifier(pin["table"])
            table = name + " FOR VERSION AS OF " + str(int(pin["snapshot_id"]))
            schema = {r["Column"]: r["Type"] for r in query("DESCRIBE " + name)}
            for column in contract["schema"]["columns"]:
                expected = {"string": "varchar", "long": "bigint", "integer": "integer"}.get(column["data_type"], column["data_type"])
                assert schema.get(column["name"]) == expected, (source, column["name"], schema.get(column["name"]), expected)
            keys = contract["grain"]["business_key"]
            required = set(keys) | {c["name"] for c in contract["schema"]["columns"] if not c.get("nullable", True)}
            checks = {"invalid_required": " OR ".join(identifier(c) + " IS NULL" for c in sorted(required))}
            checks["invalid_lineage"] = ("_source_id <> '" + source + "' OR _source_id IS NULL OR _ingestion_run_id IS NULL OR "
                "_ingestion_batch_id IS NULL OR _ingestion_chunk_id IS NULL OR _ingestion_chunk_id<0 OR _source_file IS NULL OR _source_checksum IS NULL")
            hard = set(checks)
            for rule in contract["data_quality"]:
                alias = rule["rule_id"].lower()
                checks[alias] = "NOT coalesce((" + contract_expression_trino(rule["sql_expr"]) + "),FALSE)"
                if rule.get("action", "QUARANTINE") != "LOG":
                    hard.add(alias)
            value = "price" if source == "worldbank_pinksheet" else "price_value" if source == "thitruongnongsan" else "value"
            checks.update(missing_observations=f"{value} IS NULL", genuine_zero=f"{value}=0", signed_negative=f"{value}<0")
            if "missing_reason" in schema:
                checks["invalid_missing"] = f"missing_reason IS NOT NULL AND {value} IS NOT NULL"
                hard.add("invalid_missing")
            total = query(f"SELECT count(*) AS rows FROM {table}")[0]["rows"]
            partitions = [None]
            if source == "faostat_trade":
                assert "year" in keys
                partitions = [p["year"] for p in query(f"SELECT year FROM {table} GROUP BY year ORDER BY year")]
                assert partitions and all(type(y) is int for y in partitions), "NULL/invalid partition domain"
            observed, outcomes = 0, []
            for year in partitions:
                where = "" if year is None else f" WHERE year={year}"
                fields = ",".join(f"count_if({expr}) AS {alias}" for alias, expr in checks.items())
                counts = query(f"SELECT count(*) AS rows,count(DISTINCT ROW({','.join(identifier(k) for k in keys)})) AS unique_keys,{fields} FROM {table}{where}")[0]
                assert counts["rows"] == counts["unique_keys"] and not any(counts[k] for k in hard), (source, year, counts)
                observed += counts["rows"]
                outcomes.append({"year": year, **counts})
            assert observed == total and total > 0, "Incomplete partition coverage"
            report["sources"][source] = {"status": "PASS", "pin": pin, "rows": total,
                "schema_columns": len(schema), "partitions": outcomes,
                "persisted_spark_status": proof["proofs"][source]["status"]}
            print("Verified " + source + ": " + str(total), flush=True)
    report.update(status="PASS", completed_at=datetime.now(timezone.utc).isoformat())
    Path("/tmp/foundation-silver-readonly.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
