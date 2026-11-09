"""Versioned read-only semantic queries over exact Gold snapshots; no catalog views."""
import hashlib
import json
import os
import re
import time
import urllib.request

from gold.keys import fingerprint
from gold.rules import SUA
from gold.spec import ROOT, load_spec

SEMANTIC_VERSION = "gold-semantic-20261026.1"
VIEW_NAMES = ("vw_nso_source_observations", "vw_fao_production_yield", "vw_sua_supported_balance", "vw_trade_reported_unit_values")
QUERY_DIR = ROOT / "queries/gold"
FACTS = dict(zip(VIEW_NAMES, ("fact_nso_observation", "fact_fao_production_observation", "fact_fao_sua_observation", "fact_fao_trade_observation")))
METRICS = {VIEW_NAMES[1]: {"5312": "area_ha", "5510": "production_t", "5412": "reported_yield_kg_ha"}, VIEW_NAMES[2]: SUA,
           VIEW_NAMES[3]: {"5610": "import_quantity_t", "5622": "import_value_usd", "5910": "export_quantity_t", "5922": "export_value_usd"}}


def load_definitions(directory=None):
    folder = directory or QUERY_DIR
    if {p.stem for p in folder.glob("*.sql")} != set(VIEW_NAMES):
        raise ValueError("Exactly four allowlisted semantic SQL definitions required")
    result = {}
    for name in VIEW_NAMES:
        sql = (folder / (name + ".sql")).read_text(encoding="utf-8").strip()
        refs = set(re.findall(r"\{\{(\w+)\}\}", sql))
        expected = {FACTS[name], "dim_source_geography" if name == VIEW_NAMES[0] else "dim_observation_type"}
        if (not sql.startswith("SELECT ") or refs != expected or ";" in sql or "--" in sql or "/*" in sql
                or re.search(r"\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|CALL|GRANT|REVOKE|EXECUTE|PREPARE|INTO)\b", sql, re.I)
                or re.search(r"\b(?:FROM|JOIN)\s+(?!\(|\{\{)[\w.]+", sql, re.I)):
            raise ValueError(f"Unsafe/noncanonical semantic SQL: {name}")
        result[name] = {"sql": sql, "sha256": hashlib.sha256((SEMANTIC_VERSION + "\n" + name + "\n" + sql + "\n").encode()).hexdigest(), "references": sorted(refs)}
    return result


def require_semantic_definitions():
    return {name: d["sha256"] for name, d in load_definitions().items()}


def definitions(outputs):
    result = {}
    for name, definition in load_definitions().items():
        sql = definition["sql"]
        for table in definition["references"]:
            snap = str(outputs[table]["snapshot_id"])
            if not snap.isdigit() or not 0 < int(snap) <= 2**63 - 1:
                raise ValueError("Semantic query requires positive pinned Gold snapshots")
            sql = sql.replace("{{" + table + "}}", f"iceberg.gold_local.{table} FOR VERSION AS OF {snap}")
        result[name] = sql
    return result


def trino_json(url, data=None, headers=None, timeout=15):
    request = urllib.request.Request(url, data=data, headers={"X-Trino-User": "tlcn_gold", **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read(4 * 1024 * 1024 + 1)
    if len(body) > 4 * 1024 * 1024:
        raise RuntimeError("Trino response exceeded bounded fetch budget")
    return json.loads(body)


def trino_endpoint():
    return "http://" + os.environ.get("TRINO_HOST", "trino") + ":" + os.environ.get("TRINO_PORT", "8080")


def trino_result(sql, max_rows=2048, max_seconds=180):
    endpoint = trino_endpoint()
    started = time.monotonic()
    payload = trino_json(endpoint + "/v1/statement", sql.encode("utf-8"), {
        "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME",
        "X-Trino-Session": "query_max_execution_time=2m,query_max_run_time=3m,query_max_planning_time=30s,query_max_memory=64MB"})
    rows, columns = [], []
    def checked_uri(uri):
        from urllib.parse import urlparse
        target, base = urlparse(uri), urlparse(endpoint)
        if (target.scheme, target.hostname, target.port) != (base.scheme, base.hostname, base.port):
            raise RuntimeError("Unexpected Trino polling host")
        return uri
    try:
        while True:
            if payload.get("error"):
                raise RuntimeError(f"Trino {payload.get('id')} {payload['error'].get('errorName')}: {payload['error'].get('message')}")
            columns = payload.get("columns", columns)
            rows.extend(payload.get("data", []))
            if len(rows) > max_rows or time.monotonic() - started > max_seconds:
                raise RuntimeError("Trino result/time budget exceeded")
            if not payload.get("nextUri"):
                return {"rows": rows, "columns": [{"name": c["name"], "type": c["type"].replace(", ", ",")} for c in columns],
                        "query_id": payload.get("id"), "stats": payload.get("stats", {}), "client_elapsed_seconds": time.monotonic() - started}
            payload = trino_json(checked_uri(payload["nextUri"]))
    except Exception:
        if payload.get("nextUri"):
            try:
                request = urllib.request.Request(checked_uri(payload["nextUri"]), method="DELETE", headers={"X-Trino-User": "tlcn_gold"})
                with urllib.request.urlopen(request, timeout=5):
                    pass
            except Exception:
                pass
        raise


def trino(sql):
    return trino_result(sql)["rows"]


def expected_schema(name):
    if name not in VIEW_NAMES:
        raise ValueError("Unknown semantic query")
    if name == VIEW_NAMES[0]:
        def dtype(c):
            return "varchar" if c.dbml_type.startswith("varchar") else "timestamp(6) with time zone" if c.dbml_type == "timestamptz" else c.dbml_type
        return [{"name": c.name, "type": dtype(c)} for c in load_spec()[FACTS[name]].columns] + [{"name": c, "type": "varchar"} for c in ("source_labels_json", "source_level", "interpretation_status")]
    keys = ["year_key", "reporter_geography_key", "partner_geography_key", "product_key"] if name == VIEW_NAMES[3] else ["year_key", "geography_key", "product_key"]
    result = [{"name": k, "type": "integer" if k == "year_key" else "varchar"} for k in keys]
    for code, measure in METRICS[name].items():
        result += [{"name": measure, "type": "decimal(22,2)" if name == VIEW_NAMES[3] else "decimal(18,2)"}, {"name": "present_" + code, "type": "bigint"}, {"name": "observation_" + code, "type": "varchar"}]
    if name == VIEW_NAMES[1]:
        result.append({"name": "computed_yield_kg_ha", "type": "decimal(25,3)"})
    if name == VIEW_NAMES[3]:
        result += [{"name": "reported_" + direction + "_unit_value_usd_t", "type": "decimal(24,2)"} for direction in ("import", "export")]
    return result


def semantic_record(name, outputs):
    if name not in VIEW_NAMES:
        raise ValueError("Query name outside allowlist")
    definition = load_definitions()[name]
    return {"version": SEMANTIC_VERSION, "definition_sha256": definition["sha256"], "bound_sql_sha256": fingerprint(definitions(outputs)[name]),
            "bindings": {t: {"uuid": outputs[t]["uuid"], "snapshot_id": str(outputs[t]["snapshot_id"])} for t in definition["references"]}, "output_schema": expected_schema(name)}


def verify_semantic_records(outputs, records):
    if set(records) != set(VIEW_NAMES):
        raise ValueError("Missing/unapproved semantic query records")
    for name, record in records.items():
        expected = semantic_record(name, outputs)
        if any(record.get(k) != v for k, v in expected.items()) or record.get("validation", {}).get("status") != "ACCEPT":
            raise ValueError(f"Semantic definition/version/snapshot/schema mismatch: {name}")
        if name == VIEW_NAMES[3]:
            from gold.trade_validation import verify_trade_summary
            verify_trade_summary(record["validation"], int(outputs[FACTS[name]]["rows"]))


def validate_semantic_queries(outputs, dq, store=None, context=None):
    sqls, records = definitions(outputs), {}
    for name, sql in sqls.items():
        from gold.trade_validation import resource_guard
        resource_guard()
        schema = trino_result(f"SELECT * FROM ({sql}) q LIMIT 0")["columns"]
        expected = expected_schema(name)
        dq.record("DQ10", name, "Trino parse/bind + exact expected output schema", expected, schema, expected == schema)
        if name == VIEW_NAMES[3]:
            from gold.trade_validation import validate_trade
            validation = validate_trade(outputs, dq, store=store, context=context)
            records[name] = {**semantic_record(name, outputs), "validation": validation}
            continue
        fact = FACTS[name]
        source = f"iceberg.gold_local.{fact} FOR VERSION AS OF {outputs[fact]['snapshot_id']}"
        years = [r[0] for r in trino(f"SELECT DISTINCT year_key FROM {source} ORDER BY year_key")]
        total = 0
        # The three small subjects have <=40k admitted observations. Validate
        # their entire scope exactly once, avoiding hundreds of retained plans.
        # Larger future inputs retain disjoint year work units. Trade always
        # uses its independently checkpointed reporter buckets above.
        work_years = [None] if int(outputs[fact]["rows"]) <= 40000 else years
        for year in work_years:
            resource_guard()
            condition = "TRUE" if year is None else f"year_key={int(year)}"
            label = "ALL_YEARS" if year is None else str(year)
            scoped = f"({sql}) v WHERE {condition}"
            rows = trino(f"SELECT count(*) FROM {scoped}")[0][0]
            total += rows
            keys = ["year_key", "reporter_geography_key", "partner_geography_key", "product_key"] if name == VIEW_NAMES[3] else ["year_key", "geography_key", "product_key"]
            expected_rows = trino(f"SELECT count(*) FROM {source} WHERE {condition}")[0][0] if name == VIEW_NAMES[0] else trino(f"SELECT count(*) FROM (SELECT {','.join(keys)} FROM {source} WHERE {condition} GROUP BY {','.join(keys)})")[0][0]
            dq.record("DQ10", f"{name}/{label}", "exact source observation/group coverage", expected_rows, rows, rows == expected_rows)
            if name in METRICS:
                predicates = [f"present_{c} NOT IN (0,1) OR ((present_{c}=0) <> (observation_{c} IS NULL))" for c in METRICS[name]]
                bad = trino(f"SELECT count(*) FROM {scoped} AND (" + " OR ".join(predicates) + ")")[0][0]
                dq.record("DQ10", f"{name}/{label}/presence", "present-null vs absent; unique contributor", 0, bad, bad == 0)
                # One pivot plan, not N duplicated pivot/join/scan plans. The
                # original UNION of 12 SUA pivots exhausted the local heap.
                cells = ",".join(f"ROW('{c}',v.observation_{c},v.{m},v.present_{c})" for c, m in METRICS[name].items())
                unpivot = f"SELECT {','.join('v.'+k for k in keys)},u.metric,u.observation_key,u.value FROM ({sql}) v CROSS JOIN UNNEST(ARRAY[{cells}]) AS u(metric,observation_key,value,presence) WHERE {condition} AND u.presence=1"
                metric = f"iceberg.gold_local.dim_observation_type FOR VERSION AS OF {outputs['dim_observation_type']['snapshot_id']}"
                expected_cells = f"SELECT {','.join('f.'+k for k in keys)},t.source_metric_id,f.observation_key,f.value FROM {source} f JOIN {metric} t ON f.observation_type_key=t.observation_type_key WHERE {condition}"
                diff = trino(f"SELECT count(*) FROM (({unpivot}) EXCEPT ALL ({expected_cells}))")[0][0]
                reverse = trino(f"SELECT count(*) FROM (({expected_cells}) EXCEPT ALL ({unpivot}))")[0][0]
                dq.record("DQ10", f"{name}/{label}/cells", "full grain/metric/contributor/value multiset both directions", 0, diff + reverse, diff + reverse == 0)
            if name == VIEW_NAMES[1]:
                bad = trino(f"SELECT count(*) FROM {scoped} AND computed_yield_kg_ha IS DISTINCT FROM (CASE WHEN area_ha>0 AND production_t IS NOT NULL THEN 1000.0*production_t/NULLIF(area_ha,0) END)")[0][0]
                dq.record("DQ12", f"{name}/{label}", "paired yield/denominator correctness", 0, bad, bad == 0)
            if name == VIEW_NAMES[3]:
                for direction in ("import", "export"):
                    bad = trino(f"SELECT count(*) FROM {scoped} AND reported_{direction}_unit_value_usd_t IS DISTINCT FROM (CASE WHEN {direction}_quantity_t>0 AND {direction}_value_usd IS NOT NULL THEN {direction}_value_usd/NULLIF({direction}_quantity_t,0) END)")[0][0]
                    dq.record("DQ12", f"{name}/{year}/{direction}", "paired trade unit value; zero denominator NULL", 0, bad, bad == 0)
        record = semantic_record(name, outputs)
        record["validation"] = {"status": "ACCEPT", "rows": total, "partition_years": years, "coverage": "FULL_SOURCE_SCOPE" if work_years == [None] else "ALL_DISJOINT_SOURCE_YEARS", "assertions": "schema/grain/presence/exact cells/paired ratios"}
        records[name] = record
    return records
