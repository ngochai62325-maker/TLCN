"""Exact, checkpointed Trade validation. Partitions never separate paired metrics.

A group's (year, reporter, partner, product) contains (year, reporter bucket).
Filtering on that subset commutes with its pivot/group-by. The four disjoint
reporter buckets cover every non-null K1 reporter, including present NULL cells.
No semantic SELECT definition, business grain or physical table is changed.
"""
import datetime
import logging
import time

from gold.keys import fingerprint

VERSION = "trade-exact-year-reporter4-20261026.1"
BUCKETS = 4
NAME = "vw_trade_reported_unit_values"
FACT = "fact_fao_trade_observation"
KEYS = ("year_key", "reporter_geography_key", "partner_geography_key", "product_key")
LOG = logging.getLogger(__name__)


def bucket(reporter):
    import re
    if not isinstance(reporter, str) or not re.fullmatch("[0-9a-f]{64}", reporter):
        raise ValueError("Trade reporter requires exact non-null K1 identity")
    return int(reporter[:2], 16) % BUCKETS


def bucket_sql(alias):
    return f"mod(from_base(substr({alias}.reporter_geography_key,1,2),16),{BUCKETS})"


def predicate(year, part, alias):
    if type(year) is not int or type(part) is not int or not 0 <= part < BUCKETS:
        raise ValueError("Invalid Trade partition")
    return f"{alias}.year_key={year} AND {bucket_sql(alias)}={part}"


def relations(outputs):
    from gold.views import definitions
    # Validates the allowlist and all snapshot literals before constructing SQL.
    semantic = definitions(outputs)[NAME]
    fact = f"iceberg.gold_local.{FACT} FOR VERSION AS OF {outputs[FACT]['snapshot_id']}"
    metric = "iceberg.gold_local.dim_observation_type FOR VERSION AS OF " + str(outputs["dim_observation_type"]["snapshot_id"])
    return semantic, fact, metric


def domain_sql(outputs):
    _, fact, metric = relations(outputs)
    invalid = " OR ".join(f"f.{k} IS NULL" for k in KEYS)
    return f"""SELECT f.year_key,{bucket_sql('f')} AS bucket,count(*) AS observations,
      count_if(t.observation_type_key IS NULL OR t.source_metric_id NOT IN ('5610','5622','5910','5922')
        OR {invalid} OR NOT regexp_like(f.reporter_geography_key,'^[0-9a-f]{{64}}$')) AS invalid
      FROM {fact} f LEFT JOIN {metric} t ON f.observation_type_key=t.observation_type_key
      GROUP BY f.year_key,{bucket_sql('f')} ORDER BY 1,2"""


def partitions(rows, expected_rows):
    result, seen = [], set()
    for year, part, count, invalid in rows:
        predicate(year, part, "f")
        if (year, part) in seen or type(count) is not int or count <= 0 or invalid != 0:
            raise ValueError("Invalid/overlapping Trade partition domain")
        seen.add((year, part))
        result.append({"year": year, "bucket": part, "input_rows": count})
    if sum(p["input_rows"] for p in result) != expected_rows:
        raise ValueError("Trade partition observations do not exhaust pinned physical input")
    return sorted(result, key=lambda p: (p["year"], p["bucket"]))


def partition_sql(outputs, year, part):
    from gold.views import METRICS
    sql, fact, metric = relations(outputs)
    fields = ",".join(KEYS)
    selected_keys = ",".join("v." + k for k in KEYS)
    cells = ",".join(f"ROW('{c}',v.observation_{c},v.{m},v.present_{c})" for c, m in METRICS[NAME].items())
    bad_presence = " OR ".join(f"present_{c} NOT IN (0,1) OR ((present_{c}=0) <> (observation_{c} IS NULL)) OR (present_{c}=0 AND {m} IS NOT NULL)" for c, m in METRICS[NAME].items())
    bad_ratio = " OR ".join(f"reported_{d}_unit_value_usd_t IS DISTINCT FROM (CASE WHEN {d}_quantity_t>0 AND {d}_value_usd IS NOT NULL THEN {d}_value_usd/NULLIF({d}_quantity_t,0) END)" for d in ("import", "export"))
    return f"""WITH v AS (SELECT * FROM ({sql}) q WHERE {predicate(year, part, 'q')}),
      e AS (SELECT f.year_key,f.reporter_geography_key,f.partner_geography_key,f.product_key,
        t.source_metric_id AS metric,f.observation_key,f.value
        FROM {fact} f JOIN {metric} t ON f.observation_type_key=t.observation_type_key
        WHERE {predicate(year, part, 'f')}),
      a AS (SELECT {selected_keys},u.metric,u.observation_key,u.value FROM v
        CROSS JOIN UNNEST(ARRAY[{cells}]) AS u(metric,observation_key,value,presence) WHERE u.presence=1)
      SELECT 'actual' AS kind,count(*) AS rows,count(DISTINCT ROW({fields})) AS groups,
        count_if({bad_presence}) AS presence_bad,count_if({bad_ratio}) AS ratio_bad FROM v
      UNION ALL SELECT 'expected',count(*),count(DISTINCT ROW({fields})),0,0 FROM e
      UNION ALL SELECT 'forward',count(*),0,0,0 FROM ((SELECT * FROM a) EXCEPT ALL (SELECT * FROM e))
      UNION ALL SELECT 'reverse',count(*),0,0,0 FROM ((SELECT * FROM e) EXCEPT ALL (SELECT * FROM a))"""


def check_result(rows, partition):
    result = {}
    for kind, count, groups, presence, ratio in rows:
        if kind in result or kind not in {"actual", "expected", "forward", "reverse"}:
            raise ValueError("Duplicate/unexpected Trade validation result")
        if any(type(x) is not int or x < 0 for x in (count, groups, presence, ratio)):
            raise ValueError("Invalid Trade validation counters")
        result[kind] = {"rows": count, "groups": groups, "presence_bad": presence, "ratio_bad": ratio}
    if set(result) != {"actual", "expected", "forward", "reverse"}:
        raise ValueError("Incomplete Trade validation result")
    a, e = result["actual"], result["expected"]
    if (e["rows"] != partition["input_rows"] or a["rows"] != a["groups"] or a["groups"] != e["groups"]
            or a["presence_bad"] or a["ratio_bad"] or result["forward"]["rows"] or result["reverse"]["rows"]):
        raise ValueError("Exact Trade partition mismatch: " + str(result))
    return result


def heap_state():
    from gold.views import trino_json, trino_endpoint
    data = trino_json(trino_endpoint() + "/v1/jmx/mbean/java.lang:type=Memory")
    heap = next(a["value"] for a in data["attributes"] if a["name"] == "HeapMemoryUsage")
    return {k: int(heap[k]) for k in ("used", "max", "committed")}


def resource_guard(max_wait=960):
    """Backpressure only: no configuration changes, GC requests or query retries.

    Let completed-query history expire normally before submitting more plans.
    The runtime's 15-minute history window can exceed query.max-history briefly.
    A persistent high heap or an active unrelated query fails admission closed.
    """
    from gold.views import trino_json, trino_endpoint
    started = time.monotonic()
    while True:
        heap = heap_state()
        history = trino_json(trino_endpoint() + "/v1/query")
        if any(q["state"] not in {"FINISHED", "FAILED"} for q in history):
            raise RuntimeError("Trade validation requires sequential idle Trino")
        now = datetime.datetime.now(datetime.timezone.utc)
        recent = sum((now - datetime.datetime.fromisoformat(q["queryStats"]["createTime"].replace("Z", "+00:00"))).total_seconds() < 900 for q in history)
        if heap["used"] < heap["max"] * .70 and recent < 80:
            return {**heap, "recent_queries": recent, "retained_queries": len(history), "wait_seconds": str(time.monotonic() - started)}
        if time.monotonic() - started >= max_wait:
            raise RuntimeError(f"Trino heap/history admission blocked: {heap}, recent={recent}")
        LOG.warning("Trade backpressure: heap=%s recent=%s; no query submitted", heap, recent)
        time.sleep(5)


def settle_heap(max_wait=90):
    """Accept a completed result only after natural GC restores headroom.

    A single used-heap sample includes reclaimable young-generation garbage.
    Wait without submitting SQL/forcing GC; persist failure if pressure remains.
    """
    started = time.monotonic()
    peak, samples = 0, 0
    while True:
        heap = heap_state()
        peak, samples = max(peak, heap["used"]), samples + 1
        if heap["used"] < heap["max"] * .70:
            return {**heap, "sampled_peak_bytes": peak, "samples": samples, "cooldown_seconds": str(time.monotonic() - started)}
        if time.monotonic() - started >= max_wait:
            raise RuntimeError(f"Trino heap did not settle after completed partition: {heap}")
        time.sleep(2)


def scope_identity(outputs, context):
    from gold.views import semantic_record
    return {"validator": VERSION, "context": context, "semantic": semantic_record(NAME, outputs),
            "outputs": {n: {k: o[k] for k in ("uuid", "snapshot_id", "metadata_location", "rows", "data_files")} for n, o in outputs.items()}}


def valid_checkpoint(saved, scope, partition):
    if (not saved or saved.get("status") != "ACCEPT" or saved.get("scope") != scope or saved.get("partition") != partition
            or saved.get("seal") != fingerprint({k: v for k, v in saved.items() if k != "seal"})):
        raise ValueError("Trade checkpoint incomplete/tampered/incompatible")
    rows = [[kind, *[r[k] for k in ("rows", "groups", "presence_bad", "ratio_bad")]] for kind, r in saved["result"].items()]
    check_result(rows, partition)
    if not saved.get("query_id"):
        raise ValueError("Trade checkpoint lacks execution provenance")
    return saved


def audit_values(value):
    # Runtime statistics are descriptive strings, never binary-float measures.
    if isinstance(value, float):
        return str(value)
    if isinstance(value, dict):
        return {k: audit_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [audit_values(v) for v in value]
    return value


def verify_trade_summary(validation, input_rows):
    import re
    domain = validation.get("domain", [])
    checked = partitions([[p["year"], p["bucket"], p["input_rows"], 0] for p in domain], input_rows)
    seals = validation.get("checkpoint_seals", [])
    if (validation.get("status") != "ACCEPT" or validation.get("validator") != VERSION
            or validation.get("coverage") != "ALL_DISJOINT_YEAR_REPORTER_BUCKETS" or checked != domain
            or validation.get("input_rows") != input_rows or validation.get("partitions") != len(domain)
            or validation.get("expected_partitions") != len(domain) or len(seals) != len(domain) or len(set(seals)) != len(seals)
            or any(not isinstance(s, str) or not re.fullmatch("[0-9a-f]{64}", s) for s in seals)
            or validation.get("partition_years") != sorted({p["year"] for p in domain})
            or not re.fullmatch("[0-9a-f]{64}", validation.get("scope", ""))
            or validation.get("progress_key") != "validation/" + validation["scope"] + "/progress.json"):
        raise ValueError("Incomplete/incompatible Trade validation coverage")


def validate_trade(outputs, dq, store=None, context=None, year=None, part=None):
    from gold.views import trino_result
    if store is None or not context or not context.get("code_fingerprint"):
        raise ValueError("Trade validation requires persistent store and pinned code context")
    admission = resource_guard()
    domain_result = trino_result(domain_sql(outputs))
    domain = partitions(domain_result["rows"], int(outputs[FACT]["rows"]))
    identity = scope_identity(outputs, context)
    scope = fingerprint(identity)
    prefix = f"validation/{scope}/"
    selected = [p for p in domain if (year is None or p["year"] == year) and (part is None or p["bucket"] == part)]
    if (year is not None or part is not None) and not selected:
        raise ValueError("Selected partition outside actual pinned Trade domain")
    progress = {"scope": scope, "identity": identity, "domain": domain, "domain_query_id": domain_result["query_id"], "status": "INCOMPLETE", "accepted": [], "admission": admission}
    total_groups, completed_rows, reused = 0, 0, 0
    started = time.monotonic()
    try:
        for partition in selected:
            key = prefix + str(partition["year"]) + "_" + str(partition["bucket"]) + ".json"
            saved = store.get(key)
            if saved is not None:
                saved = valid_checkpoint(saved, scope, partition)
                reused += 1
            else:
                before = resource_guard()
                outcome = trino_result(partition_sql(outputs, partition["year"], partition["bucket"]))
                result = check_result(outcome["rows"], partition)
                progress["last_attempt"] = {"partition": partition, "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "exact_result": result}
                after = settle_heap()
                saved = {"status": "ACCEPT", "scope": scope, "partition": partition, "result": result,
                         "query_id": outcome["query_id"], "stats": audit_values(outcome["stats"]), "elapsed_seconds": str(outcome["client_elapsed_seconds"]),
                         "heap_before": before, "heap_after": after, "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
                saved["seal"] = fingerprint(saved)
                store.put(key, saved, immutable=True)
            result = saved["result"]
            label = f"{NAME}/{partition['year']}/bucket{partition['bucket']}"
            dq.record("DQ10", label, "disjoint exhaustive partition; exact groups and unique grain", result["expected"]["groups"], result["actual"]["groups"])
            dq.record("DQ10", label + "/cells", "full grain/metric/contributor/value EXCEPT ALL both directions", [0, 0], [result["forward"]["rows"], result["reverse"]["rows"]])
            dq.record("DQ10", label + "/presence", "present NULL vs absent; exact contributor multiplicity", 0, result["actual"]["presence_bad"])
            dq.record("DQ12", label, "both paired trade unit values; zero/missing denominators NULL", 0, result["actual"]["ratio_bad"])
            completed_rows += partition["input_rows"]
            total_groups += result["actual"]["groups"]
            progress["accepted"].append({"partition": partition, "seal": saved["seal"], "checkpoint": key})
            store.put(prefix + "progress.json", progress)
            LOG.info("TRADE_PARTITION_ACCEPT year=%s bucket=%s observations=%s groups=%s reused=%s", partition["year"], partition["bucket"], partition["input_rows"], result["actual"]["groups"], reused)
        complete = len(selected) == len(domain) and completed_rows == int(outputs[FACT]["rows"])
        progress.update(status="ACCEPT" if complete else "INCOMPLETE", completed_input_rows=completed_rows, groups=total_groups, reused_checkpoints=reused, elapsed_seconds=str(time.monotonic() - started))
        store.put(prefix + "progress.json", progress)
        return {"status": progress["status"], "validator": VERSION, "scope": scope, "rows": total_groups, "input_rows": completed_rows,
                "partition_years": sorted({p["year"] for p in selected}), "partitions": len(selected), "expected_partitions": len(domain),
                "coverage": "ALL_DISJOINT_YEAR_REPORTER_BUCKETS" if complete else "PARTIAL", "progress_key": prefix + "progress.json",
                "checkpoint_seals": [p["seal"] for p in progress["accepted"]], "domain": selected}
    except Exception as exc:
        progress.update(status="INCOMPLETE", error={"type": type(exc).__name__, "message": str(exc)})
        store.put(prefix + "progress.json", progress)
        raise
