"""Serial stage executor shared by the allowlisted runner and manual Airflow DAG."""
import logging

from gold import OUTPUT_NAMESPACE, ACTIONS
from gold.admission import batch_context, runtime_resources, success_context, writer_guard
from gold.catalog import identity, pinned_read, check_owned, write_snapshot, file_state
from gold.dimensions import build_dimensions
from gold.facts import BUILD_ORDER, TARGETS, transform, monthly_snapshot_frame
from gold.publication import Publication
from gold.quality import Quality, validate_input, validate_table, reconcile
from gold.spec import load_spec, ROOT
from gold.views import require_semantic_definitions, validate_semantic_queries

LOG = logging.getLogger(__name__)


def inventory(spark, specs):
    namespaces = [r[0] for r in spark.sql("SHOW NAMESPACES IN iceberg").collect()]
    if "gold_local" not in namespaces:
        return
    found = spark.sql("SHOW TABLES IN iceberg.gold_local").collect()
    for row in found:
        if row.tableName not in specs:
            raise ValueError(f"Uninventoried Gold table: {row.tableName}")
        check_owned(identity(spark, f"{OUTPUT_NAMESPACE}.{row.tableName}"), row.tableName)


def read_inputs(spark, proof, dq, validate=False):
    from silver.contract.loader import SilverContractLoader
    from silver.local_profile import local_contract
    loader = SilverContractLoader(str(ROOT / "contracts/silver"))
    inputs, contracts = {}, {}
    for source, pin in proof["pins"].items():
        current = identity(spark, pin["table"])
        if not current or current["uuid"] != pin["uuid"] or current["snapshot_id"] != pin["snapshot_id"]:
            raise ValueError(f"Silver moved beyond persisted successful proof: {source}")
        df = pinned_read(spark, pin["table"], pin)
        contract = local_contract(loader, source)
        if validate:
            validate_input(source, df, contract, pin, proof["proofs"][source], dq)
        inputs[source], contracts[source] = df, contract
    return inputs, contracts


def current_dimensions(spark, specs):
    return {name: spark.table(f"{OUTPUT_NAMESPACE}.{name}") for name in specs if name.startswith("dim_") and spark.catalog.tableExists(f"{OUTPUT_NAMESPACE}.{name}")}


def execute(spark, action, owner_run_id, silver_run_id=None, source=None):
    if action not in ACTIONS or not owner_run_id or (action == "fact" and source not in BUILD_ORDER):
        raise ValueError("Unapproved Gold stage/source/owner")
    with writer_guard() as guard:
        proof = success_context(silver_run_id)
        specs = load_spec()
        inventory(spark, specs)
        resources = runtime_resources()
        # Fail before creating namespace, publication objects, or any physical Gold table.
        require_semantic_definitions()
        store = Publication(guard=guard)
        dq = Quality(proof["pins"])
        inputs, contracts = read_inputs(spark, proof, dq, validate=action in {"preflight", "dimensions"})
        context = batch_context(proof)
        if action == "preflight":
            store.check_claim(context, owner_run_id)
            return {"status": "READY", "context": context, "resources": resources, "dq": dq.results}
        state = store.claim(context, owner_run_id)
        state["resources"] = resources
        if state["status"] == "FAILED_NOT_PUBLISHED":
            state.setdefault("failed_attempts", []).append({"error": state.get("error"), "dq": state["dq"]})
            state["dq"] = []
        try:
            guard()
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold_local")
            if action == "dimensions":
                dimensions = {}
                for name, raw in build_dimensions(spark, inputs):
                    LOG.info("Gold dimension %s", name)
                    df = specs[name].project(raw)
                    validate_table(name, df, specs[name], dimensions, dq)
                    state["outputs"][name] = write_snapshot(spark, specs[name], df, dq, guard)
                    dimensions[name] = spark.table(f"{OUTPUT_NAMESPACE}.{name}")
                    store.save(state)
                state["status"] = "DIMENSIONS_BUILT"
            elif action == "fact":
                expected_previous = list(BUILD_ORDER[:BUILD_ORDER.index(source)])
                if not set(expected_previous).issubset(state["completed_sources"]) or not all(n in state["outputs"] for n in specs if n.startswith("dim_")):
                    raise ValueError("Gold source dependency/order not satisfied")
                dimensions = current_dimensions(spark, specs)
                frames = transform(source, inputs[source], contracts[source].business_key, proof["pins"][source])
                for name, raw in frames.items():
                    if name == "fact_price_monthly":
                        raw = monthly_snapshot_frame(source, raw, inputs, {s: c.business_key for s, c in contracts.items()}, proof["pins"], state["completed_sources"])
                    df = specs[name].project(raw)
                    validate_table(name, df, specs[name], dimensions, dq)
                    state["outputs"][name] = write_snapshot(spark, specs[name], df, dq, guard)
                    store.save(state)
                outputs = {name: spark.table(f"{OUTPUT_NAMESPACE}.{name}") for name in frames}
                reconcile(source, inputs[source], {n: specs[n].project(f) for n, f in frames.items()}, outputs, contracts[source].business_key, dq)
                if source not in state["completed_sources"]:
                    state["completed_sources"].append(source)
                state["status"] = "FACTS_BUILDING"
            elif action == "validate":
                if set(state["completed_sources"]) != set(BUILD_ORDER) or set(state["outputs"]) != set(specs):
                    raise ValueError("Cannot validate incomplete Gold scope")
                inputs, contracts = read_inputs(spark, proof, dq, validate=True)
                outputs = {}
                for name, saved in state["outputs"].items():
                    now = identity(spark, f"{OUTPUT_NAMESPACE}.{name}")
                    if now["uuid"] != saved["uuid"] or now["snapshot_id"] != saved["snapshot_id"]:
                        raise ValueError(f"Gold output moved since stage readback: {name}")
                    outputs[name] = pinned_read(spark, f"{OUTPUT_NAMESPACE}.{name}", saved)
                dims = {n: d for n, d in outputs.items() if n.startswith("dim_")}
                for name, df in outputs.items():
                    validate_table(name, df, specs[name], dims, dq)
                for src in BUILD_ORDER:
                    frames = transform(src, inputs[src], contracts[src].business_key, proof["pins"][src])
                    reconcile(src, inputs[src], {n: specs[n].project(f) for n, f in frames.items()}, outputs, contracts[src].business_key, dq)
                # Dimensions rebuild deterministically; exact set equality includes labels/FDs/calendar.
                for name, raw in build_dimensions(spark, inputs):
                    dq.same("DQ03", name, specs[name].project(raw), outputs[name], "dimension rebuild exact multiset equality")
                guard()
                state["semantic_queries"] = validate_semantic_queries(state["outputs"], dq, store=store, context=context)
                semantic_checks(outputs, dq)
                state["reconciliation"] = {s: {"silver_rows": proof["proofs"][s]["commit_verified"]["rows"], "key_set": "EXACT", "values_and_lineage": "EXACT"} for s in BUILD_ORDER}
                state["status"] = "VERIFIED"
            elif action == "publish":
                if state["status"] not in {"VERIFIED", "PUBLISHED"}:
                    raise ValueError("Publication requires completed readback/DQ")
                for name, saved in state["outputs"].items():
                    now = identity(spark, f"{OUTPUT_NAMESPACE}.{name}")
                    if now["uuid"] != saved["uuid"] or now["snapshot_id"] != saved["snapshot_id"] or file_state(spark, f"{OUTPUT_NAMESPACE}.{name}") != saved["data_files"]:
                        raise ValueError(f"Output changed before publication: {name}")
                if state["status"] == "PUBLISHED":
                    return {"status": "PUBLISHED", "batch": state["batch"], "replay": "NOOP"}
                state["dq"].extend(dq.results)
                store.publish(state)
                return {"status": "PUBLISHED", "batch": state["batch"], "outputs": state["outputs"]}
            state["dq"].extend(dq.results)
            store.save(state)
            return {"status": state["status"], "batch": state["batch"], "outputs": {n: {"rows": v["rows"], "snapshot_id": v["snapshot_id"], "write": v["write"]} for n, v in state["outputs"].items()}}
        except Exception as exc:
            state["dq"].extend(dq.results)
            state["status"] = "FAILED_NOT_PUBLISHED"
            state["error"] = {"type": type(exc).__name__, "message": str(exc), "action": action, "source": source, "recovery": "same owner/context; inspect catalog; no DROP/rollback"}
            store.save(state)
            raise


def semantic_checks(outputs, dq):
    """Scope restrictions are explicit: no unsupported aggregate is published."""
    metrics = outputs["dim_observation_type"]
    from pyspark.sql import functions as F
    from gold.dimensions import ZERO_GEOGRAPHY
    dq.zero("DQ13", "metrics", "stocks not across time; ratios never additive", metrics.where("(source_metric_id IN ('5113','Beginning Stocks','Ending Stocks') AND aggregation_rule <> 'NO_SUM_OVER_TIME') OR (source_metric_id IN ('5412','Yield','Milling Rate (.9999)') AND aggregation_rule <> 'NEVER_SUM')"))
    dq.zero("DQ18", "WB geography", "technical member restricted to WB benchmark", outputs["dim_price_series"].where((F.col("geography_key") == ZERO_GEOGRAPHY) != (F.col("source_system_key") == __import__("gold.keys", fromlist=["key"]).key("dim_source_system", ["worldbank_pinksheet"]))))
    dq.record("DQ17", "outputs", "all 17 output snapshots reconciled before publication", 17, len(outputs), len(outputs) == 17)
