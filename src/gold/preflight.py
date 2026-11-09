"""Cheap read-only physical baseline without allocating a Spark driver."""
import os

from gold.admission import runtime_resources, success_context
from gold.spec import ROOT, validate_mapping


def validate_gold_schema(table, spec):
    actual = [(f.name, str(f.field_type).replace(" ", ""), f.required) for f in table.schema().fields]
    expected = [(c.name, "string" if c.dbml_type.startswith("varchar") else {"integer": "int", "bigint": "long"}.get(c.dbml_type, c.dbml_type), c.required) for c in spec.columns]
    if actual != expected:
        raise ValueError(f"Gold physical schema/requiredness drift: {spec.name}")


def inspect():
    from pyiceberg.catalog import load_catalog
    from silver.contract.loader import SilverContractLoader
    from gold.views import require_semantic_definitions, trino
    properties = {"type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse",
                  "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"),
                  "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1"}
    catalog = load_catalog("gold_readonly_preflight", **properties)
    report = {"static": validate_mapping(), "resources": runtime_resources(), "sources": {}, "status": "READY"}
    proof = success_context()
    loader = SilverContractLoader(str(ROOT / "contracts/silver"))
    for source, pin in proof["pins"].items():
        table = catalog.load_table(pin["table"].removeprefix("iceberg."))
        snapshot = table.current_snapshot()
        actual = {f.name: str(f.field_type).replace(" ", "") for f in table.schema().fields}
        expected = {c["name"]: c["data_type"].replace("integer", "int").replace("bigint", "long").replace(" ", "") for c in loader.load_contract(source).columns}
        expected.update({c: "string" for c in ("_ingestion_run_id", "_ingestion_batch_id", "_source_id", "_source_file", "_source_checksum")})
        expected.update({c: "long" for c in ("_ingestion_chunk_id", "_source_snapshot_id")})
        expected.update({c: "timestamptz" for c in ("_ingestion_timestamp", "_silver_processed_at")})
        warnings = table.schema().find_field("_dq_warnings").field_type.element_type
        warning_ok = [(f.name, str(f.field_type)) for f in warnings.fields] == [(n, "string") for n in ("rule_id", "error_message", "failed_column")]
        if (str(table.metadata.table_uuid) != pin["uuid"] or not snapshot or str(snapshot.snapshot_id) != pin["snapshot_id"]
                or any(actual.get(c) != t for c, t in expected.items()) or not warning_ok
                or set(actual) != set(expected) | {"_dq_warnings"}):
            raise ValueError(f"Physical Silver proof/schema mismatch: {source}")
        report["sources"][source] = {"uuid": str(table.metadata.table_uuid), "snapshot_id": str(snapshot.snapshot_id), "contract_schema": "MATCH", "metadata_total_records": dict(snapshot.summary.additional_properties).get("total-records"), "silver_success_proof_rows": proof["proofs"][source]["commit_verified"]["rows"]}
    namespaces = catalog.list_namespaces()
    report["gold_tables"] = [list(x) for x in catalog.list_tables("gold_local")] if ("gold_local",) in namespaces else []
    from gold import OWNER, MODEL_VERSION
    from gold.spec import load_spec
    for identifier in report["gold_tables"]:
        gold = catalog.load_table(tuple(identifier))
        if (identifier[-1] not in load_spec() or gold.metadata.properties.get("gold.owner") != OWNER
                or gold.metadata.properties.get("gold.model") != MODEL_VERSION
                or not gold.metadata.location.startswith("s3://warehouse/gold_local/")):
            raise ValueError(f"Uninventoried/unowned Gold table: {identifier}")
        validate_gold_schema(gold, load_spec()[identifier[-1]])
    from gold.publication import Publication, PREFIX
    store = Publication()
    active = store.get("active.json")
    if active and (active.get("owner") != OWNER or active.get("status") != "PUBLISHED"):
        raise ValueError("Incomplete/unowned Gold batch; inspect catalog and resume its owner run")
    if not active and store.client.list_objects_v2(Bucket="warehouse", Prefix=PREFIX, MaxKeys=1).get("Contents"):
        raise ValueError("Uninventoried Gold publication prefix")
    report["trino_version"] = trino("SELECT version()")[0][0]
    report["semantic_delivery"] = "VERSIONED_READ_ONLY_SQL"
    report["semantic_definitions"] = require_semantic_definitions()
    report["silver_run_id"] = proof["silver_run_id"]
    return report
