"""Gold Layer Orchestrator (Star Schema: 8 Dimensions + 9 Facts + 4 Semantic Views).

Mental Model — 4-Step Gold Execution Flow:
==========================================
- Step 1: Admission & Preflight (`action="preflight"`)
    Verify that all 9 Silver tables succeeded in Airflow, check snapshot UUIDs,
    and run input data-quality rules (`validate_input`) before any Gold write.
- Step 2: Build 8 Dimensions (`action="dimensions"`, see `dimensions.py`)
    Build and checkpoint `dim_source_system`, `dim_year`, `dim_month`, `dim_date`,
    `dim_source_geography`, `dim_source_product`, `dim_observation_type`, and
    `dim_price_series` into `iceberg.gold_local.*`.
- Step 3: Build 9 Facts & Quality Gates (`action="fact"`, see `facts.py` & `quality.py`)
    Project each Silver source into its Gold fact table(s), verify primary/foreign
    keys (`validate_table`), write the Iceberg snapshot, and reconcile row counts
    and values against Silver (`reconcile`).
- Step 4: Validate Semantic SQL Views & Publish (`action="validate"` -> `"publish"`, see `views.py`)
    Bind the 4 semantic SQL queries (`queries/gold/*.sql`) to exact Gold snapshot IDs,
    validate them through Trino, and atomically publish `latest.json` in MinIO.
"""
from contextlib import contextmanager, ExitStack
import copy
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import urllib.error
import urllib.request

from gold import OUTPUT_NAMESPACE, ACTIONS, MODEL_VERSION, OWNER, BUILD_ORDER
from gold.keys import canonical_json, fingerprint
from gold.spec import (
    load_spec,
    validate_mapping,
    ROOT,
    identity,
    pinned_read,
    check_owned,
    write_snapshot,
    file_state,
)
from gold.views import (
    SEMANTIC_VERSION,
    VIEW_NAMES,
    definitions,
    expected_schema,
    require_semantic_definitions,
    trino_result,
    validate_semantic_queries,
    verify_semantic_records,
)
from silver.framework import DATASETS, MAPPINGS, require_profile, validate_registry

_BUILD_ORDER = BUILD_ORDER

LOG = logging.getLogger(__name__)
PREFIX = "gold_publications/local_nine/"
_VERIFIED_BATCH_CACHE = {}


# ---------------------------------------------------------------------------
# Admission, Locking & Resources (formerly `admission.py`)
# ---------------------------------------------------------------------------

@contextmanager
def writer_guard():
    """Reuse the exact existing source lock protocol; one Gold writer across APIs/CLI."""
    from ingestion.storage.metadata_repository import MetadataRepository
    if not os.environ.get("INGESTION_DB_URL"):
        raise ValueError("Gold requires explicit INGESTION_DB_URL")
    repo = MetadataRepository()
    conn = repo._get_connection()
    lock_key = repo._derive_lock_key("tlcn-local:gold_local")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another Gold writer owns the advisory lock")
        def assert_alive():
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("Gold writer lock connection lost")
        with ExitStack() as stack:
            for source in sorted(DATASETS):
                stack.enter_context(repo.source_lock(source))
            yield assert_alive
    finally:
        conn.close()  # Session advisory lock is released, including on failure.


def _read_xcom(cursor, run_id, task_id):
    cursor.execute("SELECT value FROM xcom WHERE dag_id=%s AND run_id=%s AND task_id=%s AND key='return_value' AND map_index=-1", ("rice_lakehouse_silver_pipeline", run_id, task_id))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError(f"Missing/ambiguous persisted Silver proof: {task_id}")
    value = rows[0][0]
    if isinstance(value, memoryview):
        value = value.tobytes()
    return json.loads(value)


def success_context(silver_run_id=None):
    """Read Airflow metadata with psycopg2, available in both existing images."""
    from ingestion.storage.metadata_repository import MetadataRepository
    require_profile("local_nine")
    validate_registry()
    validate_mapping()
    repo = MetadataRepository()
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dag_id,run_id,state FROM dag_run WHERE dag_id IN ('rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline') AND state IN ('running','queued')")
            if cur.fetchall():
                raise ValueError("Active Bronze/Silver run: Gold admission blocked")
            latest = {}
            for dag in ("rice_lakehouse_ingestion", "rice_lakehouse_silver_pipeline"):
                cur.execute("SELECT run_id,state FROM dag_run WHERE dag_id=%s ORDER BY execution_date DESC LIMIT 1", (dag,))
                row = cur.fetchone()
                if row is None or row[1] != "success":
                    raise ValueError(f"Latest {dag} outcome is not SUCCESS; review before Gold")
                latest[dag] = row[0]
            run_id = silver_run_id or latest["rice_lakehouse_silver_pipeline"]
            if run_id != latest["rice_lakehouse_silver_pipeline"]:
                raise ValueError("Gold admits the latest proven Silver batch only")
            signal = _read_xcom(cur, run_id, "publish_silver_signal")
            if signal.get("silver_status") != "DQ_PASSED" or signal.get("profile") != "local_nine" or set(signal.get("datasets", [])) != set(DATASETS):
                raise ValueError("Incomplete Silver success signal")
            audit = _read_xcom(cur, run_id, "silver_data_quality_audit")
            if len(audit) != 9 or any(a.get("status") != "PASSED" for a in audit):
                raise ValueError("Persisted Silver DQ did not pass all sources")
            proofs, pins = {}, {}
            for source, dataset, target in MAPPINGS:
                cur.execute("SELECT state FROM task_instance WHERE dag_id=%s AND run_id=%s AND task_id=%s AND map_index=-1", ("rice_lakehouse_silver_pipeline", run_id, "transform_" + dataset))
                row = cur.fetchone()
                if row is None or row[0] != "success":
                    raise ValueError(f"Silver task not successful: {source}")
                proof = _read_xcom(cur, run_id, "transform_" + dataset)[source]
                commit = proof.get("commit_verified", {})
                if (proof.get("status") != "SUCCESS" or proof.get("pipeline_run_id") != run_id
                        or proof.get("target_table") != f"iceberg.silver_local.{target}"
                        or not commit.get("uuid") or not str(commit.get("snapshot_id", "")).isdigit()
                        or int(commit["snapshot_id"]) <= 0):
                    raise ValueError(f"Incomplete physical Silver proof: {source}")
                proofs[source] = proof
                pins[source] = {"table": f"iceberg.silver_local.{target}", "uuid": commit["uuid"], "snapshot_id": str(commit["snapshot_id"])}
    return {"silver_run_id": run_id, "pins": pins, "proofs": proofs}


def code_fingerprint():
    paths = sorted([*(ROOT / "src/gold").glob("*.py"), *(ROOT / "jobs/gold").glob("*.py"), *(ROOT / "queries/gold").glob("*.sql"), ROOT / "dags/dag_gold_pipeline.py", ROOT / "jobs/silver/spark_api_server.py"])
    return fingerprint({str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})


def batch_context(proof):
    refs = [ROOT / "config/ingestion/source_registry.yaml", ROOT / "docs/gold/dim_fact_v3.dbml", ROOT / "docs/gold/source_to_gold_mapping.csv", *(ROOT / "contracts/silver").glob("*.yaml")]
    return {"model": MODEL_VERSION, "code_fingerprint": code_fingerprint(), "reference_fingerprints": {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(refs)}, "source_scope": list(DATASETS), "inputs": proof["pins"]}


def runtime_resources():
    """Actual WSL headroom; runner is capped at 3 GiB by canonical Compose."""
    if not Path("/proc/meminfo").exists():
        raise RuntimeError("Run Gold only in the existing Linux runner")
    memory = {k: int(v.strip().split()[0]) for k, v in (line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())}
    available = memory["MemAvailable"]
    if available < 1536 * 1024:
        raise RuntimeError(f"Gold resource admission blocked: WSL MemAvailable={available} KiB (<1.5 GiB)")
    return {"wsl_available_kib": available}


# ---------------------------------------------------------------------------
# Publication & Manifest Store (formerly `publication.py`)
# ---------------------------------------------------------------------------

def verify_manifest(manifest):
    if (manifest.get("owner") != OWNER or manifest.get("model") != MODEL_VERSION
            or manifest.get("publication_status") != "PUBLISHED" or manifest.get("dq") != "ACCEPT"
            or fingerprint(manifest.get("context")) != manifest.get("batch")
            or set(manifest.get("outputs", {})) != set(load_spec())):
        raise ValueError("Invalid Gold manifest/fingerprint/table inventory")
    verify_semantic_records(manifest["outputs"], manifest.get("semantic_queries", {}))


class ArrowObjects:
    """Small S3 object adapter using pyarrow already pinned in the Spark image."""
    def __init__(self):
        from pyarrow import fs
        self.fs = fs.S3FileSystem(access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                                  region=os.getenv("AWS_REGION", "us-east-1"), scheme="http",
                                  endpoint_override=os.environ["MINIO_ENDPOINT"].removeprefix("http://"))

    def get_object(self, Bucket, Key):
        path = Bucket + "/" + Key
        from pyarrow import fs
        if self.fs.get_file_info(path).type == fs.FileType.NotFound:
            raise FileNotFoundError(path)
        with self.fs.open_input_file(path) as stream:
            body = stream.read()
        return {"Body": io.BytesIO(body)}

    def put_object(self, Bucket, Key, Body, ContentType):
        with self.fs.open_output_stream(Bucket + "/" + Key) as stream:
            stream.write(Body)

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        from pyarrow import fs
        selector = fs.FileSelector(Bucket + "/" + Prefix.rstrip("/"), recursive=True, allow_not_found=True)
        paths = self.fs.get_file_info(selector)
        return {"Contents": [{"Key": p.path.removeprefix(Bucket + "/")} for p in paths if p.type == fs.FileType.File][:MaxKeys]}


class Publication:
    def __init__(self, client=None, guard=lambda: None):
        if client is None:
            client = ArrowObjects()
        self.client = client
        self.bucket = "warehouse"
        self.guard = guard

    def get(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=PREFIX + key)
            return json.loads(response["Body"].read())
        except FileNotFoundError:
            return None

    def put(self, key, value, immutable=False):
        existing = self.get(key)
        if immutable and existing is not None:
            if existing != value:
                raise ValueError(f"Immutable publication conflict: {key}")
            return
        body = canonical_json(value).encode("utf-8")
        self.guard()
        self.client.put_object(Bucket=self.bucket, Key=PREFIX + key, Body=body, ContentType="application/json")
        if self.get(key) != value:
            raise RuntimeError(f"Publication readback failed: {key}")

    def check_claim(self, context, owner_run_id):
        """Read-only admission; a READY preflight must not hide an old owner."""
        batch = fingerprint(context)
        active = self.get("active.json")
        saved = self.get(f"working/{batch}.json")
        if saved and (saved.get("owner") != OWNER or saved.get("model") != MODEL_VERSION or saved["context"] != context):
            raise ValueError("Batch fingerprint collision")
        if active:
            if active.get("owner") != OWNER:
                raise ValueError("Unowned Gold publication prefix")
            if active["status"] != "PUBLISHED" and (active["batch"] != batch or active["owner_run_id"] != owner_run_id):
                raise ValueError("Another/incomplete batch owns Gold; resume its run after catalog inspection")
        else:
            unknown = self.client.list_objects_v2(Bucket=self.bucket, Prefix=PREFIX, MaxKeys=2).get("Contents", [])
            initial_claim = (saved and saved.get("owner_run_id") == owner_run_id and saved.get("status") == "ADMITTED"
                             and not saved.get("outputs") and {x["Key"] for x in unknown} == {PREFIX + f"working/{batch}.json"})
            if unknown and not initial_claim:
                raise ValueError("Uninventoried Gold publication prefix")
        return batch, saved

    def claim(self, context, owner_run_id):
        batch, saved = self.check_claim(context, owner_run_id)
        state = saved or {"owner": OWNER, "batch": batch, "context": context, "outputs": {}, "completed_sources": [], "dq": [], "status": "ADMITTED", "model": MODEL_VERSION}
        state["owner_run_id"] = owner_run_id
        self.save(state)
        return state

    def save(self, state):
        self.put(f"working/{state['batch']}.json", state)
        self.put("active.json", {k: state[k] for k in ("owner", "batch", "owner_run_id", "status")})

    def publish(self, state):
        if state["status"] != "VERIFIED" or not state.get("semantic_queries") or any(r["status"] not in {"ACCEPT", "WARN"} for r in state["dq"]):
            raise ValueError("Partial/failed batch cannot publish")
        payload = {"owner": OWNER, "model": MODEL_VERSION, "batch": state["batch"], "context": state["context"], "outputs": {k: {f: v[f] for f in ("uuid", "snapshot_id", "rows", "data_files")} for k, v in state["outputs"].items()}, "semantic_queries": state["semantic_queries"], "dq": "ACCEPT", "reconciliation": state["reconciliation"], "publication_status": "PUBLISHED"}
        verify_manifest(payload)
        self.put(f"{state['batch']}.json", payload, immutable=True)
        self.put("published.json", {"owner": OWNER, "batch": state["batch"], "manifest": PREFIX + state["batch"] + ".json"})
        state["status"] = "PUBLISHED"
        self.save(state)
        return payload


# ---------------------------------------------------------------------------
# Preflight, Recovery, Consumer Window & Runner API (formerly separate files)
# ---------------------------------------------------------------------------

def validate_gold_schema(table, spec):
    actual = [(f.name, str(f.field_type).replace(" ", ""), f.required) for f in table.schema().fields]
    expected = [(c.name, "string" if c.dbml_type.startswith("varchar") else {"integer": "int", "bigint": "long"}.get(c.dbml_type, c.dbml_type), c.required) for c in spec.columns]
    if actual != expected:
        raise ValueError(f"Gold physical schema/requiredness drift: {spec.name}")


def inspect():
    from pyiceberg.catalog import load_catalog
    from silver.engine import SilverContractLoader
    from gold.views import trino
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
    for identifier in report["gold_tables"]:
        gold = catalog.load_table(tuple(identifier))
        if (identifier[-1] not in load_spec() or gold.metadata.properties.get("gold.owner") != OWNER
                or gold.metadata.properties.get("gold.model") != MODEL_VERSION
                or not gold.metadata.location.startswith("s3://warehouse/gold_local/")):
            raise ValueError(f"Uninventoried/unowned Gold table: {identifier}")
        validate_gold_schema(gold, load_spec()[identifier[-1]])
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


def recovered_state(previous, context, owner_run_id, actual_outputs):
    if (previous.get("status") != "FAILED_NOT_PUBLISHED" or previous.get("owner") != OWNER
            or previous.get("model") != MODEL_VERSION or previous.get("error", {}).get("action") != "validate"
            or set(previous.get("outputs", {})) != set(load_spec())
            or {k: v for k, v in previous["context"].items() if k != "code_fingerprint"} != {k: v for k, v in context.items() if k != "code_fingerprint"}):
        raise ValueError("Recovery requires failed validation, full owned outputs and identical pinned inputs/references/model/scope")
    for name, output in previous["outputs"].items():
        actual = actual_outputs[name]
        for field in ("uuid", "snapshot_id", "metadata_location", "rows", "data_files"):
            if actual[field] != output[field]:
                raise ValueError(f"Unknown/drifted commit outcome: {name}/{field}")
    result = copy.deepcopy(previous)
    result.update(batch=fingerprint(context), context=context, owner_run_id=owner_run_id, status="RECOVERY_ADMITTED", dq=[])
    result.pop("error", None)
    result.pop("semantic_queries", None)
    result.pop("reconciliation", None)
    result["recovery"] = {"from_batch": previous["batch"], "from_run": previous["owner_run_id"], "reason": previous["error"], "output_snapshot_set_verified": True, "data_writes": False}
    return result


@contextmanager
def read_window():
    from ingestion.storage.metadata_repository import MetadataRepository
    repo = MetadataRepository()
    with repo._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock_shared(%s)", (repo._derive_lock_key("tlcn-local:gold_local"),))
            if not cur.fetchone()[0]:
                raise RuntimeError("Gold writer active; read window closed")
        try:
            store = Publication()
            pointer, active = store.get("published.json"), store.get("active.json")
            if not pointer or not active or active.get("status") != "PUBLISHED" or active["batch"] != pointer["batch"]:
                raise RuntimeError("Gold batch not published; read window closed")
            batch_id = pointer["batch"]
            if os.environ.get("EDA_WORKER_MODE") == "1" and batch_id in _VERIFIED_BATCH_CACHE:
                yield _VERIFIED_BATCH_CACHE[batch_id]
                return
            manifest = store.get(batch_id + ".json")
            if (not manifest or manifest.get("batch") != batch_id or pointer.get("owner") != OWNER
                    or pointer.get("manifest") != PREFIX + batch_id + ".json"):
                raise RuntimeError("Invalid Gold publication")
            verify_manifest(manifest)
            trade = manifest["semantic_queries"][VIEW_NAMES[3]]["validation"]
            progress = store.get(trade["progress_key"])
            if (not progress or progress.get("status") != "ACCEPT" or progress.get("scope") != trade["scope"]
                    or progress.get("identity", {}).get("context") != manifest["context"]
                    or progress.get("domain") != trade["domain"]
                    or [p["seal"] for p in progress.get("accepted", [])] != trade["checkpoint_seals"]):
                raise RuntimeError("Unverified Trade partition checkpoints")
            from pyiceberg.catalog import load_catalog
            catalog = load_catalog("gold_consumer", **{"type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse",
                "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"),
                "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1"})
            for name, output in manifest["outputs"].items():
                table = catalog.load_table("gold_local." + name)
                snapshot = table.current_snapshot()
                if str(table.metadata.table_uuid) != output["uuid"] or not snapshot or str(snapshot.snapshot_id) != output["snapshot_id"]:
                    raise RuntimeError(f"Unpublished Gold state at {name}")
                validate_gold_schema(table, load_spec()[name])
            if os.environ.get("EDA_WORKER_MODE") == "1":
                _VERIFIED_BATCH_CACHE[batch_id] = manifest
            yield manifest
        finally:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock_shared(%s)", (repo._derive_lock_key("tlcn-local:gold_local"),))


def query(view, year=None):
    if view not in VIEW_NAMES:
        raise ValueError("Choose one canonical Gold semantic query")
    with read_window() as manifest:
        sql = definitions(manifest["outputs"])[view]
        years = manifest["semantic_queries"][view]["validation"]["partition_years"]
        if year is not None and (type(year) is not int or year not in years):
            raise ValueError("Year outside validated published coverage")
        rows, schema, sampled = [], None, []
        sampled_partitions = []
        for selected in ([year] if year is not None else reversed(years)):
            predicates = [f"year_key={int(selected)}"]
            if view == VIEW_NAMES[3]:
                from gold.quality import predicate
                parts = [p for p in manifest["semantic_queries"][view]["validation"]["domain"] if p["year"] == selected]
                predicates = [predicate(selected, p["bucket"], "q") for p in parts]
            for position, condition in enumerate(predicates):
                result = trino_result(f"SELECT * FROM ({sql}) q WHERE {condition} LIMIT {20-len(rows)}")
                if result["columns"] != expected_schema(view):
                    raise RuntimeError("Semantic output schema changed")
                rows.extend(result["rows"])
                schema = result["columns"]
                if view == VIEW_NAMES[3]:
                    sampled_partitions.append({"year": selected, "bucket": parts[position]["bucket"]})
                if len(rows) >= 20:
                    break
            sampled.append(selected)
            if len(rows) >= 20:
                break
        return {"batch": manifest["batch"], "view": view, "semantic_version": manifest["semantic_queries"][view]["version"], "sample_years": sampled,
                "sample_partitions": sampled_partitions, "columns": schema, "rows": rows}


def command(request, root):
    allowed = {"job", "profile", "action", "source", "owner_run_id", "silver_run_id"}
    if set(request) - allowed or request.get("job") != "run_gold.py" or request.get("profile") != "local_nine":
        raise ValueError("Unapproved Gold request fields/profile/job")
    action = request.get("action")
    if action not in ACTIONS or (action == "fact" and request.get("source") not in _BUILD_ORDER) or (action != "fact" and request.get("source") is not None):
        raise ValueError("Unapproved Gold action/source")
    for field in ("owner_run_id", "silver_run_id"):
        value = request.get(field)
        if value is not None and (not isinstance(value, str) or not 0 < len(value) <= 250 or any(ord(c) < 32 for c in value)):
            raise ValueError(f"Invalid {field}")
    if not request.get("owner_run_id"):
        raise ValueError("Gold requires explicit run ownership")
    args = ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "2g", "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.execution.arrow.pyspark.enabled=true", "--conf", "spark.sql.shuffle.partitions=4", str(Path(root) / "jobs/gold/run_gold.py"), "--action", action, "--owner-run-id", request["owner_run_id"]]
    for field in ("silver_run_id", "source"):
        if request.get(field) is not None:
            args += ["--" + field.replace("_", "-"), request[field]]
    return args


def run_stage(action, owner_run_id, silver_run_id, source=None):
    url = os.environ.get("SPARK_API_URL", "http://runner:5005/run")
    with urllib.request.urlopen(url.rsplit("/", 1)[0] + "/health", timeout=10) as response:
        health = json.load(response)
    if (health.get("gold_model") != MODEL_VERSION or health.get("local_profile") != "local_nine"
            or health.get("gold_semantic_version") != SEMANTIC_VERSION or health.get("gold_code_fingerprint") != code_fingerprint()):
        raise RuntimeError("Runner has not loaded the reviewed Gold allowlist; controlled reload required")
    payload = {"job": "run_gold.py", "profile": "local_nine", "action": action, "owner_run_id": owner_run_id, "silver_run_id": silver_run_id}
    if source is not None:
        payload["source"] = source
    command(payload, "/workspace")
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=3400) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Gold stage {action}/{source} failed: {exc.read().decode('utf-8')}") from exc
    if result.get("status") != "SUCCESS" or not result.get("result"):
        raise RuntimeError("Missing successful Gold stage result")
    return result["result"]


# ---------------------------------------------------------------------------
# Input & Catalog Helpers
# ---------------------------------------------------------------------------

def inventory(spark, specs, verify_ownership=True):
    """Ensure `iceberg.gold_local` only contains known Star Schema tables."""
    namespaces = [r[0] for r in spark.sql("SHOW NAMESPACES IN iceberg").collect()]
    if "gold_local" not in namespaces:
        return
    found = spark.sql("SHOW TABLES IN iceberg.gold_local").collect()
    for row in found:
        if row.tableName not in specs:
            raise ValueError(f"Uninventoried Gold table: {row.tableName}")
        if verify_ownership:
            check_owned(identity(spark, f"{OUTPUT_NAMESPACE}.{row.tableName}"), row.tableName)


def read_inputs(spark, proof, dq, validate=False, only_sources=None):
    """Read pinned Silver snapshots (`option('snapshot-id', ...)`) for reproducibility."""
    from gold.quality import validate_input
    from silver.engine import SilverContractLoader
    from silver.framework import local_contract
    loader = SilverContractLoader(str(ROOT / "contracts/silver"))
    inputs, contracts = {}, {}
    for source, pin in proof["pins"].items():
        if only_sources is not None and source not in only_sources:
            continue
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
    """Load the 8 already-built Dimension tables from `iceberg.gold_local`."""
    return {name: spark.table(f"{OUTPUT_NAMESPACE}.{name}") for name in specs if name.startswith("dim_") and spark.catalog.tableExists(f"{OUTPUT_NAMESPACE}.{name}")}


# ---------------------------------------------------------------------------
# 4-Step Gold Execution Pipeline
# ---------------------------------------------------------------------------

def execute(spark, action, owner_run_id, silver_run_id=None, source=None):
    """Execute one stage of the 4-step Gold pipeline under an advisory writer lock."""
    if action not in ACTIONS or not owner_run_id or (action == "fact" and source not in BUILD_ORDER):
        raise ValueError("Unapproved Gold stage/source/owner")
    from gold.dimensions import build_dimensions
    from gold.facts import transform, monthly_snapshot_frame
    from gold.quality import Quality, validate_table, reconcile
    with writer_guard() as guard:
        # --- STEP 1: Admission & Preflight check (verify Silver inputs) ---
        proof = success_context(silver_run_id)
        specs = load_spec()
        inventory(spark, specs, verify_ownership=(action in {"preflight", "dimensions"}))
        resources = runtime_resources()
        # Fail before creating namespace, publication objects, or any physical Gold table.
        require_semantic_definitions()
        store = Publication(guard=guard)
        dq = Quality(proof["pins"])
        if action == "fact":
            needed = {source, "faostat_monthly_price", "worldbank_pinksheet"} if source in {"faostat_monthly_price", "worldbank_pinksheet"} else {source}
        elif action in {"validate", "publish"}:
            needed = set()
        else:
            needed = None
        inputs, contracts = read_inputs(spark, proof, dq, validate=(action == "preflight"), only_sources=needed)
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

            # --- STEP 2: Build 8 Dimensions (`dimensions.py`) ---
            if action == "dimensions":
                dimensions = {}
                for name, raw in build_dimensions(spark, inputs):
                    LOG.info("Gold dimension %s", name)
                    df = specs[name].project(raw).localCheckpoint(eager=True)
                    validate_table(name, df, specs[name], dimensions, dq)
                    state["outputs"][name] = write_snapshot(spark, specs[name], df, dq, guard)
                    dimensions[name] = spark.table(f"{OUTPUT_NAMESPACE}.{name}")
                    try:
                        dimensions[name]._gold_snapshot_id = state["outputs"][name]["snapshot_id"]
                    except Exception:
                        pass
                    store.save(state)
                state["status"] = "DIMENSIONS_BUILT"

            # --- STEP 3: Build 9 Facts (`facts.py`) & Quality gates (`quality.py`) ---
            elif action == "fact":
                from pyspark import StorageLevel
                expected_previous = list(BUILD_ORDER[:BUILD_ORDER.index(source)])
                if not set(expected_previous).issubset(state["completed_sources"]) or not all(n in state["outputs"] for n in specs if n.startswith("dim_")):
                    raise ValueError("Gold source dependency/order not satisfied")
                dimensions = current_dimensions(spark, specs)
                for d_name, d_df in dimensions.items():
                    try:
                        d_df._gold_snapshot_id = state["outputs"][d_name]["snapshot_id"]
                    except Exception:
                        pass
                frames = transform(source, inputs[source], contracts[source].business_key, proof["pins"][source])
                projected = {}
                persisted = []
                try:
                    for name, raw in frames.items():
                        branch_df = specs[name].project(raw).persist(StorageLevel.MEMORY_AND_DISK)
                        persisted.append(branch_df)
                        projected[name] = branch_df
                        if name == "fact_price_monthly":
                            merged = monthly_snapshot_frame(source, raw, inputs, {s: c.business_key for s, c in contracts.items()}, proof["pins"], state["completed_sources"])
                            df = branch_df if merged is raw else specs[name].project(merged).persist(StorageLevel.MEMORY_AND_DISK)
                            if df is not branch_df:
                                persisted.append(df)
                        else:
                            df = branch_df
                        validate_table(name, df, specs[name], dimensions, dq)
                        state["outputs"][name] = write_snapshot(spark, specs[name], df, dq, guard)
                    outputs = {name: spark.table(f"{OUTPUT_NAMESPACE}.{name}") for name in frames}
                    reconcile(source, inputs[source], projected, outputs, contracts[source].business_key, dq)
                finally:
                    for cached_df in persisted:
                        cached_df.unpersist()
                if source not in state["completed_sources"]:
                    state["completed_sources"].append(source)
                state["status"] = "FACTS_BUILDING"

            # --- STEP 4A: Validate 4 Semantic SQL Views (`views.py`) & Cross-table DQ ---
            elif action == "validate":
                if set(state["completed_sources"]) != set(BUILD_ORDER) or set(state["outputs"]) != set(specs):
                    raise ValueError("Cannot validate incomplete Gold scope")
                outputs = {}
                for name, saved in state["outputs"].items():
                    now = identity(spark, f"{OUTPUT_NAMESPACE}.{name}")
                    if now["uuid"] != saved["uuid"] or now["snapshot_id"] != saved["snapshot_id"]:
                        raise ValueError(f"Gold output moved since stage readback: {name}")
                    outputs[name] = pinned_read(spark, f"{OUTPUT_NAMESPACE}.{name}", saved)
                guard()
                state["semantic_queries"] = validate_semantic_queries(state["outputs"], dq, store=store, context=context)
                semantic_checks(outputs, dq)
                state["reconciliation"] = {s: {"silver_rows": proof["proofs"][s]["commit_verified"]["rows"], "key_set": "EXACT", "values_and_lineage": "EXACT"} for s in BUILD_ORDER}
                state["status"] = "VERIFIED"

            # --- STEP 4B: Publish Semantic SQL Views & Record Audit Metadata ---
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
    """Cross-table Star Schema rules checked before publishing a batch."""
    metrics = outputs["dim_observation_type"]
    from pyspark.sql import functions as F
    from gold.dimensions import ZERO_GEOGRAPHY
    dq.zero("DQ13", "metrics", "stocks not across time; ratios never additive", metrics.where("(source_metric_id IN ('5113','Beginning Stocks','Ending Stocks') AND aggregation_rule <> 'NO_SUM_OVER_TIME') OR (source_metric_id IN ('5412','Yield','Milling Rate (.9999)') AND aggregation_rule <> 'NEVER_SUM')"))
    dq.zero("DQ18", "WB geography", "technical member restricted to WB benchmark", outputs["dim_price_series"].where((F.col("geography_key") == ZERO_GEOGRAPHY) != (F.col("source_system_key") == __import__("gold.keys", fromlist=["key"]).key("dim_source_system", ["worldbank_pinksheet"]))))
    dq.record("DQ17", "outputs", "all 17 output snapshots reconciled before publication", 17, len(outputs), len(outputs) == 17)
