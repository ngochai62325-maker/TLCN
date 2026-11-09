"""Persistent batch ownership and immutable publications, under a PostgreSQL lock.

This is an application protocol, not an atomic Iceberg multi-table transaction.
Every caller must hold gold_local writer advisory lock for its full stage.
"""
import json
import io
import os

from gold import OWNER, MODEL_VERSION
from gold.keys import canonical_json, fingerprint

PREFIX = "gold_publications/local_nine/"


def verify_manifest(manifest):
    from gold.spec import load_spec
    from gold.views import verify_semantic_records
    if (manifest.get("owner") != OWNER or manifest.get("model") != MODEL_VERSION
            or manifest.get("publication_status") != "PUBLISHED" or manifest.get("dq") != "ACCEPT"
            or fingerprint(manifest.get("context")) != manifest.get("batch")
            or set(manifest.get("outputs", {})) != set(load_spec())):
        raise ValueError("Invalid Gold manifest/fingerprint/table inventory")
    verify_semantic_records(manifest["outputs"], manifest.get("semantic_queries", {}))


class ArrowObjects:
    """Small S3 object adapter using pyarrow already pinned in the Spark image.

    No startup pip install, boto3 dependency or new infrastructure required.
    """
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
            # Recover a lost response between the first working-state write and
            # first ownership pointer. No data stage was admitted at this point.
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
        # This pointer is the only advertised batch; a partial retry never advances it.
        self.put("published.json", {"owner": OWNER, "batch": state["batch"], "manifest": PREFIX + state["batch"] + ".json"})
        state["status"] = "PUBLISHED"
        self.save(state)
        return payload
