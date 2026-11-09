import copy
import io
import json

import pytest

from gold.publication import Publication


class MemoryObjects:
    def __init__(self):
        self.objects = {}
        self.fail = None

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FileNotFoundError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType):
        if self.fail and self.fail in Key:
            raise IOError("injected metadata publication failure")
        self.objects[Key] = Body

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        return {"Contents": [{"Key": k} for k in self.objects if k.startswith(Prefix)][:MaxKeys]}


def state(store, context=None, owner="run-a"):
    return store.claim(context or {"input": {"uuid": "silver-one", "snapshot_id": "123"}, "model": "test"}, owner)


def verified(s):
    from gold.spec import load_spec
    from gold.views import semantic_record, VIEW_NAMES
    s["status"] = "VERIFIED"
    s["reconciliation"] = {"source": "EXACT"}
    s["outputs"] = {n: {"uuid": "gold-one-" + n, "snapshot_id": "456", "rows": 3, "data_files": ["s3://warehouse/gold_local/" + n + "/file.parquet"], "write": "COMMITTED"} for n in load_spec()}
    s["semantic_queries"] = {n: {**semantic_record(n, s["outputs"]), "validation": {"status": "ACCEPT", "rows": 3, "partition_years": [2024]}} for n in VIEW_NAMES}
    from gold.trade_validation import VERSION
    s["semantic_queries"][VIEW_NAMES[3]]["validation"].update(validator=VERSION, input_rows=3, partitions=1, expected_partitions=1,
        coverage="ALL_DISJOINT_YEAR_REPORTER_BUCKETS", scope="a"*64, progress_key="validation/"+"a"*64+"/progress.json",
        domain=[{"year":2024,"bucket":0,"input_rows":3}], checkpoint_seals=["b"*64])
    return s


def test_partial_commits_never_publish():
    store = Publication(MemoryObjects())
    s = state(store)
    s["outputs"] = {"fact": {"snapshot_id": "456"}}
    with pytest.raises(ValueError):
        store.publish(s)
    assert store.get("published.json") is None
    assert s["outputs"]["fact"]["snapshot_id"] == "456"


def test_owner_fencing_and_changed_snapshot():
    store = Publication(MemoryObjects())
    s = state(store)
    with pytest.raises(ValueError):
        state(store, owner="run-b")
    with pytest.raises(ValueError):
        state(store, {"input": {"uuid": "silver-one", "snapshot_id": "124"}})
    assert state(store)["batch"] == s["batch"]


def test_publish_then_replay_is_immutable():
    client = MemoryObjects()
    store = Publication(client)
    s = verified(state(store))
    first = store.publish(s)
    s["status"] = "VERIFIED"
    s["outputs"]["fact_fao_production_observation"]["write"] = "NOOP"
    assert store.publish(s) == first
    s["status"] = "VERIFIED"
    s["outputs"]["fact_fao_production_observation"]["snapshot_id"] = "457"
    with pytest.raises(ValueError):
        store.publish(s)


def test_failed_dq_cannot_publish():
    store = Publication(MemoryObjects())
    s = verified(state(store))
    s["dq"] = [{"status": "FAIL"}]
    with pytest.raises(ValueError):
        store.publish(s)


def test_lost_publication_response_and_retry():
    client = MemoryObjects()
    store = Publication(client)
    s = verified(state(store))
    client.fail = "published.json"
    with pytest.raises(IOError):
        store.publish(s)
    assert store.get("published.json") is None
    assert store.get(s["batch"] + ".json")["publication_status"] == "PUBLISHED"
    client.fail = None
    store.publish(s)
    assert store.get("published.json")["batch"] == s["batch"]


def test_unowned_prefix_rejected():
    client = MemoryObjects()
    client.objects["gold_publications/local_nine/foreign.json"] = b'{}'
    with pytest.raises(ValueError):
        state(Publication(client))


def test_initial_claim_metadata_failure_resumes_same_owner_only():
    client = MemoryObjects()
    store = Publication(client)
    client.fail = "active.json"
    with pytest.raises(IOError):
        state(store)
    client.fail = None
    with pytest.raises(ValueError):
        state(store, owner="different-owner")
    resumed = state(store)
    assert resumed["status"] == "ADMITTED"
    assert store.get("active.json")["owner_run_id"] == "run-a"
