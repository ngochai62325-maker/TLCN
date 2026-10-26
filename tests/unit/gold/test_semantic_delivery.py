import copy
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gold import MODEL_VERSION, OWNER
from gold.consumer import query, read_window
from gold.keys import fingerprint
from gold.publication import PREFIX, verify_manifest
from gold.spec import load_spec
from gold.views import VIEW_NAMES, definitions, expected_schema, load_definitions, semantic_record, verify_semantic_records


def published():
    outputs = {n: {"uuid": "uuid-" + n, "snapshot_id": "456", "rows": 1, "data_files": []} for n in load_spec()}
    records = {n: {**semantic_record(n, outputs), "validation": {"status": "ACCEPT", "rows": 1, "partition_years": [2024]}} for n in VIEW_NAMES}
    from gold.trade_validation import VERSION
    records[VIEW_NAMES[3]]["validation"].update(validator=VERSION, input_rows=1, partitions=1, expected_partitions=1,
        coverage="ALL_DISJOINT_YEAR_REPORTER_BUCKETS", scope="a"*64, progress_key="validation/"+"a"*64+"/progress.json",
        domain=[{"year":2024,"bucket":0,"input_rows":1}], checkpoint_seals=["b"*64])
    context = {"model": MODEL_VERSION, "inputs": {"fixture": "pinned"}}
    return {"owner": OWNER, "model": MODEL_VERSION, "batch": fingerprint(context), "context": context, "outputs": outputs, "semantic_queries": records, "dq": "ACCEPT", "publication_status": "PUBLISHED"}


class DefinitionsDirectory:
    def __init__(self, sqls):
        self.sqls = sqls

    def glob(self, pattern):
        return [Path(n + ".sql") for n in self.sqls]

    def __truediv__(self, path):
        return SimpleNamespace(read_text=lambda **kwargs: self.sqls[Path(path).stem])


def test_exact_names_loading_hashes_and_pinning():
    definitions_a = load_definitions()
    assert tuple(definitions_a) == VIEW_NAMES
    assert definitions_a == load_definitions()
    bound = definitions(published()["outputs"])
    assert all("FOR VERSION AS OF 456" in sql and "{{" not in sql for sql in bound.values())
    assert all(len(d["sha256"]) == 64 for d in definitions_a.values())


def test_missing_or_extra_definition_rejected():
    sqls = {n: d["sql"] for n, d in load_definitions().items()}
    sqls.pop(VIEW_NAMES[0])
    with pytest.raises(ValueError):
        load_definitions(DefinitionsDirectory(sqls))


@pytest.mark.parametrize("suffix", ["; DROP TABLE x", " -- hidden", " /* hidden */", " UNION SELECT * FROM foreign_catalog.table"])
def test_unsafe_statement_rejected(suffix):
    sqls = {n: d["sql"] for n, d in load_definitions().items()}
    sqls[VIEW_NAMES[0]] += suffix
    with pytest.raises(ValueError):
        load_definitions(DefinitionsDirectory(sqls))


@pytest.mark.parametrize("mutation", ["fingerprint", "definition", "snapshot", "schema", "missing", "query_failed"])
def test_manifest_tampering_rejected(mutation):
    manifest = published()
    record = manifest["semantic_queries"][VIEW_NAMES[0]]
    if mutation == "fingerprint":
        manifest["context"]["changed"] = True
    elif mutation == "definition":
        record["definition_sha256"] = "0" * 64
    elif mutation == "snapshot":
        manifest["outputs"]["fact_nso_observation"]["snapshot_id"] = "457"
    elif mutation == "schema":
        record["output_schema"][0]["type"] = "double"
    elif mutation == "missing":
        manifest["semantic_queries"].pop(VIEW_NAMES[0])
    else:
        record["validation"]["status"] = "FAIL"
    with pytest.raises(ValueError):
        verify_manifest(manifest)


def test_current_sql_tampering_changes_hash():
    original = load_definitions()
    sqls = {n: d["sql"] for n, d in original.items()}
    sqls[VIEW_NAMES[1]] = sqls[VIEW_NAMES[1]].replace("1000.0", "100.0")
    assert load_definitions(DefinitionsDirectory(sqls))[VIEW_NAMES[1]]["sha256"] != original[VIEW_NAMES[1]]["sha256"]
    with patch("gold.views.QUERY_DIR", DefinitionsDirectory(sqls)):
        with pytest.raises(ValueError):
            verify_manifest(published_original())


def published_original():
    # Caller must build the manifest before patching the SQL directory.
    return ORIGINAL_MANIFEST


ORIGINAL_MANIFEST = published()


def test_consumer_allowlist_precedes_storage():
    with patch("gold.consumer.read_window", side_effect=AssertionError("no storage")):
        with pytest.raises(ValueError):
            query("arbitrary_sql")


def test_valid_published_query_and_execution_failure():
    manifest = published()
    @contextmanager
    def window():
        yield manifest
    result = {"columns": expected_schema(VIEW_NAMES[1]), "rows": [["sample"]]}
    with patch("gold.consumer.read_window", window), patch("gold.consumer.trino_result", return_value=result) as execute:
        assert query(VIEW_NAMES[1])["rows"] == [["sample"]]
        assert "FOR VERSION AS OF 456" in execute.call_args[0][0]
    with patch("gold.consumer.read_window", window), patch("gold.consumer.trino_result", side_effect=RuntimeError("query failure")):
        with pytest.raises(RuntimeError):
            query(VIEW_NAMES[1])


def test_changed_gold_physical_schema_rejected():
    from gold.preflight import validate_gold_schema
    spec = load_spec()["dim_year"]
    table = SimpleNamespace(schema=lambda: SimpleNamespace(fields=[SimpleNamespace(name="year_key", field_type="long", required=True)]))
    with pytest.raises(ValueError):
        validate_gold_schema(table, spec)


def test_unpublished_consumer_rejected():
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (True,)
    repo = MagicMock()
    repo._get_connection.return_value = conn
    with patch("ingestion.storage.metadata_repository.MetadataRepository", return_value=repo), patch("gold.consumer.Publication") as store:
        store.return_value.get.return_value = None
        with pytest.raises(RuntimeError, match="not published"):
            with read_window():
                raise AssertionError("unpublished read admitted")
