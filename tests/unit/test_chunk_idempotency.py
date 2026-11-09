"""Stateful exact readback tests: no live catalog or JVM, no SQL fallback."""
from types import SimpleNamespace
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

from ingestion.storage.bronze_writer import BronzeIcebergWriter


class Catalog:
    def __init__(self, expected, fail=None):
        self.expected = expected
        self.rows = expected.slice(0, 0)
        self.fail = fail
        self.commits = 0
        self.reads = 0

    def load_table(self, name):
        self.reads += 1
        if self.fail == "readback" and self.commits:
            raise OSError("catalog down")
        return self

    def current_snapshot(self):
        return SimpleNamespace(snapshot_id=self.commits or None)

    def scan(self, **kwargs):
        return SimpleNamespace(to_arrow=lambda: self.rows)

    def append(self, rows):
        self.commits += 1
        if self.fail == "before":
            raise OSError("before commit")
        self.rows = rows if self.fail != "partial" else rows.slice(0, 1)
        if self.fail == "lost_response":
            raise TimeoutError("response lost after commit")


@pytest.fixture
def proposed():
    return pa.table({"_ingestion_run_id": ["r"] * 3, "_ingestion_chunk_id": [0] * 3,
                     "_source_checksum": ["checksum"] * 3,
                     "value": pa.array([Decimal("-0.0123"), None, Decimal("0")], type=pa.decimal128(18, 4)),
                     "_ingestion_timestamp": ["old"] * 3})


def test_one_append_then_exact_physical_noop(proposed):
    catalog = Catalog(proposed)
    writer = BronzeIcebergWriter()
    assert writer._commit_chunk(catalog, "owned.table", proposed, "r", 0) == 3
    changed_time = proposed.set_column(4, "_ingestion_timestamp", pa.array(["new"] * 3))
    assert writer._commit_chunk(catalog, "owned.table", changed_time, "r", 0) == 3
    assert catalog.commits == 1


def test_lost_response_is_recovered_only_by_fresh_exact_readback(proposed):
    catalog = Catalog(proposed, "lost_response")
    assert BronzeIcebergWriter()._commit_chunk(catalog, "owned.table", proposed, "r", 0) == 3
    assert catalog.commits == 1 and catalog.reads == 2


@pytest.mark.parametrize("failure", ["before", "partial", "readback"])
def test_unknown_outcome_never_falls_back_or_reports_success(proposed, failure):
    catalog = Catalog(proposed, failure)
    with pytest.raises(RuntimeError, match="UNKNOWN_COMMIT"):
        BronzeIcebergWriter()._commit_chunk(catalog, "owned.table", proposed, "r", 0)
    assert catalog.commits == 1


@pytest.mark.parametrize("change", ["checksum", "value", "missing", "duplicate"])
def test_conflicting_or_partial_chunk_preserved_without_delete(proposed, change):
    catalog = Catalog(proposed)
    if change == "checksum":
        catalog.rows = proposed.set_column(2, "_source_checksum", pa.array(["changed"] * 3))
    elif change == "value":
        catalog.rows = proposed.set_column(3, "value", pa.array([Decimal("1"), None, Decimal("0")], type=pa.decimal128(18, 4)))
    elif change == "missing":
        catalog.rows = proposed.slice(0, 1)
    else:
        catalog.rows = pa.concat_tables([proposed, proposed.slice(0, 1)])
    original = catalog.rows
    with pytest.raises(RuntimeError, match="RECOVERY_REQUIRED"):
        BronzeIcebergWriter()._commit_chunk(catalog, "owned.table", proposed, "r", 0)
    assert catalog.commits == 0 and catalog.rows.equals(original)


def test_invalid_identity_before_catalog_or_mutation():
    writer = BronzeIcebergWriter()
    frame = pd.DataFrame({"value": [1]})
    for run, chunk, error in [("", 0, ValueError), ("r", -1, ValueError), ("r", "abc", TypeError)]:
        with pytest.raises(error):
            writer.write_chunk("test", frame, run, "batch", "checksum", chunk)


def test_no_native_catalog_fails_before_any_delete_or_insert():
    writer = BronzeIcebergWriter()
    writer.ensure_table = MagicMock(return_value="iceberg.owned.table")
    writer.execute_query = MagicMock(return_value=([], [[c, "varchar"] for c in [
        "value", "_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp",
        "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id"]]))
    writer.get_iceberg_catalog = lambda: None
    with pytest.raises(RuntimeError, match="unsafe"):
        writer.write_chunk("test", pd.DataFrame({"value": ["raw"]}), "r", "b", "sha", 0)
    assert all(c.args[0].startswith("DESCRIBE") for c in writer.execute_query.call_args_list)
