"""A repair must refuse populated/unsupported columns before any mutation."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ingestion.storage.nso_schema_repair import repair_nso_schema


def make_writer(field_type="double", non_null=0, extra_fields=()):
    table = MagicMock()
    table.schema.return_value.fields = [
        SimpleNamespace(name="col_1995", field_type=field_type, required=False), *extra_fields,
    ]
    table.current_snapshot.return_value = SimpleNamespace(snapshot_id=123)
    writer = MagicMock(schema="bronze", catalog="iceberg")
    writer.get_iceberg_catalog.return_value.load_table.return_value = table
    writer.execute_query.return_value = ([], [[71, non_null]])
    return writer, table


def test_populated_double_is_refused_before_transaction():
    writer, table = make_writer(non_null=1)
    with pytest.raises(ValueError, match="containing data: col_1995"):
        repair_nso_schema(writer, ["col_1995"], execute=True)
    table.transaction.assert_not_called()


def test_all_null_double_dry_run_preserves_snapshot_and_rows():
    writer, table = make_writer()
    report = repair_nso_schema(writer, ["col_1995"])
    assert report["status"] == "planned"
    assert report["row_count"] == 71 and report["snapshot_id"] == 123
    assert report["replacement_columns"] == {"col_1995": "_nso_legacy_null_col_1995"}
    table.transaction.assert_not_called()


def test_valid_schema_is_a_noop_even_with_execute():
    writer, table = make_writer(field_type="string")
    writer.execute_query.return_value = ([], [[833]])
    report = repair_nso_schema(writer, ["col_1995"], execute=True)
    assert report["status"] == "already_valid"
    table.transaction.assert_not_called()


@pytest.mark.parametrize("columns,field_type,message", [
    (["col_1995"], "long", "expected nullable DOUBLE"),
    (["col_2024"], "double", "Missing NSO source column"),
    (["raw name"], "double", "sanitized NSO names"),
])
def test_unexpected_schema_is_refused_before_query(columns, field_type, message):
    writer, table = make_writer(field_type=field_type)
    with pytest.raises(ValueError, match=message):
        repair_nso_schema(writer, columns, execute=True)
    writer.execute_query.assert_not_called()
    table.transaction.assert_not_called()


def test_existing_legacy_field_is_refused_before_query():
    writer, table = make_writer(extra_fields=[SimpleNamespace(name="_nso_legacy_null_col_1995")])
    with pytest.raises(ValueError, match="Reserved legacy column"):
        repair_nso_schema(writer, ["col_1995"], execute=True)
    writer.execute_query.assert_not_called()
    table.transaction.assert_not_called()
