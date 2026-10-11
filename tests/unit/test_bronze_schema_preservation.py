"""A schema mismatch must fail before an existing chunk can be deleted."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from ingestion.storage.bronze_writer import BronzeIcebergWriter, TECHNICAL_METADATA_COLUMNS
from ingestion.utils.error_classifier import SchemaError


def test_arrow_alignment_refuses_extra_matrix_column():
    writer = BronzeIcebergWriter()
    schema = SimpleNamespace(fields=[SimpleNamespace(name="nam")])
    with pytest.raises(SchemaError, match="col_2024"):
        writer._dataframe_to_arrow(pd.DataFrame({"nam": [None], "col_2024": ["123"]}), schema)


def test_schema_failure_precedes_retry_delete(monkeypatch):
    writer = BronzeIcebergWriter()
    monkeypatch.setattr(writer, "ensure_table", lambda *args: "iceberg.bronze.nso_vietnam")
    query = MagicMock(return_value=([], [[c, "VARCHAR"] for c in ["nam", *TECHNICAL_METADATA_COLUMNS]]))
    monkeypatch.setattr(writer, "execute_query", query)
    with pytest.raises(SchemaError, match="col_2024"):
        writer.write_chunk("nso_vietnam", pd.DataFrame({"col_2024": ["123"]}), "run", "batch", "sha", 1)
    assert query.call_count == 1 and query.call_args.args[0].startswith("DESCRIBE")


def test_sanitized_header_collision_fails_before_table_access(monkeypatch):
    writer = BronzeIcebergWriter()
    ensure = MagicMock()
    monkeypatch.setattr(writer, "ensure_table", ensure)
    with pytest.raises(SchemaError, match="collision"):
        writer.write_chunk("nso_vietnam", pd.DataFrame({"A B": [1], "A_B": [2]}), "run", "batch", "sha", 0)
    ensure.assert_not_called()


def test_trino_fallback_does_not_implicitly_evolve_existing_schema(monkeypatch):
    writer = BronzeIcebergWriter()
    monkeypatch.setattr(writer, "ensure_schema", MagicMock())
    monkeypatch.setattr(writer, "get_iceberg_catalog", lambda: None)
    query = MagicMock(return_value=([], [[c, "VARCHAR"] for c in ["nam", *TECHNICAL_METADATA_COLUMNS]]))
    monkeypatch.setattr(writer, "execute_query", query)
    with pytest.raises(SchemaError, match="col_2024"):
        writer.ensure_table("nso_vietnam", pd.DataFrame({"col_2024": ["123"]}))
    assert query.call_count == 1 and query.call_args.args[0].startswith("DESCRIBE")


def schema(*source_columns):
    return SimpleNamespace(fields=[SimpleNamespace(name=c, field_type="string")
                                  for c in (*source_columns, *TECHNICAL_METADATA_COLUMNS)])


def test_approved_evolution_is_scoped_to_source_and_explicit_nullable_strings():
    writer = BronzeIcebergWriter(schema_policy="approved_evolution",
                                 approved_schema_additions={"nso_vietnam": {"col_2024": "string"}})
    assert writer.plan_schema_additions("nso_vietnam", ["nam", "col_2024"], schema("nam")) == ["col_2024"]
    for source, incoming in [("another_source", ["col_2024"]), ("nso_vietnam", ["col_2023"])]:
        with pytest.raises(SchemaError, match="schema lacks"):
            writer.plan_schema_additions(source, incoming, schema("nam"))


@pytest.mark.parametrize("approved", [{"col_2024": "long"}, {"_source_file": "string"}, {"2024": "string"}])
def test_unsafe_evolution_definition_is_refused(approved):
    writer = BronzeIcebergWriter(schema_policy="approved_evolution", approved_schema_additions={"nso_vietnam": approved})
    with pytest.raises(SchemaError, match="nullable source strings"):
        writer.plan_schema_additions("nso_vietnam", ["nam"], schema("nam"))


def test_existing_type_change_and_missing_lineage_are_refused():
    writer = BronzeIcebergWriter(schema_policy="approved_evolution", approved_schema_additions={"nso_vietnam": {"nam": "string"}})
    unsafe = schema("nam")
    unsafe.fields[0].field_type = "long"
    with pytest.raises(SchemaError, match="Unsafe type"):
        writer.plan_schema_additions("nso_vietnam", ["nam"], unsafe)
    with pytest.raises(SchemaError, match="technical metadata"):
        writer.plan_schema_additions("nso_vietnam", ["nam"], SimpleNamespace(fields=unsafe.fields[:1]))


@pytest.mark.parametrize("value", ["broken", "1.5", "9223372036854775808"])
def test_invalid_numeric_schema_value_fails_before_delete(monkeypatch, value):
    writer = BronzeIcebergWriter()
    monkeypatch.setattr(writer, "ensure_table", lambda *args: "iceberg.bronze.test")
    query = MagicMock(return_value=([], [["value", "BIGINT"], *[[c, "VARCHAR"] for c in TECHNICAL_METADATA_COLUMNS]]))
    monkeypatch.setattr(writer, "execute_query", query)
    with pytest.raises(SchemaError, match="refusing lossy coercion"):
        writer.write_chunk("test", pd.DataFrame({"value": [value]}), "run", "batch", "sha", 0)
    assert query.call_count == 1 and query.call_args.args[0].startswith("DESCRIBE")


def test_historical_alignment_would_drop_new_source_column():
    frame = pd.DataFrame({"nam": [None], "col_2024": ["123"]})
    historical = frame[["nam"]]  # Original field-only Arrow alignment.
    assert "col_2024" not in historical and frame.col_2024.notna().sum() == 1
    with pytest.raises(SchemaError):
        BronzeIcebergWriter()._dataframe_to_arrow(frame, schema("nam"))
