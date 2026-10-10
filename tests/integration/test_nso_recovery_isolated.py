"""No shared catalog: actual PyIceberg transactions in temporary SQLite/filesystem."""
import io
from pathlib import Path
import zipfile

import pandas as pd
import pytest

from ingestion.recovery.nso import atomic_recover, digest_rows, parse_csv, prepare_recovery
from ingestion.storage.bronze_writer import BronzeIcebergWriter, TECHNICAL_METADATA_COLUMNS
from ingestion.utils.error_classifier import SchemaError

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestamptzType


@pytest.fixture
def catalog(tmp_path):
    result = SqlCatalog("isolated", uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}", warehouse=str(tmp_path))
    result.create_namespace("isolated")
    yield result
    result.engine.dispose()


def bronze_schema(*names):
    types = {"_ingestion_chunk_id": LongType(), "_source_snapshot_id": LongType(), "_ingestion_timestamp": TimestamptzType()}
    return Schema(*[NestedField(i + 1, name, types.get(name, StringType()), required=False)
                    for i, name in enumerate([*names, *TECHNICAL_METADATA_COLUMNS])])


def frame(**source):
    result = pd.DataFrame(source)
    for name in TECHNICAL_METADATA_COLUMNS:
        result[name] = pd.Timestamp("2026-10-04", tz="UTC") if name == "_ingestion_timestamp" else 0 if name in ("_ingestion_chunk_id", "_source_snapshot_id") else name
    return result


def test_multiple_csv_schema_evolution_preserves_null_nonascii_and_lineage(catalog, monkeypatch):
    writer = BronzeIcebergWriter(schema_policy="approved_evolution", approved_schema_additions={"nso_vietnam": {"col_2024": "string"}})
    monkeypatch.setattr(writer, "ensure_schema", lambda: None)
    monkeypatch.setattr(writer, "get_iceberg_catalog", lambda: catalog)
    table = catalog.create_table("isolated.nso_vietnam", bronze_schema("nam"))
    writer.schema = "isolated"
    first = frame(nam=["2023", None])
    table.append(writer._dataframe_to_arrow(first, table.schema()))
    raw, _, _ = parse_csv('Tỉnh,2024\nHà Nội,123\nHuế,..\nKhác,\n'.encode("utf-8"))
    # Unknown non-ASCII field must fail without a schema commit.
    before = table.metadata_location
    with pytest.raises(SchemaError):
        writer.ensure_table("nso_vietnam", raw)
    assert table.metadata_location == before
    writer.approved_schema_additions["nso_vietnam"]["t_nh"] = "string"
    writer.ensure_table("nso_vietnam", raw)
    table.refresh()
    second = frame(**raw.to_dict("list"))
    table.append(writer._dataframe_to_arrow(second, table.schema()))
    actual = table.scan().to_arrow().to_pandas()
    assert len(actual) == 5 and actual.col_2024.notna().sum() == 2
    assert set(actual.t_nh.dropna()) == {"Hà Nội", "Huế", "Khác"}
    assert ".." in set(actual.col_2024.dropna()) and actual["_source_checksum"].notna().all()
    assert all(not f.required for f in table.schema().fields)


def test_atomic_failure_retry_noop_and_rollback(catalog):
    table = catalog.create_table("isolated.nso_vietnam", bronze_schema("nam"))
    writer = BronzeIcebergWriter()
    baseline = frame(nam=["1990", None])
    table.append(writer._dataframe_to_arrow(baseline, table.schema()))
    before = table.current_snapshot().snapshot_id
    recovered = baseline.assign(col_2024=[None, ".."])
    def failure():
        raise RuntimeError("injected")
    with pytest.raises(RuntimeError, match="injected"):
        atomic_recover(table, recovered, {"col_2024": "string"}, before, before_commit=failure)
    table.refresh()
    assert table.current_snapshot().snapshot_id == before and len(table.schema().fields) == len(baseline.columns)
    success = atomic_recover(table, recovered, {"col_2024": "string"}, before)
    assert success["status"] == "RECOVERED"
    assert digest_rows(table.scan().to_arrow().to_pandas()[list(recovered)]) == digest_rows(recovered)
    assert atomic_recover(table, recovered, {"col_2024": "string"}, before)["status"] == "NOOP"
    with pytest.raises(ValueError, match="Snapshot changed"):
        atomic_recover(table, recovered.assign(col_2024="999"), {"col_2024": "string"}, before)
    table.manage_snapshots().rollback_to_snapshot(before).commit()
    table.refresh()
    assert digest_rows(table.scan().to_arrow().to_pandas()[list(baseline)]) == digest_rows(baseline)
    assert len(table.metadata.snapshots) >= 2


def test_optimistic_concurrent_append_aborts_recovery(catalog):
    from pyiceberg.exceptions import CommitFailedException, ValidationException
    table = catalog.create_table("isolated.nso_vietnam", bronze_schema("nam"))
    writer = BronzeIcebergWriter()
    baseline = frame(nam=["1990"])
    table.append(writer._dataframe_to_arrow(baseline, table.schema()))
    before = table.current_snapshot().snapshot_id
    other = catalog.load_table("isolated.nso_vietnam")
    def concurrent():
        other.append(writer._dataframe_to_arrow(frame(nam=["2000"]), other.schema()))
    with pytest.raises((CommitFailedException, ValidationException)):
        atomic_recover(table, baseline.assign(col_2024="123"), {"col_2024": "string"}, before, before_commit=concurrent)
    table.refresh()
    assert set(table.scan().to_arrow().to_pandas().nam) == {"1990", "2000"}
    assert "col_2024" not in table.scan().to_arrow().column_names


def test_checksum_mismatch_refuses_recovery_before_catalog_access():
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as archive:
        for n in range(12, 25):
            archive.writestr(f"V06.{n}.csv", "Năm,2024\n1990,123\n")
    with pytest.raises(ValueError, match="checksum"):
        prepare_recovery(content.getvalue(), pd.DataFrame(), bronze_schema("nam"), manifest_checksum="wrong")
