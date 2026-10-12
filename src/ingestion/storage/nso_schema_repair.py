"""Repair the NSO all-NULL union inference bug without deleting Bronze rows."""
from __future__ import annotations

from typing import Iterable

from ingestion.storage.bronze_writer import BronzeIcebergWriter, sanitize_column_name


def repair_nso_schema(
    writer: BronzeIcebergWriter, source_columns: Iterable[str], *, execute: bool = False
) -> dict:
    """Plan or atomically replace all-NULL DOUBLE source fields with strings.

    Caller must hold MetadataRepository.source_lock('nso_vietnam'). Old field
    IDs and files remain available under reserved legacy names. Numeric fields
    containing any data are refused because they require a data migration.
    """
    catalog = writer.get_iceberg_catalog()
    if catalog is None:
        raise RuntimeError("Native Iceberg catalog is required for safe NSO schema repair")
    table = catalog.load_table((writer.schema, "nso_vietnam"))
    fields = {field.name: field for field in table.schema().fields}
    expected = sorted(set(source_columns))
    if not expected or any(sanitize_column_name(name) != name for name in expected):
        raise ValueError("Expected source columns must be nonempty, sanitized NSO names")
    replacements = {}
    for name in expected:
        field = fields.get(name)
        if field is None:
            raise ValueError(f"Missing NSO source column: {name}")
        if str(field.field_type) == "string":
            continue
        if str(field.field_type) != "double" or field.required:
            raise ValueError(f"Cannot repair {name}: expected nullable DOUBLE, got {field}")
        legacy_name = f"_nso_legacy_null_{name}"
        if legacy_name in fields:
            raise ValueError(f"Reserved legacy column already exists: {legacy_name}")
        replacements[name] = legacy_name

    table_name = f'{writer.catalog}."{writer.schema}"."nso_vietnam"'
    counts = ', '.join(f'count("{name}")' for name in replacements)
    _, rows = writer.execute_query(f"SELECT count(*){', ' + counts if counts else ''} FROM {table_name}")
    row_count, *non_null_counts = rows[0]
    populated = [name for name, count in zip(replacements, non_null_counts) if count]
    if populated:
        raise ValueError(f"Refusing to replace columns containing data: {', '.join(populated)}")

    snapshot = table.current_snapshot()
    report = {
        "table": f"{writer.catalog}.{writer.schema}.nso_vietnam",
        "status": "planned" if replacements else "already_valid",
        "row_count": row_count,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "replacement_columns": replacements,
    }
    if execute and replacements:
        from pyiceberg.types import StringType

        # Stage two schema updates in ONE catalog transaction. Adding a name
        # in the same UpdateSchema that renames it would collide with the old
        # schema. New fields receive new IDs; the legacy IDs stay intact.
        with table.transaction() as transaction:
            with transaction.update_schema() as update:
                for name, legacy_name in replacements.items():
                    update.rename_column(name, legacy_name)
            with transaction.update_schema() as update:
                for name in replacements:
                    update.add_column(name, StringType())
        _, after_rows = writer.execute_query(f"SELECT count(*) FROM {table_name}")
        table.refresh()
        after_snapshot = table.current_snapshot()
        after_snapshot_id = after_snapshot.snapshot_id if after_snapshot else None
        if after_rows[0][0] != row_count or after_snapshot_id != report["snapshot_id"]:
            raise RuntimeError("NSO data changed during schema repair; inspect concurrent writers")
        if any(not isinstance(table.schema().find_field(name).field_type, StringType) for name in expected):
            raise RuntimeError("NSO source schema is still invalid after repair")
        report["status"] = "repaired"
    return report
