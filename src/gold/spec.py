"""Read the approved DBML subset; fail closed on drift, never invent a second schema."""
import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from gold import MODEL_VERSION

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Column:
    name: str
    dbml_type: str
    required: bool
    primary: bool
    reference: tuple | None

    @property
    def spark_type(self):
        if self.dbml_type.startswith("varchar"):
            return "string"
        return {"integer": "int", "timestamptz": "timestamp"}.get(self.dbml_type, self.dbml_type)


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple
    unique: tuple

    @property
    def primary_key(self):
        return tuple(c.name for c in self.columns if c.primary)

    def project(self, df):
        from pyspark.sql import functions as F
        if set(df.columns) != {c.name for c in self.columns}:
            raise ValueError(f"Gold projection drift: {self.name}: {df.columns}")
        return df.select(*(F.col(c.name).cast(c.spark_type).alias(c.name) for c in self.columns))


def load_spec(root=ROOT):
    text = (root / "docs/gold/dim_fact_v3.dbml").read_text(encoding="utf-8")
    if not text.startswith(f"// Canonical Gold schema: {MODEL_VERSION}"):
        raise ValueError("Unapproved Gold model version")
    result = {}
    for name, body in re.findall(r"Table gold_local\.(\w+) \{(.*?)(?=\n\})", text, re.S):
        columns = []
        for col, dtype, attrs in re.findall(r"^  (\w+) ([\w(),]+) \[([^\n]*)", body, re.M):
            ref = re.search(r"ref: > gold_local\.(\w+)\.(\w+)", attrs)
            pk = bool(re.match(r"pk(?:,|\])", attrs))
            columns.append(Column(col, dtype, pk or "not null" in attrs, pk, ref.groups() if ref else None))
        uniques = [tuple(x.strip() for x in v.split(",")) for v in re.findall(r"^    \(([^)]+)\) \[unique", body, re.M)]
        uniques += [(c,) for c, attrs in re.findall(r"^  (\w+) [\w(),]+ \[([^\n]*)", body, re.M) if re.search(r"\bunique\b", attrs)]
        if name in result or len({c.name for c in columns}) != len(columns):
            raise ValueError("Duplicate Gold table/column")
        result[name] = Table(name, tuple(columns), tuple(uniques))
    if len(result) != 17 or sum(len(t.columns) for t in result.values()) != 262:
        raise ValueError("Canonical Gold inventory must be 17 tables / 262 columns")
    for table in result.values():
        if len(table.primary_key) != 1:
            raise ValueError(f"Missing/ambiguous PK: {table.name}")
        for col in table.columns:
            if col.reference:
                parent, field = col.reference
                target = next((c for c in result[parent].columns if c.name == field), None)
                if target is None or target.dbml_type != col.dbml_type:
                    raise ValueError(f"Invalid FK: {table.name}.{col.name}")
    return result


def validate_mapping(root=ROOT):
    import yaml
    from silver.local_profile import MAPPINGS
    tables = load_spec(root)
    targets = {d: f"iceberg.silver_local.{t}" for _, d, t in MAPPINGS}
    contracts = {d: yaml.safe_load((root / f"contracts/silver/{d}.yaml").read_text(encoding="utf-8")) for d in targets}
    technical = {"_ingestion_run_id", "_ingestion_batch_id", "_ingestion_chunk_id", "_ingestion_timestamp", "_source_id", "_source_file", "_source_checksum", "_source_snapshot_id", "_dq_warnings", "_silver_processed_at"}
    rows = list(csv.DictReader((root / "docs/gold/source_to_gold_mapping.csv").open(encoding="utf-8-sig")))
    covered, branches = set(), set()
    for row in rows:
        pair = row["gold_table"], row["gold_column"]
        branch = (*pair, row["source_dataset"])
        if branch in branches:
            raise ValueError(f"Duplicate mapping branch: {branch}")
        branches.add(branch)
        covered.add(pair)
        col = next(c for c in tables[pair[0]].columns if c.name == pair[1])
        source = row["source_dataset"]
        valid_cols = {c["name"] for c in contracts[source]["schema"]["columns"]} | technical
        if (row["schema_version"] != MODEL_VERSION or row["gold_type"] != col.dbml_type
                or row["gold_nullable"] != str(not col.required).upper()
                or row["source_silver_table"] != targets[source]
                or not set(json.loads(row["source_columns_json"])).issubset(valid_cols)
                or row["mapping_status"] not in {"DIRECT", "DERIVED", "LOOKUP"}
                or row["source_contract_sha256"] != hashlib.sha256((root / f"contracts/silver/{source}.yaml").read_bytes()).hexdigest()):
            raise ValueError(f"Incompatible mapping: {branch}")
    if covered != {(t.name, c.name) for t in tables.values() for c in t.columns} or len(rows) != 488:
        raise ValueError("Incomplete canonical mapping")
    return {"tables": 17, "columns": 262, "mapping_branches": len(rows), "foreign_keys": sum(bool(c.reference) for t in tables.values() for c in t.columns)}
