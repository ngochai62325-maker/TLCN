"""Gold Schema Specification, Metric/Unit Rules & Guarded Iceberg Catalog Writes.

Consolidates:
- DBML Star Schema spec loader (`load_spec`, `validate_mapping`, `Column`, `Table`)
- Metric/unit conversion & aggregation rules (`metric_rule`, `checked_decimal`, `SUA`, `PSD`, ...)
- Guarded Iceberg snapshot reads/writes (`identity`, `pinned_read`, `check_owned`, `file_state`, `write_snapshot`)
"""
import csv
from decimal import Decimal, localcontext
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from gold import MODEL_VERSION, OWNER, OUTPUT_NAMESPACE

ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Metric & Unit Rules (formerly `rules.py`)
# ---------------------------------------------------------------------------

SUA = {"5016": "loss_t", "5023": "processed_t", "5071": "stock_variation_t", "5113": "opening_stocks_t", "5141": "food_supply_quantity_t", "5165": "other_uses_t", "5166": "residuals_t", "5510": "production_t", "5520": "feed_t", "5525": "seed_t", "5610": "import_quantity_t", "5910": "export_quantity_t"}
PSD = {"Area Harvested": "1000 HA", "Beginning Stocks": "1000 MT", "Domestic Consumption": "1000 MT", "Ending Stocks": "1000 MT", "Exports": "1000 MT", "Imports": "1000 MT", "Milling Rate (.9999)": "fraction", "Production": "1000 MT", "Rough Production": "1000 MT", "TY Exports": "1000 MT", "TY Imp. from U.S.": "1000 MT", "TY Imports": "1000 MT", "Total Distribution": "1000 MT", "Total Supply": "1000 MT", "Yield": "MT/HA"}
PRODUCTION = {"5312": "hectare", "5510": "tonne", "5412": "kg/hectare"}
TRADE = {"5610": "tonne", "5910": "tonne", "5622": "1000 USD", "5922": "1000 USD"}
PRICE_UNITS = {"5530": "LCU/tonne", "5531": "SLC/tonne", "5532": "USD/tonne", "5539": "index(2014-2016=100)"}


def metric_rule(source, metric, unit):
    expected = {"faostat_production": PRODUCTION, "faostat_supply_utilization": {c: "tonne" for c in SUA}, "faostat_trade": TRADE, "usda_psd": PSD}[source]
    if metric not in expected or unit != expected[metric]:
        raise ValueError(f"Unapproved metric/unit: {source}, {metric!r}, {unit!r}")
    factor = 1000 if unit in {"1000 USD", "1000 MT", "1000 HA"} else 1
    gold_unit = {"1000 USD": "USD", "1000 MT": "tonne", "1000 HA": "hectare", "MT/HA": "tonne/hectare"}.get(unit, unit)
    rule = "AREA_OR_FLOW_NONOVERLAP"
    if (source == "faostat_production" and metric == "5412") or metric in {"Yield", "Milling Rate (.9999)"}:
        rule = "NEVER_SUM"
    elif (source == "faostat_supply_utilization" and metric == "5113") or metric in {"Beginning Stocks", "Ending Stocks"}:
        rule = "NO_SUM_OVER_TIME"
    elif metric in {"Total Supply", "Total Distribution"}:
        rule = "NO_SUM_WITH_COMPONENTS"
    elif source == "faostat_supply_utilization" and metric in {"5071", "5166"}:
        rule = "SIGNED_FLOW"
    elif source == "usda_psd" and metric in {"Imports", "Exports"}:
        rule = "MY_FLOW"
    elif metric in {"TY Imports", "TY Exports"}:
        rule = "TY_FLOW"
    elif metric == "TY Imp. from U.S.":
        rule = "TY_SUBSET"
    return gold_unit, Decimal(factor), rule


def checked_decimal(value, factor, precision, scale):
    if value is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 60
        result = Decimal(value) * Decimal(factor)
        quantized = result.quantize(Decimal(1).scaleb(-scale))
        if not result.is_finite() or result != quantized or abs(result) >= Decimal(10) ** (precision - scale):
            raise ValueError(f"Decimal overflow/rounding: decimal({precision},{scale})")
        return quantized


# ---------------------------------------------------------------------------
# DBML Schema Specification & Mapping Validation
# ---------------------------------------------------------------------------


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
    pk_override: tuple = ()

    @property
    def primary_key(self):
        return self.pk_override or tuple(c.name for c in self.columns if c.primary)

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
        pk_indexes = [tuple(x.strip() for x in v.split(",")) for v in re.findall(r"^    \(([^)]+)\) \[pk", body, re.M)]
        pk_override = pk_indexes[0] if len(pk_indexes) == 1 else ()
        raw_cols = []
        for col, dtype, attrs in re.findall(r"^  (\w+) ([\w(),]+) \[([^\n]*)", body, re.M):
            ref = re.search(r"ref: > gold_local\.(\w+)\.(\w+)", attrs)
            pk = bool(re.match(r"pk(?:,|\])", attrs))
            raw_cols.append((col, dtype, pk or "not null" in attrs, pk, ref.groups() if ref else None))
        inline_pk = [c[0] for c in raw_cols if c[3]]
        if bool(inline_pk) == bool(pk_indexes) or (inline_pk and len(inline_pk) != 1) or (pk_indexes and len(pk_indexes) != 1):
            raise ValueError(f"Missing/ambiguous PK: {name}")
        if pk_override:
            col_req = {c[0]: c[2] for c in raw_cols}
            if not set(pk_override).issubset(col_req) or any(not col_req[c] for c in pk_override):
                raise ValueError(f"Composite PK requires existing NOT NULL columns: {name}")
        pk_set = set(pk_override)
        columns = [Column(col, dtype, req, pk or col in pk_set, ref) for col, dtype, req, pk, ref in raw_cols]
        uniques = [tuple(x.strip() for x in v.split(",")) for v in re.findall(r"^    \(([^)]+)\) \[unique", body, re.M)]
        uniques += [(c,) for c, attrs in re.findall(r"^  (\w+) [\w(),]+ \[([^\n]*)", body, re.M) if re.search(r"\bunique\b", attrs)]
        if name in result or len({c.name for c in columns}) != len(columns):
            raise ValueError("Duplicate Gold table/column")
        result[name] = Table(name, tuple(columns), tuple(uniques), pk_override)
    if len(result) != 17 or sum(len(t.columns) for t in result.values()) != 116:
        raise ValueError("Canonical Gold inventory must be 17 tables / 116 columns")
    for table in result.values():
        if not table.primary_key:
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
    if covered != {(t.name, c.name) for t in tables.values() for c in t.columns} or len(rows) != 326:
        raise ValueError("Incomplete canonical mapping")
    return {"tables": 17, "columns": 116, "mapping_branches": len(rows), "foreign_keys": sum(bool(c.reference) for t in tables.values() for c in t.columns)}


# ---------------------------------------------------------------------------
# Guarded Iceberg Catalog Snapshots & Writes (formerly `catalog.py`)
# ---------------------------------------------------------------------------

def identity(spark, name):
    if not spark.catalog.tableExists(name):
        return None
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    table.refresh()
    meta = table.operations().current()
    snap = meta.currentSnapshot()
    return {"uuid": str(meta.uuid()), "snapshot_id": str(snap.snapshotId()) if snap else None,
            "metadata_location": str(meta.metadataFileLocation()), "location": str(meta.location()),
            "owner": str(meta.properties().get("gold.owner")), "model": str(meta.properties().get("gold.model"))}


def pinned_read(spark, name, pin):
    current = identity(spark, name)
    if not current or current["uuid"] != pin["uuid"]:
        raise ValueError(f"Input table identity changed: {name}")
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    if table.snapshot(int(pin["snapshot_id"])) is None:
        raise ValueError(f"Expired/missing pinned input snapshot: {name}")
    return spark.read.format("iceberg").option("snapshot-id", pin["snapshot_id"]).load(name)


def check_owned(state, name):
    if (not state or state["owner"] != OWNER or state["model"] != MODEL_VERSION
            or not state["location"].startswith("s3://warehouse/gold_local/")):
        raise ValueError(f"Uninventoried/unowned Gold table: {name}")


def file_state(spark, name):
    # Sorted file paths are a compact real physical replay proof; data never collected.
    return [r[0] for r in spark.sql(f"SELECT file_path FROM {name}.files ORDER BY file_path").collect()]


def write_snapshot(spark, spec, df, dq, guard=lambda: None):
    name = f"{OUTPUT_NAMESPACE}.{spec.name}"
    before = identity(spark, name)
    if before:
        check_owned(before, name)
        current = spark.table(name)
        actual = [(f.name, f.dataType.simpleString()) for f in current.schema]
        expected = [(f.name, f.dataType.simpleString()) for f in df.schema]
        if actual != expected:
            raise ValueError(f"Existing Gold physical schema drift: {name}")
        before_files = file_state(spark, name)
        from gold.quality import _multiset_diff_count
        changed = _multiset_diff_count(df, current)
        if not changed:
            after = identity(spark, name)
            after_files = file_state(spark, name)
            dq.record("DQ16", spec.name, "physical NOOP: identical UUID/snapshot/metadata/files", before, after, before == after and before_files == after_files)
            try:
                df._verified_snapshot = (after["uuid"], after["snapshot_id"])
            except Exception:
                pass
            return {**after, "rows": current.count(), "write": "NOOP", "data_files": after_files}
    else:
        fields = ", ".join(f"`{c.name}` {c.spark_type}" + (" NOT NULL" if c.required else "") for c in spec.columns)
        guard()
        spark.sql(f"CREATE TABLE {name} ({fields}) USING iceberg TBLPROPERTIES ('format-version'='2','gold.owner'='{OWNER}','gold.model'='{MODEL_VERSION}','write.target-file-size-bytes'='67108864','write.distribution-mode'='none')")
    # Overwrite the *entire* admitted snapshot; withdrawn Silver rows disappear.
    # Do not overwritePartitions (empty/removed partitions would survive).
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql import types as T
    # UDF outputs are nullable to Spark's analyzer even after the DQ non-null gate.
    # Bind requiredness explicitly in the JVM without a Python RDD roundtrip;
    # keep Iceberg's write-time nullability checks enabled.
    schema = T.StructType([T.StructField(c.name, df.schema[c.name].dataType, not c.required) for c in spec.columns])
    if type(df) is DataFrame:
        jschema = spark._jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
        write_df = DataFrame(spark._jsparkSession.createDataFrame(df._jdf.rdd(), jschema), spark)
    else:
        write_df = spark.createDataFrame(df.rdd, schema, verifySchema=True)
    try:
        guard()
        write_df.writeTo(name).overwrite(F.lit(True))
    except Exception as exc:
        # A lost response is not proof the commit failed. Read catalog and data first.
        after = identity(spark, name)
        if after:
            check_owned(after, name)
            persisted = spark.table(name)
            same = not (df.exceptAll(persisted).limit(1).count() or persisted.exceptAll(df).limit(1).count())
            if same:
                return {**after, "rows": persisted.count(), "write": "COMMIT_VERIFIED_AFTER_ERROR", "data_files": file_state(spark, name)}
        raise RuntimeError(f"Unknown/failed Gold commit at {name}; retained snapshots; retry requires readback") from exc
    after = identity(spark, name)
    guard()
    check_owned(after, name)
    dq.same("DQ17", spec.name, df, spark.table(name), "committed full table readback")
    try:
        df._verified_snapshot = (after["uuid"], after["snapshot_id"])
    except Exception:
        pass
    return {**after, "rows": spark.table(name).count(), "write": "COMMITTED", "data_files": file_state(spark, name)}
