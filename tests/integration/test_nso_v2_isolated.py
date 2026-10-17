"""All accessible NSO CSVs, independent oracle, real temporary Iceberg writes.

Run only with a Hadoop catalog in a network-disabled disposable container.
The repository contract stays DRAFT; the execution copy is temporary and local.
"""
import copy
import csv
import hashlib
import uuid
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession, functions as F, types as T

from ingestion.storage.bronze_writer import sanitize_column_name
from silver.audit import audit_tables
from silver.contract.loader import SilverContract, SilverContractLoader
from silver.engine.quality import SilverQualityEngine
from silver.framework import AUDIT_COLUMNS, SilverTransformationFramework
from silver.transformers.nso import NsoTransformer

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data/raw/nso"
DATASET = "nso"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (SparkSession.builder.master("local[2]").appName("NsoV2Isolated")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.defaultCatalog", "spark_catalog")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.catalog.nso_v2_test", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.catalog.nso_v2_test.type", "hadoop")
               .config("spark.sql.catalog.nso_v2_test.warehouse", tmp_path_factory.mktemp("nso-v2-warehouse").as_uri())
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    for namespace in ("bronze", "silver"):
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS nso_v2_test.{namespace}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw_input():
    """Expected observations come from Raw headers and metadata, not the transformer."""
    titles = {int(n): title for n, title in re.findall(r"^(\d+)\. (.+)$", (RAW / "metadata.txt").read_text(encoding="utf-8"), re.M)}
    records, expected, widths, structural, hashes = [], {}, {}, [], {}
    files = sorted(RAW.glob("V06.*.csv"))
    assert {int(p.stem.split(".")[1]) for p in files} == set(range(12, 25)), "Incomplete accessible Raw coverage"
    for path in files:
        hashes[str(path)] = checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        lines = path.read_text(encoding="cp1252").splitlines()
        # Python 3.10 csv rejects NUL, while 3.11+ preserves it. Handle only
        # the exact terminal footer and retain that byte in the test Bronze row.
        nul_footer = lines[-1] == "\x00"
        rows = list(csv.reader(lines[:-1] if nul_footer else lines))
        if nul_footer:
            rows.append(["\x00"])
        header, number = rows[0], int(path.stem.split(".")[1])
        columns = [sanitize_column_name(c) for c in header]
        assert len(columns) == len(set(columns))
        national = number == 12
        assert len(columns) == (10 if national else 31)
        widths[path.name] = len(header) - (2 if national else 1)
        title = titles[number].lower()
        matrix_measure = "area" if "diện tích" in title else "yield" if "năng suất" in title else "production"
        matrix_season = ("summer_autumn_autumn_winter" if "hè thu và thu đông" in title else
                         "winter_spring" if "đông xuân" in title else "main_season" if "lúa mùa" in title else "annual")
        for row_index, row in enumerate(rows[1:], 1):
            footer = row == ["\x00"] and row_index == len(rows) - 1
            empty = all(not c.strip() for c in row)
            assert len(row) == len(header) or footer or empty
            values = dict(zip(columns, row + [None] * (len(header) - len(row))))
            values.update(_ingestion_run_id="national-run" if national else "matrix-run",
                          _ingestion_batch_id=f"batch-{number}", _ingestion_chunk_id=0,
                          _ingestion_timestamp=datetime(2026, 10, 11), _source_id="nso",
                          _source_file=f"raw/nso/{path.name}", _source_checksum=checksum, _source_snapshot_id=0)
            records.append(values)
            if footer or empty:
                structural.append((path.name, widths[path.name]))
                continue
            for index in range(2 if national else 1, len(header)):
                label = header[index].lower()
                measure = ("area" if "di?n tích" in label else "production") if national else matrix_measure
                season = (("winter_spring" if "ông xuân" in label else "summer_autumn" if "hè thu" in label
                           else "main_season" if "mùa" in label else "annual") if national else matrix_season)
                kind = "development_index" if national and "=100" in row[0] else "quantity"
                period = row[1] if national else header[index]
                assert re.fullmatch(r"(?:So b\? )?\d{4}", period)
                geography = None if national else row[0]
                geo_key = "__national_table_scope__" if national else "raw:" + hashlib.sha256(geography.encode("utf-8")).hexdigest()
                key = (path.name, geo_key, int(period[-4:]), measure, season, kind)
                value_raw = row[index]
                missing = value_raw.strip() == ".." or not value_raw.strip()
                source_value = None if missing else Decimal(value_raw)
                unit_raw = ("%" if kind == "development_index" else re.search(r"\(([^)]+)\)", header[index])[1]) if national else None
                expected[key] = dict(source_column=columns[index], geography_raw=geography, period_raw=period,
                                     value_raw=value_raw, value_source=source_value, unit_raw=unit_raw,
                                     value=source_value * (1 if kind == "development_index" else 100 if measure == "yield" else 1000) if not missing else None,
                                     unit=("percent" if kind == "development_index" else "hectare" if measure == "area" else "kg/hectare" if measure == "yield" else "tonne"),
                                     quality_status="MISSING" if missing else "NORMALIZED",
                                     release_status="provisional" if period.startswith("So b? ") else "unconfirmed",
                                     **{c: values[c] for c in AUDIT_COLUMNS})
        assert len(rows[1:]) * widths[path.name] >= sum(1 for k in expected if k[0] == path.name)
    assert len(expected) == sum(widths[r["_source_file"].split("/")[-1]] for r in records) - sum(w for _, w in structural), "Raw key conflict/duplicate needs review"
    return records, expected, structural, widths, hashes


def dataframe(spark, records):
    columns = sorted(set().union(*(r.keys() for r in records)))
    fields = [T.StructField(c, T.TimestampType() if c == "_ingestion_timestamp" else
                            T.LongType() if c in {"_ingestion_chunk_id", "_source_snapshot_id"} else T.StringType(), True) for c in columns]
    return spark.createDataFrame([tuple(r.get(c) for c in columns) for r in records], T.StructType(fields))


def isolated_framework(spark, tmp_path, suffix):
    data = copy.deepcopy(SilverContractLoader().load_contract(DATASET)._data)
    assert data["contract_status"] == "READY"
    data["dataset"]["bronze_input"] = f"nso_v2_test.bronze.{suffix}"
    data["dataset"]["silver_output"] = f"nso_v2_test.silver.{suffix}"
    (tmp_path / f"{DATASET}.yaml").write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    framework = SilverTransformationFramework(spark, str(tmp_path))
    framework.quarantine_manager.target_table = f"nso_v2_test.silver.{suffix}_dead"
    return framework, SilverContract(data)


def native_prepare(spark, transformer, df):
    """Even guard tests bind an actual native Iceberg snapshot."""
    from silver.framework import BronzeReadContext
    table = "nso_v2_test.bronze.direct_" + uuid.uuid4().hex
    df.writeTo(table).using("iceberg").create()
    snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()[0]
    read = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(table)
    return transformer.preprocess_with_read_context(
        read.withColumn("_bronze_read_snapshot_id", F.lit(snapshot).cast("long")), BronzeReadContext(table, snapshot))


def table_identity(spark, table):
    """Native snapshot identity plus schema and all fields, including processing time."""
    snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1").first()[0]
    df = spark.table(table)
    payloads = sorted(df.toJSON().collect())
    return snapshot, df.schema.json(), hashlib.sha256("\n".join(payloads).encode()).hexdigest()


def test_all_raw_full_incremental_replays_and_native_noop(spark, tmp_path, raw_input):
    records, expected, structural, widths, hashes = raw_input
    framework, contract = isolated_framework(spark, tmp_path, "all_raw")
    raw = dataframe(spark, records)
    raw.writeTo(contract.bronze_input).using("iceberg").create()
    bronze_before = table_identity(spark, contract.bronze_input)
    first = framework.run(DATASET, "first")
    counts = Counter(r["quality_status"] for r in expected.values())
    assert first["bronze_count"] == len(records)
    assert first["observation_count"] == sum(widths[r["_source_file"].split("/")[-1]] for r in records)
    assert first["excluded_count"] == sum(w for _, w in structural)
    assert first["excluded_record_count"] == len(structural)
    assert first["prepared_count"] == first["valid_count"] == first["dedup_count"] == len(expected)
    assert first["quarantine_count"] == first["duplicate_count"] == 0
    assert first["observation_count"] == first["valid_count"] + first["quarantine_count"] + first["excluded_count"]
    result = spark.table(contract.silver_output)
    assert set(result.columns) == {c["name"] for c in contract.columns} | set(AUDIT_COLUMNS) | {"_dq_warnings", "_silver_processed_at"}
    assert all(result.schema[c].dataType == raw.schema[c].dataType for c in AUDIT_COLUMNS)
    observed = {}
    for row in result.collect():
        d = row.asDict()
        key = tuple(d[c] for c in contract.business_key)
        assert key not in observed
        observed[key] = d
        assert key in expected
        for c, value in expected[key].items():
            assert d[c] == value, (key, c, d[c], value)
        pending = d["source_table"] != "V06.12.csv"
        assert bool(d["review_reason"]) == pending
        assert bool(d["_dq_warnings"]) == (pending or d["diagnostic_evidence"] != "[]")
        assert d["bronze_read_snapshot_id"] == bronze_before[0]
        assert d["geography_code"] is None and d["boundary_version"] is None
        assert d["unit_mapping_version"] == "nso-units-20261015.1"
        assert d["geography_level"] in {"national", "region", "province"}
        assert d["missing_reason"] == ("source_marker_unspecified" if d["quality_status"] == "MISSING" else None)
    assert len(records) == 833 and len(structural) == 13
    assert len(observed) == 23060
    assert sum(d["value_source"] is not None for d in observed.values()) == 22114
    assert sum(d["missing_reason"] == "source_marker_unspecified" for d in observed.values()) == 946
    assert sum(d["value_source"] == 0 for d in observed.values()) == 5
    assert sum(d["source_table"] == "V06.12.csv" for d in observed.values()) == 560
    assert sum(d["release_status"] == "provisional" for d in observed.values()) == 766
    assert observed.keys() == expected.keys()
    assert Counter(d["quality_status"] for d in observed.values()) == counts
    assert any(d["geography_raw"] == "Hà Tây" and d["geography_status"] == "candidate_review" for d in observed.values())
    assert sum(d["value_source"] == 0 for d in observed.values()) == sum(e["value_source"] == 0 for e in expected.values())
    dead = spark.table(framework.quarantine_manager.target_table).collect()
    assert len(dead) == len(structural)
    assert all(r.source_dataset == DATASET + "_exclusions" for r in dead)
    assert all(any(e.rule_id == "NSO_V2_STRUCTURAL" for e in r.errors) for r in dead)
    assert all(json.loads(r.payload)["_source_checksum"] for r in dead)
    target_before = table_identity(spark, contract.silver_output)
    replays = [("full-again", {}), ("incremental", {"mode": "incremental", "run_id": "matrix-run"}),
               ("incremental-again", {"mode": "incremental", "run_id": "matrix-run"}),
               ("empty", {"mode": "incremental", "run_id": "absent"})]
    for name, args in replays:
        replay = framework.run(DATASET, name, **args)
        selected = {k: e for k, e in expected.items() if not args or e["_ingestion_run_id"] == args["run_id"]}
        assert replay["dedup_count"] == len(selected)
        assert table_identity(spark, contract.silver_output) == target_before, name
        assert spark.table(framework.quarantine_manager.target_table).count() == len(structural)
    assert table_identity(spark, contract.bronze_input) == bronze_before
    assert audit_tables(spark, [contract])["status"] == "PASSED"
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == digest for p, digest in hashes.items())
    print("NSO_V2_FULL_INPUT", json.dumps(dict(raw_records=len(records), observations=len(expected), states=dict(counts),
                                            structural_records=len(structural), excluded_slots=sum(w for _, w in structural),
                                            quarantine=first["quarantine_count"], native_snapshot=target_before[0],
                                            replay_noop=[n for n, _ in replays])))


def national_row(raw_input, year="2020", value="1.5"):
    row = copy.deepcopy(next(r for r in raw_input[0] if r["_source_file"].endswith("V06.12.csv") and r["gi_tr_v_ch_s_ph_t_tri_n"] == "Giá tr?"))
    row["nam"] = year
    row["t_ng_di_n_t_ch_ngh_n_ha_"] = value
    return row


def test_numeric_missing_quarantine_and_exclusion_payloads(spark, tmp_path, raw_input):
    tokens = [None, "", "  ", "..", "0", "-0.0", " 2.5 ", "bad", "NaN", "Infinity", "1,234",
              "NA", "N/A", "NULL", "...", "…", "-1", "-0.0000000001", "1.000000001", "9" * 30,
              "99999999999999999999", "\x00"]
    bad_tokens = tokens[7:]
    rows = [national_row(raw_input, str(2000 + i), token) for i, token in enumerate(tokens)]
    rows += [dict(national_row(raw_input, "bad-year"), _ingestion_batch_id="bad-year"),
             dict(national_row(raw_input, "2100"), gi_tr_v_ch_s_ph_t_tri_n="unknown-kind"),
             dict(national_row(raw_input, "2101"), _source_checksum=None)]
    footer = national_row(raw_input)
    for c in list(footer):
        if not c.startswith("_"):
            footer[c] = None
    rows += [footer, dict(footer, gi_tr_v_ch_s_ph_t_tri_n="\x00")]
    framework, contract = isolated_framework(spark, tmp_path, "invalid")
    dataframe(spark, rows).writeTo(contract.bronze_input).using("iceberg").create()
    result = framework.run(DATASET, "bad-input")
    assert result["excluded_record_count"] == 2
    assert result["quarantine_count"] == len(bad_tokens) + 3 * 8
    assert result["prepared_count"] == (len(rows) - 2) * 8
    assert result["valid_count"] + result["quarantine_count"] == result["prepared_count"]
    valid = spark.table(contract.silver_output)
    assert valid.filter("quality_status = 'MISSING'").count() == 4
    assert valid.filter("source_column = 't_ng_di_n_t_ch_ngh_n_ha_' AND value_source = 0").count() == 2
    assert valid.filter("source_column = 't_ng_di_n_t_ch_ngh_n_ha_' AND value_raw = ' 2.5 '").first().value == Decimal("2500")
    dead = spark.table(framework.quarantine_manager.target_table).collect()
    quarantine = [r for r in dead if r.source_dataset == DATASET]
    assert len(quarantine) == result["quarantine_count"]
    payloads = [json.loads(r.payload) for r in quarantine]
    assert all("_source_payload" in p and p["_source_file"].endswith("V06.12.csv") for p in payloads)
    assert {p["value_raw"] for p in payloads if p["source_column"] == "t_ng_di_n_t_ch_ngh_n_ha_"} >= set(bad_tokens)
    before = table_identity(spark, contract.silver_output)
    framework.run(DATASET, "bad-replay", mode="incremental", run_id="national-run")
    assert table_identity(spark, contract.silver_output) == before
    assert spark.table(framework.quarantine_manager.target_table).count() == len(dead)


def test_duplicate_conflict_drift_and_lineage_guards(spark, raw_input):
    transformer = NsoTransformer()
    row = national_row(raw_input)
    duplicate = dict(row, _ingestion_batch_id="retry", _ingestion_timestamp=datetime(2026, 10, 12))
    prepared = native_prepare(spark, transformer, dataframe(spark, [row, duplicate]))
    assert prepared.count() == 16
    engine = SilverTransformationFramework(spark).merge_engine
    winners = engine.deduplicate(prepared, transformer.business_keys).collect()
    assert len(winners) == 8 and all(r._ingestion_batch_id == "retry" for r in winners)
    for changed, message in [(dict(row, t_ng_di_n_t_ch_ngh_n_ha_="99"), "conflicting source observations"),
                             (dict(row, _source_checksum="new-release"), "source checksum/schema changed"),
                             (dict(row, col_2025="1"), "source checksum/schema changed"),
                             (dict(row, extra_measure="1"), "source checksum/schema changed"),
                             (dict(row, _source_file="V06.25.csv"), "unprofiled NSO files")]:
        with pytest.raises(ValueError, match=message):
            native_prepare(spark, transformer, dataframe(spark, [row, changed]))
    incomplete = dataframe(spark, [row]).drop("nam")
    with pytest.raises(ValueError, match="missing source columns"):
        native_prepare(spark, transformer, incomplete)
    with pytest.raises(ValueError, match="missing lineage columns"):
        native_prepare(spark, transformer, dataframe(spark, [row]).drop("_source_snapshot_id"))
    matrix = copy.deepcopy(next(r for r in raw_input[0] if r["_source_file"].endswith("V06.13.csv") and r["t_nh_th_nh_ph_"] == "C? NU?C"))
    matrix["t_nh_th_nh_ph_"] = " "
    out = native_prepare(spark, transformer, dataframe(spark, [matrix]))
    contract = SilverContractLoader().load_contract(DATASET)
    normalized = SilverTransformationFramework(spark).normalize(out, contract)
    _, invalid = SilverQualityEngine(contract.data_quality_rules + transformer.quality_rules()).apply_rules(normalized)
    assert invalid.count() == len([c for c in matrix if c.startswith("col_") or c.startswith("so_b_")])


def test_native_duplicate_replay_and_conflict_fail_before_write(spark, tmp_path, raw_input):
    row = national_row(raw_input)
    retry = dict(row, _ingestion_batch_id="retry", _ingestion_timestamp=datetime(2026, 10, 12))
    framework, contract = isolated_framework(spark, tmp_path, "duplicates")
    dataframe(spark, [row, retry]).writeTo(contract.bronze_input).using("iceberg").create()
    result = framework.run(DATASET, "duplicates")
    width = raw_input[3]["V06.12.csv"]
    assert result["valid_count"] == 2 * width
    assert result["dedup_count"] == result["duplicate_count"] == width
    assert all(r._ingestion_batch_id == "retry" for r in spark.table(contract.silver_output).collect())
    before = table_identity(spark, contract.silver_output)
    framework.run(DATASET, "duplicate-replay", mode="incremental", run_id="national-run")
    assert table_identity(spark, contract.silver_output) == before
    changed = dict(row, t_ng_di_n_t_ch_ngh_n_ha_="99", _ingestion_batch_id="conflict")
    dataframe(spark, [changed]).writeTo(contract.bronze_input).append()
    with pytest.raises(ValueError, match="conflicting source observations"):
        framework.run(DATASET, "must-not-select-release-by-ingestion-time")
    assert table_identity(spark, contract.silver_output) == before
    assert spark.table(framework.quarantine_manager.target_table).count() == 0


def test_draft_contract_refuses_execution_before_any_write(spark, tmp_path):
    contract = SilverContractLoader().load_contract(DATASET)
    assert contract.status == "READY"
    draft = copy.deepcopy(contract._data)
    draft["contract_status"] = "DRAFT"
    (tmp_path / f"{DATASET}.yaml").write_text(yaml.safe_dump(draft, allow_unicode=True), encoding="utf-8")
    before = spark.sql("SHOW TABLES IN nso_v2_test.silver").collect()
    with pytest.raises(ValueError, match="contract is DRAFT"):
        SilverTransformationFramework(spark, str(tmp_path)).run(DATASET, "must-not-write")
    assert spark.sql("SHOW TABLES IN nso_v2_test.silver").collect() == before
