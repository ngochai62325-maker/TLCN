"""Real NSO adapter/PG/S3/REST/Spark/PyIceberg/Trino, isolated endpoints only."""
import copy
import hashlib
import json
import os
import shutil
import uuid
from collections import Counter
from dataclasses import replace
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession, functions as F

from ingestion.config.registry import SourceRegistry
from ingestion.storage.bronze_writer import BronzeIcebergWriter
from scripts.validate_silver_local_snapshots import configure_rebuild
from silver.contract.loader import SilverContractLoader
from silver.framework import AUDIT_COLUMNS, SilverTransformationFramework
from tests.integration.test_nso_v2_isolated import RAW, DATASET, raw_input, table_identity


def _catalog_identity(writer, namespace):
    """Physical identity plus all row bytes, including original audit timestamps."""
    table = writer.get_iceberg_catalog().load_table(f"{namespace}.nso_vietnam")
    rows = table.scan().to_arrow().to_pylist()
    return dict(uuid=str(table.metadata.table_uuid), snapshot=table.current_snapshot().snapshot_id,
                metadata_location=table.metadata_location, rows=len(rows),
                checksum=hashlib.sha256(json.dumps(sorted(json.dumps(r, sort_keys=True, default=str)
                                                         for r in rows)).encode()).hexdigest())


def _stage_unapproved_revision(engine, raw, namespace):
    """Test-only publication staging; never change FullLoader filename policy."""
    assert os.environ.get("TLCN_ISOLATED_REBUILD") == "1"
    assert os.environ.get("ICEBERG_REST_URI") == "http://rest-test:8181"
    assert namespace.startswith("nso_revision_")
    assert raw.resolve() != RAW.resolve()
    path = raw / "V06.13.csv"
    original = path.read_bytes()
    revised = original.replace(b"7192.5", b"7192.6", 1)
    assert revised != original
    # Keep the approved publication bytes outside the adapter's input directory.
    preserved = raw.parent / "accepted-V06.13.csv"
    preserved.write_bytes(original)
    path.write_bytes(revised)
    accepted_writer = engine.bronze_writer
    writer = BronzeIcebergWriter(catalog="rebuild_test", schema=namespace)
    try:
        engine.bronze_writer = writer
        result = engine.run_with_readiness_check("nso_vietnam")
    finally:
        engine.bronze_writer = accepted_writer
    assert result.status.value == "SUCCESS" and result.records_extracted == 833
    rows = writer.get_iceberg_catalog().load_table(f"{namespace}.nso_vietnam").scan().to_arrow().to_pylist()
    changed = [r for r in rows if Path(r["_source_file"]).name == path.name]
    checksum = hashlib.sha256(revised).hexdigest()
    assert changed and {r["_source_checksum"] for r in changed} == {checksum}
    assert any(str(v) == "7192.6" for r in changed for v in r.values())
    assert {r["_source_id"] for r in rows} == {"nso_vietnam"}
    assert {r["_ingestion_run_id"] for r in rows} == {result.run_id}
    assert preserved.read_bytes() == original and (RAW / path.name).read_bytes() == original
    return result, writer, dict(original_sha256=hashlib.sha256(original).hexdigest(),
                               revised_sha256=checksum, preserved_original=True,
                               revised_rows=len(changed), table=f"rebuild_test.{namespace}.nso_vietnam")


@pytest.mark.skipif(os.getenv("TLCN_ISOLATED_REBUILD") != "1", reason="Independent NSO test services required")
def test_revision_staging_same_filename_separate_bronze(tmp_path, monkeypatch):
    """Real adapter/PG/S3/REST regression before the full Spark/Trino acceptance.

    A dedicated metadata DB preserves the fresh full-E2E DB. No Spark JVM runs.
    Both publications and every committed object remain available afterwards.
    """
    import psycopg2
    from psycopg2 import sql
    from urllib.parse import urlsplit, urlunsplit
    from ingestion.storage.metadata_repository import MetadataRepository
    from ingestion.core.ingestion_engine import IngestionEngine

    raw = tmp_path / "raw"
    shutil.copytree(RAW, raw)
    registry = SourceRegistry()
    registry.load()
    config = replace(registry.get_source("nso_vietnam"), local_path=str(raw), local_fallback=str(raw))
    _, engine, _ = configure_rebuild(SparkSession.builder, tmp_path, {"nso_vietnam": config})
    dsn = urlsplit(os.environ["INGESTION_DB_URL"])
    assert dsn.hostname == "postgres-test" and dsn.path == "/silver_rebuild_test"
    database = "nso_staging_" + uuid.uuid4().hex[:12]
    conn = psycopg2.connect(os.environ["INGESTION_DB_URL"])
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    finally:
        conn.close()
    monkeypatch.setenv("INGESTION_DB_URL", urlunsplit(dsn._replace(path="/" + database)))
    metadata = MetadataRepository()
    metadata.initialize()
    engine = IngestionEngine(registry=engine.registry, metadata_repo=metadata,
                             minio_storage=engine.minio_storage,
                             bronze_writer=BronzeIcebergWriter(catalog="rebuild_test", schema="nso_staging_accepted"))
    first = engine.run_with_readiness_check("nso_vietnam")
    assert first.status.value == "SUCCESS" and first.records_extracted == 833
    before = _catalog_identity(engine.bronze_writer, "nso_staging_accepted")
    revision, writer, proof = _stage_unapproved_revision(engine, raw, "nso_revision_staging")
    after = _catalog_identity(engine.bronze_writer, "nso_staging_accepted")
    revised = _catalog_identity(writer, "nso_revision_staging")
    assert before == after and revised["uuid"] != before["uuid"]
    assert revision.run_id != first.run_id
    report = dict(status="PASS", original=before, revision=revised, proof=proof,
                  original_run_id=first.run_id, revised_run_id=revision.run_id, metadata_database=database)
    Path("/tmp/nso-v2-staging-regression.json").write_text(json.dumps(report, indent=2))
    print("NSO_V2_STAGING_REGRESSION=" + json.dumps(report))


@pytest.mark.skipif(os.getenv("TLCN_ISOLATED_REBUILD") != "1", reason="Independent NSO test services required")
def test_raw_bronze_silver_trino_and_blocked_revision(tmp_path, raw_input):
    # configure_rebuild enforces the existing test-only endpoint/database guards.
    raw = tmp_path / "raw"
    shutil.copytree(RAW, raw)
    registry = SourceRegistry()
    registry.load()
    config = replace(registry.get_source("nso_vietnam"), local_path=str(raw), local_fallback=str(raw))
    builder = (SparkSession.builder.master("local[2]").appName("NsoV2RestE2E")
               .config("spark.ui.enabled", "false").config("spark.sql.defaultCatalog", "spark_catalog")
               .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"))
    builder, engine, query = configure_rebuild(builder, tmp_path, {"nso_vietnam": config})
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ingestion = engine.run_with_readiness_check("nso_vietnam")
        assert ingestion.status.value == "SUCCESS" and ingestion.records_extracted == 833
        bronze = "rebuild_test.bronze.nso_vietnam"
        bronze_before = table_identity(spark, bronze)
        accepted_bronze = _catalog_identity(engine.bronze_writer, "bronze")
        assert spark.table(bronze).count() == 833
        data = copy.deepcopy(SilverContractLoader().load_contract(DATASET)._data)
        assert data["contract_status"] == "DRAFT"
        data["contract_status"] = "READY"
        target = "rebuild_test.silver.nso_rice_statistics_v2"
        data["dataset"].update(bronze_input=bronze, silver_output=target)
        contracts = tmp_path / "contracts"
        contracts.mkdir()
        (contracts / f"{DATASET}.yaml").write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
        framework = SilverTransformationFramework(spark, str(contracts))
        framework.quarantine_manager.target_table = "rebuild_test.silver.dead_letters"
        with engine.metadata_repo.source_lock("nso_vietnam"):
            metrics = framework.run(DATASET, "nso-isolated-first", bronze_snapshot_id=bronze_before[0])
        assert {k: metrics[k] for k in ["bronze_count", "excluded_record_count", "prepared_count", "valid_count", "quarantine_count", "dedup_count", "duplicate_count"]} == dict(
            bronze_count=833, excluded_record_count=13, prepared_count=23060, valid_count=23060,
            quarantine_count=0, dedup_count=23060, duplicate_count=0)
        expected = raw_input[1]
        metadata = json.loads(Path("config/silver/nso_v2_metadata.json").read_text(encoding="utf-8"))
        assert hashlib.sha256((RAW / "metadata.txt").read_bytes()).hexdigest() == metadata["source_metadata_sha256"]
        raw_hashes = {Path(p).name: digest for p, digest in raw_input[4].items()}
        assert raw_hashes == {p: spec["sha256"] for p, spec in metadata["tables"].items()}
        output = spark.table(target)
        collected = output.collect()
        observed = {tuple(r[k] for k in framework.contract(DATASET).business_key): r.asDict() for r in collected}
        assert len(observed) == len(collected) == 23060  # No business-key collision hidden by a dict.
        assert observed.keys() == expected.keys()
        bronze_rows = {}
        for row in spark.table(bronze).collect():
            d = row.asDict()
            file = Path(d["_source_file"]).name
            assert d["_source_checksum"] == raw_hashes[file] and d["_source_id"] == "nso_vietnam"
            identity = (file, d.get("gi_tr_v_ch_s_ph_t_tri_n"), d.get("nam")) if file == "V06.12.csv" else (file, d.get("t_nh_th_nh_ph_"))
            bronze_rows[identity] = d
        for key, d in observed.items():
            for name, value in expected[key].items():
                if name not in AUDIT_COLUMNS:
                    assert d[name] == value, (key, name)
            identity = ((key[0], "Ch? s? ph\u00e1t tri?n (Nam tru?c =100) - %" if key[-1] == "development_index" else "Gi\u00e1 tr?", d["period_raw"])
                        if key[0] == "V06.12.csv" else (key[0], d["geography_raw"]))
            source = bronze_rows[identity]
            assert all(d[c] == source[c] for c in AUDIT_COLUMNS)
            assert d["_ingestion_run_id"] == ingestion.run_id
            assert d["bronze_read_snapshot_id"] == bronze_before[0] != d["_source_snapshot_id"]
            assert d["geography_code"] is None and d["boundary_version"] is None
            assert d["metadata_version"] == metadata["metadata_version"]
            assert d["unit_mapping_version"] == metadata["unit_mapping_version"]
            assert d["geography_mapping_version"] == metadata["geography_mapping_version"]
            candidate = metadata["geography_candidates"].get(d["geography_raw"])
            assert d["geography_name_candidate"] == (candidate[0] if candidate else None)
            pending = d["source_table"] != "V06.12.csv"
            assert bool(d["review_reason"]) == pending
            assert bool(d["_dq_warnings"]) == (pending or d["diagnostic_evidence"] != "[]")
            assert d["missing_reason"] == ("source_marker_unspecified" if d["quality_status"] == "MISSING" else None)
        counts = dict(numeric=sum(d["value_source"] is not None for d in observed.values()),
                      missing=sum(d["missing_reason"] == "source_marker_unspecified" for d in observed.values()),
                      zeros=sum(d["value_source"] == 0 for d in observed.values()),
                      national=sum(d["source_table"] == "V06.12.csv" for d in observed.values()),
                      provisional=sum(d["release_status"] == "provisional" for d in observed.values()))
        assert counts == dict(numeric=22114, missing=946, zeros=5, national=560, provisional=766)
        assert {d["season"] for d in observed.values()} == {"annual", "winter_spring", "summer_autumn", "summer_autumn_autumn_winter", "main_season"}
        assert any(d["geography_raw"] == "H\u00e0 T\u00e2y" and d["geography_status"] == "candidate_review" for d in observed.values())
        diagnostics = Counter(e["rule_id"] for d in observed.values() for e in json.loads(d["diagnostic_evidence"]))
        assert diagnostics == {"NSO_CROSS_TABLE": 12, "NSO_AREA_YIELD_PRODUCTION": 309, "NSO_SEASON_TOTAL": 28}
        exclusions = spark.table(framework.quarantine_manager.target_table).collect()
        assert len(exclusions) == 13
        assert all(r.source_dataset == DATASET + "_exclusions" and any(e.rule_id == "NSO_V2_STRUCTURAL" for e in r.errors) for r in exclusions)
        cat = engine.bronze_writer.get_iceberg_catalog()
        iceberg = cat.load_table("silver.nso_rice_statistics_v2")
        arrow = iceberg.scan().to_arrow()
        assert arrow.num_rows == len(observed)
        assert str(iceberg.schema().find_field("bronze_read_snapshot_id").field_type) == "long"
        assert str(iceberg.schema().find_field("value").field_type) == "decimal(28, 8)"
        for d in arrow.to_pylist():
            key = tuple(d[k] for k in framework.contract(DATASET).business_key)
            assert d["value"] == observed[key]["value"] and d["value_source"] == observed[key]["value_source"]
            assert d["bronze_read_snapshot_id"] == bronze_before[0]
        url = "http://trino-test:8080"
        schema = {r["Column"]: r["Type"] for r in query(url, "DESCRIBE " + target)}
        assert schema["bronze_read_snapshot_id"] == "bigint" and schema["value"] == "decimal(28,8)"
        assert schema["value_source"] == "decimal(28,8)"
        keys = framework.contract(DATASET).business_key
        fields = [*[c["name"] for c in framework.contract(DATASET).columns], *AUDIT_COLUMNS]
        decimals = ["value", "value_source", "conversion_factor"]
        selection = ",".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' if c in [*decimals, "_ingestion_timestamp"] else f'"{c}"' for c in fields)
        trino = query(url, f"SELECT {selection} FROM {target}")
        assert len(trino) == len(observed)
        for d in trino:
            key = tuple(d[k] for k in keys)
            for c in fields:
                value = Decimal(d[c]) if c in decimals and d[c] is not None else d[c]
                if c == "_ingestion_timestamp":
                    from datetime import datetime
                    value = datetime.fromisoformat(value.replace(" UTC", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
                assert value == observed[key][c], (key, c, value, observed[key][c])
        before = table_identity(spark, target)
        accepted_target_uuid = str(iceberg.metadata.table_uuid)
        replays = []
        for mode in ["full", "incremental"]:
            with engine.metadata_repo.source_lock("nso_vietnam"):
                result = framework.run(DATASET, "nso-" + mode + "-replay", mode=mode,
                                       run_id=ingestion.run_id if mode == "incremental" else None,
                                       bronze_snapshot_id=bronze_before[0])
            assert table_identity(spark, target) == before
            assert result["bronze_read_snapshot_id"] == bronze_before[0]
            replays.append({"mode": mode, "status": "NOOP", "real_run_id": ingestion.run_id})
        assert engine.run_with_readiness_check("nso_vietnam").status.value == "SKIPPED"
        assert table_identity(spark, bronze) == bronze_before
        # A new publication is deliberately unapproved: ingest test Raw only,
        # then block before any Silver/dead-letter commit. Keep accepted state.
        dead_before = table_identity(spark, framework.quarantine_manager.target_table)
        revision, revision_writer, revision_proof = _stage_unapproved_revision(engine, raw, "nso_revision_unapproved")
        assert revision.run_id != ingestion.run_id
        revised_identity = _catalog_identity(revision_writer, "nso_revision_unapproved")
        changed_snapshot = revised_identity["snapshot"]
        assert revised_identity["uuid"] != accepted_bronze["uuid"]
        assert _catalog_identity(engine.bronze_writer, "bronze") == accepted_bronze
        # Only the test contract overlay reads the isolated revised publication.
        data["dataset"]["bronze_input"] = revision_proof["table"]
        (contracts / f"{DATASET}.yaml").write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
        revision_framework = SilverTransformationFramework(spark, str(contracts))
        revision_framework.quarantine_manager.target_table = framework.quarantine_manager.target_table
        with pytest.raises(ValueError, match="source checksum/schema changed") as failure:
            with engine.metadata_repo.source_lock("nso_vietnam"):
                revision_framework.run(DATASET, "blocked-unapproved-publication", mode="incremental", run_id=revision.run_id,
                                       bronze_snapshot_id=changed_snapshot)
        assert failure.value.silver_failure["stage"] == "transformation"
        assert not failure.value.silver_failure["quarantine_committed"]
        assert not failure.value.silver_failure["exclusions_committed"]
        assert table_identity(spark, target) == before
        assert str(cat.load_table("silver.nso_rice_statistics_v2").metadata.table_uuid) == accepted_target_uuid
        assert table_identity(spark, bronze) == bronze_before
        assert table_identity(spark, framework.quarantine_manager.target_table) == dead_before
        assert spark.table(framework.quarantine_manager.target_table).count() == 13
        report = dict(status="NSO_V2_ISOLATED_VALIDATED_PENDING_OWNER", environment="independent internal test network", metrics=metrics,
                      numeric=sum(d["value_source"] is not None for d in observed.values()), missing=sum(d["missing_reason"] is not None for d in observed.values()),
                      zeros=sum(d["value_source"] == 0 for d in observed.values()), provisional=sum(d["release_status"] == "provisional" for d in observed.values()),
                      diagnostic_observations=sum(d["diagnostic_evidence"] != "[]" for d in observed.values()),
                      diagnostic_rule_counts={}, target_uuid=str(iceberg.metadata.table_uuid), target_snapshot=before[0], bronze_snapshot=bronze_before[0],
                      actual_counts=counts, raw_csv_count=len(raw_hashes), source_records=metrics["bronze_count"]-metrics["excluded_record_count"],
                      raw_checksums=raw_hashes, metadata_version=metadata["metadata_version"],
                      seasons=dict(Counter(d["season"] for d in observed.values())), units=dict(Counter(d["unit"] for d in observed.values())),
                      geography_status=dict(Counter(d["geography_status"] for d in observed.values())),
                      unique_business_keys=len(observed), trino_rows=len(trino), trino_schema=schema,
                      target_checksum=before[2], bronze_identity=accepted_bronze, ingestion_run_id=ingestion.run_id,
                      full_incremental_replays=replays, revision=dict(status="BLOCKED_ACCEPTED_STATE_PRESERVED", run_id=revision.run_id,
                          identity=revised_identity, proof=revision_proof, failure=failure.value.silver_failure),
                      spark="PASS", pyiceberg="PASS", trino="PASS", lineage="8/8 + actual pinned snapshot", production_contract="DRAFT")
        for d in observed.values():
            for event in json.loads(d["diagnostic_evidence"]):
                name = event["rule_id"]
                report["diagnostic_rule_counts"][name] = report["diagnostic_rule_counts"].get(name, 0) + 1
        Path("/tmp/nso-v2-rest-acceptance.json").write_text(json.dumps(report, indent=2, default=str))
        print("NSO_V2_REST_ACCEPTANCE=" + json.dumps(report, default=str))
    finally:
        spark.stop()
