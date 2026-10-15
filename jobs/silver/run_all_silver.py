#!/usr/bin/env python3
"""Run all READY contracts through the shared Silver framework."""
import argparse
import json
import logging
import sys
import uuid
import os
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from silver.contract.loader import SilverContractLoader
from silver.core.spark_session import get_spark_session
from silver.framework import SilverTransformationFramework, RELEASE_NAMESPACE, RELEASE_SOURCES

ALIASES = {"usda_export_price": "usda_rice_yearbook"}


def datasets(contract_dir):
    loader = SilverContractLoader(str(contract_dir))
    return {p.stem: loader.load_contract(p.stem) for p in sorted(Path(contract_dir).glob("*.yaml"))}


def selected_sources(dataset, contracts, release=None):
    if release and release != RELEASE_NAMESPACE:
        raise ValueError(f"Unapproved Silver release: {release}")
    if release and dataset == "all":
        raise ValueError("Release requires explicit source IDs; --dataset all is forbidden")
    targets = ([k for k, c in contracts.items() if c.status == "READY"] if dataset == "all"
               else [ALIASES.get(k.strip(), k.strip()) for k in dataset.split(",")])
    if len(targets) != len(set(targets)) or any(k not in contracts for k in targets):
        raise ValueError("Unknown or duplicate source ID")
    if release and any(k not in RELEASE_SOURCES for k in targets):
        raise ValueError("Source outside Silver V2 release")
    for target in targets:
        contracts[target].validate_executable()
    return targets


def source_lock(source, release):
    if not release:
        return nullcontext()
    if not os.environ.get("INGESTION_DB_URL"):
        raise ValueError("Release requires explicit INGESTION_DB_URL; no unlocked fallback")
    from ingestion.storage.metadata_repository import MetadataRepository
    return MetadataRepository().source_lock(source)


def run_pipeline(dataset="all", mode="full", run_id=None, watermark=None,
                 pipeline_run_id=None, contract_dir=None, spark=None, release=None):
    contract_dir = Path(contract_dir or ROOT / "contracts/silver")
    contracts = datasets(contract_dir)
    targets = selected_sources(dataset, contracts, release)
    if dataset == "all":
        for key, contract in contracts.items():
            if contract.status != "READY":
                logging.warning("Pending Silver dataset %s: %s; excluded from all", key, contract.status)
    if any(k not in contracts for k in targets):
        raise ValueError(f"Unknown dataset {dataset}; available: {list(contracts)}")
    if mode == "incremental" and not (run_id or watermark):
        raise ValueError("Incremental mode requires --run-id or --watermark")
    for target in targets:
        contracts[target].validate_executable()
    session = spark or get_spark_session("RiceLakehouseSilver")
    framework = SilverTransformationFramework(session, str(contract_dir), release=release)
    pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
    results = {}
    try:
        for target in targets:
            with source_lock(target, release):
                if release:
                    from validate_silver_e2e import table_identity
                    table = framework.contract(target).silver_output
                    before = table_identity(session, table)
                    pinned = table_identity(session, framework.contract(target).bronze_input)["snapshot_id"]
                try:
                    results[target] = framework.run(target, pipeline_run_id, mode=mode, run_id=run_id, watermark=watermark,
                                                    **({"bronze_snapshot_id": pinned} if release else {}))
                    if release:
                        results[target]["commit_verified"] = {**table_identity(session, table), "rows": session.table(table).count()}
                except Exception as exc:
                    if release:
                        recovery = {**getattr(exc, "silver_failure", {}), "target_before": before,
                                    "commit_outcome": "UNKNOWN", "retry_requires_catalog_readback": True}
                        try:
                            recovery["target_after"] = table_identity(session, table)
                        except Exception as read_error:
                            recovery["catalog_readback_error"] = str(read_error)
                        exc.silver_failure = recovery
                    raise
        return results
    finally:
        if spark is None:
            session.stop()


def main(default_dataset="all"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=default_dataset)
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--run-id", help="Bronze ingestion run ID; required for incremental unless watermark supplied")
    parser.add_argument("--watermark", type=datetime.fromisoformat, help="Exclusive UTC Bronze timestamp watermark (ISO 8601)")
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--contract-dir", default=str(ROOT / "contracts/silver"))
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--check-bronze", action="store_true", help="Read-only readiness/schema check")
    parser.add_argument("--audit", action="store_true", help="Read-only Silver key/type audit")
    parser.add_argument("--release", choices=[RELEASE_NAMESPACE], help="Opt-in V2 routing; requires explicit source IDs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if args.audit or args.check_bronze:
        from silver.audit import audit_tables, check_bronze
        session = get_spark_session("SilverAudit")
        try:
            contracts = datasets(args.contract_dir)
            framework = SilverTransformationFramework(session, args.contract_dir, release=args.release)
            selected = [framework.contract(k) for k in selected_sources(args.dataset, contracts, args.release)]
            if not selected:
                raise ValueError("No READY datasets selected")
            result = check_bronze(session, selected) if args.check_bronze else audit_tables(session, selected)
        finally:
            session.stop()
    else:
        result = run_pipeline(args.dataset, args.mode, args.run_id, args.watermark,
                              args.pipeline_run_id, args.contract_dir, release=args.release)
    encoded = json.dumps(result, ensure_ascii=False, default=str, indent=2)
    print("SILVER_RESULT=" + json.dumps(result, default=str))
    if args.output_json:
        args.output_json.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
