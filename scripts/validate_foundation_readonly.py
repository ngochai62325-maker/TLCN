"""Read-only evidence under the published Gold consumer gate. Run in runner.

Never starts jobs, creates catalog objects, or changes pipeline state. stdout is
safe evidence: identifiers/hashes only, no environment values or credentials.
"""
import json
import os
import argparse
import time
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumers", action="store_true", help="Also execute the four bounded published samples")
    parser.add_argument("--coverage", action="store_true", help="Also read compact pinned fact/metric coverage summaries")
    args = parser.parse_args()
    from gold.consumer import read_window
    from gold.keys import fingerprint
    from gold.publication import Publication
    from ingestion.storage.metadata_repository import MetadataRepository
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("foundation_readonly", **{
        "type": "rest", "uri": os.environ["ICEBERG_REST_URI"], "warehouse": "s3://warehouse",
        "s3.endpoint": "http://" + os.environ["MINIO_ENDPOINT"].removeprefix("http://"),
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1",
    })
    evidence = {"captured_at": datetime.now(timezone.utc).isoformat(), "tables": {}}
    with read_window() as manifest:
        evidence.update(manifest_hash=fingerprint(manifest), pointer=Publication().get("published.json"),
                        context=manifest["context"], published_inputs=manifest["context"]["inputs"],
                        semantic_queries={k: {p: v.get(p) for p in ("definition_sha256", "bound_sql_sha256", "version")}
                                          for k, v in manifest["semantic_queries"].items()})
        namespaces = {n[0] for n in catalog.list_namespaces()}
        for namespace in ("bronze", "bronze_local", "silver_local", "gold_local"):
            if namespace not in namespaces:
                continue
            for identifier in catalog.list_tables(namespace):
                table = catalog.load_table(identifier)
                snapshot = table.current_snapshot()
                evidence["tables"][".".join(identifier)] = {
                    "uuid": str(table.metadata.table_uuid),
                    "snapshot_id": str(snapshot.snapshot_id) if snapshot else None,
                    "metadata_location": table.metadata_location,
                    "files": sorted(str(task.file.file_path) for task in table.scan().plan_files()),
                }
        with MetadataRepository()._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT run_id, state FROM dag_run WHERE dag_id=%s ORDER BY execution_date",
                            ("rice_lakehouse_gold_pipeline",))
                evidence["gold_dagruns"] = [dict(zip(("run_id", "state"), row)) for row in cur.fetchall()]
        if args.coverage:
            from gold.views import trino_result
            def relation(name):
                return "iceberg.gold_local." + name + " FOR VERSION AS OF " + str(int(manifest["outputs"][name]["snapshot_id"]))
            periods = {"fact_fao_production_observation": "year_key", "fact_fao_sua_observation": "year_key",
                       "fact_fao_trade_observation": "year_key", "fact_nso_observation": "year_key",
                       "fact_usda_psd_observation": "market_year", "fact_usda_price_reference": "reference_year",
                       "fact_fao_price_annual": "year_key", "fact_price_monthly": "month_key", "fact_vn_market_quote_daily": "date_key"}
            evidence["coverage"] = {}
            for name, period in periods.items():
                result = trino_result(f"SELECT count(*) AS rows,count(value) AS numeric_observations,min({period}) AS min_period,max({period}) AS max_period FROM {relation(name)}")
                evidence["coverage"][name] = dict(zip([c["name"] for c in result["columns"]], result["rows"][0]))
            result = trino_result(f"SELECT s.source_id,t.source_metric_id,t.silver_unit,t.gold_unit,t.aggregation_rule FROM {relation('dim_observation_type')} t JOIN {relation('dim_source_system')} s ON s.source_system_key=t.source_system_key ORDER BY 1,2")
            evidence["metric_inventory"] = [dict(zip([c["name"] for c in result["columns"]], row)) for row in result["rows"]]
    if args.consumers:
        from gold.consumer import query
        from gold.views import VIEW_NAMES
        evidence["consumers"] = {}
        for name in VIEW_NAMES:
            started = time.monotonic()
            result = query(name)
            if result["batch"] != evidence["pointer"]["batch"]:
                raise RuntimeError("Publication changed during readonly verification")
            evidence["consumers"][name] = {"status": "PASS", "rows": len(result["rows"]),
                "batch": result["batch"], "sample_years": result["sample_years"], "seconds": time.monotonic() - started}
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
