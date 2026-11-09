#!/usr/bin/env python3
"""Run a single guarded, resumable Gold stage in the existing Spark runner."""
import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def main():
    from gold.facts import BUILD_ORDER
    from gold.pipeline import ACTIONS, execute
    from silver.core.spark_session import get_spark_session
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=ACTIONS, required=True)
    parser.add_argument("--owner-run-id", required=True)
    parser.add_argument("--silver-run-id")
    parser.add_argument("--source", choices=BUILD_ORDER)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Cheap capability checks precede JVM allocation and every catalog/object write.
    from gold.admission import runtime_resources, success_context
    from gold.views import require_semantic_definitions
    runtime_resources()
    success_context(args.silver_run_id)
    require_semantic_definitions()
    spark = get_spark_session("TLCNGold-" + args.action)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        result = execute(spark, args.action, args.owner_run_id, args.silver_run_id, args.source)
        print("GOLD_RESULT=" + json.dumps(result, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
