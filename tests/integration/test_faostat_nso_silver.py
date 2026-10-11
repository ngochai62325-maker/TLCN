"""Hoang integration checks; live Bronze/Silver tables are never mutated.

Default tests use Spark SQL fixtures. Optional actual Iceberg MERGE tests require
HOANG_ISOLATED_ICEBERG_TESTS=1 and a compatible Iceberg runtime on Spark's classpath.
They use a fresh Hadoop catalog/warehouse in pytest's temporary directory.
"""

from unittest.mock import MagicMock
import os

import pytest
from pyspark.sql import SparkSession

from silver.framework import SilverTransformationFramework
from silver.engine.merge import IcebergMergeEngine
from silver.engine.quarantine import SilverQuarantineManager
from silver.transformers.faostat_production import FaostatProductionTransformer
from silver.transformers.faostat_monthly_price import FaostatMonthlyPriceTransformer
from silver.transformers.faostat_supply_utilization import FaostatSupplyUtilizationTransformer
from tests.unit.silver.faostat_nso_fixtures import observations


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("HoangIntegration")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


def test_existing_framework_discovers_production_adapter(spark, monkeypatch):
    raw = observations(spark, {"unit": "kg", "value": "2500"}, {"value": "-1"})
    framework = SilverTransformationFramework(spark)
    monkeypatch.setattr(spark, "table", lambda table: raw)
    # Replace only external writes; use actual dispatch, normalization, DQ and dedup.
    framework.quarantine_manager = MagicMock()
    captured = {}
    monkeypatch.setattr(framework.merge_engine, "merge", lambda df, **kw: captured.update(df=df, **kw))
    result = framework.run("faostat_production", "read-only-fixture-run")
    assert result["bronze_count"] == 2
    assert result["quarantine_count"] == 1
    assert result["valid_count"] == result["dedup_count"] == 1
    row = captured["df"].first()
    assert row.commodity_code == "0113" and row.unit == "tonne"
    assert float(row.value) == 2.5
    framework.quarantine_manager.route_quarantine.assert_called_once()


@pytest.fixture(scope="module")
def isolated_iceberg(tmp_path_factory):
    if os.environ.get("HOANG_ISOLATED_ICEBERG_TESTS") != "1":
        pytest.skip("Actual Iceberg MERGE requires explicit isolated runtime opt-in")
    root = tmp_path_factory.mktemp("hoang-iceberg").as_uri()
    session = (SparkSession.builder.master("local[1]").appName("HoangIsolatedIceberg")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.catalog.hoang_test", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.catalog.hoang_test.type", "hadoop")
               .config("spark.sql.catalog.hoang_test.warehouse", root).getOrCreate())
    # This is a fresh filesystem catalog, separate from REST/MinIO/shared tables.
    session.sql("CREATE NAMESPACE IF NOT EXISTS hoang_test.fixtures")
    yield session
    session.stop()


@pytest.mark.integration
@pytest.mark.parametrize("cls,source", [
    (FaostatProductionTransformer, {}),
    (FaostatMonthlyPriceTransformer, {"element_code": "5530", "element": "Producer Price (LCU/tonne)", "unit": "LCU"}),
    (FaostatSupplyUtilizationTransformer, {}),
])
def test_isolated_actual_iceberg_rerun_and_quarantine(isolated_iceberg, cls, source):
    spark = isolated_iceberg
    transformer = cls()
    raw = observations(spark, source, dict(source, value="-10"))
    valid, quarantine = transformer.validate(transformer.transform(raw))
    assert valid.count() == quarantine.count() == 1
    engine = IcebergMergeEngine(spark)
    target = f"hoang_test.fixtures.{transformer.dataset_id}"
    dead_letters = SilverQuarantineManager(spark)
    # Existing constructor ignores target_table; explicitly scope the instance.
    dead_letters.target_table = f"hoang_test.fixtures.dead_letters_{transformer.dataset_id}"
    for run in ("first", "rerun"):
        engine.merge(engine.deduplicate(valid, transformer.contract.business_key), target, transformer.contract.business_key)
        dead_letters.route_quarantine(quarantine, transformer.dataset_id, run, transformer.contract.business_key)
        assert spark.table(target).count() == 1
        assert spark.table(dead_letters.target_table).count() == 1
    assert spark.table(dead_letters.target_table).first().pipeline_run_id == "rerun"
