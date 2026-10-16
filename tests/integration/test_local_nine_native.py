"""Native local overlay/readiness/NOOP proof on a temporary Hadoop catalog only."""
from pyspark.sql import types as T

from tests.integration.test_nso_v2_isolated import spark, raw_input, dataframe, national_row, table_identity
from tests.unit.test_local_nine_profile import local_env
from silver.framework import SilverTransformationFramework
from silver.audit import check_bronze


def test_local_nso_readiness_and_noop_use_actual_pinned_context(spark, tmp_path, raw_input, local_env):
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.type", "hadoop")
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", (tmp_path / "warehouse").as_uri())
    spark.sql("CREATE NAMESPACE iceberg.bronze")
    bronze = "iceberg.bronze.nso_vietnam"
    dataframe(spark, [national_row(raw_input, value="..")]).writeTo(bronze).using("iceberg").create()
    pinned = table_identity(spark, bronze)[0]
    framework = SilverTransformationFramework(spark, profile="local_nine")
    contract = framework.contract("nso_vietnam_v2")
    assert check_bronze(spark, [contract], profile="local_nine")["status"] == "PASSED"
    first = framework.run("nso_vietnam_v2", "local-native", bronze_snapshot_id=pinned)
    assert first["dedup_count"] == 8 and first["quarantine_count"] == 0
    assert first["bronze_read_snapshot_id"] == pinned
    assert framework.quarantine_manager.target_table == "iceberg.silver_local.dead_letters"
    output = spark.table(contract.silver_output)
    assert output.schema["bronze_read_snapshot_id"].dataType == T.LongType()
    assert output.filter("value_raw = '..'").first().bronze_read_snapshot_id == pinned
    assert {r.bronze_read_snapshot_id for r in output.collect()} == {pinned}
    before = table_identity(spark, contract.silver_output)
    for mode in ["full", "incremental"]:
        framework.run("nso_vietnam_v2", "local-"+mode, mode=mode,
                      run_id="national-run" if mode=="incremental" else None, bronze_snapshot_id=pinned)
        assert table_identity(spark, contract.silver_output) == before
