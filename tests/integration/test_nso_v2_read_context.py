"""Native Iceberg proof of the opt-in read binding; no shared services."""
import pytest
from pyspark.sql import functions as F, types as T

from silver.framework import BronzeReadContext, ReadContextTransformer, RELEASE_SOURCES
from silver.transformers.nso import NsoTransformer
from tests.integration.test_nso_v2_isolated import (
    spark, raw_input, dataframe, national_row, isolated_framework, table_identity,
)


def test_pinned_read_survives_concurrent_bronze_append(spark, tmp_path, raw_input, monkeypatch):
    framework, contract = isolated_framework(spark, tmp_path, "read_context")
    old = national_row(raw_input, "2020", "..")
    dataframe(spark, [old]).writeTo(contract.bronze_input).using("iceberg").create()
    before = table_identity(spark, contract.bronze_input)[0]
    transformer = NsoTransformer()
    original = transformer.preprocess_with_read_context
    seen = []

    def concurrent(df, context):
        seen.append(context.bronze_read_snapshot_id)
        dataframe(spark, [national_row(raw_input, "2099", "3")]).writeTo(contract.bronze_input).append()
        assert table_identity(spark, contract.bronze_input)[0] != before
        return original(df, context)

    monkeypatch.setattr(transformer, "preprocess_with_read_context", concurrent)
    monkeypatch.setattr(framework, "transformer", lambda _: transformer)
    result = framework.run("nso", "concurrent", bronze_snapshot_id=before)
    assert seen == [before] and result["bronze_read_snapshot_id"] == before
    output = spark.table(contract.silver_output)
    assert output.count() == 8
    assert output.schema["bronze_read_snapshot_id"].dataType == T.LongType()
    assert {r.bronze_read_snapshot_id for r in output.collect()} == {before}
    assert output.filter("year = 2099").count() == 0
    assert output.filter("value_raw = '..'").first().bronze_read_snapshot_id == before
    assert {r._source_snapshot_id for r in output.collect()} == {0}
    with pytest.raises(ValueError, match="Bronze snapshot drift"):
        framework.run("nso", "wrong-expected-snapshot", bronze_snapshot_id=before)


def test_context_missing_invalid_or_misbound_fails_closed(spark, raw_input):
    transformer = NsoTransformer()
    raw = dataframe(spark, [national_row(raw_input)])
    with pytest.raises(ValueError, match="requires pinned"):
        transformer.preprocess(raw)
    for value in [None, True, 0, -1, 2**63, "123", 1.5]:
        with pytest.raises(ValueError, match="invalid pinned"):
            transformer.preprocess_with_read_context(raw, BronzeReadContext("isolated.bronze.raw", value))
    bound = raw.withColumn("_bronze_read_snapshot_id", F.lit(123).cast("long"))
    with pytest.raises(ValueError, match="does not match"):
        transformer.preprocess_with_read_context(bound, BronzeReadContext("isolated.bronze.raw", 124))


def test_closed_transformers_do_not_opt_in(spark):
    from silver.framework import SilverTransformationFramework
    framework = SilverTransformationFramework(spark)
    for source in RELEASE_SOURCES:
        assert not isinstance(framework.transformer(source), ReadContextTransformer), source
