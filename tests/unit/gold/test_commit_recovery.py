"""Failure-injection unit tests. Physical NOOP is still an E2E gate."""
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import types as T

from gold.catalog import write_snapshot
from gold.spec import load_spec


def setup_frames():
    spark, proposed, current, dq = (MagicMock() for _ in range(4))
    schema = T.StructType([T.StructField("year_key", T.IntegerType()), T.StructField("decade", T.IntegerType()), T.StructField("is_leap_year", T.BooleanType())])
    proposed.schema = current.schema = schema
    proposed.exceptAll.return_value.limit.return_value.count.return_value = 0
    current.exceptAll.return_value.limit.return_value.count.return_value = 0
    current.count.return_value = 2
    spark.table.return_value = current
    before = {"uuid": "gold-uuid", "snapshot_id": "10", "metadata_location": "metadata-v1", "location": "s3://warehouse/gold_local/dim_year", "owner": "tlcn-local", "model": "gold-20261025.1"}
    after = {**before, "snapshot_id": "11", "metadata_location": "metadata-v2"}
    return spark, proposed, current, dq, before, after


def test_noop_does_not_call_writer_and_checks_metadata_files():
    spark, df, current, dq, before, after = setup_frames()
    with patch("gold.catalog.identity", return_value=before), patch("gold.catalog.file_state", return_value=["file-a"]) as files:
        result = write_snapshot(spark, load_spec()["dim_year"], df, dq)
    assert result["write"] == "NOOP"
    assert files.call_count == 2
    spark.createDataFrame.assert_not_called()
    spark.sql.assert_not_called()
    dq.record.assert_called_once()


def test_commit_succeeds_response_lost_inspect_before_retry():
    spark, df, current, dq, before, after = setup_frames()
    df.exceptAll.return_value.limit.return_value.count.side_effect = [1, 0]
    spark.createDataFrame.return_value.writeTo.return_value.overwrite.side_effect = IOError("response lost")
    with patch("gold.catalog.identity", side_effect=[before, after]), patch("gold.catalog.file_state", return_value=["file-new"]), patch("pyspark.sql.functions.lit", return_value=True):
        result = write_snapshot(spark, load_spec()["dim_year"], df, dq)
    assert result["write"] == "COMMIT_VERIFIED_AFTER_ERROR"
    assert result["snapshot_id"] == "11"
    assert spark.createDataFrame.return_value.writeTo.return_value.overwrite.call_count == 1
    spark.sql.assert_not_called()


def test_interrupted_write_unknown_state_fails_closed():
    spark, df, current, dq, before, after = setup_frames()
    df.exceptAll.return_value.limit.return_value.count.return_value = 1
    spark.createDataFrame.return_value.writeTo.return_value.overwrite.side_effect = IOError("interrupted write")
    with patch("gold.catalog.identity", return_value=before), patch("gold.catalog.file_state", return_value=["file-old"]), patch("pyspark.sql.functions.lit", return_value=True):
        with pytest.raises(RuntimeError, match="Unknown/failed Gold commit"):
            write_snapshot(spark, load_spec()["dim_year"], df, dq)
    spark.sql.assert_not_called()  # no DROP / rollback / destructive repair


def test_wrong_owner_never_writes():
    spark, df, current, dq, before, after = setup_frames()
    with patch("gold.catalog.identity", return_value={**before, "owner": "someone-else"}):
        with pytest.raises(ValueError, match="Uninventoried/unowned"):
            write_snapshot(spark, load_spec()["dim_year"], df, dq)
    spark.createDataFrame.assert_not_called()


def test_writer_lock_loss_prevents_commit():
    spark, df, current, dq, before, after = setup_frames()
    df.exceptAll.return_value.limit.return_value.count.return_value = 1
    def lost_lock():
        raise RuntimeError("lock lost")
    with patch("gold.catalog.identity", return_value=before), patch("gold.catalog.file_state", return_value=[]):
        with pytest.raises(RuntimeError):
            write_snapshot(spark, load_spec()["dim_year"], df, dq, lost_lock)
    spark.createDataFrame.return_value.writeTo.assert_not_called()
