"""Tests for ingestion.utils.error_classifier."""

import pytest
from ingestion.utils.error_classifier import (
    IngestionError,
    TransientError,
    PermanentError,
    DataQualityError,
    SchemaError,
    classify_error,
    classify_http_status,
    create_error_record,
)
from ingestion.core.enums import ErrorType


# ── classify_http_status ────────────────────────────────────────────
class TestClassifyHttpStatus:
    @pytest.mark.parametrize("code", [429, 500, 502, 503, 504])
    def test_transient_http(self, code):
        assert classify_http_status(code) == ErrorType.TRANSIENT

    @pytest.mark.parametrize("code", [400, 401, 403, 404])
    def test_permanent_http(self, code):
        assert classify_http_status(code) == ErrorType.PERMANENT

    def test_unknown_http(self):
        assert classify_http_status(418) == ErrorType.SYSTEM


# ── classify_error ──────────────────────────────────────────────────
class TestClassifyError:
    def test_transient_error(self):
        assert classify_error(TransientError("x")) == ErrorType.TRANSIENT

    def test_permanent_error(self):
        assert classify_error(PermanentError("x")) == ErrorType.PERMANENT

    def test_data_quality_error(self):
        assert classify_error(DataQualityError("x")) == ErrorType.DATA_QUALITY

    def test_schema_error(self):
        assert classify_error(SchemaError("x")) == ErrorType.SCHEMA

    def test_connection_error(self):
        assert classify_error(ConnectionError("x")) == ErrorType.TRANSIENT

    def test_timeout_error(self):
        assert classify_error(TimeoutError("x")) == ErrorType.TRANSIENT

    def test_file_not_found(self):
        assert classify_error(FileNotFoundError("x")) == ErrorType.PERMANENT

    def test_value_error(self):
        assert classify_error(ValueError("x")) == ErrorType.PERMANENT

    def test_generic_exception(self):
        assert classify_error(RuntimeError("x")) == ErrorType.SYSTEM


# ── create_error_record ─────────────────────────────────────────────
class TestCreateErrorRecord:
    def test_creates_complete_record(self):
        exc = TransientError("Network timeout")
        record = create_error_record("run_1", "batch_1", "src_1", exc, "download")
        assert record["run_id"] == "run_1"
        assert record["batch_id"] == "batch_1"
        assert record["source_id"] == "src_1"
        assert record["error_type"] == "TRANSIENT"
        assert record["error_code"] == "TransientError"
        assert record["error_message"] == "Network timeout"
        assert record["stage"] == "download"
        assert record["retryable"] is True

    def test_permanent_not_retryable(self):
        exc = PermanentError("Bad config")
        record = create_error_record("r1", "b1", "s1", exc, "init")
        assert record["retryable"] is False
        assert record["error_type"] == "PERMANENT"


# ── Exception hierarchy ─────────────────────────────────────────────
class TestExceptionHierarchy:
    def test_all_inherit_ingestion_error(self):
        assert issubclass(TransientError, IngestionError)
        assert issubclass(PermanentError, IngestionError)
        assert issubclass(DataQualityError, IngestionError)
        assert issubclass(SchemaError, IngestionError)

    def test_ingestion_error_is_exception(self):
        assert issubclass(IngestionError, Exception)
