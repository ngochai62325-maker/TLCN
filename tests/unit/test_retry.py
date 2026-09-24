"""Tests for ingestion.utils.retry."""

import pytest
from unittest.mock import patch, MagicMock

from ingestion.core.config import RetryConfig
from ingestion.utils.retry import retry_with_backoff, retryable
from ingestion.utils.error_classifier import TransientError, PermanentError


@pytest.fixture
def fast_retry():
    return RetryConfig(
        max_attempts=3,
        initial_delay_seconds=0.01,
        max_delay_seconds=0.1,
        exponential_backoff=True,
        jitter=False,
    )


class TestRetryWithBackoff:
    def test_succeeds_first_attempt(self, fast_retry):
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        wrapped = retry_with_backoff(my_func, fast_retry)
        assert wrapped() == "ok"
        assert call_count == 1

    @patch("ingestion.utils.retry.time.sleep")
    def test_succeeds_after_transient_failures(self, mock_sleep, fast_retry):
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientError("temporary")
            return "recovered"

        wrapped = retry_with_backoff(my_func, fast_retry)
        assert wrapped() == "recovered"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("ingestion.utils.retry.time.sleep")
    def test_exhausts_max_attempts(self, mock_sleep, fast_retry):
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            raise TransientError("always fails")

        wrapped = retry_with_backoff(my_func, fast_retry)
        with pytest.raises(TransientError, match="always fails"):
            wrapped()

    def test_permanent_error_no_retry(self, fast_retry):
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            raise PermanentError("fatal")

        wrapped = retry_with_backoff(my_func, fast_retry)
        with pytest.raises(PermanentError, match="fatal"):
            wrapped()
        assert call_count == 1

    @patch("ingestion.utils.retry.time.sleep")
    def test_exponential_backoff_delays(self, mock_sleep):
        config = RetryConfig(
            max_attempts=3,
            initial_delay_seconds=1.0,
            max_delay_seconds=10.0,
            exponential_backoff=True,
            jitter=False,
        )
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            raise TransientError("fail")

        wrapped = retry_with_backoff(my_func, config)
        with pytest.raises(TransientError):
            wrapped()
        # 3 attempts = 2 sleeps: delay=1.0, delay=2.0
        delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert delays[0] == pytest.approx(1.0)
        assert delays[1] == pytest.approx(2.0)

    @patch("ingestion.utils.retry.random.uniform", return_value=0.05)
    @patch("ingestion.utils.retry.time.sleep")
    def test_jitter_applied(self, mock_sleep, mock_uniform):
        config = RetryConfig(
            max_attempts=2,
            initial_delay_seconds=1.0,
            max_delay_seconds=10.0,
            exponential_backoff=True,
            jitter=True,
        )

        def my_func():
            raise TransientError("fail")

        wrapped = retry_with_backoff(my_func, config)
        with pytest.raises(TransientError):
            wrapped()
        assert mock_uniform.called

    @patch("ingestion.utils.retry.time.sleep")
    def test_respect_retry_after(self, mock_sleep, fast_retry):
        call_count = 0

        def my_func():
            nonlocal call_count
            call_count += 1
            err = TransientError("rate limited")
            err.retry_after = 5.0
            raise err

        wrapped = retry_with_backoff(my_func, fast_retry)
        with pytest.raises(TransientError):
            wrapped()
        # First sleep should use retry_after=5.0
        assert mock_sleep.call_args_list[0].args[0] == pytest.approx(5.0)


class TestRetryableDecorator:
    @patch("ingestion.utils.retry.time.sleep")
    def test_retryable_decorator(self, mock_sleep):
        config = RetryConfig(max_attempts=2, initial_delay_seconds=0.01, jitter=False)
        call_count = 0

        @retryable(config)
        def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TransientError("fail")
            return "ok"

        assert my_func() == "ok"
        assert call_count == 2
