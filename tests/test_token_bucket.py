"""Tests for TokenBucket rate limiting.

These tests verify burst capacity, rejection after capacity is exhausted, and
time-based refill behavior.
"""

import pytest

from algorithms.token_bucket import TokenBucket


def test_burst_capacity_is_respected() -> None:
    bucket = TokenBucket(rate=1.0, capacity=3.0)

    assert bucket.consume()
    assert bucket.consume()
    assert bucket.consume()
    assert not bucket.consume()
    assert bucket.total_consumed == 3
    assert bucket.total_rejected == 1


def test_tokens_refill_at_expected_rate() -> None:
    bucket = TokenBucket(rate=2.0, capacity=5.0)
    assert bucket.consume(5.0)

    bucket._last_refill -= 1.0

    assert bucket.available == pytest.approx(2.0, abs=0.05)


def test_requests_beyond_capacity_are_rejected() -> None:
    bucket = TokenBucket(rate=0.0, capacity=1.0)

    assert bucket.consume()
    assert not bucket.consume()
