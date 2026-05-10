"""Tests for CircuitBreaker failure and recovery states.

These tests cover the important safety transitions: closed to open, open to
half-open after cooldown, and half-open back to closed after success.
"""

from algorithms.circuit_breaker import CircuitBreaker


def test_closed_to_open_after_failure_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=2)

    breaker.record_failure()
    assert breaker.state == "CLOSED"

    breaker.record_failure()
    assert breaker.state == "OPEN"
    assert not breaker.allow_request()


def test_open_to_half_open_after_cooldown() -> None:
    breaker = CircuitBreaker(failure_threshold=1)
    breaker.record_failure()
    breaker._open_since -= breaker._cooldown

    assert breaker.allow_request()
    assert breaker.state == "HALF_OPEN"


def test_half_open_to_closed_on_success() -> None:
    breaker = CircuitBreaker(failure_threshold=1)
    breaker.record_failure()
    breaker.probe()

    breaker.record_success()

    assert breaker.state == "CLOSED"
    assert breaker.allow_request()
