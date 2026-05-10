"""Tests for LamportClock and VectorClock ordering.

These tests verify logical time increments, receive updates, causal ordering,
and concurrent event detection.
"""

from algorithms.clocks import LamportClock, VectorClock


def test_lamport_increment_and_receive_update() -> None:
    clock = LamportClock()

    assert clock.increment() == 1
    assert clock.increment() == 2
    assert clock.update(10) == 11
    assert clock.value == 11


def test_lamport_receive_uses_max_local_and_received() -> None:
    clock = LamportClock()
    clock.increment()
    clock.increment()

    assert clock.update(1) == 3


def test_vector_clock_detects_concurrent_events() -> None:
    node_a = VectorClock(node_id=0, num_nodes=2)
    node_b = VectorClock(node_id=1, num_nodes=2)

    event_a = node_a.tick()
    event_b = node_b.tick()

    assert VectorClock.compare(event_a, event_b) == "concurrent"


def test_vector_clock_detects_causal_ordering() -> None:
    node_a = VectorClock(node_id=0, num_nodes=2)
    node_b = VectorClock(node_id=1, num_nodes=2)

    sent = node_a.send()
    received = node_b.receive(sent)

    assert VectorClock.compare(sent, received) == "before"
    assert VectorClock.compare(received, sent) == "after"
