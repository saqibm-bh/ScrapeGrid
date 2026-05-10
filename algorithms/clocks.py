"""Logical clocks for ordering distributed events.

Exposes:
    LamportClock: A single logical counter for total event ordering.
    VectorClock: Per-node counters for detecting causality and concurrency.

Design notes:
    These clocks model event ordering only; they do not represent wall-clock time.
"""

import threading
from typing import List


class LamportClock:
    """Thread-safe Lamport logical clock shared by the simulation."""

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Tick forward one step and return the new value."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received: int) -> int:
        """Apply receive rule: max(local, received) + 1."""
        with self._lock:
            self._value = max(self._value, received) + 1
            return self._value

    @property
    def value(self) -> int:
        """Return the current logical time."""
        with self._lock:
            return self._value


class VectorClock:
    """Per-node vector clock for causality tracking."""

    def __init__(self, node_id: int, num_nodes: int) -> None:
        self.node_id = node_id
        self.num_nodes = num_nodes
        self._vector = [0] * num_nodes
        self._lock = threading.Lock()

    def tick(self) -> List[int]:
        """Increment this node's own slot and return a snapshot."""
        with self._lock:
            self._vector[self.node_id] += 1
            return list(self._vector)

    def send(self) -> List[int]:
        """Tick before sending a message and return the vector."""
        return self.tick()

    def receive(self, remote: List[int]) -> List[int]:
        """Merge a remote vector, then increment this node's own slot."""
        with self._lock:
            n = min(len(self._vector), len(remote))
            for i in range(n):
                self._vector[i] = max(self._vector[i], remote[i])
            self._vector[self.node_id] += 1
            return list(self._vector)

    def snapshot(self) -> List[int]:
        """Return a copy of the vector."""
        with self._lock:
            return list(self._vector)

    @staticmethod
    def compare(va: List[int], vb: List[int]) -> str:
        """Compare two vectors as before, after, concurrent, or equal."""
        n = min(len(va), len(vb))
        le = all(va[i] <= vb[i] for i in range(n))
        ge = all(va[i] >= vb[i] for i in range(n))
        eq = all(va[i] == vb[i] for i in range(n))
        if eq:
            return "equal"
        if le:
            return "before"
        if ge:
            return "after"
        return "concurrent"
