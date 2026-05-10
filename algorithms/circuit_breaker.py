"""Circuit breaker state machine for simulated IP blocks.

Exposes:
    CircuitBreaker: Gates requests with CLOSED, OPEN, and HALF_OPEN states.
    BASE_DELAY: The first cooldown duration before exponential growth.

Design notes:
    Cooldowns are intentionally short so classroom demos stay interactive.
"""

import time


BASE_DELAY = 0.5


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 1) -> None:
        self.failure_threshold = failure_threshold
        self.state = "CLOSED"
        self._failure_count = 0
        self._block_count = 0
        self._open_since = 0.0
        self._cooldown = 0.0

    def allow_request(self) -> bool:
        """Return True if the circuit allows a request through."""
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self._open_since >= self._cooldown:
                self.state = "HALF_OPEN"
                return True
            return False
        if self.state == "HALF_OPEN":
            return True
        return False

    def record_success(self) -> None:
        """Record a successful request and close a recovering circuit."""
        self._failure_count = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"

    def record_failure(self) -> None:
        """Record a failed request and open the circuit if needed."""
        self._failure_count += 1
        self._block_count += 1
        if self._failure_count >= self.failure_threshold:
            self._open()

    def _open(self) -> None:
        self.state = "OPEN"
        self._failure_count = 0
        self._cooldown = (2 ** min(self._block_count, 5)) * BASE_DELAY
        self._open_since = time.time()

    def probe(self) -> None:
        """Force transition to HALF_OPEN for testing recovery."""
        if self.state == "OPEN":
            self.state = "HALF_OPEN"

    @property
    def cooldown_remaining(self) -> float:
        """Return seconds left before an OPEN circuit can probe again."""
        if self.state != "OPEN":
            return 0.0
        return max(0.0, self._cooldown - (time.time() - self._open_since))

    @property
    def total_blocks(self) -> int:
        """Return how many failures have opened the circuit."""
        return self._block_count
