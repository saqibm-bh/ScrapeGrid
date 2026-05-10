"""Token bucket rate limiter for per-domain request throttling.

Exposes:
    TokenBucket: Refills tokens over time and consumes one token per request.

Design notes:
    Refills are lazy, meaning the bucket updates itself only when inspected or used.
"""

import threading
import time


class TokenBucket:
    def __init__(self, rate: float = 10.0, capacity: float = 20.0) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self.total_consumed = 0
        self.total_rejected = 0

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_refill
        added = elapsed * self.rate
        self._tokens = min(self.capacity, self._tokens + added)
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens from the bucket."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self.total_consumed += 1
                return True
            self.total_rejected += 1
            return False

    @property
    def fill_level(self) -> float:
        """Return current fill as a 0-to-1 fraction."""
        with self._lock:
            self._refill()
            return self._tokens / self.capacity

    @property
    def available(self) -> float:
        """Return currently available tokens."""
        with self._lock:
            self._refill()
            return self._tokens
