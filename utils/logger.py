"""Thread-safe event logging for ScrapeGrid.

Exposes:
    EventLog: Keeps recent simulation events sorted by Lamport timestamp.

Design notes:
    The log is intentionally in-memory because the dashboard is a live simulator,
    not a persistent production crawler.
"""

import threading
from typing import Dict, List

class EventLog:
    """Store recent simulation events with Lamport timestamps."""

    def __init__(self):
        self._events: List[Dict[str, object]] = []
        self._lock   = threading.Lock()

    def log(self, source: str, level: str, message: str, lamport: int, t: float) -> None:
        with self._lock:
            self._events.append({
                "lamport": lamport,
                "t":       round(t, 2),
                "source":  source,
                "level":   level,   # error | warning | success | info
                "message": message,
            })
            if len(self._events) > 500:
                self._events = self._events[-500:]

    def all(self) -> list:
        with self._lock:
            return sorted(self._events, key=lambda e: e["lamport"])
