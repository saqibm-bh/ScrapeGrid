"""Gossip protocol model for decentralized cluster state sharing.

Exposes:
    GossipProtocol: Stores per-node views and merges them through peer exchanges.

Design notes:
    This is an in-memory simulation of gossip convergence. It does not send
    network messages; tests and dashboards call methods directly.
"""

import threading
from typing import Dict, List, Optional


class GossipProtocol:
    """Track node views and merge them through simulated peer exchanges."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.rounds = 0
        self.views: Dict[int, Dict[int, Dict[str, object]]] = {}
        self.recent_exchanges: List[Dict[str, object]] = []

    def update_self(self, node_id: int, status: str, load_score: float = 1.0) -> None:
        """Update one node's own state in its local view."""
        with self._lock:
            self.views.setdefault(node_id, {})
            self.views[node_id][node_id] = {
                "status": status,
                "load_score": load_score,
            }

    def exchange(
        self,
        node_a: int,
        node_b: int,
        state_snapshot: Optional[Dict[str, object]] = None,
    ) -> None:
        """Merge the known views for two nodes."""
        with self._lock:
            self.views.setdefault(node_a, {})
            self.views.setdefault(node_b, {})

            if state_snapshot:
                raw_states = state_snapshot.get("node_states", {})
                if isinstance(raw_states, dict):
                    for raw_id, info in raw_states.items():
                        known_id = int(raw_id)
                        if isinstance(info, dict):
                            self.views[node_a][known_id] = dict(info)

            merged = {**self.views[node_a], **self.views[node_b]}
            self.views[node_a] = dict(merged)
            self.views[node_b] = dict(merged)
            self.rounds += 1
            self.recent_exchanges.append(
                {
                    "round": self.rounds,
                    "from": node_a,
                    "to": node_b,
                    "nodes_shared": len(merged),
                }
            )
            if len(self.recent_exchanges) > 20:
                self.recent_exchanges = self.recent_exchanges[-20:]

    def known_nodes(self, node_id: int) -> Dict[int, Dict[str, object]]:
        """Return the states known by one node."""
        with self._lock:
            return dict(self.views.get(node_id, {}))

    def stats(self) -> Dict[str, object]:
        """Return aggregate convergence information."""
        with self._lock:
            all_nodes = set(self.views)
            for view in self.views.values():
                all_nodes.update(view)
            expected = max(len(all_nodes), 1)
            converged_views = sum(1 for view in self.views.values() if len(view) >= expected)
            convergence = converged_views / max(len(self.views), 1)
            return {
                "rounds": self.rounds,
                "known_nodes": len(all_nodes),
                "convergence": round(convergence, 3),
                "recent_exchanges": list(self.recent_exchanges[-5:]),
            }
