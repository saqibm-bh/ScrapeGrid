"""Consistent hash ring for stable shard assignment.

Exposes:
    ConsistentHashRing: Maps keys to named nodes using virtual ring positions.
    _ring_hash: Internal helper that maps a string to a 32-bit position.

Design notes:
    The ring uses MD5 for deterministic simulation behavior, not for security.
"""

import bisect
import hashlib
from typing import Dict, List, Optional, Set, Tuple


VIRTUAL_NODES_PER_NODE = 150


def _ring_hash(key: str) -> int:
    """Map an arbitrary string to a 32-bit ring position."""
    return int(hashlib.md5(key.encode()).hexdigest()[:8], 16)


class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = VIRTUAL_NODES_PER_NODE) -> None:
        self.virtual_nodes = virtual_nodes
        self._ring: List[Tuple[int, str]] = []
        self._positions: List[int] = []
        self._nodes: Set[str] = set()

    def add_node(self, node_name: str) -> None:
        """Add a real node by scattering virtual-node replicas across the ring."""
        if node_name in self._nodes:
            return
        self._nodes.add(node_name)
        for i in range(self.virtual_nodes):
            pos = _ring_hash(f"{node_name}:{i}")
            bisect.insort(self._ring, (pos, node_name))
        self._positions = [p for p, _ in self._ring]

    def remove_node(self, node_name: str) -> None:
        """Remove all virtual replicas of a node."""
        if node_name not in self._nodes:
            return
        self._nodes.discard(node_name)
        self._ring = [(p, n) for p, n in self._ring if n != node_name]
        self._positions = [p for p, _ in self._ring]

    def get_node(self, key: str) -> Optional[str]:
        """Return the node responsible for this key."""
        if not self._ring:
            return None
        pos = _ring_hash(key)
        idx = bisect.bisect_left(self._positions, pos) % len(self._ring)
        return self._ring[idx][1]

    def get_nodes(self) -> Set[str]:
        """Return the currently registered real nodes."""
        return set(self._nodes)

    def ring_snapshot(self) -> List[Dict[str, object]]:
        """Return ring state for visualization."""
        seen: Dict[str, int] = {}
        for pos, node in self._ring:
            if node not in seen:
                seen[node] = pos
        return [
            {"node": node, "position": position, "angle": (position / 2**32) * 360}
            for node, position in seen.items()
        ]
