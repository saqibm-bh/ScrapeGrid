"""Tests for GossipProtocol convergence.

These tests verify that repeated peer exchanges spread node state until every
node has a complete view of the small simulated cluster.
"""

from algorithms.gossip import GossipProtocol


def test_state_converges_across_nodes_after_rounds() -> None:
    gossip = GossipProtocol()
    node_count = 5

    for node_id in range(node_count):
        gossip.update_self(node_id, status="alive", load_score=1.0)

    for _ in range(node_count):
        for node_id in range(1, node_count):
            gossip.exchange(0, node_id)

    assert gossip.stats()["convergence"] == 1.0
    for node_id in range(node_count):
        assert set(gossip.known_nodes(node_id)) == set(range(node_count))
