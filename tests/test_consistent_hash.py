"""Tests for ConsistentHashRing shard assignment.

These tests check that keys spread across nodes and that adding one node moves
only part of the keyspace instead of reshuffling everything.
"""

from typing import List

from algorithms.consistent_hash import ConsistentHashRing


def test_adding_node_remaps_only_some_keys(sample_keys: List[str]) -> None:
    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("node-a")
    ring.add_node("node-b")
    before = {key: ring.get_node(key) for key in sample_keys}

    ring.add_node("node-c")
    after = {key: ring.get_node(key) for key in sample_keys}

    remapped = sum(1 for key in sample_keys if before[key] != after[key])
    assert remapped > 0
    assert remapped < len(sample_keys) * 0.5


def test_keys_distribute_across_all_nodes(sample_keys: List[str], node_names: List[str]) -> None:
    ring = ConsistentHashRing(virtual_nodes=150)
    for node in node_names:
        ring.add_node(node)

    assigned = {ring.get_node(key) for key in sample_keys}

    assert assigned == set(node_names)
