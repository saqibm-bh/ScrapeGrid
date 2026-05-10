"""Tests for the simplified RaftNode coordinator model.

These tests check leader election, log append behavior, and crash recovery at
the level used by the dashboard simulation.
"""

from algorithms.raft import RaftNode


def test_candidate_with_majority_becomes_leader() -> None:
    nodes = [RaftNode(i, 3) for i in range(3)]
    for node in nodes:
        node.set_peers(nodes)

    nodes[0]._start_election()

    assert nodes[0].state == "leader"
    assert nodes[1].leader_id == 0
    assert nodes[2].leader_id == 0


def test_leader_appends_and_replicates_log_entry() -> None:
    nodes = [RaftNode(i, 3) for i in range(3)]
    for node in nodes:
        node.set_peers(nodes)
    nodes[0]._start_election()

    assert nodes[0].append_log({"action": "assign", "node": 1})
    assert len(nodes[0].log) == 1
    assert len(nodes[1].log) == 1
    assert len(nodes[2].log) == 1


def test_crashed_node_can_recover() -> None:
    node = RaftNode(0, 1)

    node.force_crash()
    assert node.crashed

    node.recover()
    assert not node.crashed
