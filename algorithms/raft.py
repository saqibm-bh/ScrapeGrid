"""Simplified Raft consensus nodes for coordinator failover.

Exposes:
    RaftNode: A thread-driven follower/candidate/leader simulation.

Design notes:
    RPCs are direct Python method calls and log replication is simplified for a
    classroom demo. It shows the shape of Raft, not every production edge case.
"""

import random
import threading
import time
from typing import Dict, List, Optional


HEARTBEAT_INTERVAL = 0.15
ELECTION_TIMEOUT_MIN = 0.30
ELECTION_TIMEOUT_MAX = 0.60


class RaftNode:
    def __init__(self, node_id: int, total_nodes: int) -> None:
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.peers: List["RaftNode"] = []

        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[Dict[str, object]] = []

        self.state = "follower"
        self.leader_id: Optional[int] = None
        self.commit_index = -1

        self._last_heartbeat = time.time()
        self._election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

        self._running = False
        self._lock = threading.Lock()

        self.elections_started = 0
        self.votes_cast = 0
        self.log_entries_committed = 0
        self.crashed = False

    def set_peers(self, peers: List["RaftNode"]) -> None:
        """Set peer references after all nodes have been created."""
        self.peers = [p for p in peers if p.node_id != self.node_id]

    def run(self) -> None:
        """Run the main Raft loop."""
        self._running = True
        while self._running:
            if self.crashed:
                time.sleep(0.1)
                continue

            with self._lock:
                state = self.state
                elapsed = time.time() - self._last_heartbeat
                timeout = self._election_timeout

            if state == "leader":
                self._send_heartbeats()
                time.sleep(HEARTBEAT_INTERVAL)
            elif state in ("follower", "candidate"):
                if elapsed > timeout:
                    self._start_election()
                else:
                    time.sleep(0.05)

    def stop(self) -> None:
        """Stop the node loop."""
        self._running = False

    def force_crash(self) -> None:
        """Simulate a node crash."""
        with self._lock:
            self.crashed = True
            self.state = "follower"

    def recover(self) -> None:
        """Recover a crashed node and reset its election timer."""
        with self._lock:
            self.crashed = False
            self._last_heartbeat = time.time()
            self._election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _start_election(self) -> None:
        with self._lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            term = self.current_term
            log_len = len(self.log)
            self.elections_started += 1
            self._election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            self._last_heartbeat = time.time()

        votes = 1
        for peer in self.peers:
            granted = peer._request_vote(term, self.node_id, log_len)
            if granted:
                votes += 1

        with self._lock:
            if votes > self.total_nodes // 2 and self.state == "candidate":
                self.state = "leader"
                self.leader_id = self.node_id
                for peer in self.peers:
                    peer._accept_leader(self.node_id, self.current_term)

    def _request_vote(self, term: int, candidate_id: int, candidate_log_len: int) -> bool:
        with self._lock:
            if self.crashed:
                return False
            if term < self.current_term:
                return False
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "follower"
            if self.voted_for in (None, candidate_id):
                if candidate_log_len >= len(self.log):
                    self.voted_for = candidate_id
                    self.votes_cast += 1
                    self._last_heartbeat = time.time()
                    return True
            return False

    def _send_heartbeats(self) -> None:
        with self._lock:
            term = self.current_term
            leader_id = self.node_id
            commit = self.commit_index
        for peer in self.peers:
            peer._receive_heartbeat(term, leader_id, commit)

    def _receive_heartbeat(self, term: int, leader_id: int, commit_index: int) -> None:
        with self._lock:
            if self.crashed:
                return
            if term >= self.current_term:
                self.current_term = term
                self.state = "follower"
                self.leader_id = leader_id
                self._last_heartbeat = time.time()
                self._election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _accept_leader(self, leader_id: int, term: int) -> None:
        with self._lock:
            if not self.crashed and term >= self.current_term:
                self.state = "follower"
                self.leader_id = leader_id
                self.current_term = term
                self._last_heartbeat = time.time()

    def append_log(self, command: Dict[str, object]) -> bool:
        """Append a command to the leader log and replicate it to peers."""
        if self.state != "leader" or self.crashed:
            return False
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        self.commit_index = len(self.log) - 1
        self.log_entries_committed += 1
        for peer in self.peers:
            peer._append_entry(entry)
        return True

    def _append_entry(self, entry: Dict[str, object]) -> None:
        with self._lock:
            if not self.crashed:
                self.log.append(entry)
                self.commit_index = len(self.log) - 1

    def status(self) -> Dict[str, object]:
        """Return dashboard-friendly node state."""
        with self._lock:
            return {
                "id": self.node_id,
                "state": self.state,
                "term": self.current_term,
                "leader": self.leader_id,
                "log_len": len(self.log),
                "committed": self.log_entries_committed,
                "elections": self.elections_started,
                "crashed": self.crashed,
            }
