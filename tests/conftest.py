"""Shared pytest fixtures for ScrapeGrid algorithm tests.

Exposes:
    sample_keys: Stable keys used by hashing and distribution tests.
    node_names: Small node set used by distributed algorithm tests.

Design notes:
    Fixtures are deterministic so failures point to code changes, not randomness.
"""

from typing import List

import pytest


@pytest.fixture
def sample_keys() -> List[str]:
    """Return stable keys for hashing tests."""
    return [f"url-{i}" for i in range(1000)]


@pytest.fixture
def node_names() -> List[str]:
    """Return stable node names for ring tests."""
    return ["node-a", "node-b", "node-c"]
