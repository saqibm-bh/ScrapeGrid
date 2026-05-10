"""Bloom filter support for fast URL deduplication.

Exposes:
    BloomFilter: A probabilistic set that can quickly answer "seen before?"
    _hash_at: Internal hash-position helper used by the filter.

Design notes:
    False positives are acceptable in this simulator because they only skip a
    fake URL. False negatives are avoided, so known URLs are not reprocessed.
"""

import hashlib
import math
from typing import Dict


def _hash_at(item: str, seed: int, bit_size: int) -> int:
    """Compute hash position for item using MD5 with a seed prefix."""
    raw = hashlib.md5(f"{seed}:{item}".encode()).digest()
    return int.from_bytes(raw[:8], "big") % bit_size


class BloomFilter:
    def __init__(self, capacity: int = 30000, fp_rate: float = 0.01) -> None:
        """
        capacity: expected number of items
        fp_rate: target false positive probability

        Optimal formulas:
          m = -n * ln(p) / (ln2)^2
          k = (m/n) * ln2
        """
        self.capacity = capacity
        self.fp_rate = fp_rate
        m = int(-capacity * math.log(fp_rate) / (math.log(2) ** 2))
        k = max(1, int((m / capacity) * math.log(2)))
        self.bit_size = m
        self.hash_count = k
        self.bits = bytearray(m // 8 + 1)
        self.item_count = 0
        self.duplicates_caught = 0

    def add(self, item: str) -> None:
        """Set k bit positions for this item."""
        for seed in range(self.hash_count):
            pos = _hash_at(item, seed, self.bit_size)
            self.bits[pos >> 3] |= 1 << (pos & 7)
        self.item_count += 1

    def __contains__(self, item: str) -> bool:
        """Return True if all k positions are set, with possible false positives."""
        return all(
            self.bits[_hash_at(item, seed, self.bit_size) >> 3]
            & (1 << (_hash_at(item, seed, self.bit_size) & 7))
            for seed in range(self.hash_count)
        )

    def merge(self, other: "BloomFilter") -> None:
        """Merge another filter by OR-ing bit arrays."""
        assert self.bit_size == other.bit_size, "Filters must have the same bit_size to merge"
        for i in range(len(self.bits)):
            self.bits[i] |= other.bits[i]

    @property
    def fill_ratio(self) -> float:
        """Fraction of bits set; higher values increase false positive risk."""
        return sum(bin(b).count("1") for b in self.bits) / self.bit_size

    def stats(self) -> Dict[str, object]:
        """Return dashboard-friendly filter metrics."""
        return {
            "capacity": self.capacity,
            "items_added": self.item_count,
            "duplicates_caught": self.duplicates_caught,
            "bit_size": self.bit_size,
            "hash_functions": self.hash_count,
            "target_fp_rate": self.fp_rate,
            "fill_ratio": round(self.fill_ratio, 4),
        }
