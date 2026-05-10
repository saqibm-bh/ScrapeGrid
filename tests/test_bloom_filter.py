"""Tests for BloomFilter URL deduplication behavior.

These tests verify that inserted URLs are found, unseen URLs are mostly rejected,
and the configured false-positive rate stays reasonable for the simulator.
"""

from algorithms.bloom_filter import BloomFilter


def test_inserted_items_are_members() -> None:
    bloom = BloomFilter(capacity=1000, fp_rate=0.01)
    urls = [f"https://example.com/page/{i}" for i in range(100)]

    for url in urls:
        bloom.add(url)

    assert all(url in bloom for url in urls)


def test_missing_item_is_not_member_before_saturation() -> None:
    bloom = BloomFilter(capacity=1000, fp_rate=0.01)
    bloom.add("https://example.com/known")

    assert "https://example.com/unknown" not in bloom


def test_false_positive_rate_stays_under_two_percent() -> None:
    bloom = BloomFilter(capacity=1000, fp_rate=0.01)
    inserted = [f"https://example.com/inserted/{i}" for i in range(1000)]
    unseen = [f"https://other.example.com/unseen/{i}" for i in range(5000)]

    for url in inserted:
        bloom.add(url)

    false_positives = sum(1 for url in unseen if url in bloom)
    assert false_positives / len(unseen) < 0.02
