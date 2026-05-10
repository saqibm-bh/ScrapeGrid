"""Algorithm modules used by ScrapeGrid simulations.

Exposes:
    bloom_filter, circuit_breaker, clocks, consistent_hash, gossip, raft,
    and token_bucket modules.

Design notes:
    Modules are imported directly by the simulation to keep the classroom app
    simple and easy to inspect.
"""
