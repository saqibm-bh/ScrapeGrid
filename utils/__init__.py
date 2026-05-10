"""Utility modules for ScrapeGrid.

Exposes:
    logger: In-memory event logging.
    url_generator: Deterministic fake URL and page-content generation.

Design notes:
    Utilities avoid real network or database dependencies so the simulator runs
    locally with a small setup.
"""
