# Changelog

All notable changes to ScrapeGrid will be documented here.

## 0.1.0 - 2026-05-10

### Added

- Streamlit dashboard for the ScrapeGrid simulator.
- Single-node versus distributed-fleet race view.
- Algorithm modules for Bloom filter, consistent hashing, circuit breaker,
  token bucket, logical clocks, Raft, and gossip.
- Unit tests for all algorithm modules.
- GitHub Actions CI with ruff, pytest, and coverage upload.
- README, requirements files, `.gitignore`, MIT license, and contribution guide.

### Changed

- Consolidated event logging into `utils.logger.EventLog`.
- Replaced legacy path injection with package imports.

### Removed

- Deleted stale `sim2.py` legacy engine.
- Removed empty `core/` directory.
