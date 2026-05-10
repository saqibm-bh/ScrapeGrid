# Contributing to ScrapeGrid

Thanks for improving ScrapeGrid. This project is meant to be easy to read, run,
and explain, so small focused changes are best.

## Local Setup

```bash
python -m venv venv
```

Windows:

```powershell
venv\Scripts\activate
```

Mac/Linux:

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements-dev.txt
```

## Before You Commit

Run the same checks used by CI:

```bash
ruff check .
pytest tests/ --cov=. --cov-report=xml
```

## Code Style

- Keep modules small and readable.
- Add tests for algorithm changes.
- Prefer plain names over clever names.
- Do not add real web scraping behavior without clear safety rules.

## Good First Improvements

- Add screenshots to `assets/screenshots/`.
- Split `simulation.py` into smaller modules.
- Add a Dockerfile for easier demos.
- Add a real multi-process worker mode.
