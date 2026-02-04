# EU ETS Scraper

[![CI](https://github.com/vegardege/euets/actions/workflows/ci.yml/badge.svg)](https://github.com/vegardege/euets/actions/workflows/ci.yml)

Scrape carbon quotas from EU ETS.

## Installation

```bash
pip install euets-scraper@git+https://github.com/vegardege/euets
```

### With CLI

```bash
pip install euets-scraper[cli]@git+https://github.com/vegardege/euets
```

Then run:

```bash
euets --help
```

## Development

```bash
git clone https://github.com/vegardege/euets
cd euets
uv sync
uv run playwright install chromium
```

### Testing

```bash
# Run unit tests (excluding slow integration tests)
uv run pytest -m "not slow"

# Run slow integration tests only
uv run pytest -m slow

# Run all tests
uv run pytest
```
