# EU ETS Scraper

[![CI](https://github.com/vegardege/euets/actions/workflows/ci.yml/badge.svg)](https://github.com/vegardege/euets/actions/workflows/ci.yml)

Scrape carbon quotas from EU ETS.

## Installation

```bash
pip install euets-scraper
```

### With CLI

```bash
pip install euets-scraper[cli]
```

Then run:

```bash
euets --help
```

Or run without installing:

```bash
# uvx
uvx --from euets-scraper[cli] euets

# pipx
pipx run euets-scraper[cli]
```
