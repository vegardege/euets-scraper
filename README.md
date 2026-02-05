# EU ETS Scraper

[![CI](https://github.com/vegardege/euets/actions/workflows/ci.yml/badge.svg)](https://github.com/vegardege/euets/actions/workflows/ci.yml)

Scrape carbon quotas from EU ETS.

## Installation

```bash
pip install euets-scraper@git+https://github.com/vegardege/euets
```

### With full historical data support

To scrape all historical datasets (requires `playwright`):

```bash
pip install euets-scraper[full]@git+https://github.com/vegardege/euets
playwright install chromium
```

### With CLI

```bash
pip install euets-scraper[cli]@git+https://github.com/vegardege/euets
```

## CLI

```bash
# List all datasets in a table
euets ls

# Include historical datasets (requires playwright)
euets ls --full

# JSON output for scripting
euets ls --json | jq '.[0].direct_download'

# Get the ID of the most recent dataset
euets latest
```

## Usage

```python
import asyncio
from euets_scraper import download_datasets

async def main():
    # Simple scrape (httpx) - gets current + one superseded dataset
    result = await download_datasets()

    # Full scrape (playwright) - gets all historical datasets
    # result = await download_datasets(full=True)

    for dataset in result.datasets:
        print(f"{dataset.title} ({dataset.temporal_coverage[0]}-{dataset.temporal_coverage[1]})")
        print(f"  Download: {dataset.direct_download}")

    # Check for parsing errors
    for error in result.errors:
        print(f"Failed to parse {error.dataset_id}: {error.message}")

asyncio.run(main())
```

### Data structures

```python
from euets_scraper import Dataset, ETSResult, ParseError, Link

# ETSResult contains:
#   datasets: list[Dataset]  - successfully parsed datasets
#   errors: list[ParseError] - parsing failures with dataset_id and message

# Dataset contains:
#   dataset_id: str
#   title: str
#   format: str
#   superseded: bool
#   published: datetime | None
#   temporal_coverage: tuple[int, int]
#   metadata_factsheet: AnyUrl
#   direct_download: AnyUrl
#   links: list[Link]
```

## Development

```bash
git clone https://github.com/vegardege/euets
cd euets
uv sync
uv run playwright install chromium  # needed for integration tests
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
