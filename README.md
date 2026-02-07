# EU ETS Scraper

[![CI](https://github.com/vegardege/euets-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/vegardege/euets-scraper/actions/workflows/ci.yml)

A Python library and CLI to download data files from the [European Union Emissions Trading System (EU ETS)](https://www.eea.europa.eu/en/datahub/datahubitem-view/98f04097-26de-4fca-86c4-63834818c0c0) datahub.

The EU ETS dataset includes verified emissions, allocated allowances, and surrendered units for installations and aircraft operators covered by the EU ETS.

This library enables you to explore and monitor the datasets in the datahub, and download whole or parts of the associated files to the local file system or a cloud provider (S3, GCS, and Azure out of the box).

It can be used as a Python library or a cli tool.

## Installation

```bash
pip install euets-scraper@git+https://github.com/vegardege/euets-scraper
```

### With historical data support

To scrape all historical datasets (requires playwright):

```bash
pip install "euets-scraper[playwright] @ git+https://github.com/vegardege/euets-scraper"
playwright install chromium
```

### With CLI tool

```bash
pip install "euets-scraper[cli] @ git+https://github.com/vegardege/euets-scraper"
```

### With cloud storage support

To download directly to S3, GCS, or Azure:

```bash
pip install "euets-scraper[s3] @ git+https://github.com/vegardege/euets-scraper"
pip install "euets-scraper[gcs] @ git+https://github.com/vegardege/euets-scraper"
pip install "euets-scraper[azure] @ git+https://github.com/vegardege/euets-scraper"
# Or use [cloud] for generic fsspec support and provide your own cloud specific driver
```

## CLI

```bash
# List all datasets
euets ls
euets ls --full    # include historical (requires [playwright] extra)
euets ls --json    # JSON output for scripting

# Get dataset info (latest by default, or specify --id)
euets latest              # print latest dataset ID
euets check --since ID    # exit 0 if newer dataset exists (for cron/ETL)
euets url                 # print archive download URL
euets url --id 1087604    # URL for specific dataset (prefix match supported)
euets files               # list files in archive
euets files --json        # JSON output

# Download archive
euets download                       # download to ./[dataset_id].zip
euets download ./data/               # download to ./data/[dataset_id].zip
euets download s3://bucket/data.zip  # download to S3 (requires [s3] extra)
euets download --id 1087604          # download specific dataset

# Extract specific files
euets extract "*.csv"                    # extract CSVs to current directory
euets extract "Allowances*" ./data/      # extract matching files to ./data/
euets extract "*.csv" s3://bucket/data/  # extract to S3 (requires [s3] extra)
euets extract "*.csv" --id 1087604       # extract from specific dataset

# Scripting
euets -q download ./data/    # --quiet suppresses status messages, outputs path only
euets -q extract "*.csv"     # outputs extracted paths only (one per line)
```

## Usage

```python
import asyncio
from euets_scraper import fetch_datasets

async def main():
    # Fetch dataset metadata
    result = await fetch_datasets()
    # result = await fetch_datasets(full=True)  # all historical datasets

    # Get the current (non-superseded) dataset
    dataset = next(ds for ds in result.datasets if not ds.superseded)

    # List files in the archive
    for f in await dataset.files():
        print(f"{f.name} ({f.file_type}, {f.size} bytes)")

    # Download the archive
    path = await dataset.download("./data/")  # -> ./data/[dataset_id].zip

    # Or extract specific files
    paths = await dataset.extract("*.csv", "./data/")  # extract CSVs
    paths = await dataset.extract("*.csv", "s3://bucket/data/")  # to cloud

    # Or get the direct URL for custom handling
    url = await dataset.url()

asyncio.run(main())
```

### Data structures

```python
from euets_scraper import Dataset, ArchiveFile, ETSResult, ParseError, Link

# ETSResult: result of fetching datasets
#   datasets: list[Dataset]
#   errors: list[ParseError]

# Dataset: a dataset from the datahub
#   dataset_id: str
#   title: str
#   format: str
#   superseded: bool
#   published: datetime | None
#   temporal_coverage: tuple[int, int]
#   factsheet: AnyUrl
#   links: list[Link]
#   async url() -> str | None
#   async files() -> list[ArchiveFile]
#   async download(path) -> str
#   async extract(pattern, output_dir) -> list[str]

# ArchiveFile: a file in the dataset archive
#   name: str
#   size: int
#   file_type: str
```

## Development

```bash
git clone https://github.com/vegardege/euets-scraper
cd euets-scraper
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
