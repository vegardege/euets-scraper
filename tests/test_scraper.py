from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from euets_scraper.scraper import (
    Link,
    _parse_accordion,
    _parse_accordions,
    download_datasets,
    download_datasets_simple,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_parse_accordion_current():
    html = (FIXTURES_DIR / "accordion.html").read_text()
    soup = BeautifulSoup(html, "html.parser")
    accordion = soup.select_one(".accordion.ui")
    assert accordion is not None

    dataset = _parse_accordion(accordion)

    assert dataset.dataset_id == "1098253"
    assert "EU ETS" in dataset.title
    assert dataset.format == "ascii (.csv, .txt, .sql)"
    assert dataset.superseded is False
    assert dataset.temporal_coverage == (2005, 2024)
    assert "sdi.eea.europa.eu/data" in str(dataset.direct_download)
    assert len(dataset.links) == 5  # 1 other download + 4 links
    assert all(isinstance(link, Link) for link in dataset.links)
    assert dataset.links[0].label == "EU Emissions Trading System data viewer Background note (July 2025)"


def test_parse_accordion_superseded():
    html = (FIXTURES_DIR / "accordion_superseded.html").read_text()
    soup = BeautifulSoup(html, "html.parser")
    accordion = soup.select_one(".accordion.ui")
    assert accordion is not None

    dataset = _parse_accordion(accordion)

    assert dataset.dataset_id == "1087604"
    assert dataset.superseded is True
    assert dataset.published == datetime(2023, 4, 20)
    assert dataset.temporal_coverage == (2005, 2023)


def test_parse_accordions_collects_errors():
    """Test that parsing errors are collected without stopping valid datasets."""
    html = (FIXTURES_DIR / "accordions_with_errors.html").read_text()
    soup = BeautifulSoup(html, "html.parser")

    result = _parse_accordions(soup)

    # Valid dataset should be parsed
    assert len(result.datasets) == 1
    assert result.datasets[0].dataset_id == "valid-1"
    assert result.datasets[0].temporal_coverage == (2005, 2024)

    # Errors should be collected for malformed accordions
    assert len(result.errors) == 2
    error_ids = {e.dataset_id for e in result.errors}
    assert error_ids == {"missing-coverage", "missing-download"}

    # Check error messages are descriptive
    errors_by_id = {e.dataset_id: e for e in result.errors}
    assert "temporal coverage" in errors_by_id["missing-coverage"].message.lower()
    assert "direct download" in errors_by_id["missing-download"].message.lower()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_download_datasets_simple_integration():
    """Integration test for simple (httpx) scrape."""
    result = await download_datasets_simple()

    assert len(result.datasets) >= 1  # At least the current dataset
    assert len(result.errors) == 0


@pytest.mark.slow
@pytest.mark.asyncio
async def test_download_datasets_full_integration():
    """Integration test for full (playwright) scrape."""
    result = await download_datasets(full=True)

    assert len(result.datasets) >= 20  # Should have many datasets
    assert any(not ds.superseded for ds in result.datasets)  # At least one current
    assert any(ds.superseded for ds in result.datasets)  # At least one superseded
    assert len(result.errors) == 0
