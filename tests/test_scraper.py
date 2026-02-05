from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from euets_scraper.scraper import (
    Link,
    _parse_accordion,
    _parse_accordions,
    _resolve_download_url_from_html,
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
    assert len(dataset.links) == 6
    assert all(isinstance(link, Link) for link in dataset.links)
    assert dataset.links[0].label == "Direct download"


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

    # Valid datasets should be parsed (Direct download is optional)
    assert len(result.datasets) == 2
    dataset_ids = {ds.dataset_id for ds in result.datasets}
    assert dataset_ids == {"valid-1", "missing-download"}

    # Errors should be collected for malformed accordions
    assert len(result.errors) == 1
    assert result.errors[0].dataset_id == "missing-coverage"
    assert "temporal coverage" in result.errors[0].message.lower()


def test_resolve_download_url_from_html():
    """Test extracting zip URL from download page HTML."""
    html = (FIXTURES_DIR / "download_page.html").read_text()
    url = _resolve_download_url_from_html(html)

    assert url == "https://sdi.eea.europa.eu/datashare/s/qWap6qsoLxQorSq/download"


def test_resolve_download_url_from_html_missing_link():
    """Test error when download link is missing."""
    html = "<html><body><span>No download here</span></body></html>"

    with pytest.raises(ValueError, match="Download all files"):
        _resolve_download_url_from_html(html)


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


@pytest.mark.slow
@pytest.mark.asyncio
async def test_dataset_url_and_files_integration():
    """Integration test for Dataset.url() and Dataset.files()."""
    result = await download_datasets_simple()
    current = [ds for ds in result.datasets if not ds.superseded]
    assert len(current) >= 1

    dataset = current[0]

    # Test url() returns a valid zip URL
    url = await dataset.url()
    assert url is not None
    assert url.endswith("/download")

    # Test files() returns archive contents
    files = await dataset.files()
    assert len(files) > 0
    assert all(f.name for f in files)
    assert all(f.size > 0 for f in files)
