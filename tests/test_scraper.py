from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from euets_scraper.scraper import Link, _parse_accordion, download_datasets

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_parse_accordion_current():
    html = (FIXTURES_DIR / "accordion.html").read_text()
    soup = BeautifulSoup(html, "html.parser")
    accordion = soup.select_one(".accordion.ui")
    assert accordion is not None

    dataset = _parse_accordion(accordion)

    assert dataset.id == "1098253"
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

    assert dataset.id == "1087604"
    assert dataset.superseded is True
    assert dataset.published == datetime(2023, 4, 20)
    assert dataset.temporal_coverage == (2005, 2023)


@pytest.mark.slow
@pytest.mark.asyncio
async def test_download_datasets_integration():
    """Integration test that hits the live site. Run with: pytest -m slow"""
    result = await download_datasets()

    assert len(result.datasets) >= 20  # Should have many datasets
    assert any(not ds.superseded for ds in result.datasets)  # At least one current
    assert any(ds.superseded for ds in result.datasets)  # At least one superseded
    assert len(result.errors) == 0  # No errors expected
