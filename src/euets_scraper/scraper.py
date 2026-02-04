from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright
from pydantic import AnyUrl, BaseModel

T = TypeVar("T")

ROOT_URL = (
    "https://www.eea.europa.eu/en/datahub/datahubitem-view/"
    "98f04097-26de-4fca-86c4-63834818c0c0"
)


class Link(BaseModel):
    """A labeled link associated with a dataset."""

    label: str
    url: AnyUrl


class Dataset(BaseModel):
    """An EU ETS dataset available in the datahub."""

    id: str

    title: str
    format: str
    superseded: bool

    published: datetime | None
    temporal_coverage: tuple[int, int]

    metadata_factsheet: AnyUrl
    direct_download: AnyUrl
    links: list[Link]


class ScrapeError(BaseModel):
    """An error encountered while scraping a dataset."""

    accordion_id: str | None
    message: str


class ScrapeResult(BaseModel):
    """Result of scraping datasets from the EU ETS datahub."""

    datasets: list[Dataset]
    errors: list[ScrapeError]


def _parse_date(text: str) -> datetime | None:
    """Parse date from format like '9 May 2019' or '1 Jul 2025'."""
    text = text.strip()
    try:
        return datetime.strptime(text, "%d %b %Y")
    except ValueError:
        return None


def _parse_years(text: str) -> tuple[int, int] | None:
    """Parse temporal coverage from format like '2005-2024'."""
    try:
        return int(text[0:4]), int(text[5:9])
    except ValueError:
        return None


def _extract_field(
    container: Tag,
    label: str,
    parser: Callable[[str], T | None],
) -> T | None:
    """Extract a field from a <strong>Label:</strong> value pattern."""
    for strong in container.find_all("strong"):
        if strong.string and label in strong.string:
            if strong.parent:
                text = strong.parent.get_text(strip=True)
                text = text.replace(label, "").strip()
                return parser(text)
    return None


def _select(parent: Tag, selector: str) -> Tag:
    """Select a required element or raise ValueError."""
    element = parent.select_one(selector)
    if element is None:
        raise ValueError(f"Missing required element: {selector}")
    return element


def _parse_accordion(accordion: Tag) -> Dataset:
    """Parse a single accordion element into a Dataset."""
    acc_id = str(accordion.get("id", ""))

    # Extract required elements upfront
    title_span = _select(accordion, ".dataset-title")
    content = _select(accordion, ".content")

    formats_span = _select(title_span, ".formats")
    format_label = _select(formats_span, ".dh-label")

    # Title, format, and superseded status
    full_title = next(title_span.stripped_strings)
    format_text = format_label.get_text(strip=True)
    superseded = "Superseded" in formats_span.get_text()

    # Metadata
    published = _extract_field(content, "Published:", _parse_date)
    coverage = _extract_field(content, "Temporal coverage:", _parse_years)
    if not coverage:
        raise ValueError("Missing required field: temporal coverage")

    # Collect all links, then extract special ones
    all_links: dict[str, str] = {}
    for a in content.select("a"):
        label = a.get_text(strip=True)
        href = a.get("href")
        if isinstance(href, str):
            all_links[label] = href

    if "Direct download" not in all_links:
        raise ValueError("Missing required field: Direct download")
    if "Metadata Factsheet" not in all_links:
        raise ValueError("Missing required field: Metadata Factsheet")

    direct_download = all_links.pop("Direct download")
    metadata_factsheet = all_links.pop("Metadata Factsheet")
    links = [Link(label=label, url=AnyUrl(url)) for label, url in all_links.items()]

    return Dataset(
        id=acc_id,
        title=full_title,
        format=format_text,
        superseded=superseded,
        published=published,
        temporal_coverage=coverage,
        metadata_factsheet=AnyUrl(metadata_factsheet),
        direct_download=AnyUrl(direct_download),
        links=links,
    )


async def download_datasets(url: str = ROOT_URL) -> ScrapeResult:
    """Download all available datasets from the EU ETS datahub.

    Args:
        url: Root URL to EU ETS datahub

    Returns:
        A ScrapeResult containing successfully parsed datasets and any errors
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.goto(url)
            await page.wait_for_selector(".datasets-tab .accordion.ui")

            # Click through all year tabs to load all datasets
            # Each tab reveals different datasets (e.g., 2005-2024, 2005-2023, etc.)
            seen_ids: set[str] = set()
            datasets: list[Dataset] = []
            errors: list[ScrapeError] = []

            menu_items = page.locator(".datasets-tab .ui.menu .item")
            for item in await menu_items.all():
                await item.click()
                await page.wait_for_timeout(300)

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                for accordion in soup.select(".datasets-tab .accordion.ui"):
                    acc_id = accordion.get("id")
                    if isinstance(acc_id, str) and acc_id not in seen_ids:
                        seen_ids.add(acc_id)
                        try:
                            datasets.append(_parse_accordion(accordion))
                        except ValueError as e:
                            errors.append(
                                ScrapeError(accordion_id=acc_id, message=str(e))
                            )
        finally:
            await browser.close()

    return ScrapeResult(datasets=datasets, errors=errors)
