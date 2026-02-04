import logging
from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright
from pydantic import AnyUrl, BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T")

# This page appears to be updated regularly. On 2025-07-29, it was listed as:
#
#   Published 29 Sept 2022  Last modified 17 Jun 2025
#
# If the page stops updating, we may need a more sophisticated way of finding the
# latest version of the data.
ROOT_URL = (
    "https://www.eea.europa.eu/en/datahub/datahubitem-view/"
    "98f04097-26de-4fca-86c4-63834818c0c0"
)


class Link(BaseModel):
    """A labeled link."""

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


def _parse_date(text: str) -> datetime | None:
    """Parse date from format like '9 May 2019' or '1 Jul 2025'."""
    text = text.strip()
    try:
        return datetime.strptime(text, "%d %b %Y")
    except ValueError:
        logger.warning(f"Could not parse published date: {text.strip()}")
        return None


def _parse_years(text: str) -> tuple[int, int] | None:
    """Parse temporal coverage from format like '2005-2024'."""
    text = text.strip()
    if len(text) != 9 or "-" not in text:
        logger.warning(f"Unexpected temporal coverage format: {text}")
    try:
        return int(text[0:4]), int(text[5:9])
    except ValueError:
        logger.warning(f"Unexpected temporal coverage format: {text}")
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


def _parse_accordion(accordion: Tag) -> Dataset:
    """Parse a single accordion element into a Dataset."""
    acc_id = accordion.get("id", "")

    # Title and format
    title_span = accordion.select_one(".dataset-title")
    if not title_span:
        raise ValueError("No .dataset-title found")

    formats_span = title_span.select_one(".formats")
    full_title = title_span.get_text(strip=True)

    # Extract format and superseded status
    format_text = ""
    superseded = False
    if formats_span:
        formats_text = formats_span.get_text(strip=True)
        full_title = full_title.replace(formats_text, "").strip()

        # Check for format label (non-inverted)
        format_label = formats_span.select_one(".dh-label:not(.inverted)")
        if format_label:
            format_text = format_label.get_text(strip=True)

        # Check for superseded (inverted label)
        inverted_label = formats_span.select_one(".dh-label.inverted")
        if inverted_label:
            superseded = "superseded" in inverted_label.get_text(strip=True).lower()

    # Content section
    content = accordion.select_one(".content")
    if not content:
        raise ValueError("No .content found")

    dataset_content = content.select_one(".dataset-content")

    # Published date
    published: datetime | None = None
    if dataset_content:
        published = _extract_field(dataset_content, "Published:", _parse_date)

    # Temporal coverage (required)
    if not dataset_content:
        raise ValueError("No .dataset-content found")
    temporal_coverage = _extract_field(
        dataset_content, "Temporal coverage:", _parse_years
    )
    if not temporal_coverage:
        raise ValueError("No temporal coverage found")

    # Metadata factsheet (required)
    meta_link = dataset_content.select_one(".dataset-pdf a")
    if not meta_link:
        raise ValueError("No metadata factsheet link found")
    metadata_factsheet = meta_link.get("href")
    if not isinstance(metadata_factsheet, str):
        raise ValueError("Invalid metadata factsheet href")

    # Direct download (required)
    direct_download: str | None = None
    for span in content.find_all("span"):
        if span.string == "Direct download":
            direct_link = span.find_parent("a")
            if direct_link:
                href = direct_link.get("href")
                if isinstance(href, str):
                    direct_download = href
            break
    if not direct_download:
        raise ValueError("No direct download link found")

    # Other downloads (not "Direct download") and links
    links: list[Link] = []

    # Collect non-direct downloads
    for h5 in content.find_all("h5"):
        if h5.string and "Download" in h5.string:
            downloads_container = h5.find_next_sibling()
            if downloads_container:
                for link in downloads_container.select("a"):
                    href = link.get("href")
                    label = link.get_text(strip=True)
                    if isinstance(href, str) and label != "Direct download":
                        links.append(Link(label=label, url=AnyUrl(href)))
            break

    # Collect links section
    for h5 in content.find_all("h5"):
        if h5.string and "Links" in h5.string:
            links_container = h5.find_next_sibling()
            if links_container:
                for link in links_container.select("a"):
                    href = link.get("href")
                    label = link.get_text(strip=True)
                    if isinstance(href, str):
                        links.append(Link(label=label, url=AnyUrl(href)))
            break

    return Dataset(
        id=str(acc_id),
        title=full_title,
        format=format_text,
        superseded=superseded,
        published=published,
        temporal_coverage=temporal_coverage,
        metadata_factsheet=AnyUrl(metadata_factsheet),
        direct_download=AnyUrl(direct_download),
        links=links,
    )


async def download_datasets(url: str = ROOT_URL) -> list[Dataset]:
    """Download all available datasets from the EU ETS datahub.

    Args:
        url: Root URL to EU ETS datahub

    Returns:
        A list of dataset objects
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
                        datasets.append(_parse_accordion(accordion))
        finally:
            await browser.close()

    return datasets
