from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import AnyUrl, BaseModel, PrivateAttr

if TYPE_CHECKING:
    from euets_scraper.archive import ArchiveFile

T = TypeVar("T")

# EU ETS DataHub root page
ROOT_URL = (
    "https://www.eea.europa.eu/en/datahub/datahubitem-view/"
    "98f04097-26de-4fca-86c4-63834818c0c0"
)

# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 30.0


class Link(BaseModel):
    """A labeled link associated with a dataset."""

    label: str
    url: AnyUrl


class Dataset(BaseModel):
    """An EU ETS dataset available in the datahub."""

    dataset_id: str

    title: str
    format: str
    superseded: bool

    published: datetime | None
    temporal_coverage: tuple[int, int]

    factsheet: AnyUrl
    links: list[Link]

    _cached_archive_url: str | None = PrivateAttr(default=None)
    _cached_archive_bytes: bytes | None = PrivateAttr(default=None)

    async def _get_archive_bytes(self) -> bytes:
        """Get archive bytes, fetching and caching if needed."""
        if self._cached_archive_bytes is None:
            from euets_scraper.archive import fetch_archive

            url = await self.url()
            if not url:
                raise ValueError("No download URL available for this dataset")
            self._cached_archive_bytes = await fetch_archive(url)
        return self._cached_archive_bytes

    async def url(self) -> str | None:
        """Get a direct URL to the zip archive of files for this dataset.

        This is _not_ the same as the "Direct download" link, which points to a
        web reader and not the zip file itself.
        """
        if self._cached_archive_url is None:
            for link in self.links:
                if link.label == "Direct download":
                    self._cached_archive_url = await resolve_download_url(link.url)
        return self._cached_archive_url

    async def files(self) -> list["ArchiveFile"]:
        """Get a list of all files in the 'Direct download' archive."""
        from euets_scraper.archive import list_files_from_bytes

        data = await self._get_archive_bytes()
        return list_files_from_bytes(data)

    async def download(self, path: str | Path = ".") -> str:
        """Download the archive to a local or cloud path.

        Args:
            path: Destination path (local or cloud like s3://bucket/file.zip).
                  If a directory (local path or ends with /), uses {dataset_id}.zip as filename.

        Returns:
            The final path where the file was saved.

        For cloud paths, requires the [cloud] extra.
        """
        from euets_scraper.archive import write_bytes_to_path

        data = await self._get_archive_bytes()

        # Detect if path is a directory (local dir or trailing slash for cloud paths)
        path_str = str(path)
        is_dir = path_str.endswith("/") or Path(path).is_dir()
        if is_dir:
            path_str = f"{path_str.rstrip('/')}/{self.dataset_id}.zip"

        write_bytes_to_path(data, path_str)
        return path_str

    async def extract(self, pattern: str, output_dir: str | Path = ".") -> list[str]:
        """Extract files matching a pattern from the archive.

        Args:
            pattern: Glob pattern to match filenames (e.g., "*.csv", "Allowances*")
            output_dir: Directory to extract to (local or cloud like s3://bucket/data/)

        Returns:
            List of paths where files were extracted.

        For cloud paths, requires the [cloud] extra.
        """
        from euets_scraper.archive import extract_files_from_bytes

        data = await self._get_archive_bytes()
        return extract_files_from_bytes(data, pattern, output_dir)


class ParseError(BaseModel):
    """An error encountered while parsing a dataset."""

    dataset_id: str | None
    message: str


class ETSResult(BaseModel):
    """Result of fetching datasets from the EU ETS datahub."""

    datasets: list[Dataset]
    errors: list[ParseError]


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
        start, end = text.split("-")
        return int(start), int(end)
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
            sibling = strong.next_sibling
            if sibling:
                return parser(str(sibling).strip())
    return None


def _select(parent: Tag, selector: str) -> Tag:
    """Select a required element or raise ValueError."""
    element = parent.select_one(selector)
    if element is None:
        raise ValueError(f"Missing required element: {selector}")
    return element


def _parse_accordion(accordion: Tag) -> Dataset:
    """Parse a single accordion element into a Dataset."""
    dataset_id = str(accordion.get("id", ""))

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

    if "Metadata Factsheet" not in all_links:
        raise ValueError("Missing required field: Metadata Factsheet")

    factsheet = all_links.pop("Metadata Factsheet")
    links = [Link(label=label, url=AnyUrl(url)) for label, url in all_links.items()]

    return Dataset(
        dataset_id=dataset_id,
        title=full_title,
        format=format_text,
        superseded=superseded,
        published=published,
        temporal_coverage=coverage,
        factsheet=AnyUrl(factsheet),
        links=links,
    )


def _parse_accordions(soup: BeautifulSoup) -> ETSResult:
    """Parse all accordion elements from a BeautifulSoup document."""
    datasets: list[Dataset] = []
    errors: list[ParseError] = []

    for accordion in soup.select(".datasets-tab .accordion.ui"):
        acc_id = accordion.get("id")
        if not isinstance(acc_id, str):
            continue
        try:
            datasets.append(_parse_accordion(accordion))
        except ValueError as e:
            errors.append(ParseError(dataset_id=acc_id, message=str(e)))

    return ETSResult(datasets=datasets, errors=errors)


async def fetch_datasets_simple(url: str = ROOT_URL) -> ETSResult:
    """Fast scrape using httpx. Only gets datasets visible without JavaScript.

    This typically returns the current dataset and one superseded dataset.
    Use fetch_datasets_full() to get all historical datasets.

    Args:
        url: Root URL to EU ETS datahub

    Returns:
        An ETSResult containing successfully parsed datasets and any errors
    """
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        response = await client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    return _parse_accordions(soup)


async def fetch_datasets_full(url: str = ROOT_URL) -> ETSResult:
    """Full scrape using playwright. Gets all datasets including older tabs.

    Requires the [playwright] extra.

    Args:
        url: Root URL to EU ETS datahub

    Returns:
        An ETSResult containing successfully parsed datasets and any errors
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise ImportError("playwright is required for full scrape") from None

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
            errors: list[ParseError] = []

            menu_items = page.locator(".datasets-tab .ui.menu .item")
            for item in await menu_items.all():
                await item.click()
                # Brief wait for tab content to render; no reliable selector to wait for
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
                            errors.append(ParseError(dataset_id=acc_id, message=str(e)))
        finally:
            await browser.close()

    return ETSResult(datasets=datasets, errors=errors)


async def fetch_datasets(url: str = ROOT_URL, *, full: bool = False) -> ETSResult:
    """Fetch dataset metadata from the EU ETS datahub.

    Args:
        url: Root URL to EU ETS datahub.
        full: If True, use playwright to get all historical datasets.
              Requires the [playwright] extra.

    Returns:
        An ETSResult containing successfully parsed datasets and any errors
    """
    if full:
        return await fetch_datasets_full(url)
    else:
        return await fetch_datasets_simple(url)


def _resolve_download_url_from_html(html: str) -> str:
    """Extract the zip file URL from download page HTML.

    Args:
        html: HTML content of the download page

    Returns:
        The direct URL to the zip file

    Raises:
        ValueError: If the download link cannot be found
    """
    soup = BeautifulSoup(html, "html.parser")

    for span in soup.find_all("span"):
        if span.string == "Download all files":
            if link := span.find_parent("a"):
                if href := link.get("href"):
                    return str(href)

    raise ValueError("Could not find 'Download all files' link on page")


async def resolve_download_url(download_page: str | AnyUrl) -> str:
    """Resolve a download page URL to the actual zip file URL.

    The download page contains a "Download all files" button linking to the zip.

    Args:
        download_page: URL of the "Direct download" page

    Returns:
        The direct URL to the zip file

    Raises:
        ValueError: If the download link cannot be found on the page
    """
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        response = await client.get(str(download_page))
        response.raise_for_status()

    return _resolve_download_url_from_html(response.text)
