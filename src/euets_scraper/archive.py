"""Archive operations for EU ETS datasets."""

import fnmatch
import io
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
from pydantic import AnyUrl, BaseModel


class ArchiveFile(BaseModel):
    """A file contained in a dataset archive."""

    name: str
    size: int
    file_type: str


def _is_cloud_path(path: str) -> bool:
    """Check if path is a cloud path (has scheme like s3://, gs://, etc.)."""
    return "://" in path and not path.startswith("file://")


@contextmanager
def _open_for_write(path: str | Path) -> Iterator[Any]:
    """Open a file for writing, supporting both local and cloud paths.

    For cloud paths (s3://, gs://, az://), requires the [cloud] extra.
    """
    path_str = str(path)

    if _is_cloud_path(path_str):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "Cloud paths require the [cloud] extra: "
                "pip install euets-scraper[cloud]"
            ) from None

        with fsspec.open(path_str, "wb") as f:
            yield f
    else:
        with open(path_str, "wb") as f:
            yield f


def _get_file_type(filename: str) -> str:
    """Get file type from extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return ext if ext != filename.lower() else ""


async def list_archive_files(url: str | AnyUrl) -> list[ArchiveFile]:
    """List the files contained in a remote zip archive.

    Downloads the archive to memory and reads its contents.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(str(url))
        resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        return [
            ArchiveFile(
                name=info.filename.rsplit("/", 1)[-1],
                size=info.file_size,
                file_type=_get_file_type(info.filename),
            )
            for info in z.infolist()
            if not info.is_dir()
        ]


async def download_archive(url: str | AnyUrl, path: str | Path) -> None:
    """Download a remote zip archive to a local or cloud path.

    Args:
        url: URL of the zip archive
        path: Destination path (local or cloud like s3://bucket/file.zip)

    For cloud paths, requires the [cloud] extra.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(str(url))
        resp.raise_for_status()

    with _open_for_write(path) as f:
        f.write(resp.content)


async def extract_files(
    url: str | AnyUrl,
    pattern: str,
    output_dir: str | Path = ".",
) -> list[str]:
    """Extract files matching a pattern from a remote zip archive.

    Args:
        url: URL of the zip archive
        pattern: Glob pattern to match filenames (e.g., "*.csv", "Allowances*")
        output_dir: Directory to extract to (local or cloud like s3://bucket/data/)

    Returns:
        List of paths where files were extracted.

    For cloud paths, requires the [cloud] extra.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(str(url))
        resp.raise_for_status()

    extracted: list[str] = []
    output_str = str(output_dir).rstrip("/")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        for info in z.infolist():
            if info.is_dir():
                continue

            # Match against the basename only
            basename = info.filename.rsplit("/", 1)[-1]
            if not fnmatch.fnmatch(basename, pattern):
                continue

            # Determine output path
            if _is_cloud_path(output_str):
                out_path = f"{output_str}/{basename}"
            else:
                Path(output_str).mkdir(parents=True, exist_ok=True)
                out_path = str(Path(output_str) / basename)

            with _open_for_write(out_path) as f:
                f.write(z.read(info.filename))

            extracted.append(out_path)

    return extracted
