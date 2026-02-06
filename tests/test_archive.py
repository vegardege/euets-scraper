from pathlib import Path

import pytest

from euets_scraper.archive import (
    _get_file_type,
    _is_cloud_path,
    extract_files_from_bytes,
    list_files_from_bytes,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def archive_bytes() -> bytes:
    """Load the test archive fixture."""
    return (FIXTURES_DIR / "archive.zip").read_bytes()


def test_is_cloud_path():
    assert _is_cloud_path("s3://bucket/file.zip") is True
    assert _is_cloud_path("gs://bucket/file.zip") is True
    assert _is_cloud_path("az://container/file.zip") is True
    assert _is_cloud_path("./local/file.zip") is False
    assert _is_cloud_path("/absolute/path/file.zip") is False
    assert _is_cloud_path("file://local/file.zip") is False


def test_get_file_type():
    assert _get_file_type("data.csv") == "csv"
    assert _get_file_type("report.PDF") == "pdf"
    assert _get_file_type("archive.tar.gz") == "gz"
    assert _get_file_type("README") == ""
    assert _get_file_type("Makefile") == ""
    assert _get_file_type(".gitignore") == "gitignore"


def test_list_files_from_bytes(archive_bytes: bytes):
    files = list_files_from_bytes(archive_bytes)

    assert len(files) == 4
    names = {f.name for f in files}
    assert names == {"emissions.csv", "allowances.csv", "README.md", "metadata.xml"}

    # Check file types are detected
    csv_files = [f for f in files if f.file_type == "csv"]
    assert len(csv_files) == 2

    # Check sizes are positive
    assert all(f.size > 0 for f in files)


def test_extract_files_from_bytes_glob_pattern(archive_bytes: bytes, tmp_path: Path):
    extracted = extract_files_from_bytes(archive_bytes, "*.csv", tmp_path)

    assert len(extracted) == 2
    assert all(p.endswith(".csv") for p in extracted)

    # Verify files were actually written
    for path in extracted:
        assert Path(path).exists()
        assert Path(path).stat().st_size > 0


def test_extract_files_from_bytes_specific_file(archive_bytes: bytes, tmp_path: Path):
    extracted = extract_files_from_bytes(archive_bytes, "README.md", tmp_path)

    assert len(extracted) == 1
    assert extracted[0].endswith("README.md")
    assert Path(extracted[0]).read_text() == "# Test archive\n"


def test_extract_files_from_bytes_no_match(archive_bytes: bytes, tmp_path: Path):
    extracted = extract_files_from_bytes(archive_bytes, "*.pdf", tmp_path)

    assert len(extracted) == 0


def test_extract_files_from_bytes_creates_directory(archive_bytes: bytes, tmp_path: Path):
    output_dir = tmp_path / "nested" / "output"
    assert not output_dir.exists()

    extracted = extract_files_from_bytes(archive_bytes, "*.csv", output_dir)

    assert len(extracted) == 2
    assert output_dir.exists()
