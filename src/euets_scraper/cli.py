import asyncio

try:
    import typer
    from rich.console import Console
    from rich.table import Table
except ImportError:
    import sys

    print("CLI requires the 'cli' extra: pip install euets-scraper[cli]")
    sys.exit(1)

from euets_scraper.scraper import Dataset, fetch_datasets

app = typer.Typer(
    help="EU ETS Scraper - fetch carbon quota data from the EU ETS datahub.",
    no_args_is_help=True,
)

# stdout console for data output (tables)
console = Console()
# stderr console for status messages and errors
err = Console(stderr=True)

#
# Helper functions
#


def _get_dataset(dataset_id: str | None = None) -> Dataset:
    """Get a dataset by ID, or the latest non-superseded dataset.

    If dataset_id is provided, uses full=True to fetch all historical datasets.
    """
    full = dataset_id is not None
    result = asyncio.run(fetch_datasets(full=full))

    if dataset_id:
        for ds in result.datasets:
            if ds.dataset_id == dataset_id or ds.dataset_id.startswith(dataset_id):
                return ds
        err.print(f"[red]Dataset not found: {dataset_id}[/red]")
        raise typer.Exit(1)

    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        err.print("[red]No current dataset found.[/red]")
        raise typer.Exit(1)

    return current[0]


def _format_size(size: int) -> str:
    """Format file size in human-readable form."""
    sizef = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if sizef < 1024:
            return f"{sizef:.1f} {unit}" if unit != "B" else f"{sizef} {unit}"
        sizef /= 1024
    return f"{sizef:.1f} TB"


#
# Commands
#


@app.command("ls")
def ls(
    full: bool = typer.Option(
        False,
        "--full",
        "-f",
        help="Use playwright to fetch all historical datasets",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        "-j",
        help="Output as JSON for scripting",
    ),
) -> None:
    """List available datasets from the EU ETS datahub."""
    result = asyncio.run(fetch_datasets(full=full))

    if json_output:
        import json

        print(json.dumps([ds.model_dump(mode="json") for ds in result.datasets]))
        return

    if not result.datasets and not result.errors:
        err.print("[yellow]No datasets found.[/yellow]")
        return

    if result.datasets:
        table = Table(title="EU ETS Datasets")
        table.add_column("ID", style="dim", no_wrap=True)
        table.add_column("Source", style="cyan")
        table.add_column("Coverage", style="blue")
        table.add_column("Published", style="magenta")
        table.add_column("Links", justify="right")
        table.add_column("Status")

        for ds in result.datasets:
            short_id = ds.dataset_id[:8] if len(ds.dataset_id) > 8 else ds.dataset_id

            source = ds.title
            prefix = "European Union Emissions Trading System (EU ETS) data from "
            if source.startswith(prefix):
                source = source[len(prefix) :]
            if source.startswith("the "):
                source = source[4:]

            coverage = f"{ds.temporal_coverage[0]}-{ds.temporal_coverage[1]}"
            published = ds.published.strftime("%Y-%m-%d") if ds.published else "-"
            status = (
                "[dim]superseded[/dim]" if ds.superseded else "[green]current[/green]"
            )

            table.add_row(
                short_id,
                source,
                coverage,
                published,
                str(len(ds.links)),
                status,
            )

        console.print(table)

    if result.errors:
        err.print()
        error_count = len(result.errors)
        if error_count == 1:
            parse_err = result.errors[0]
            err.print(
                f"[red]1 error:[/red] {parse_err.dataset_id or 'unknown'}: {parse_err.message}"
            )
        else:
            err.print(f"[red]{error_count} errors while parsing:[/red]")
            by_message: dict[str, list[str]] = {}
            for parse_err in result.errors:
                key = parse_err.message
                by_message.setdefault(key, []).append(parse_err.dataset_id or "unknown")

            for message, ids in by_message.items():
                if len(ids) <= 3:
                    err.print(f"  - {message}: {', '.join(ids)}")
                else:
                    err.print(
                        f"  - {message}: {', '.join(ids[:2])} +{len(ids) - 2} more"
                    )


@app.command("latest")
def latest() -> None:
    """Print the ID of the most recent dataset."""
    result = asyncio.run(fetch_datasets(full=False))
    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        raise typer.Exit(1)
    print(current[0].dataset_id)


@app.command("check")
def check(
    since: str = typer.Option(
        ...,
        "--since",
        "-s",
        help="Dataset ID to compare against. Prefix match supported.",
    ),
) -> None:
    """Check if a newer dataset exists since the given ID.

    Exits 0 if a newer dataset is available, 1 otherwise.
    Useful for cron jobs: euets check --since abc123 && euets download
    """
    result = asyncio.run(fetch_datasets(full=False))
    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        raise typer.Exit(1)

    latest_id = current[0].dataset_id

    # Check if latest matches the since ID (prefix match supported)
    if latest_id == since or latest_id.startswith(since):
        # No newer dataset
        raise typer.Exit(1)

    # Newer dataset exists - print its ID
    print(latest_id)
    raise typer.Exit(0)


@app.command("url")
def url(
    dataset_id: str | None = typer.Option(
        None,
        "--id",
        "-i",
        help="Dataset ID (default: latest). Prefix match supported.",
    ),
) -> None:
    """Print the URL to the file archive of a dataset."""
    dataset = _get_dataset(dataset_id)
    archive_url = asyncio.run(dataset.url())
    if not archive_url:
        raise typer.Exit(1)
    print(archive_url)


@app.command("files")
def files(
    dataset_id: str | None = typer.Option(
        None,
        "--id",
        "-i",
        help="Dataset ID (default: latest). Prefix match supported.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        "-j",
        help="Output as JSON for scripting",
    ),
) -> None:
    """Print a list of files in the archive of a dataset."""
    dataset = _get_dataset(dataset_id)
    archive_files = asyncio.run(dataset.files())
    if not archive_files:
        raise typer.Exit(1)

    if json_output:
        import json

        print(json.dumps([f.model_dump() for f in archive_files]))
        return

    table = Table(title="Archive Files")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="dim")
    table.add_column("Size", justify="right")

    for f in archive_files:
        table.add_row(f.name, f.file_type, _format_size(f.size))

    console.print(table)


@app.command("download")
def download(
    path: str = typer.Argument(
        ".",
        help="Destination path (local or cloud like s3://bucket/file.zip)",
    ),
    dataset_id: str | None = typer.Option(
        None,
        "--id",
        "-i",
        help="Dataset ID (default: latest). Prefix match supported.",
    ),
) -> None:
    """Download the archive of a dataset to a file."""
    dataset = _get_dataset(dataset_id)
    err.print(f"Downloading {dataset.dataset_id}...", style="dim")
    final_path = asyncio.run(dataset.download(path))
    print(final_path)


@app.command("extract")
def extract(
    pattern: str = typer.Argument(
        ...,
        help="Glob pattern to match filenames (e.g., '*.csv', 'Allowances*')",
    ),
    output_dir: str = typer.Argument(
        ".",
        help="Output directory (local or cloud like s3://bucket/data/)",
    ),
    dataset_id: str | None = typer.Option(
        None,
        "--id",
        "-i",
        help="Dataset ID (default: latest). Prefix match supported.",
    ),
) -> None:
    """Extract files matching a pattern from a dataset's archive."""
    dataset = _get_dataset(dataset_id)
    err.print(f"Extracting from {dataset.dataset_id}...", style="dim")
    extracted = asyncio.run(dataset.extract(pattern, output_dir))

    if not extracted:
        err.print(f"[yellow]No files matched pattern: {pattern}[/yellow]")
        raise typer.Exit(1)

    for path in extracted:
        print(path)
