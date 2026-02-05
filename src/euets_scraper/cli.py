import asyncio

try:
    import typer
    from rich.console import Console
    from rich.table import Table
except ImportError:
    import sys

    print("CLI requires the 'cli' extra: pip install euets-scraper[cli]")
    sys.exit(1)

from euets_scraper.scraper import download_datasets

# Common prefixes to strip from titles
PREFIX = "European Union Emissions Trading System (EU ETS) data from "

app = typer.Typer(
    help="EU ETS Scraper - fetch carbon quota data from the EU ETS datahub.",
    no_args_is_help=True,
)
console = Console()


@app.command("latest")
def latest() -> None:
    """Print the ID of the most recent dataset."""
    result = asyncio.run(download_datasets(full=False))
    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        raise typer.Exit(1)
    print(current[0].dataset_id)


@app.command("url")
def url() -> None:
    """Print the URL to the file archive of the latest dataset."""
    result = asyncio.run(download_datasets(full=False))
    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        raise typer.Exit(1)
    archive_url = asyncio.run(current[0].url())
    if not archive_url:
        raise typer.Exit(1)
    print(archive_url)


def _format_size(size: int) -> str:
    """Format file size in human-readable form."""
    sizef = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if sizef < 1024:
            return f"{sizef:.1f} {unit}" if unit != "B" else f"{sizef} {unit}"
        sizef /= 1024
    return f"{sizef:.1f} TB"


@app.command("files")
def files(
    json_output: bool = typer.Option(
        False,
        "--json",
        "-j",
        help="Output as JSON for scripting",
    ),
) -> None:
    """Print a list of files in the archive of the latest dataset."""
    result = asyncio.run(download_datasets(full=False))
    current = [ds for ds in result.datasets if not ds.superseded]
    if not current:
        raise typer.Exit(1)

    archive_files = asyncio.run(current[0].files())
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
    result = asyncio.run(download_datasets(full=full))

    if json_output:
        import json

        print(json.dumps([ds.model_dump(mode="json") for ds in result.datasets]))
        return

    if not result.datasets and not result.errors:
        console.print("[yellow]No datasets found.[/yellow]")
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
            if source.startswith(PREFIX):
                source = source[len(PREFIX) :]
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
        console.print()
        error_count = len(result.errors)
        if error_count == 1:
            err = result.errors[0]
            console.print(
                f"[red]1 error:[/red] {err.dataset_id or 'unknown'}: {err.message}"
            )
        else:
            console.print(f"[red]{error_count} errors while parsing:[/red]")
            by_message: dict[str, list[str]] = {}
            for err in result.errors:
                key = err.message
                by_message.setdefault(key, []).append(err.dataset_id or "unknown")

            for message, ids in by_message.items():
                if len(ids) <= 3:
                    console.print(f"  - {message}: {', '.join(ids)}")
                else:
                    console.print(
                        f"  - {message}: {', '.join(ids[:2])} +{len(ids) - 2} more"
                    )
