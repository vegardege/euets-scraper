try:
    import typer
except ImportError:
    import sys

    print("CLI requires the 'cli' extra")
    sys.exit(1)

app = typer.Typer()


@app.command()
def main() -> None:
    """EU ETS Scraper CLI."""
    typer.echo("Hello from euets-scraper CLI!")
