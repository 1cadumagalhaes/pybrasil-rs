import typer
from pathlib import Path
from .utils import setup_logging, print_section
from .data_generation import generate_synthetic_data
from .benchmarks import run_benchmarks

app = typer.Typer()
logger = setup_logging()


@app.command()
def generate(
    size: str = typer.Option(
        "all",
        "--size",
        "-s",
        help="Size of dataset to generate: small (1K), medium (1M), large (10M), xlarge (100M), or all",
    ),
) -> None:
    """Generate synthetic parquet data for benchmarking."""
    print_section("Synthetic Data Generation")

    valid_sizes = ["all", "small", "medium", "large", "xlarge"]
    if size not in valid_sizes:
        typer.echo(
            typer.style(
                f"❌ Invalid size: {size}. Must be one of: {', '.join(valid_sizes)}",
                fg=typer.colors.RED,
            )
        )
        raise typer.Exit(1)

    try:
        generate_synthetic_data(size)
    except Exception as e:
        typer.echo(typer.style(f"❌ Error generating data: {e}", fg=typer.colors.RED))
        raise typer.Exit(1)


@app.command()
def benchmark(
    scenario: str = typer.Option(
        "all",
        "--scenario",
        help="Benchmark scenario: small, medium, large, xlarge, or all",
    ),
    engine: str = typer.Option(
        "all",
        "--engine",
        help="Engine to benchmark: pandas, pandas-pyarrow, polars, or all",
    ),
) -> None:
    """Run performance benchmarks comparing Pandas and Polars."""
    print_section("Performance Benchmarks")

    valid_scenarios = ["all", "small", "medium", "large", "xlarge"]
    valid_engines = ["all", "pandas", "pandas-pyarrow", "polars"]

    if scenario not in valid_scenarios:
        typer.echo(
            typer.style(
                f"❌ Invalid scenario: {scenario}. Must be one of: {', '.join(valid_scenarios)}",
                fg=typer.colors.RED,
            )
        )
        raise typer.Exit(1)

    if engine not in valid_engines:
        typer.echo(
            typer.style(
                f"❌ Invalid engine: {engine}. Must be one of: {', '.join(valid_engines)}",
                fg=typer.colors.RED,
            )
        )
        raise typer.Exit(1)

    data_dir = Path("data")
    if not data_dir.exists():
        typer.echo(
            typer.style(
                "❌ Data directory not found. Run 'generate' command first.",
                fg=typer.colors.RED,
            )
        )
        raise typer.Exit(1)

    try:
        run_benchmarks(scenario, engine)
    except Exception as e:
        typer.echo(
            typer.style(f"❌ Error running benchmarks: {e}", fg=typer.colors.RED)
        )
        raise typer.Exit(1)


@app.command()
def cleanup() -> None:
    """Remove generated data and results."""
    import shutil

    print_section("Cleanup")

    data_dir = Path("data")
    results_dir = Path("results")

    deleted_count = 0

    if data_dir.exists():
        shutil.rmtree(data_dir)
        typer.echo(typer.style(f"✓ Removed {data_dir}", fg=typer.colors.GREEN))
        deleted_count += 1

    if results_dir.exists():
        shutil.rmtree(results_dir)
        typer.echo(typer.style(f"✓ Removed {results_dir}", fg=typer.colors.GREEN))
        deleted_count += 1

    if deleted_count == 0:
        typer.echo(typer.style("No data to clean up", fg=typer.colors.YELLOW))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
