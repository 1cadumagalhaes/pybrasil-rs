from rich.console import Console
from rich.logging import RichHandler
import logging
import psutil
import platform
from datetime import datetime

console = Console()


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    return logging.getLogger("pybrasil")


def print_section(title: str) -> None:
    console.print(f"\n[bold cyan]{'=' * 60}[/bold cyan]")
    console.print(f"[bold cyan]{title:^60}[/bold cyan]")
    console.print(f"[bold cyan]{'=' * 60}[/bold cyan]\n")


def print_success(message: str) -> None:
    console.print(f"[green]✓ {message}[/green]")


def print_info(message: str) -> None:
    console.print(f"[blue]ℹ {message}[/blue]")


def print_table(data: list[dict], title: str = "") -> None:
    from rich.table import Table

    if not data:
        return

    table = Table(title=title, show_header=True, header_style="bold magenta")
    columns = list(data[0].keys())

    for col in columns:
        table.add_column(col)

    for row in data:
        table.add_row(*[str(row[col]) for col in columns])

    console.print(table)


def get_system_info() -> dict:
    """Collect comprehensive system information for benchmarking context."""
    cpu_freq = psutil.cpu_freq()
    memory = psutil.virtual_memory()

    return {
        "timestamp": datetime.now().isoformat(),
        "cpu_count": psutil.cpu_count(logical=False),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_name": platform.processor(),
        "cpu_freq_mhz": round(cpu_freq.current) if cpu_freq else None,
        "memory_total_gb": round(memory.total / (1024**3), 2),
        "memory_available_gb": round(memory.available / (1024**3), 2),
    }
