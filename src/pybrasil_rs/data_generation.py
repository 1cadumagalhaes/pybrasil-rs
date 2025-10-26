import random
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl
from rich.progress import Progress
from .utils import print_success, print_info


RANDOM_SEED = 42
DATA_DIR = Path("data")


def generate_synthetic_data(size: str = "all") -> None:
    scenarios = {
        "small": 1_000,
        "medium": 1_000_000,
        "large": 10_000_000,
        "xlarge": 100_000_000,
    }

    DATA_DIR.mkdir(exist_ok=True)

    if size == "all":
        sizes_to_generate = scenarios.items()
    else:
        sizes_to_generate = [(size, scenarios[size])]

    for scenario_name, num_rows in sizes_to_generate:
        _generate_fact_table(scenario_name, num_rows)
        _generate_dim_table(scenario_name, num_rows)


def _generate_fact_table(scenario: str, num_rows: int) -> None:
    random.seed(RANDOM_SEED)

    print_info(
        f"Generating fact_content_performance ({scenario}: {num_rows:,} rows)..."
    )

    base_date = datetime(2024, 1, 1)

    with Progress() as progress:
        task = progress.add_task("[cyan]Generating data...", total=num_rows)

        data = {
            "content_id": [],
            "event_date": [],
            "region_country": [],
            "views": [],
            "engagement_score": [],
            "process_status": [],
        }

        regions = ["BR", "US", "ES", "DE", "FR", "UK", "CA", "AU", "MX", "IN"]
        statuses = ["Completed", "Processing", "Failed"]

        for i in range(num_rows):
            data["content_id"].append(f"content_{i % 500_000:06d}")
            data["event_date"].append(
                (base_date + timedelta(days=random.randint(0, 364))).date()
            )
            data["region_country"].append(
                random.choices(regions, weights=[45, 30, 10, 5, 4, 3, 2, 1, 1, 1])[0]
            )
            data["views"].append(random.randint(1, 1_000))
            data["engagement_score"].append(round(random.uniform(0, 10), 2))
            data["process_status"].append(random.choice(statuses))

            if (i + 1) % 10_000 == 0:
                progress.update(task, advance=10_000)

    df = pl.DataFrame(data)

    output_path = DATA_DIR / f"fact_content_performance_{scenario}.parquet"
    df.write_parquet(output_path)

    print_success(f"Created {output_path} ({num_rows:,} rows)")


def _generate_dim_table(scenario: str, num_rows: int) -> None:
    random.seed(RANDOM_SEED)

    categories = [
        "Python",
        "Rust",
        "MLOps",
        "APIs",
        "Cloud",
        "DevOps",
        "Web",
        "Data",
        "Mobile",
        "AI",
    ]

    print_info(
        f"Generating dim_content_metadata ({scenario}: {num_rows // 1000:,} unique contents)..."
    )

    data = {
        "content_id": [f"content_{i:06d}" for i in range(500_000)],
        "content_category": [random.choice(categories) for _ in range(500_000)],
    }

    df = pl.DataFrame(data)

    output_path = DATA_DIR / f"dim_content_metadata_{scenario}.parquet"
    df.write_parquet(output_path)

    print_success(f"Created {output_path}")
