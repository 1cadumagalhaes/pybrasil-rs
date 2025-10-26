import time
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl
from rich.progress import Progress

from .utils import print_success, print_table

RESULTS_DIR = Path("results")
DATA_DIR = Path("data")


class BenchmarkResult:
    def __init__(
        self, engine: str, scenario: str, time_seconds: float, memory_mb: float
    ):
        self.engine = engine
        self.scenario = scenario
        self.time_seconds = round(time_seconds, 4)
        self.memory_mb = round(memory_mb, 2)

    def to_dict(self) -> dict:
        return {
            "scenario": self.scenario,
            "time_seconds": self.time_seconds,
            "memory_mb": self.memory_mb,
        }


def run_benchmarks(scenario: str = "all", engine: str = "all") -> None:
    RESULTS_DIR.mkdir(exist_ok=True)

    scenarios = ["small", "medium", "large", "xlarge"]
    engines = ["pandas", "pandas-pyarrow", "polars"]

    if scenario != "all":
        scenarios = [scenario]
    if engine != "all":
        engines = [engine]

    results_by_engine = {e: [] for e in engines}

    total_benchmarks = len(scenarios) * len(engines)
    with Progress() as progress:
        task = progress.add_task(
            "[cyan]Running benchmarks...",
            total=total_benchmarks,
        )

        for scn in scenarios:
            for eng in engines:
                progress.update(
                    task,
                    description=f"[cyan]Running {eng} - {scn}...",
                )

                result = _run_single_benchmark(eng, scn)
                results_by_engine[eng].append(result)

                progress.update(task, advance=1)

    _save_results(results_by_engine)
    _display_results(results_by_engine)


def _run_single_benchmark(engine: str, scenario: str) -> BenchmarkResult:
    fact_file = DATA_DIR / f"fact_content_performance_{scenario}.parquet"
    dim_file = DATA_DIR / f"dim_content_metadata_{scenario}.parquet"

    if not fact_file.exists() or not dim_file.exists():
        raise FileNotFoundError(f"Data files not found for scenario: {scenario}")

    if engine == "pandas":
        return _benchmark_pandas(fact_file, dim_file, scenario, use_pyarrow=False)
    elif engine == "pandas-pyarrow":
        return _benchmark_pandas(fact_file, dim_file, scenario, use_pyarrow=True)
    elif engine == "polars":
        return _benchmark_polars(fact_file, dim_file, scenario)
    else:
        raise ValueError(f"Unknown engine: {engine}")


def _measure_execution(func: Callable) -> tuple[float, float]:
    import tracemalloc

    tracemalloc.start()

    start_time = time.perf_counter()

    _ = func()

    end_time = time.perf_counter()

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    elapsed_time = end_time - start_time
    peak_memory = peak / (1024 * 1024)

    return elapsed_time, peak_memory


def _benchmark_pandas(
    fact_file: Path,
    dim_file: Path,
    scenario: str,
    use_pyarrow: bool = False,
) -> BenchmarkResult:
    def query():
        fact = pd.read_parquet(
            fact_file, engine="pyarrow" if use_pyarrow else "fastparquet"
        )
        dim = pd.read_parquet(
            dim_file, engine="pyarrow" if use_pyarrow else "fastparquet"
        )

        merged = fact.merge(dim, on="content_id", how="inner")

        filtered = merged[merged["process_status"] != "Failed"]

        result = (
            filtered.groupby("region_country")
            .agg({
                "views": "sum",
                "engagement_score": "mean",
            })
            .reset_index()
            .sort_values("views", ascending=False)
        )

        return result

    elapsed_time, peak_memory = _measure_execution(query)
    return BenchmarkResult(
        "pandas" if not use_pyarrow else "pandas-pyarrow",
        scenario,
        elapsed_time,
        peak_memory,
    )


def _benchmark_polars(
    fact_file: Path,
    dim_file: Path,
    scenario: str,
) -> BenchmarkResult:
    def query():
        fact = pl.scan_parquet(fact_file)
        dim = pl.scan_parquet(dim_file)

        result = (
            fact.join(dim, on="content_id", how="inner")
            .filter(pl.col("process_status") != "Failed")
            .group_by("region_country")
            .agg([
                pl.col("views").sum(),
                pl.col("engagement_score").mean(),
            ])
            .sort("views", descending=True)
            .collect()
        )

        return result

    elapsed_time, peak_memory = _measure_execution(query)
    return BenchmarkResult("polars", scenario, elapsed_time, peak_memory)


def _save_results(results_by_engine: dict[str, list[BenchmarkResult]]) -> None:
    for engine, results in results_by_engine.items():
        data = [result.to_dict() for result in results]
        df = pd.DataFrame(data)

        output_path = RESULTS_DIR / f"{engine}.csv"
        df.to_csv(output_path, index=False)

        print_success(f"Results saved to {output_path}")


def _display_results(results_by_engine: dict[str, list[BenchmarkResult]]) -> None:
    print("\n")
    for engine, results in results_by_engine.items():
        data = [result.to_dict() for result in results]
        print_table(data, title=f"Results: {engine}")
