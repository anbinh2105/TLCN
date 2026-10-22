from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime


# =====================================================================
# Silver pipeline configuration
# =====================================================================

@dataclass(frozen=True)
class SilverJob:
    source: str
    module: str


SILVER_JOBS = [
    SilverJob(
        source="weather",
        module="transformation.silver.weather_to_silver",
    ),
    SilverJob(
        source="rainfall",
        module="transformation.silver.rainfall_to_silver",
    ),
    SilverJob(
        source="river_discharge",
        module="transformation.silver.river_discharge_to_silver",
    ),
    SilverJob(
        source="water_level",
        module="transformation.silver.water_level_to_silver",
    ),
    SilverJob(
        source="tide",
        module="transformation.silver.tide_to_silver",
    ),
]


# =====================================================================
# Console helpers
# =====================================================================

def print_section(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    return f"{minutes}m {remaining_seconds:.1f}s"


# =====================================================================
# Job execution
# =====================================================================

def run_job(job: SilverJob) -> dict:
    """
    Execute one Bronze -> Silver transformation as an isolated
    Python process.

    Each transformation module manages its own SparkSession.
    A non-zero return code means the job failed.
    """

    print_section(
        f"SILVER JOB START: {job.source.upper()}"
    )

    print(f"Module : {job.module}")
    print(f"Python : {sys.executable}")

    start_time = time.perf_counter()

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            job.module,
        ],
        check=False,
    )

    elapsed = time.perf_counter() - start_time

    success = result.returncode == 0

    status = "PASS" if success else "FAIL"

    print()
    print(
        f"SILVER JOB {status}: "
        f"{job.source.upper()}"
    )
    print(
        f"Duration: "
        f"{format_duration(elapsed)}"
    )
    print(
        f"Return code: "
        f"{result.returncode}"
    )

    return {
        "source": job.source,
        "module": job.module,
        "status": status,
        "return_code": result.returncode,
        "duration_seconds": elapsed,
    }


# =====================================================================
# Pipeline
# =====================================================================

def run_pipeline(
    selected_sources: set[str] | None = None,
) -> int:
    """
    Run all Silver jobs sequentially.

    Fail-fast behavior:
    if one source fails, downstream Silver jobs are not executed.

    This prevents the overall pipeline from being reported as
    successful when an upstream transformation has failed.
    """

    print_section(
        "CAN THO FLOOD LAKEHOUSE - SILVER PIPELINE"
    )

    pipeline_started_at = datetime.now()

    print(
        "Started at : "
        f"{pipeline_started_at:%Y-%m-%d %H:%M:%S}"
    )

    jobs = SILVER_JOBS

    if selected_sources:
        jobs = [
            job
            for job in SILVER_JOBS
            if job.source in selected_sources
        ]

    if not jobs:
        print("No Silver jobs selected.")
        return 1

    print()
    print("Execution order:")

    for index, job in enumerate(jobs, start=1):
        print(
            f"  {index}. "
            f"{job.source}"
        )

    results = []

    pipeline_start = time.perf_counter()

    for job in jobs:
        result = run_job(job)

        results.append(result)

        if result["status"] == "FAIL":
            print_section(
                "SILVER PIPELINE STOPPED"
            )

            print(
                f"Failed source : "
                f"{job.source}"
            )

            print(
                f"Return code   : "
                f"{result['return_code']}"
            )

            print()
            print(
                "Remaining jobs were NOT executed."
            )

            break

    pipeline_elapsed = (
        time.perf_counter() - pipeline_start
    )

    # -------------------------------------------------------------
    # Pipeline summary
    # -------------------------------------------------------------

    print_section(
        "SILVER PIPELINE SUMMARY"
    )

    for result in results:
        print(
            f"{result['source']:<20} "
            f"{result['status']:<6} "
            f"{format_duration(result['duration_seconds'])}"
        )

    executed_sources = {
        result["source"]
        for result in results
    }

    for job in jobs:
        if job.source not in executed_sources:
            print(
                f"{job.source:<20} "
                f"{'SKIPPED':<6}"
            )

    print("-" * 100)

    passed = sum(
        result["status"] == "PASS"
        for result in results
    )

    failed = sum(
        result["status"] == "FAIL"
        for result in results
    )

    skipped = len(jobs) - len(results)

    print(f"PASS    : {passed}")
    print(f"FAIL    : {failed}")
    print(f"SKIPPED : {skipped}")

    print(
        "Duration: "
        f"{format_duration(pipeline_elapsed)}"
    )

    pipeline_finished_at = datetime.now()

    print(
        "Finished at: "
        f"{pipeline_finished_at:%Y-%m-%d %H:%M:%S}"
    )

    # -------------------------------------------------------------
    # Final pipeline status
    # -------------------------------------------------------------

    if failed > 0:
        print()
        print("SILVER PIPELINE STATUS: FAILED")

        return 1

    if passed != len(jobs):
        print()
        print("SILVER PIPELINE STATUS: INCOMPLETE")

        return 1

    print()
    print("SILVER PIPELINE STATUS: PASS")

    return 0


# =====================================================================
# CLI
# =====================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run Can Tho Flood Lakehouse "
            "Bronze -> Silver transformations."
        )
    )

    parser.add_argument(
        "--source",
        action="append",
        choices=[
            job.source
            for job in SILVER_JOBS
        ],
        help=(
            "Run only selected source(s). "
            "Can be specified multiple times."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    selected_sources = (
        set(args.source)
        if args.source
        else None
    )

    exit_code = run_pipeline(
        selected_sources=selected_sources,
    )

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()