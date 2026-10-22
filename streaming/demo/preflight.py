
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


TABLES = {
    "silver_stream": "lakehouse.silver.water_level_stream",
    "gold_hourly_stream": (
        "lakehouse.gold.water_level_hourly_stream"
    ),
    "gold_integrated_stream": (
        "lakehouse.gold.hourly_hydrology_stream"
    ),
    "gold_batch": "lakehouse.gold.hourly_hydrology",
}

DEMO_BASELINE = {
    "silver_stream": 10,
    "gold_hourly_stream": 3,
    "gold_integrated_stream": 3,
}


def check(check_id, passed, observed, expected=None):
    status = "PASS" if passed else "FAIL"

    print(
        f"{check_id}: {status} "
        f"(observed={observed}, expected={expected})",
        flush=True,
    )

    return {
        "check_id": check_id,
        "status": status,
        "observed": observed,
        "expected": expected,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--demo-baseline",
        action="store_true",
    )
    args = parser.parse_args()

    print("\n=== S8.1 DEMO PREFLIGHT ===", flush=True)

    checks = []
    counts = {}

    spark = create_spark_session(
        "S8-Demo-Preflight"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        print("SPARK VERSION:", spark.version)
        print(
            "SPARK MASTER:",
            spark.sparkContext.master,
        )
        print(
            "TIMEZONE:",
            spark.conf.get(
                "spark.sql.session.timeZone"
            ),
        )

        print("\n=== TABLE AVAILABILITY ===")

        available = {}

        for name, table in TABLES.items():
            exists = spark.catalog.tableExists(table)
            available[name] = exists

            checks.append(
                check(
                    f"TABLE-{name.upper()}",
                    exists,
                    exists,
                    True,
                )
            )

        print("\n=== RECORD COUNTS ===")

        for name, table in TABLES.items():
            if not available[name]:
                continue

            count = spark.table(table).count()
            counts[name] = count

            print(f"{name}: {count}")

            checks.append(
                check(
                    f"NONEMPTY-{name.upper()}",
                    count > 0,
                    count,
                    "> 0",
                )
            )

            if args.demo_baseline and name in DEMO_BASELINE:
                expected = DEMO_BASELINE[name]

                checks.append(
                    check(
                        f"BASELINE-{name.upper()}",
                        count == expected,
                        count,
                        expected,
                    )
                )

        print("\n=== REPLAY LINEAGE ===")

        for name in [
            "silver_stream",
            "gold_hourly_stream",
            "gold_integrated_stream",
        ]:
            if not available[name]:
                continue

            df = spark.table(TABLES[name])

            if "simulation" not in df.columns:
                checks.append(
                    check(
                        f"SIMULATION-{name.upper()}",
                        False,
                        "column missing",
                        True,
                    )
                )
                continue

            non_simulated = df.filter(
                F.col("simulation").isNull()
                | (F.col("simulation") != F.lit(True))
            ).count()

            checks.append(
                check(
                    f"SIMULATION-{name.upper()}",
                    non_simulated == 0,
                    non_simulated,
                    0,
                )
            )

        print("\n=== BATCH PROTECTION ===")

        if available["gold_batch"]:
            print(
                "Batch Gold table exists and was "
                "read without modification."
            )

        passed = sum(
            item["status"] == "PASS"
            for item in checks
        )

        failed = sum(
            item["status"] == "FAIL"
            for item in checks
        )

        overall = "PASS" if failed == 0 else "FAIL"

        report = {
            "stage": "S8.1",
            "demo_type": "historical_replay",
            "generated_at_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "record_counts": counts,
            "summary": {
                "pass": passed,
                "fail": failed,
            },
            "overall_status": overall,
            "checks": checks,
        }

        output_dir = Path(
            "reports/streaming_demo"
        )
        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        filename = (
            "preflight_"
            + datetime.now(timezone.utc).strftime(
                "%Y%m%dT%H%M%SZ"
            )
            + ".json"
        )

        output_path = output_dir / filename

        output_path.write_text(
            json.dumps(
                report,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        print("\n=== S8.1 RESULT ===")
        print("STATUS:", overall)
        print("PASS:", passed)
        print("FAIL:", failed)
        print("REPORT:", output_path)
        print("NO DATA WRITTEN")

        if failed:
            raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
