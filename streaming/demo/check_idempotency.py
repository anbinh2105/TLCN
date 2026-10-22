
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


TABLES = {
    "silver": {
        "table": "lakehouse.silver.water_level_stream",
        "keys": ["event_id"],
        "expected": 10,
    },
    "gold_hourly": {
        "table": "lakehouse.gold.water_level_hourly_stream",
        "keys": ["hour_timestamp", "station_code"],
        "expected": 3,
    },
    "gold_integrated": {
        "table": "lakehouse.gold.hourly_hydrology_stream",
        "keys": ["hour_start", "location_id", "station_code"],
        "expected": 3,
    },
}


def fingerprint(df):
    """
    Deterministic hash of the complete table content.
    Suitable for the small, fixed demo dataset.
    """

    columns = sorted(df.columns)

    rows = [
        row.asDict(recursive=True)
        for row in df.select(*columns).collect()
    ]

    normalized = [
        json.dumps(
            row,
            sort_keys=True,
            default=str,
            ensure_ascii=False,
        )
        for row in rows
    ]

    normalized.sort()

    payload = "\n".join(normalized).encode("utf-8")

    return hashlib.sha256(payload).hexdigest()


def check_table(spark, name, config):
    table = config["table"]
    keys = config["keys"]
    expected = config["expected"]

    print(f"\n=== {name.upper()} ===", flush=True)

    df = spark.table(table)

    count_before = df.count()
    hash_before = fingerprint(df)

    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Second read; no data is written.
    df_again = spark.table(table)

    count_after = df_again.count()
    hash_after = fingerprint(df_again)

    checks = {
        "expected_count": count_before == expected,
        "unique_keys": duplicates == 0,
        "stable_count": count_before == count_after,
        "stable_content": hash_before == hash_after,
    }

    status = (
        "PASS"
        if all(checks.values())
        else "FAIL"
    )

    print("COUNT BEFORE:", count_before)
    print("COUNT AFTER:", count_after)
    print("DUPLICATE GROUPS:", duplicates)
    print("CONTENT STABLE:", hash_before == hash_after)
    print("STATUS:", status)

    return {
        "table": table,
        "status": status,
        "count_before": count_before,
        "count_after": count_after,
        "duplicate_groups": duplicates,
        "hash_before": hash_before,
        "hash_after": hash_after,
        "checks": checks,
    }


def main():
    print("\n=== S8.3 IDEMPOTENCY PRECHECK ===")

    spark = create_spark_session(
        "S8-Idempotency-Precheck"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        results = {}

        for name, config in TABLES.items():
            results[name] = check_table(
                spark, name, config
            )

        overall = (
            "PASS"
            if all(
                item["status"] == "PASS"
                for item in results.values()
            )
            else "FAIL"
        )

        report = {
            "stage": "S8.3",
            "mode": "read_only_precheck",
            "generated_at_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "overall_status": overall,
            "results": results,
        }

        report_dir = Path("reports/streaming_demo")
        report_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        timestamp = datetime.now(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%S%fZ")

        report_path = (
            report_dir
            / f"idempotency_precheck_{timestamp}.json"
        )

        report_path.write_text(
            json.dumps(
                report,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        print("\n=== S8.3 RESULT ===")
        print("STATUS:", overall)
        print("MODE: READ_ONLY")
        print("REPORT:", report_path)
        print("NO DATA WRITTEN")

        if overall != "PASS":
            raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
