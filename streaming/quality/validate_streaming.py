
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session
from streaming.quality.rules import (
    result,
    check_schema,
    check_not_null,
    check_unique,
    check_finite,
    check_range_order,
    check_allowed,
    check_condition,
    check_warning_count,
    check_equal_datasets,
)


SILVER = "lakehouse.silver.water_level_stream"
HOURLY = "lakehouse.gold.water_level_hourly_stream"
INTEGRATED = "lakehouse.gold.hourly_hydrology_stream"

REPORT_DIR = Path("reports/streaming_quality")


def add(results, check):
    results.append(check)
    print(
        f"[{check['status']}] "
        f"{check['rule_id']}: "
        f"{check['message']} "
        f"(observed={check['observed']})"
    )


def validate(spark, demo_baseline=False):
    results = []
    tables = {}

    print("\n=== S7 TABLE CHECK ===")

    for name in [SILVER, HOURLY, INTEGRATED]:
        exists = spark.catalog.tableExists(name)
        add(results, result(
            "S7-TABLE", name, exists, exists,
            "Iceberg table exists"
        ))
        if exists:
            tables[name] = spark.table(name)

    if len(tables) != 3:
        return results, {}

    s = tables[SILVER]
    h = tables[HOURLY]
    g = tables[INTEGRATED]

    counts = {
        "silver": s.count(),
        "gold_hourly": h.count(),
        "gold_integrated": g.count(),
    }

    print("\n=== S7 RECORD COUNTS ===")
    print(counts)

    print("\n=== S7 SCHEMA ===")

    schemas = [
        (
            s, SILVER, {
                "event_id": "string",
                "station_code": "string",
                "timestamp": "timestamp",
                "water_level_m": "double",
            }
        ),
        (
            h, HOURLY, {
                "hour_timestamp": "timestamp",
                "station_code": "string",
                "water_level_mean": "double",
                "water_level_obs_count": "int",
                "expected_obs_count": "int",
                "hour_quality_flag": "string",
                "simulation": "boolean",
            }
        ),
        (
            g, INTEGRATED, {
                "hour_start": "timestamp",
                "location_id": "string",
                "station_code": "string",
                "water_level_mean": "double",
                "water_level_quality_flag": "string",
                "present_source_count": "int",
                "usable_source_count": "int",
                "simulation": "boolean",
                "water_level_origin": "string",
            }
        ),
    ]

    for df, name, required in schemas:
        add(results, check_schema(
            df, name, required, "S7-SCHEMA"
        ))

    # Avoid obscure Spark errors if the expected schema
    # is not available.
    if any(
        r["status"] == "FAIL"
        and r["rule_id"] == "S7-SCHEMA"
        for r in results
    ):
        return results, counts

    print("\n=== S7 SILVER QUALITY ===")

    add(results, check_not_null(
        s, SILVER,
        ["event_id", "station_code", "timestamp"],
        "S7-SILVER-NULL"
    ))

    add(results, check_unique(
        s, SILVER, ["event_id"], "S7-SILVER-UNIQUE"
    ))

    add(results, check_unique(
        s, SILVER, ["station_code", "timestamp"],
        "S7-SILVER-TIME-UNIQUE"
    ))

    add(results, check_finite(
        s, SILVER, ["water_level_m"],
        "S7-SILVER-NUMERIC"
    ))

    print("\n=== S7 GOLD HOURLY QUALITY ===")

    add(results, check_not_null(
        h, HOURLY,
        ["hour_timestamp", "station_code"],
        "S7-HOURLY-NULL"
    ))

    add(results, check_unique(
        h, HOURLY,
        ["hour_timestamp", "station_code"],
        "S7-HOURLY-UNIQUE"
    ))

    add(results, check_finite(
        h, HOURLY,
        ["water_level_mean", "water_level_min",
         "water_level_max", "water_level_last"],
        "S7-HOURLY-NUMERIC"
    ))

    add(results, check_range_order(
        h, HOURLY,
        "water_level_min",
        "water_level_mean",
        "water_level_max",
        "S7-HOURLY-RANGE"
    ))

    add(results, check_allowed(
        h, HOURLY, "hour_quality_flag",
        ["COMPLETE", "PARTIAL", "EXCESS"],
        "S7-HOURLY-FLAG"
    ))

    count_valid = (
        (F.col("water_level_obs_count") > 0)
        & (F.col("expected_obs_count") > 0)
        & (
            (F.col("hour_quality_flag") == "COMPLETE")
            & (
                F.col("water_level_obs_count")
                == F.col("expected_obs_count")
            )
            |
            (F.col("hour_quality_flag") == "PARTIAL")
            & (
                F.col("water_level_obs_count")
                < F.col("expected_obs_count")
            )
            |
            (F.col("hour_quality_flag") == "EXCESS")
            & (
                F.col("water_level_obs_count")
                > F.col("expected_obs_count")
            )
        )
    )

    add(results, check_condition(
        h, HOURLY, count_valid,
        "S7-HOURLY-COUNT",
        "Observation count agrees with quality flag"
    ))

    add(results, check_condition(
        h, HOURLY,
        F.col("hour_timestamp") == F.date_trunc(
            "hour", F.col("hour_timestamp")
        ),
        "S7-HOURLY-GRAIN",
        "Timestamp aligned to full hour"
    ))

    print("\n=== S7 GOLD INTEGRATED QUALITY ===")

    add(results, check_not_null(
        g, INTEGRATED,
        ["hour_start", "location_id", "station_code"],
        "S7-INTEGRATED-NULL"
    ))

    add(results, check_unique(
        g, INTEGRATED,
        ["hour_start", "location_id", "station_code"],
        "S7-INTEGRATED-UNIQUE"
    ))

    add(results, check_range_order(
        g, INTEGRATED,
        "water_level_min",
        "water_level_mean",
        "water_level_max",
        "S7-INTEGRATED-RANGE"
    ))

    add(results, check_condition(
        g, INTEGRATED,
        (F.col("present_source_count").between(0, 5))
        & (F.col("usable_source_count").between(0, 5))
        & (
            F.col("usable_source_count")
            <= F.col("present_source_count")
        ),
        "S7-INTEGRATED-COUNTS",
        "Source counts within valid range"
    ))

    add(results, check_condition(
        g, INTEGRATED,
        (
            F.col("all_five_present")
            == (F.col("present_source_count") == 5)
        )
        & (
            F.col("all_five_usable")
            == (F.col("usable_source_count") == 5)
        ),
        "S7-INTEGRATED-ALL-FIVE",
        "All-five flags agree with source counts"
    ))

    source_names = [
        "weather", "rainfall", "discharge",
        "water_level", "tide"
    ]

    for suffix, total_column in [
        ("present", "present_source_count"),
        ("usable", "usable_source_count"),
    ]:
        expected = sum(
            [
                F.when(
                    F.coalesce(
                        F.col(f"{name}_{suffix}"),
                        F.lit(False)
                    ), 1
                ).otherwise(0)
                for name in source_names
            ]
        )

        add(results, check_condition(
            g, INTEGRATED,
            F.col(total_column) == expected,
            f"S7-SOURCE-{suffix.upper()}",
            f"{total_column} equals component flags"
        ))

    add(results, check_condition(
        g, INTEGRATED,
        (
            F.col("water_level_present") == F.lit(True)
        )
        & (
            F.col("water_level_usable")
            == (
                F.col("water_level_quality_flag")
                == "COMPLETE"
            )
        ),
        "S7-WATER-USABLE",
        "Water-level usability follows S6 policy"
    ))

    add(results, check_condition(
        g, INTEGRATED,
        F.col("water_level_origin") == "STREAM_REPLAY",
        "S7-LINEAGE-ORIGIN",
        "Water-level origin is historical replay"
    ))

    add(results, check_condition(
        g, INTEGRATED,
        F.col("simulation") == F.lit(True),
        "S7-LINEAGE-SIMULATION",
        "Replay rows are marked as simulation"
    ))

    print("\n=== S7 CROSS-LAYER CONSISTENCY ===")

    hourly_comparable = h.select(
        F.col("hour_timestamp").alias("hour_start"),
        "station_code",
        "water_level_mean",
        "water_level_min",
        "water_level_max",
        "water_level_last",
        "water_level_obs_count",
        F.col("hour_quality_flag").alias(
            "water_level_quality_flag"
        ),
        "simulation",
        "mixed_simulation",
    )

    integrated_comparable = g.select(
        "hour_start",
        "station_code",
        "water_level_mean",
        "water_level_min",
        "water_level_max",
        "water_level_last",
        "water_level_obs_count",
        "water_level_quality_flag",
        "simulation",
        "mixed_simulation",
    )

    columns_to_compare = [
        ("water_level_mean", "water_level_mean"),
        ("water_level_min", "water_level_min"),
        ("water_level_max", "water_level_max"),
        ("water_level_last", "water_level_last"),
        ("water_level_obs_count", "water_level_obs_count"),
        ("water_level_quality_flag", "water_level_quality_flag"),
        ("simulation", "simulation"),
        ("mixed_simulation", "mixed_simulation"),
    ]

    add(results, check_equal_datasets(
        hourly_comparable,
        integrated_comparable,
        ["hour_start", "station_code"],
        columns_to_compare,
        INTEGRATED,
        "S7-CROSS-LAYER"
    ))

    print("\n=== S7 WARNINGS ===")

    add(results, check_warning_count(
        g, INTEGRATED,
        F.col("water_level_quality_flag") == "PARTIAL",
        "S7-WARN-PARTIAL",
        "Hours with incomplete water-level observations"
    ))

    add(results, check_warning_count(
        g, INTEGRATED,
        F.col("tide_present") == F.lit(False),
        "S7-WARN-TIDE",
        "Hours without tide observations"
    ))

    if demo_baseline:
        print("\n=== S7 DEMO BASELINE ===")
        expected_counts = {
            "silver": 10,
            "gold_hourly": 3,
            "gold_integrated": 3,
        }

        for layer, expected in expected_counts.items():
            add(results, result(
                "S7-DEMO-COUNT",
                layer,
                counts[layer] == expected,
                counts[layer],
                f"Expected demo count: {expected}"
            ))

    return results, counts


def write_report(results, counts):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc)
    filename = (
        "streaming_dq_"
        + timestamp.strftime("%Y%m%dT%H%M%SZ")
        + ".json"
    )

    summary = {
        status: sum(
            r["status"] == status for r in results
        )
        for status in ["PASS", "FAIL", "WARN"]
    }

    report = {
        "generated_at_utc": timestamp.isoformat(),
        "pipeline": "historical_replay_streaming",
        "record_counts": counts,
        "summary": summary,
        "overall_status": (
            "FAIL" if summary["FAIL"] else "PASS"
        ),
        "checks": results,
    }

    path = REPORT_DIR / filename

    path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )

    print("\n=== S7 QUALITY REPORT ===")
    print("REPORT:", path)
    print("SUMMARY:", summary)
    print("OVERALL:", report["overall_status"])

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--demo-baseline",
        action="store_true"
    )
    args = parser.parse_args()

    spark = create_spark_session(
        "S7-Streaming-Data-Quality"
    )

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        results, counts = validate(
            spark,
            demo_baseline=args.demo_baseline
        )

        report = write_report(results, counts)

        if report["overall_status"] == "FAIL":
            raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
