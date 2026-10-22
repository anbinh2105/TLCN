
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)


START = "2018-04-25 00:00:00"
END = "2026-09-18 23:00:00"

OUTPUT = Path(
    "reports/gold/quality/gold_coverage_revalidation.json"
)

SOURCES = {
    "weather": {
        "table": "lakehouse.gold.weather_hourly",
        "timestamp": "hour_start",
        "measurements": [
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
        ],
        "quality_candidates": ["weather_quality_flag"],
    },
    "rainfall": {
        "table": "lakehouse.gold.rainfall_hourly",
        "timestamp": "hour_start",
        "measurements": ["rainfall_mm"],
        "quality_candidates": ["rainfall_quality_flag"],
    },
    "river_discharge": {
        "table": "lakehouse.gold.discharge_hourly",
        "timestamp": "hour_start",
        "measurements": ["discharge_m3s"],
        "quality_candidates": ["discharge_quality_flag"],
    },
    "water_level": {
        "table": "lakehouse.gold.water_level_hourly",
        "timestamp": "hour_start",
        "measurements": [
            "water_level_mean",
            "water_level_min",
            "water_level_max",
            "water_level_last",
        ],
        "quality_candidates": [
            "water_level_quality_flag",
            "quality_flag",
        ],
    },
    "tide": {
        "table": "lakehouse.gold.tide_hourly",
        "timestamp": "hour_start",
        "measurements": ["tide_height_m"],
        "quality_candidates": [
            "tide_quality_flag",
            "quality_flag",
        ],
    },
}


def to_json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat(sep=" ")
    if isinstance(obj, dict):
        return {
            k: to_json_safe(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [to_json_safe(v) for v in obj]
    return obj


def make_timeline(spark):
    return spark.sql(
        f"""
        SELECT explode(
            sequence(
                timestamp('{START}'),
                timestamp('{END}'),
                interval 1 hour
            )
        ) AS hour_start
        """
    )


def find_gaps(timeline, actual_hours):
    missing = (
        timeline.join(
            actual_hours,
            "hour_start",
            "left_anti",
        )
    )

    w = Window.orderBy("hour_start")

    grouped = (
        missing
        .withColumn(
            "rn",
            F.row_number().over(w),
        )
        .withColumn(
            "grp",
            F.unix_timestamp("hour_start")
            - F.col("rn") * F.lit(3600),
        )
    )

    gaps = (
        grouped
        .groupBy("grp")
        .agg(
            F.min("hour_start").alias("start"),
            F.max("hour_start").alias("end"),
            F.count("*").alias("hours"),
        )
        .orderBy(F.desc("hours"), "start")
    )

    return gaps


def validate_source(spark, timeline, name, config):
    table = config["table"]
    timestamp_col = config["timestamp"]

    print("\n" + "=" * 70)
    print(f"SOURCE: {name}")
    print(f"TABLE : {table}")
    print("=" * 70)

    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Missing Gold table: {table}")

    df = spark.table(table)

    if timestamp_col not in df.columns:
        raise RuntimeError(
            f"{name}: missing timestamp column "
            f"{timestamp_col}; available={df.columns}"
        )

    total = df.count()

    null_timestamp = df.filter(
        F.col(timestamp_col).isNull()
    ).count()

    if null_timestamp:
        raise RuntimeError(
            f"{name}: {null_timestamp} null timestamps"
        )

    off_hour = df.filter(
        F.col(timestamp_col)
        != F.date_trunc("hour", timestamp_col)
    ).count()

    if off_hour:
        raise RuntimeError(
            f"{name}: {off_hour} off-hour timestamps"
        )

    # Coverage is calculated by hour, independently
    # of whether a table has one or more stations.
    hours = (
        df.select(
            F.col(timestamp_col).alias("hour_start")
        )
        .distinct()
    )

    unique_hours = hours.count()

    # Diagnostic: multiple rows at one hour may be
    # legitimate for a multi-station dataset.
    multirow_hours = (
        df.groupBy(timestamp_col)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    bounds = hours.agg(
        F.min("hour_start").alias("min_ts"),
        F.max("hour_start").alias("max_ts"),
    ).first()

    global_hours = timeline.count()

    # Hours outside canonical timeline are reported,
    # not silently discarded.
    outside_count = (
        hours.join(
            timeline,
            "hour_start",
            "left_anti",
        )
        .count()
    )

    present = (
        timeline.join(
            hours,
            "hour_start",
            "left_semi",
        )
    )

    present_count = present.count()
    missing_global = global_hours - present_count

    coverage_global = round(
        100 * present_count / global_hours, 4
    )

    # Source-span coverage: gaps between the first
    # and last timestamp actually available.
    span_timeline = timeline.filter(
        (F.col("hour_start") >= F.lit(bounds["min_ts"]))
        & (F.col("hour_start") <= F.lit(bounds["max_ts"]))
    )

    span_expected = span_timeline.count()

    span_present = (
        span_timeline.join(
            hours, "hour_start", "left_semi"
        ).count()
    )

    span_missing = span_expected - span_present

    span_coverage = (
        round(100 * span_present / span_expected, 4)
        if span_expected else None
    )

    gaps = find_gaps(timeline, hours)

    gap_count = gaps.count()

    largest_gaps = [
        {
            "start": row["start"],
            "end": row["end"],
            "hours": row["hours"],
        }
        for row in gaps.limit(10).collect()
    ]

    # Yearly coverage on the common timeline.
    yearly_expected = (
        timeline
        .withColumn("year", F.year("hour_start"))
        .groupBy("year")
        .agg(F.count("*").alias("expected"))
    )

    yearly_actual = (
        present
        .withColumn("year", F.year("hour_start"))
        .groupBy("year")
        .agg(F.count("*").alias("present"))
    )

    yearly = (
        yearly_expected
        .join(yearly_actual, "year", "left")
        .fillna({"present": 0})
        .withColumn(
            "missing",
            F.col("expected") - F.col("present"),
        )
        .withColumn(
            "coverage_pct",
            F.round(
                F.col("present") * 100
                / F.col("expected"),
                2,
            ),
        )
        .orderBy("year")
    )

    yearly_rows = [
        row.asDict()
        for row in yearly.collect()
    ]

    # Measurement completeness.
    requested = config["measurements"]
    available = [
        c for c in requested if c in df.columns
    ]
    unavailable = [
        c for c in requested if c not in df.columns
    ]

    null_measurements = {
        col: df.filter(
            F.col(col).isNull()
        ).count()
        for col in available
    }

    quality_columns = [
        c for c in config["quality_candidates"]
        if c in df.columns
    ]

    quality_summary = {}

    for col in quality_columns:
        quality_summary[col] = {
            str(row[col]): row["count"]
            for row in (
                df.groupBy(col)
                .count()
                .collect()
            )
        }

    print("Total rows          :", total)
    print("Unique hours        :", unique_hours)
    print("Multi-row hours     :", multirow_hours)
    print("Outside timeline    :", outside_count)
    print("First timestamp     :", bounds["min_ts"])
    print("Last timestamp      :", bounds["max_ts"])
    print("Global expected     :", global_hours)
    print("Global present      :", present_count)
    print("Global missing      :", missing_global)
    print("Global coverage (%) :", coverage_global)
    print("Source-span missing :", span_missing)
    print("Source-span cov (%) :", span_coverage)
    print("Missing gap groups  :", gap_count)
    print("Null measurements   :", null_measurements)
    print("Unavailable columns :", unavailable)
    print("Quality flags       :", quality_summary)

    print("\nYearly coverage:")
    yearly.show(20, truncate=False)

    print("\nLargest missing gaps:")
    gaps.drop("grp").show(10, truncate=False)

    if not available:
        measurement_status = "NOT_EVALUATED"
    elif unavailable:
        measurement_status = "PARTIAL"
    else:
        measurement_status = "EVALUATED"

    if not quality_columns:
        quality_status = "NOT_EVALUATED"
    else:
        quality_status = "EVALUATED"

    return {
        "table": table,
        "total_rows": total,
        "unique_hours": unique_hours,
        "multirow_hours": multirow_hours,
        "outside_timeline_hours": outside_count,
        "first_timestamp": bounds["min_ts"],
        "last_timestamp": bounds["max_ts"],
        "global_expected_hours": global_hours,
        "global_present_hours": present_count,
        "global_missing_hours": missing_global,
        "global_coverage_pct": coverage_global,
        "source_span_expected_hours": span_expected,
        "source_span_present_hours": span_present,
        "source_span_missing_hours": span_missing,
        "source_span_coverage_pct": span_coverage,
        "missing_gap_groups": gap_count,
        "largest_missing_gaps": largest_gaps,
        "yearly": yearly_rows,
        "null_measurements": null_measurements,
        "unavailable_measurements": unavailable,
        "measurement_status": measurement_status,
        "quality_flags": quality_summary,
        "quality_status": quality_status,
    }


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="gold-coverage-revalidation",
            master="local[2]",
        )

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )

        timeline = make_timeline(spark).cache()

        expected = timeline.count()
        print("Canonical timeline hours:", expected)

        if expected != 73656:
            raise RuntimeError(
                f"Unexpected timeline size: {expected}"
            )

        results = {}

        for name, config in SOURCES.items():
            results[name] = validate_source(
                spark, timeline, name, config
            )

        report = {
            "step": "8.6.3a.7",
            "name": "Gold Coverage Revalidation",
            "generated_at": datetime.now().isoformat(),
            "canonical_start": START,
            "canonical_end": END,
            "canonical_hours": expected,
            "sources": results,
        }

        OUTPUT.parent.mkdir(
            parents=True, exist_ok=True
        )

        with OUTPUT.open(
            "w", encoding="utf-8"
        ) as f:
            json.dump(
                to_json_safe(report),
                f,
                ensure_ascii=False,
                indent=2,
            )

        print("\n" + "=" * 70)
        print("GOLD COVERAGE REVALIDATION: COMPLETED")
        print("Report:", OUTPUT)
        print("=" * 70)

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
