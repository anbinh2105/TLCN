
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


REPORT_PATH = Path(
    "reports/gold/quality/gold_missing_analysis.json"
)

TABLES = {
    "water_level": {
        "table": "lakehouse.gold.water_level_hourly",
        "keys": ["station_code"],
        "measurements": [
            "water_level_mean",
            "water_level_min",
            "water_level_max",
            "water_level_last",
        ],
    },
    "rainfall": {
        "table": "lakehouse.gold.rainfall_hourly",
        "keys": [],
        "measurements": ["rainfall_mm"],
        "quality": "rainfall_quality_flag",
    },
    "weather": {
        "table": "lakehouse.gold.weather_hourly",
        "keys": [],
        "measurements": [
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
        ],
        "quality": "weather_quality_flag",
    },
    "tide": {
        "table": "lakehouse.gold.tide_hourly",
        "keys": [],
        "measurements": ["tide_height_m"],
        "quality": "tide_quality_flag",
    },
    "discharge": {
        "table": "lakehouse.gold.discharge_hourly",
        "keys": ["station"],
        "measurements": ["discharge_m3s"],
        "quality": "discharge_quality_flag",
    },
}


def collect_null_statistics(df, columns):
    """
    Count nulls for all measurement columns
    using a single Spark aggregation.
    """
    valid = [c for c in columns if c in df.columns]
    missing_columns = [
        c for c in columns if c not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing measurement columns: "
            f"{missing_columns}"
        )

    if not valid:
        return {}

    expressions = [
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in valid
    ]

    result = df.agg(*expressions).first()
    total = df.count()

    return {
        c: {
            "null_count": int(result[c] or 0),
            "null_pct": round(
                100 * int(result[c] or 0) / total, 4
            ) if total else 0,
        }
        for c in valid
    }


def analyze_gaps(df, keys):
    """
    Detect missing hourly timestamps.

    The expected range is from the first to
    the last observed hour for each station.

    No rows are generated for missing hours.
    """
    columns = keys + ["hour_start"]

    timestamps = (
        df.select(*columns)
        .filter(F.col("hour_start").isNotNull())
        .dropDuplicates(columns)
    )

    order_window = (
        Window.partitionBy(*keys).orderBy("hour_start")
        if keys
        else Window.orderBy("hour_start")
    )

    with_previous = timestamps.withColumn(
        "previous_hour",
        F.lag("hour_start").over(order_window)
    )

    # Timestamps are aligned to exact hours.
    with_gaps = with_previous.withColumn(
        "missing_between",
        F.when(
            F.col("previous_hour").isNull(), F.lit(0)
        ).otherwise(
            F.greatest(
                (
                    (
                        F.col("hour_start").cast("long")
                        - F.col("previous_hour").cast("long")
                    ) / 3600
                ).cast("long") - 1,
                F.lit(0)
            )
        )
    )

    gap_rows = (
        with_gaps
        .filter(F.col("missing_between") > 0)
        .select(
            *keys,
            F.expr(
                "previous_hour + INTERVAL 1 HOUR"
            ).alias("missing_start"),
            F.expr(
                "hour_start - INTERVAL 1 HOUR"
            ).alias("missing_end"),
            "missing_between",
        )
    )

    summary = with_gaps.agg(
        F.count("*").alias("observed_hours"),
        F.coalesce(
            F.sum("missing_between"), F.lit(0)
        ).alias("missing_hours"),
        F.sum(
            F.when(
                F.col("missing_between") > 0, 1
            ).otherwise(0)
        ).alias("gap_count"),
        F.coalesce(
            F.max("missing_between"), F.lit(0)
        ).alias("longest_streak_hours"),
    ).first()

    longest = (
        gap_rows
        .orderBy(
            F.desc("missing_between"),
            F.asc("missing_start")
        )
        .limit(5)
        .collect()
    )

    return {
        "observed_hours": int(
            summary["observed_hours"] or 0
        ),
        "missing_hours": int(
            summary["missing_hours"] or 0
        ),
        "gap_count": int(
            summary["gap_count"] or 0
        ),
        "longest_streak_hours": int(
            summary["longest_streak_hours"] or 0
        ),
        "top_5_gaps": [
            {
                **{
                    key: str(row[key])
                    for key in keys
                },
                "start": str(row["missing_start"]),
                "end": str(row["missing_end"]),
                "missing_hours": int(
                    row["missing_between"]
                ),
            }
            for row in longest
        ],
    }


def analyze_yearly_coverage(df, keys):
    """
    Count available hourly keys by calendar year.

    Coverage percentage uses full calendar years,
    so first/last partial years may have low coverage.
    """
    unique_hours = (
        df.select(*(keys + ["hour_start"]))
        .filter(F.col("hour_start").isNotNull())
        .dropDuplicates(keys + ["hour_start"])
    )

    yearly = (
        unique_hours
        .withColumn(
            "year", F.year("hour_start")
        )
        .groupBy("year")
        .agg(
            F.count("*").alias("available_hour_keys"),
            F.countDistinct(
                *keys
            ).alias("station_count")
            if keys
            else F.lit(1).alias("station_count")
        )
        .orderBy("year")
        .collect()
    )

    results = []

    for row in yearly:
        year = int(row["year"])
        stations = int(row["station_count"])
        available = int(row["available_hour_keys"])

        start = datetime(year, 1, 1)
        end = datetime(year + 1, 1, 1)
        hours_in_year = int(
            (end - start).total_seconds() / 3600
        )

        expected = hours_in_year * stations

        results.append({
            "year": year,
            "available_hour_keys": available,
            "station_count": stations,
            "calendar_expected_keys": expected,
            "calendar_coverage_pct": round(
                100 * available / expected, 2
            ) if expected else 0,
        })

    return results


def analyze_table(spark, name, config):
    table_name = config["table"]

    print("\n" + "=" * 70)
    print(f"ANALYZING: {name.upper()}")
    print(table_name)
    print("=" * 70)

    df = spark.table(table_name)

    if "hour_start" not in df.columns:
        raise ValueError(
            f"{table_name} has no hour_start"
        )

    keys = config["keys"]

    for key in keys:
        if key not in df.columns:
            raise ValueError(
                f"Missing station key: {key}"
            )

    total_rows = df.count()

    bounds = df.agg(
        F.min("hour_start").alias("min_ts"),
        F.max("hour_start").alias("max_ts")
    ).first()

    print("Rows:", total_rows)
    print("First hour:", bounds["min_ts"])
    print("Last hour:", bounds["max_ts"])

    null_stats = collect_null_statistics(
        df, config["measurements"]
    )

    print("\nNull statistics:")
    for column, stats in null_stats.items():
        print(
            f"  {column}: "
            f"{stats['null_count']} null "
            f"({stats['null_pct']}%)"
        )

    print("\nAnalyzing missing timestamps...")
    gaps = analyze_gaps(df, keys)

    print("Missing hours:", gaps["missing_hours"])
    print("Gap intervals:", gaps["gap_count"])
    print(
        "Longest missing streak:",
        gaps["longest_streak_hours"],
        "hours"
    )

    print("\nAnalyzing yearly coverage...")
    yearly = analyze_yearly_coverage(df, keys)

    for item in yearly:
        print(
            item["year"],
            "available:", item["available_hour_keys"],
            "coverage:",
            f"{item['calendar_coverage_pct']}%"
        )

    quality_stats = {}

    quality_col = config.get("quality")
    if quality_col:
        if quality_col not in df.columns:
            raise ValueError(
                f"Missing quality column: {quality_col}"
            )

        quality_stats = {
            str(row[quality_col]): int(row["count"])
            for row in (
                df.groupBy(quality_col)
                .count()
                .collect()
            )
        }

    return {
        "table": table_name,
        "row_count": total_rows,
        "first_hour": str(bounds["min_ts"]),
        "last_hour": str(bounds["max_ts"]),
        "null_statistics": null_stats,
        "timestamp_gaps": gaps,
        "yearly_coverage": yearly,
        "quality_distribution": quality_stats,
    }


def main():
    spark = create_spark_session(
        app_name="GoldMissingAnalysis",
        master="local[2]",
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )
        spark.conf.set(
            "spark.sql.adaptive.enabled", "false"
        )

        results = {}

        for name, config in TABLES.items():
            results[name] = analyze_table(
                spark, name, config
            )

        report = {
            "analysis": "gold_missing_data",
            "generated_at": datetime.now().isoformat(),
            "timezone": "Asia/Ho_Chi_Minh",
            "scope": (
                "Between first and last observed hour "
                "per station; yearly coverage uses "
                "full calendar years."
            ),
            "tables": results,
        }

        REPORT_PATH.parent.mkdir(
            parents=True, exist_ok=True
        )

        with REPORT_PATH.open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2
            )

        print("\n" + "=" * 70)
        print("GOLD MISSING DATA ANALYSIS: COMPLETED")
        print("Report:", REPORT_PATH)
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
