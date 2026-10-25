
from __future__ import annotations

from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

from transformation.silver import (
    weather_to_silver,
    rainfall_to_silver,
    river_discharge_to_silver,
    water_level_to_silver,
    tide_to_silver,
)


SOURCES = [
    (
        "weather",
        weather_to_silver.read_weather_bronze,
        weather_to_silver.transform_weather,
    ),
    (
        "rainfall",
        rainfall_to_silver.read_rainfall_bronze,
        rainfall_to_silver.transform_rainfall,
    ),
    (
        "river_discharge",
        river_discharge_to_silver.read_river_discharge_bronze,
        river_discharge_to_silver.transform_river_discharge,
    ),
    (
        "water_level",
        water_level_to_silver.read_water_level_bronze,
        water_level_to_silver.transform_water_level,
    ),
    (
        "tide",
        tide_to_silver.read_bronze_tide,
        tide_to_silver.transform_to_silver,
    ),
]



def check_quality(source, df):
    required_columns = [
        "timestamp",
        "quality_flag",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            f"{source}: missing columns {missing_columns}"
        )

    total = df.count()

    if total == 0:
        raise RuntimeError(
            f"{source}: Silver DataFrame is empty"
        )

    null_timestamps = df.filter(
        F.col("timestamp").isNull()
    ).count()

    quality_counts = {
        row["quality_flag"]: row["count"]
        for row in (
            df.groupBy("quality_flag")
            .count()
            .collect()
        )
    }

    invalid = quality_counts.get("INVALID", 0)
    missing = quality_counts.get("MISSING", 0)
    observed = quality_counts.get("OBSERVED", 0)

    print(
        f"[PROFILE] {source}: "
        f"rows={total}, "
        f"observed={observed}, "
        f"missing={missing}, "
        f"invalid={invalid}, "
        f"null_timestamps={null_timestamps}"
    )

    if source == "tide":
        if invalid > 0 or null_timestamps > 0:
            raise RuntimeError(
                f"{source}: critical quality check failed"
            )

    return total

def main():
    spark = None
    results = {}

    try:
        spark = create_spark_session(
            app_name="silver-batch-dry-run"
        )

        spark.sparkContext.setLogLevel("WARN")

        for source, reader, transformer in SOURCES:
            print()
            print("=" * 70)
            print(f"SILVER DRY-RUN: {source.upper()}")
            print("=" * 70)

            bronze_df = reader(spark)
            silver_df = transformer(bronze_df)

            if source == "tide":
                tide_to_silver.validate_silver(
                    silver_df
                )

            results[source] = check_quality(
                source,
                silver_df,
            )

        print()
        print("=" * 70)
        print("SILVER BATCH DRY-RUN: PASS")
        print("=" * 70)

        for source, count in results.items():
            print(f"{source}: {count} rows")

        print("No Iceberg write operation was executed.")

    finally:
        if spark is not None:
            stop_spark(spark)


if __name__ == "__main__":
    main()
