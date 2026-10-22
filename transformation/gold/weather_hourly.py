
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.weather"
TARGET_TABLE = "lakehouse.gold.weather_hourly"

WEATHER_COLUMNS = [
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "wind_speed",
]


def main():
    spark = create_spark_session(
        app_name="GoldWeatherHourly",
        master="local[2]",
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )
        spark.conf.set(
            "spark.sql.adaptive.enabled", "false"
        )

        print("=" * 70)
        print("STEP 8.3 - WEATHER HOURLY")
        print("=" * 70)

        # -------------------------------------------------
        # 1. Validate Silver source
        # -------------------------------------------------
        print("\n[1] Validate Silver weather")

        silver = spark.table(SOURCE_TABLE)

        required_columns = {
            "timestamp",
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
            "quality_flag",
            "latitude",
            "longitude",
            "source",
            "source_file",
        }

        missing_columns = (
            required_columns - set(silver.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing Silver columns: {missing_columns}"
            )

        silver_count = silver.count()

        print("Silver records:", silver_count)

        if silver_count == 0:
            raise ValueError(
                "Silver weather table is empty"
            )

        null_timestamps = silver.filter(
            F.col("timestamp").isNull()
        ).count()

        if null_timestamps:
            raise ValueError(
                f"Null timestamps: {null_timestamps}"
            )

        silver.groupBy(
            "quality_flag"
        ).count().show()

        print("[PASS] Silver source validation")

        # -------------------------------------------------
        # 2. Build Gold hourly
        # -------------------------------------------------
        print("\n[2] Build Weather Hourly")

        missing_measurements = (
            F.col("temperature_c").isNull()
            | F.col("humidity_pct").isNull()
            | F.col("pressure_hpa").isNull()
            | F.col("wind_speed").isNull()
            | F.col("wind_direction").isNull()
        )

        hourly = silver.select(
            F.date_trunc(
                "hour", F.col("timestamp")
            ).alias("hour_start"),

            F.col("temperature_c").cast("double"),
            F.col("humidity_pct").cast("double"),
            F.col("pressure_hpa").cast("double"),
            F.col("wind_speed").cast("double"),
            F.col("wind_direction"),

            F.col("quality_flag").alias(
                "weather_quality_flag"
            ),

            (
                F.col("quality_flag").eqNullSafe("MISSING")
                | missing_measurements
            ).alias("weather_is_missing"),

            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("source"),
            F.col("source_file"),

            F.current_timestamp().alias(
                "gold_processed_at"
            ),
        )

        # -------------------------------------------------
        # 3. Validate Gold transformation
        # -------------------------------------------------
        print("\n[3] Validate Gold transformation")

        gold_count = hourly.count()

        if gold_count != silver_count:
            raise ValueError(
                f"Row count mismatch: "
                f"Silver={silver_count}, "
                f"Gold={gold_count}"
            )

        off_hour_count = silver.filter(
            F.col("timestamp") != F.date_trunc(
                "hour", F.col("timestamp")
            )
        ).count()

        if off_hour_count:
            raise ValueError(
                f"Off-hour timestamps: {off_hour_count}"
            )

        duplicate_count = (
            hourly.groupBy("hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count:
            raise ValueError(
                f"Duplicate hourly keys: {duplicate_count}"
            )

        silver_missing = silver.filter(
            F.col("quality_flag").eqNullSafe("MISSING")
        ).count()

        gold_missing = hourly.filter(
            F.col("weather_quality_flag").eqNullSafe(
                "MISSING"
            )
        ).count()

        if silver_missing != gold_missing:
            raise ValueError(
                "MISSING quality flags were not preserved"
            )

        # Ensure measurements are not modified.
        changed_values = (
            silver.alias("s")
            .join(
                hourly.alias("g"),
                F.col("s.timestamp") == F.col("g.hour_start"),
                "inner",
            )
            .filter(
                ~(
                    F.col("s.temperature_c")
                    .eqNullSafe(F.col("g.temperature_c"))
                    & F.col("s.humidity_pct")
                    .eqNullSafe(F.col("g.humidity_pct"))
                    & F.col("s.pressure_hpa")
                    .eqNullSafe(F.col("g.pressure_hpa"))
                    & F.col("s.wind_speed")
                    .eqNullSafe(F.col("g.wind_speed"))
                    & F.col("s.wind_direction")
                    .eqNullSafe(F.col("g.wind_direction"))
                )
            )
            .count()
        )

        if changed_values:
            raise ValueError(
                f"Changed measurements: {changed_values}"
            )

        print("Gold hourly records:", gold_count)
        print("MISSING quality flags:", gold_missing)
        print("Changed measurements:", changed_values)

        hourly.groupBy(
            "weather_quality_flag"
        ).count().show()

        print("[PASS] Gold transformation validation")

        # -------------------------------------------------
        # 4. Check target table
        # -------------------------------------------------
        print("\n[4] Check target Iceberg table")

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        if spark.catalog.tableExists(TARGET_TABLE):
            raise RuntimeError(
                f"{TARGET_TABLE} already exists. "
                "Stop to avoid duplicate writes. "
                "Inspect the existing table first."
            )

        # -------------------------------------------------
        # 5. Create empty Iceberg table
        # -------------------------------------------------
        print("\n[5] Create Gold Iceberg table")

        (
            hourly.limit(0)
            .writeTo(TARGET_TABLE)
            .using("iceberg")
            .tableProperty(
                "write.distribution-mode", "hash"
            )
            .tableProperty(
                "write.target-file-size-bytes",
                "33554432"
            )
            .partitionedBy(F.months("hour_start"))
            .create()
        )

        print("[PASS] Empty Iceberg table created")

        # -------------------------------------------------
        # 6. Write year by year
        # -------------------------------------------------
        print("\n[6] Write Weather by year")

        years = [
            row["year"]
            for row in (
                hourly
                .select(
                    F.year("hour_start").alias("year")
                )
                .distinct()
                .orderBy("year")
                .collect()
            )
        ]

        for year in years:
            print(f"\nWriting year {year}...")

            existing_count = (
                spark.table(TARGET_TABLE)
                .filter(
                    F.year("hour_start") == year
                )
                .count()
            )

            if existing_count > 0:
                raise RuntimeError(
                    f"Year {year} already has "
                    f"{existing_count} rows. "
                    "Stop to prevent duplicate data."
                )

            year_data = hourly.filter(
                F.year("hour_start") == year
            )

            expected = year_data.count()

            (
                year_data
                .writeTo(TARGET_TABLE)
                .append()
            )

            actual = (
                spark.table(TARGET_TABLE)
                .filter(
                    F.year("hour_start") == year
                )
                .count()
            )

            if actual != expected:
                raise RuntimeError(
                    f"Year {year}: "
                    f"expected={expected}, "
                    f"actual={actual}"
                )

            print(
                f"[PASS] Year {year}: "
                f"{actual} records"
            )

        # -------------------------------------------------
        # 7. Verify persisted Iceberg table
        # -------------------------------------------------
        print("\n[7] Verify persisted Gold table")

        persisted = spark.table(TARGET_TABLE)
        persisted_count = persisted.count()

        print("Silver rows:", silver_count)
        print(
            "Persisted Gold rows:", persisted_count
        )

        if persisted_count != silver_count:
            raise RuntimeError(
                "Persisted row count mismatch"
            )

        persisted_duplicates = (
            persisted.groupBy("hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if persisted_duplicates:
            raise RuntimeError(
                f"Persisted duplicate keys: "
                f"{persisted_duplicates}"
            )

        persisted_missing = persisted.filter(
            F.col("weather_quality_flag").eqNullSafe(
                "MISSING"
            )
        ).count()

        if persisted_missing != silver_missing:
            raise RuntimeError(
                "Persisted MISSING count mismatch"
            )

        spark.sql(
            f"SELECT snapshot_id, operation, committed_at "
            f"FROM {TARGET_TABLE}.snapshots "
            f"ORDER BY committed_at"
        ).show(30, truncate=False)

        print("\n" + "=" * 70)
        print("GOLD WEATHER HOURLY: PASS")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
