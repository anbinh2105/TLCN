
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.tide"
TARGET_TABLE = "lakehouse.gold.tide_hourly"

EXPECTED_MISSING_HOURS = [
    "2021-11-29 01:00:00",
    "2021-11-29 02:00:00",
]


def main():
    spark = create_spark_session(
        app_name="GoldTideHourly",
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
        print("STEP 8.4 - TIDE HOURLY")
        print("=" * 70)

        # -------------------------------------------------
        # 1. Read and validate Silver
        # -------------------------------------------------
        print("\n[1] Validate Silver tide")

        silver = spark.table(SOURCE_TABLE)

        required_columns = {
            "timestamp",
            "tide_height_m",
            "quality_flag",
            "recovery_method",
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
            raise ValueError("Silver tide table is empty")

        null_timestamps = silver.filter(
            F.col("timestamp").isNull()
        ).count()

        if null_timestamps:
            raise ValueError(
                f"Null timestamps: {null_timestamps}"
            )

        silver.groupBy("quality_flag").count().show()

        print("[PASS] Silver source validation")

        # -------------------------------------------------
        # 2. Build Tide Hourly
        # -------------------------------------------------
        print("\n[2] Build Tide Hourly")

        hourly = silver.select(
            F.date_trunc(
                "hour", F.col("timestamp")
            ).alias("hour_start"),

            F.col("tide_height_m").cast("double"),

            F.col("quality_flag").alias(
                "tide_quality_flag"
            ),

            F.col("quality_flag")
            .eqNullSafe("RECOVERED")
            .alias("tide_is_recovered"),

            F.col("recovery_method"),

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
                f"Silver={silver_count}, Gold={gold_count}"
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

        silver_recovered = silver.filter(
            F.col("quality_flag").eqNullSafe("RECOVERED")
        ).count()

        gold_recovered = hourly.filter(
            F.col("tide_is_recovered")
        ).count()

        if silver_recovered != gold_recovered:
            raise ValueError(
                "RECOVERED records were not preserved"
            )

        changed_values = (
            silver.alias("s")
            .join(
                hourly.alias("g"),
                F.col("s.timestamp") == F.col("g.hour_start"),
                "inner",
            )
            .filter(
                ~F.col("s.tide_height_m").eqNullSafe(
                    F.col("g.tide_height_m")
                )
                | ~F.col("s.recovery_method").eqNullSafe(
                    F.col("g.recovery_method")
                )
            )
            .count()
        )

        if changed_values:
            raise ValueError(
                f"Changed tide values or recovery "
                f"methods: {changed_values}"
            )

        print("Gold hourly records:", gold_count)
        print("RECOVERED records:", gold_recovered)
        print("Changed values:", changed_values)

        hourly.groupBy(
            "tide_quality_flag",
            "recovery_method",
        ).count().show(truncate=False)

        # -------------------------------------------------
        # 4. Verify known missing hours
        # -------------------------------------------------
        print("\n[4] Verify known missing hours")

        for timestamp_str in EXPECTED_MISSING_HOURS:
            existing = hourly.filter(
                F.col("hour_start")
                == F.to_timestamp(F.lit(timestamp_str))
            ).count()

            if existing != 0:
                raise ValueError(
                    f"Expected missing hour unexpectedly "
                    f"exists: {timestamp_str}"
                )

            print(
                f"[PASS] Missing hour preserved: "
                f"{timestamp_str}"
            )

        print("[PASS] Gold transformation validation")

        # -------------------------------------------------
        # 5. Check target Iceberg table
        # -------------------------------------------------
        print("\n[5] Check target Iceberg table")

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        if spark.catalog.tableExists(TARGET_TABLE):
            raise RuntimeError(
                f"{TARGET_TABLE} already exists. "
                "Stop to prevent duplicate writes. "
                "Inspect the existing table first."
            )

        # -------------------------------------------------
        # 6. Create empty Iceberg table
        # -------------------------------------------------
        print("\n[6] Create Gold Iceberg table")

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
        # 7. Write by year
        # -------------------------------------------------
        print("\n[7] Write Tide by year")

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
        # 8. Verify persisted table
        # -------------------------------------------------
        print("\n[8] Verify persisted Iceberg table")

        persisted = spark.table(TARGET_TABLE)

        persisted_count = persisted.count()

        print("Silver rows:", silver_count)
        print("Persisted Gold rows:", persisted_count)

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

        persisted_recovered = persisted.filter(
            F.col("tide_is_recovered")
        ).count()

        if persisted_recovered != silver_recovered:
            raise RuntimeError(
                "Persisted RECOVERED count mismatch"
            )

        spark.sql(
            f"SELECT snapshot_id, operation, committed_at "
            f"FROM {TARGET_TABLE}.snapshots "
            f"ORDER BY committed_at"
        ).show(30, truncate=False)

        print("\n" + "=" * 70)
        print("GOLD TIDE HOURLY: PASS")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
