from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    get_silver_table,
    stop_spark,
)


SOURCE = "water_level"


def main():
    spark = None

    try:
        print("=" * 80)
        print("WATER LEVEL TIMESTAMP INVESTIGATION")
        print("=" * 80)

        spark = create_spark_session(
            app_name="inspect-water-level-timestamp",
            master="local[*]",
        )

        silver_table = get_silver_table(SOURCE)

        print(f"Silver table: {silver_table}")

        df = spark.table(silver_table)

        # ====================================================
        # 1. Find timestamps not aligned to 15-minute grid
        # ====================================================

        misaligned_df = (
            df
            .filter(F.col("timestamp").isNotNull())
            .filter(
                (F.minute("timestamp") % 15 != 0)
                | (F.second("timestamp") != 0)
            )
            .select(
                "timestamp",
                "station_code",
                "water_level_m",
                "observation_type",
                "approval_level",
                "grade",
                "quality_flag",
                "source_file",
            )
            .orderBy("timestamp")
        )

        misaligned_count = misaligned_df.count()

        print()
        print("=" * 80)
        print("1. MISALIGNED TIMESTAMPS")
        print("=" * 80)

        print(
            f"Total misaligned records: "
            f"{misaligned_count}"
        )

        print()
        print("First 50 misaligned records:")

        misaligned_df.show(
            50,
            truncate=False,
        )

        # ====================================================
        # 2. Minute distribution
        # ====================================================

        print()
        print("=" * 80)
        print("2. MINUTE DISTRIBUTION")
        print("=" * 80)

        (
            misaligned_df
            .withColumn(
                "minute",
                F.minute("timestamp"),
            )
            .groupBy("minute")
            .count()
            .orderBy("minute")
            .show(
                100,
                truncate=False,
            )
        )

        # ====================================================
        # 3. Second distribution
        # ====================================================

        print()
        print("=" * 80)
        print("3. SECOND DISTRIBUTION")
        print("=" * 80)

        (
            misaligned_df
            .withColumn(
                "second",
                F.second("timestamp"),
            )
            .groupBy("second")
            .count()
            .orderBy("second")
            .show(
                100,
                truncate=False,
            )
        )

        # ====================================================
        # 4. Distribution by year
        # ====================================================

        print()
        print("=" * 80)
        print("4. MISALIGNED RECORDS BY YEAR")
        print("=" * 80)

        (
            misaligned_df
            .withColumn(
                "year",
                F.year("timestamp"),
            )
            .groupBy("year")
            .count()
            .orderBy("year")
            .show(
                50,
                truncate=False,
            )
        )

        # ====================================================
        # 5. Distribution by year-month
        # ====================================================

        print()
        print("=" * 80)
        print("5. MISALIGNED RECORDS BY YEAR-MONTH")
        print("=" * 80)

        (
            misaligned_df
            .withColumn(
                "year_month",
                F.date_format(
                    "timestamp",
                    "yyyy-MM",
                ),
            )
            .groupBy("year_month")
            .count()
            .orderBy("year_month")
            .show(
                200,
                truncate=False,
            )
        )

        # ====================================================
        # 6. Min/max affected timestamp
        # ====================================================

        print()
        print("=" * 80)
        print("6. AFFECTED TIME RANGE")
        print("=" * 80)

        (
            misaligned_df
            .agg(
                F.min("timestamp").alias(
                    "first_misaligned"
                ),
                F.max("timestamp").alias(
                    "last_misaligned"
                ),
            )
            .show(truncate=False)
        )

        # ====================================================
        # 7. Check interval between consecutive observations
        # ====================================================

        print()
        print("=" * 80)
        print("7. INTERVAL DISTRIBUTION")
        print("=" * 80)

        from pyspark.sql.window import Window

        window_spec = (
            Window
            .partitionBy("station_code")
            .orderBy("timestamp")
        )

        interval_df = (
            df
            .filter(F.col("timestamp").isNotNull())
            .withColumn(
                "previous_timestamp",
                F.lag("timestamp").over(
                    window_spec
                ),
            )
            .withColumn(
                "interval_seconds",
                F.unix_timestamp("timestamp")
                - F.unix_timestamp(
                    "previous_timestamp"
                ),
            )
            .withColumn(
                "interval_minutes",
                F.col("interval_seconds") / 60.0,
            )
        )

        print()
        print("Most common observation intervals:")

        (
            interval_df
            .filter(
                F.col("interval_minutes")
                .isNotNull()
            )
            .groupBy("interval_minutes")
            .count()
            .orderBy(
                F.desc("count")
            )
            .show(
                30,
                truncate=False,
            )
        )

        # ====================================================
        # 8. Context around first misaligned timestamps
        # ====================================================

        print()
        print("=" * 80)
        print("8. CONTEXT AROUND MISALIGNED RECORDS")
        print("=" * 80)

        first_rows = (
            misaligned_df
            .select("timestamp")
            .limit(10)
            .collect()
        )

        for index, row in enumerate(
            first_rows,
            start=1,
        ):
            target = row["timestamp"]

            print()
            print("-" * 80)
            print(
                f"Case {index}: {target}"
            )
            print("-" * 80)

            (
                df
                .filter(
                    F.col("timestamp").between(
                        F.lit(target)
                        - F.expr("INTERVAL 1 HOUR"),
                        F.lit(target)
                        + F.expr("INTERVAL 1 HOUR"),
                    )
                )
                .select(
                    "timestamp",
                    "water_level_m",
                    "observation_type",
                    "approval_level",
                    "grade",
                    "quality_flag",
                )
                .orderBy("timestamp")
                .show(
                    20,
                    truncate=False,
                )
            )

        # ====================================================
        # 9. Summary
        # ====================================================

        total_count = df.count()

        percentage = (
            misaligned_count
            / total_count
            * 100
        )

        print()
        print("=" * 80)
        print("INVESTIGATION SUMMARY")
        print("=" * 80)

        print(
            f"Total records      : {total_count}"
        )

        print(
            f"Misaligned records : {misaligned_count}"
        )

        print(
            f"Percentage         : "
            f"{percentage:.4f}%"
        )

        print("=" * 80)

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()