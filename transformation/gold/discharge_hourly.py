
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.river_discharge"
TARGET_TABLE = "lakehouse.gold.discharge_hourly"


def main():
    spark = create_spark_session(
        app_name="GoldDischargeHourly",
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
        print("STEP 8.5 - RIVER DISCHARGE HOURLY")
        print("=" * 70)

        # -------------------------------------------------
        # 1. Read and validate Silver
        # -------------------------------------------------
        print("\n[1] Validate Silver river discharge")

        silver = spark.table(SOURCE_TABLE)

        required_columns = {
            "timestamp",
            "station",
            "discharge_m3s",
            "quality_flag",
            "source",
            "source_file",
        }

        missing_columns = required_columns - set(
            silver.columns
        )

        if missing_columns:
            raise ValueError(
                f"Missing Silver columns: {missing_columns}"
            )

        silver_count = silver.count()

        print("Silver daily records:", silver_count)

        if silver_count == 0:
            raise ValueError("Silver source is empty")

        if silver.filter(
            F.col("timestamp").isNull()
        ).count():
            raise ValueError("Null source timestamps")

        # Each station can have at most one daily record.
        duplicate_daily = (
            silver
            .groupBy(
                "station",
                F.to_date("timestamp")
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_daily:
            raise ValueError(
                f"Duplicate station/day keys: "
                f"{duplicate_daily}"
            )

        # Daily timestamps should be midnight.
        off_midnight = silver.filter(
            F.col("timestamp") != F.date_trunc(
                "day", F.col("timestamp")
            )
        ).count()

        if off_midnight:
            raise ValueError(
                f"Off-midnight source timestamps: "
                f"{off_midnight}"
            )

        silver.groupBy("quality_flag").count().show()

        print("[PASS] Silver source validation")

        # -------------------------------------------------
        # 2. Expand daily to hourly
        # -------------------------------------------------
        print("\n[2] Expand daily data to 24 hours")

        daily = silver.withColumn(
            "source_date",
            F.to_date("timestamp")
        )

        hourly = (
            daily
            .withColumn(
                "hour_start",
                F.explode(
                    F.sequence(
                        F.col("timestamp"),
                        F.expr(
                            "timestamp + INTERVAL 23 HOURS"
                        ),
                        F.expr("INTERVAL 1 HOUR")
                    )
                )
            )
            .select(
                "hour_start",
                "station",
                F.col("discharge_m3s").cast("double"),
                F.col("quality_flag").alias(
                    "discharge_quality_flag"
                ),
                F.lit("DAILY_EXPANDED").alias(
                    "alignment_method"
                ),
                "source_date",
                F.lit("DAILY").alias(
                    "source_temporal_grain"
                ),
                F.lit(False).alias(
                    "is_hourly_observation"
                ),
                "source",
                "source_file",
                F.current_timestamp().alias(
                    "gold_processed_at"
                )
            )
        )

        # -------------------------------------------------
        # 3. Validate transformation
        # -------------------------------------------------
        print("\n[3] Validate hourly expansion")

        expected_count = silver_count * 24
        actual_count = hourly.count()

        print("Expected hourly rows:", expected_count)
        print("Actual hourly rows:", actual_count)

        if actual_count != expected_count:
            raise ValueError(
                f"Expansion mismatch: "
                f"expected={expected_count}, "
                f"actual={actual_count}"
            )

        duplicates = (
            hourly
            .groupBy("station", "hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicates:
            raise ValueError(
                f"Duplicate hourly keys: {duplicates}"
            )

        invalid_alignment = hourly.filter(
            (F.col("alignment_method") != "DAILY_EXPANDED")
            | F.col("is_hourly_observation")
            | (F.col("source_temporal_grain") != "DAILY")
        ).count()

        if invalid_alignment:
            raise ValueError(
                "Invalid alignment metadata"
            )

        # Verify each station/day expands into 24 rows.
        incorrect_days = (
            hourly
            .groupBy("station", "source_date")
            .count()
            .filter(F.col("count") != 24)
            .count()
        )

        if incorrect_days:
            raise ValueError(
                f"Days with != 24 hourly rows: "
                f"{incorrect_days}"
            )

        # Check that hourly values equal daily source values.
        changed_values = (
            hourly.alias("h")
            .join(
                daily.alias("d"),
                (F.col("h.station") == F.col("d.station"))
                & (
                    F.col("h.source_date")
                    == F.col("d.source_date")
                ),
                "inner"
            )
            .filter(
                ~F.col("h.discharge_m3s").eqNullSafe(
                    F.col("d.discharge_m3s")
                )
                | ~F.col("h.discharge_quality_flag")
                .eqNullSafe(F.col("d.quality_flag"))
            )
            .count()
        )

        if changed_values:
            raise ValueError(
                f"Changed source values: {changed_values}"
            )

        print("Changed values:", changed_values)
        print("[PASS] Hourly expansion validation")

        # -------------------------------------------------
        # 4. Check or create Iceberg table
        # -------------------------------------------------
        print("\n[4] Prepare Gold Iceberg table")

        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
        )

        if not spark.catalog.tableExists(TARGET_TABLE):
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
        else:
            print(
                "[INFO] Gold table exists; "
                "checking persisted years"
            )

        # -------------------------------------------------
        # 5. Write year by year
        # -------------------------------------------------
        print("\n[5] Write River Discharge by year")

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
            year_data = hourly.filter(
                F.year("hour_start") == year
            )

            expected = year_data.count()

            existing = (
                spark.table(TARGET_TABLE)
                .filter(F.year("hour_start") == year)
                .count()
            )

            if existing > 0:
                if existing == expected:
                    print(
                        f"[SKIP] Year {year}: "
                        f"{existing} records already stored"
                    )
                    continue

                raise RuntimeError(
                    f"Year {year}: partial or inconsistent "
                    f"write detected. "
                    f"Expected={expected}, "
                    f"existing={existing}. "
                    "Stop for manual inspection."
                )

            print(
                f"\nWriting year {year}: "
                f"{expected} records"
            )

            year_data.writeTo(
                TARGET_TABLE
            ).append()

            persisted_year = (
                spark.table(TARGET_TABLE)
                .filter(F.year("hour_start") == year)
                .count()
            )

            if persisted_year != expected:
                raise RuntimeError(
                    f"Year {year}: expected={expected}, "
                    f"persisted={persisted_year}"
                )

            print(
                f"[PASS] Year {year}: "
                f"{persisted_year} records"
            )

        # -------------------------------------------------
        # 6. Verify persisted Iceberg table
        # -------------------------------------------------
        print("\n[6] Verify persisted Gold table")

        persisted = spark.table(TARGET_TABLE)

        persisted_count = persisted.count()

        print("Expected:", expected_count)
        print("Persisted:", persisted_count)

        if persisted_count != expected_count:
            raise RuntimeError(
                "Persisted row count mismatch"
            )

        persisted_duplicates = (
            persisted
            .groupBy("station", "hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if persisted_duplicates:
            raise RuntimeError(
                f"Persisted duplicate hourly keys: "
                f"{persisted_duplicates}"
            )

        spark.sql(
            f"SELECT snapshot_id, operation, committed_at "
            f"FROM {TARGET_TABLE}.snapshots "
            f"ORDER BY committed_at"
        ).show(30, truncate=False)

        print("\n" + "=" * 70)
        print("GOLD RIVER DISCHARGE HOURLY: PASS")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
