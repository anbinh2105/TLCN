
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session

SOURCE_TABLE = "lakehouse.silver.rainfall"
TARGET_TABLE = "lakehouse.gold.rainfall_hourly"


def main():
    spark = create_spark_session(
        app_name="GoldRainfallHourly",
        master="local[2]",
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        print("=" * 70)
        print("STEP 8.2 - RAINFALL HOURLY")
        print("=" * 70)

        # -------------------------------------------------
        # 1. Read and validate Silver
        # -------------------------------------------------
        print("\n[1] Validate Silver rainfall")

        silver = spark.table(SOURCE_TABLE)

        required_columns = {
            "timestamp",
            "rainfall_mm",
            "quality_flag",
            "latitude",
            "longitude",
            "source",
            "source_file",
        }

        missing_columns = required_columns - set(silver.columns)

        if missing_columns:
            raise ValueError(
                f"Missing Silver columns: {missing_columns}"
            )

        silver_count = silver.count()
        print("Silver records:", silver_count)

        if silver_count == 0:
            raise ValueError("Silver rainfall is empty")

        null_timestamps = silver.filter(
            F.col("timestamp").isNull()
        ).count()

        if null_timestamps != 0:
            raise ValueError(
                f"Null timestamps: {null_timestamps}"
            )

        silver.groupBy("quality_flag").count().show()

        print("[PASS] Silver source validation")

        # -------------------------------------------------
        # 2. Build Gold hourly
        # -------------------------------------------------
        print("\n[2] Build Rainfall Hourly")

        hourly = silver.select(
            F.date_trunc(
                "hour", F.col("timestamp")
            ).alias("hour_start"),

            F.col("rainfall_mm").cast("double"),

            F.col("quality_flag").alias(
                "rainfall_quality_flag"
            ),

            (
                F.col("rainfall_mm").isNull()
                | F.col("quality_flag").eqNullSafe("MISSING")
            ).alias("rainfall_is_missing"),

            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("source"),
            F.col("source_file"),

            F.current_timestamp().alias(
                "gold_processed_at"
            ),
        )

        # -------------------------------------------------
        # 3. Validate transformation
        # -------------------------------------------------
        print("\n[3] Validate Gold transformation")

        gold_count = hourly.count()

        if gold_count != silver_count:
            raise ValueError(
                f"Row count mismatch: "
                f"Silver={silver_count}, Gold={gold_count}"
            )

        # Rainfall hourly must be aligned to exact hour.
        off_hour = silver.filter(
            F.col("timestamp") != F.date_trunc(
                "hour", F.col("timestamp")
            )
        ).count()

        if off_hour != 0:
            raise ValueError(
                f"{off_hour} observations are not hourly aligned"
            )

        # No duplicate hourly keys.
        duplicates = (
            hourly.groupBy("hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicates != 0:
            raise ValueError(
                f"Duplicate hourly timestamps: {duplicates}"
            )

        # Preserve missing values.
        silver_missing = silver.filter(
            F.col("quality_flag").eqNullSafe("MISSING")
        ).count()

        gold_missing = hourly.filter(
            F.col("rainfall_quality_flag").eqNullSafe("MISSING")
        ).count()

        if silver_missing != gold_missing:
            raise ValueError(
                "Missing quality flags were not preserved"
            )

        print("Gold hourly records:", gold_count)
        print("MISSING quality flags:", gold_missing)

        hourly.groupBy(
            "rainfall_quality_flag"
        ).count().show()

        print("[PASS] Gold transformation validation")

        # -------------------------------------------------
        # 4. Safety guard: no overwrite of existing table
        # -------------------------------------------------
        print("\n[4] Check target Iceberg table")

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
                "33554432",
            )
            .partitionedBy(F.months("hour_start"))
            .create()
        )

        print("[PASS] Empty Iceberg table created")

        # -------------------------------------------------
        # 6. Append year by year
        # -------------------------------------------------
        print("\n[6] Write rainfall by year")

        years = [
            row["year"]
            for row in (
                hourly.select(
                    F.year("hour_start").alias("year")
                )
                .distinct()
                .orderBy("year")
                .collect()
            )
        ]

        for year in years:
            print(f"\nWriting year {year}...")

            # Recheck persisted state before each append.
            existing_count = (
                spark.table(TARGET_TABLE)
                .filter(F.year("hour_start") == year)
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
                .filter(F.year("hour_start") == year)
                .count()
            )

            if actual != expected:
                raise RuntimeError(
                    f"Year {year}: "
                    f"expected={expected}, actual={actual}"
                )

            print(
                f"[PASS] Year {year}: {actual} records"
            )

        # -------------------------------------------------
        # 7. Verify persisted Iceberg data
        # -------------------------------------------------
        print("\n[7] Verify persisted Gold table")

        persisted = spark.table(TARGET_TABLE)
        persisted_count = persisted.count()

        print("Silver rows:", silver_count)
        print("Persisted Gold rows:", persisted_count)

        if persisted_count != silver_count:
            raise RuntimeError(
                "Persisted row count mismatch"
            )

        duplicate_count = (
            persisted.groupBy("hour_start")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count != 0:
            raise RuntimeError(
                f"Persisted duplicates: {duplicate_count}"
            )

        persisted_missing = persisted.filter(
            F.col("rainfall_quality_flag").eqNullSafe(
                "MISSING"
            )
        ).count()

        if persisted_missing != silver_missing:
            raise RuntimeError(
                "Persisted MISSING flag count mismatch"
            )

        spark.sql(
            f"SELECT snapshot_id, operation, committed_at "
            f"FROM {TARGET_TABLE}.snapshots "
            f"ORDER BY committed_at"
        ).show(30, truncate=False)

        print("\n" + "=" * 70)
        print("GOLD RAINFALL HOURLY: PASS")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
