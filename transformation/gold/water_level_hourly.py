
"""
STEP 8.1 - WATER LEVEL HOURLY TRANSFORMATION

Silver:
    lakehouse.silver.water_level

Gold:
    lakehouse.gold.water_level_hourly

Grain:
    1 hour x station_code

Safety:
    - Silver is READ ONLY.
    - No DROP, UPDATE, DELETE or OVERWRITE.
    - Existing Gold table is not modified.
    - Re-running verifies existing Gold instead of appending.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.water_level"
TARGET_TABLE = "lakehouse.gold.water_level_hourly"
TARGET_NAMESPACE = "lakehouse.gold"

TIMEZONE = "Asia/Ho_Chi_Minh"

EXPECTED_COLUMNS = {
    "timestamp",
    "station_code",
    "water_level_m",
    "quality_flag",
}

ON_GRID_MINUTES = [0, 15, 30, 45]


def validate_source(df):
    print("\n[1] Validate Silver source")

    missing = EXPECTED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    if not isinstance(
        df.schema["timestamp"].dataType,
        TimestampType
    ):
        raise ValueError(
            "Expected Spark TimestampType for timestamp"
        )

    total = df.count()

    print(f"Silver records: {total}")

    if total == 0:
        raise ValueError("Silver table is empty")

    invalid_keys = df.filter(
        F.col("timestamp").isNull()
        | F.col("station_code").isNull()
    ).count()

    if invalid_keys:
        raise ValueError(
            f"Null timestamp/station_code: {invalid_keys}"
        )

    duplicate_keys = (
        df.groupBy("station_code", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys:
        raise ValueError(
            f"Duplicate station/timestamp keys: "
            f"{duplicate_keys}"
        )

    invalid_values = df.filter(
        F.col("water_level_m").isNotNull()
        & (
            F.isnan("water_level_m")
            | (F.abs("water_level_m") == float("inf"))
        )
    ).count()

    if invalid_values:
        raise ValueError(
            f"NaN/Infinity water level values: "
            f"{invalid_values}"
        )

    print("[PASS] Silver source validation")

    return total


def build_hourly(df):
    print("\n[2] Build hourly water level")

    # Explicit timezone is configured in main().
    # date_trunc creates the start of the local hour.
    prepared = (
        df
        .withColumn(
            "hour_start",
            F.date_trunc("hour", F.col("timestamp"))
        )
        .withColumn(
            "is_on_grid",
            (
                F.minute("timestamp").isin(
                    ON_GRID_MINUTES
                )
                & (F.second("timestamp") == 0)
            )
        )
        .withColumn(
            "is_valid",
            F.col("water_level_m").isNotNull()
        )
        .withColumn(
            "is_valid_on_grid",
            F.col("is_on_grid")
            & F.col("is_valid")
        )
    )

    # Obtain the last VALID measurement by event time.
    # Do not use unordered last() after groupBy.
    valid = prepared.filter(F.col("is_valid"))

    last_window = (
        Window
        .partitionBy("station_code", "hour_start")
        .orderBy(F.col("timestamp").desc())
    )

    last_values = (
        valid
        .withColumn(
            "_row_number",
            F.row_number().over(last_window)
        )
        .filter(F.col("_row_number") == 1)
        .select(
            "station_code",
            "hour_start",
            F.col("water_level_m").alias(
                "water_level_last"
            ),
            F.col("timestamp").alias(
                "water_level_last_timestamp"
            )
        )
    )

    hourly = (
        prepared
        .groupBy("station_code", "hour_start")
        .agg(
            F.avg("water_level_m").alias(
                "water_level_mean"
            ),
            F.min("water_level_m").alias(
                "water_level_min"
            ),
            F.max("water_level_m").alias(
                "water_level_max"
            ),
            F.count("water_level_m").alias(
                "water_level_obs_count"
            ),
            F.count("*").alias(
                "water_level_total_rows"
            ),
            F.sum(
                F.when(
                    F.col("is_valid_on_grid"), 1
                ).otherwise(0)
            ).cast("long").alias(
                "water_level_on_grid_count"
            ),
            F.sum(
                F.when(
                    F.col("is_valid") &
                    ~F.col("is_on_grid"), 1
                ).otherwise(0)
            ).cast("long").alias(
                "water_level_off_grid_count"
            ),
            F.sum(
                F.when(
                    F.col("quality_flag") != "OBSERVED", 1
                ).otherwise(0)
            ).cast("long").alias(
                "water_level_non_observed_count"
            ),
            F.countDistinct("source_file").alias(
                "source_file_count"
            )
        )
        .join(
            last_values,
            on=["station_code", "hour_start"],
            how="left"
        )
        .withColumn(
            "water_level_expected_count",
            F.lit(4)
        )
        .withColumn(
            "water_level_completeness",
            F.col("water_level_on_grid_count")
            / F.lit(4.0)
        )
        .withColumn(
            "water_level_quality_flag",
            F.when(
                F.col("water_level_obs_count") == 0,
                F.lit("NO_VALID_OBSERVATION")
            )
            .when(
                F.col("water_level_non_observed_count") > 0,
                F.lit("SOURCE_QUALITY_WARNING")
            )
            .when(
                F.col("water_level_off_grid_count") > 0,
                F.lit("OFF_GRID_OBSERVATIONS")
            )
            .when(
                F.col("water_level_on_grid_count") == 4,
                F.lit("COMPLETE")
            )
            .otherwise(F.lit("INCOMPLETE"))
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return hourly.select(
        "hour_start",
        "station_code",
        "water_level_mean",
        "water_level_min",
        "water_level_max",
        "water_level_last",
        "water_level_last_timestamp",
        "water_level_obs_count",
        "water_level_total_rows",
        "water_level_expected_count",
        "water_level_on_grid_count",
        "water_level_off_grid_count",
        "water_level_non_observed_count",
        "water_level_completeness",
        "water_level_quality_flag",
        "source_file_count",
        "gold_processed_at"
    )


def validate_hourly(hourly, silver_count):
    print("\n[3] Validate Gold transformation")

    total_hours = hourly.count()

    if total_hours == 0:
        raise ValueError("Gold hourly result is empty")

    duplicate_hours = (
        hourly
        .groupBy("station_code", "hour_start")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_hours:
        raise ValueError(
            f"Duplicate Gold hour keys: {duplicate_hours}"
        )

    counts = hourly.agg(
        F.sum("water_level_total_rows").alias(
            "total_input_rows"
        ),
        F.sum("water_level_obs_count").alias(
            "valid_observations"
        ),
        F.sum("water_level_on_grid_count").alias(
            "on_grid"
        ),
        F.sum("water_level_off_grid_count").alias(
            "off_grid"
        )
    ).first()

    if counts["total_input_rows"] != silver_count:
        raise ValueError(
            "Silver-to-Gold record reconciliation failed"
        )

    invalid_stats = hourly.filter(
        (
            F.col("water_level_min")
            > F.col("water_level_mean")
        )
        | (
            F.col("water_level_mean")
            > F.col("water_level_max")
        )
        | (
            F.col("water_level_completeness") > 1.0
        )
        | (
            F.col("water_level_completeness") < 0.0
        )
        | (
            F.col("water_level_obs_count")
            != (
                F.col("water_level_on_grid_count")
                + F.col("water_level_off_grid_count")
            )
        )
    ).count()

    if invalid_stats:
        raise ValueError(
            f"Invalid hourly statistics: {invalid_stats}"
        )

    print(f"Gold hourly rows: {total_hours}")
    print(
        f"Reconciled Silver rows: "
        f"{counts['total_input_rows']}"
    )
    print(
        f"Valid observations: "
        f"{counts['valid_observations']}"
    )
    print(
        f"On-grid observations: {counts['on_grid']}"
    )
    print(
        f"Off-grid observations: {counts['off_grid']}"
    )

    print("\nHourly quality distribution")

    (
        hourly
        .groupBy("water_level_quality_flag")
        .count()
        .orderBy("water_level_quality_flag")
        .show(truncate=False)
    )

    print("\nHourly samples")

    (
        hourly
        .orderBy("hour_start")
        .show(10, truncate=False)
    )

    print("[PASS] Gold transformation validation")

    return total_hours


def validate_existing_target(spark, silver_count):
    print("\n[EXISTING] Gold table already exists")

    existing = spark.table(TARGET_TABLE)

    missing = {
        "hour_start",
        "station_code",
        "water_level_total_rows",
    } - set(existing.columns)

    if missing:
        raise ValueError(
            "Existing Gold schema is incompatible: "
            f"{sorted(missing)}"
        )

    rows = existing.count()

    reconciled = existing.agg(
        F.sum("water_level_total_rows")
    ).first()[0]

    print(f"Existing Gold rows: {rows}")
    print(f"Reconciled Silver rows: {reconciled}")

    if reconciled != silver_count:
        raise ValueError(
            "Existing Gold does not reconcile with "
            "current Silver. No changes were made."
        )

    print("[PASS] Existing Gold verified")
    print("No append or overwrite performed")


def main():
    spark = create_spark_session(
        app_name="GoldWaterLevelHourly"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set(
            "spark.sql.session.timeZone",
            TIMEZONE
        )

        print("=" * 80)
        print("STEP 8.1 - WATER LEVEL HOURLY")
        print("=" * 80)

        print("Source:", SOURCE_TABLE)
        print("Target:", TARGET_TABLE)
        print(
            "Timezone:",
            spark.conf.get(
                "spark.sql.session.timeZone"
            )
        )

        source = spark.table(SOURCE_TABLE)

        silver_count = validate_source(source)

        # Safe rerun: never recreate or append.
        if spark.catalog.tableExists(TARGET_TABLE):
            validate_existing_target(
                spark, silver_count
            )
            return

        hourly = build_hourly(source)

        validate_hourly(hourly, silver_count)

        print("\n[4] Create Gold namespace")

        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS "
            f"{TARGET_NAMESPACE}"
        )

        # Re-check immediately before creating table.
        if spark.catalog.tableExists(TARGET_TABLE):
            raise RuntimeError(
                "Gold target appeared during execution. "
                "Aborting to prevent accidental changes."
            )

        print("\n[5] Write Iceberg Gold table by year")

        from pyspark.sql import functions as F

        # Giảm nguy cơ phát sinh range-distribution overhead.
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", "4")

        # Tạo namespace nếu chưa tồn tại.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

        # Chỉ tạo bảng nếu bảng chưa tồn tại.
        if not spark.catalog.tableExists(TARGET_TABLE):
            (
                hourly.limit(0)
                .writeTo(TARGET_TABLE)
                .using("iceberg")
                .tableProperty("write.distribution-mode", "hash")
                .tableProperty("write.target-file-size-bytes", "33554432")
                .partitionedBy(F.days("hour_start"))
                .create()
            )

            print("[PASS] Empty Iceberg table created")

        # Chia dữ liệu theo năm dựa trên giờ địa phương.
        years = [
            row["year"]
            for row in (
                hourly
                .select(F.year("hour_start").alias("year"))
                .distinct()
                .orderBy("year")
                .collect()
            )
        ]

        for year in years:
            print(f"\nWriting year {year}...")

            year_data = hourly.filter(
                F.year("hour_start") == year
            )

            # Không append nếu năm đã có dữ liệu.
            existing_rows = (
                spark.table(TARGET_TABLE)
                .filter(F.year("hour_start") == year)
                .limit(1)
                .count()
            )

            if existing_rows > 0:
                print(f"[SKIP] Year {year} already contains data")
                continue

            year_count = year_data.count()

            (
                year_data
                .repartition(4, F.to_date("hour_start"))
                .sortWithinPartitions("hour_start", "station_code")
                .writeTo(TARGET_TABLE)
                .option("fanout-enabled", "false")
                .append()
            )

            written_count = (
                spark.table(TARGET_TABLE)
                .filter(F.year("hour_start") == year)
                .count()
            )

            if written_count != year_count:
                raise RuntimeError(
                    f"Year {year}: expected {year_count}, "
                    f"found {written_count}"
                )

            print(f"[PASS] Year {year}: {written_count} records")

        print("\n[6] Verify persisted Iceberg table")

        persisted = spark.table(TARGET_TABLE)

        persisted_count = persisted.count()

        if persisted_count == 0:
            raise ValueError(
                "Persisted Gold table is empty"
            )

        persisted_reconciliation = (
            persisted
            .agg(
                F.sum("water_level_total_rows")
            )
            .first()[0]
        )

        if persisted_reconciliation != silver_count:
            raise ValueError(
                "Persisted Gold reconciliation failed"
            )

        print(
            f"Persisted Gold rows: "
            f"{persisted_count}"
        )

        print(
            f"Silver input rows: "
            f"{silver_count}"
        )

        print("\nIceberg snapshots")

        spark.sql(
            f"SELECT snapshot_id, operation, "
            f"committed_at "
            f"FROM {TARGET_TABLE}.snapshots "
            f"ORDER BY committed_at"
        ).show(truncate=False)

        print("\n" + "=" * 80)
        print("GOLD WATER LEVEL HOURLY: PASS")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
