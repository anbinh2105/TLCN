from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    get_silver_table,
)


SOURCE = "tide"


def print_section(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def validate_basic_quality(df) -> None:
    """
    Basic integrity checks for the Tide Silver table.
    """

    print_section("1. BASIC TIDE SILVER QUALITY")

    total = df.count()

    null_timestamp = df.filter(
        F.col("timestamp").isNull()
    ).count()

    null_height = df.filter(
        F.col("tide_height_m").isNull()
    ).count()

    invalid = df.filter(
        F.col("quality_flag") == "INVALID"
    ).count()

    duplicate_groups = (
        df.filter(F.col("timestamp").isNotNull())
        .groupBy("timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Total records              : {total:,}")
    print(f"Null timestamp             : {null_timestamp:,}")
    print(f"Null tide height           : {null_height:,}")
    print(f"INVALID records            : {invalid:,}")
    print(f"Duplicate timestamp groups : {duplicate_groups:,}")


def validate_hourly_resolution(df) -> None:
    """
    Tide data is expected to be hourly.

    This check does NOT require every hour to exist.
    It only verifies that existing observations are aligned
    to the hourly grid: HH:00:00.
    """

    print_section("2. HOURLY TIMESTAMP ALIGNMENT")

    non_hourly_df = df.filter(
        (F.minute("timestamp") != 0)
        | (F.second("timestamp") != 0)
    )

    non_hourly_count = non_hourly_df.count()

    print(f"Non-hourly timestamps: {non_hourly_count:,}")

    if non_hourly_count > 0:
        print()
        print("Sample non-hourly timestamps:")

        (
            non_hourly_df
            .select(
                "timestamp",
                "tide_height_m",
                "quality_flag",
                "source_file",
            )
            .orderBy("timestamp")
            .show(50, truncate=False)
        )
    else:
        print("All existing Tide timestamps are aligned to HH:00:00.")


def validate_recovery_scope(df) -> None:
    """
    Inspect where RECOVERED observations came from.

    Expected from previous forensic investigation:
    recovered Excel-date heights should be associated with
    affected 2021 XLSX source files.
    """

    print_section("3. RECOVERED DATA SCOPE")

    recovered = df.filter(
        F.col("quality_flag") == "RECOVERED"
    )

    recovered_count = recovered.count()

    print(f"Total RECOVERED records: {recovered_count:,}")

    print()
    print("RECOVERED records by year/month:")

    (
        recovered
        .groupBy(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month"),
        )
        .count()
        .orderBy("year", "month")
        .show(100, truncate=False)
    )

    print()
    print("RECOVERED records by source file:")

    (
        recovered
        .groupBy(
            "source_file",
            "recovery_method",
        )
        .count()
        .orderBy("source_file")
        .show(100, truncate=False)
    )

    unexpected_recovery = recovered.filter(
        (F.year("timestamp") != 2021)
        | (~F.month("timestamp").between(4, 11))
    )

    unexpected_count = unexpected_recovery.count()

    print()
    print(
        "RECOVERED outside Apr-Nov 2021: "
        f"{unexpected_count:,}"
    )

    if unexpected_count > 0:
        print()
        print("Unexpected RECOVERED records:")

        (
            unexpected_recovery
            .select(
                "timestamp",
                "tide_height_m",
                "recovery_method",
                "source_file",
            )
            .orderBy("timestamp")
            .show(100, truncate=False)
        )


def validate_november_2021(df, spark) -> None:
    """
    Compare actual November 2021 timestamps against the complete
    hourly calendar from:

        2021-11-01 00:00:00
        through
        2021-11-30 23:00:00

    Expected calendar size = 30 * 24 = 720 hours.

    IMPORTANT:
    This function only reports missing timestamps.
    It does NOT create/impute missing Tide observations.
    """

    print_section("4. NOVEMBER 2021 COMPLETENESS")

    november = (
        df.filter(
            (F.year("timestamp") == 2021)
            & (F.month("timestamp") == 11)
        )
        .select("timestamp")
        .distinct()
    )

    actual_count = november.count()

    expected = spark.sql(
        """
        SELECT explode(
            sequence(
                to_timestamp('2021-11-01 00:00:00'),
                to_timestamp('2021-11-30 23:00:00'),
                interval 1 hour
            )
        ) AS timestamp
        """
    )

    expected_count = expected.count()

    missing = (
        expected
        .join(
            november,
            on="timestamp",
            how="left_anti",
        )
        .orderBy("timestamp")
    )

    missing_count = missing.count()

    unexpected = (
        november
        .join(
            expected,
            on="timestamp",
            how="left_anti",
        )
        .orderBy("timestamp")
    )

    unexpected_count = unexpected.count()

    print(f"Expected November 2021 hours : {expected_count:,}")
    print(f"Actual November 2021 hours   : {actual_count:,}")
    print(f"Missing timestamps           : {missing_count:,}")
    print(f"Unexpected timestamps        : {unexpected_count:,}")

    if missing_count > 0:
        print()
        print("Missing timestamps:")

        missing.show(
            missing_count,
            truncate=False,
        )

    if unexpected_count > 0:
        print()
        print("Unexpected timestamps:")

        unexpected.show(
            unexpected_count,
            truncate=False,
        )


def validate_year_counts(df) -> None:
    print_section("5. RECORD COUNTS BY YEAR")

    (
        df.groupBy(
            F.year("timestamp").alias("year")
        )
        .count()
        .orderBy("year")
        .show(20, truncate=False)
    )


def main() -> None:
    spark = create_spark_session(
        app_name="ValidateTideSilver"
    )

    try:
        table_name = get_silver_table(SOURCE)

        print_section("TIDE SILVER FINAL VALIDATION")
        print(f"Table: {table_name}")

        df = spark.table(table_name).cache()

        validate_basic_quality(df)
        validate_hourly_resolution(df)
        validate_recovery_scope(df)
        validate_november_2021(df, spark)
        validate_year_counts(df)

        print_section("TIDE SILVER FINAL VALIDATION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()