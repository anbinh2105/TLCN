from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    get_silver_table,
)


# =====================================================================
# Configuration
# =====================================================================

EXPECTED_TIDE_GAPS = {
    "2021-11-29 01:00:00",
    "2021-11-29 02:00:00",
}


@dataclass(frozen=True)
class SourceConfig:
    source: str
    required_columns: tuple[str, ...]
    duplicate_keys: tuple[str, ...]
    value_columns: tuple[str, ...]
    validator: Callable[[DataFrame], list[str]] | None = None


# =====================================================================
# Source-specific validators
# =====================================================================

def validate_weather(df: DataFrame) -> list[str]:
    """
    Source-specific structural checks for Weather Silver.

    Missing individual weather measurements are allowed because
    Silver preserves source observations and marks them using
    quality_flag rather than imputing values.
    """
    issues: list[str] = []

    negative_humidity = df.filter(
        F.col("humidity_pct") < 0
    ).count()

    humidity_over_100 = df.filter(
        F.col("humidity_pct") > 100
    ).count()

    if negative_humidity > 0:
        issues.append(
            f"humidity_pct < 0: {negative_humidity:,}"
        )

    if humidity_over_100 > 0:
        issues.append(
            f"humidity_pct > 100: {humidity_over_100:,}"
        )

    return issues


def validate_rainfall(df: DataFrame) -> list[str]:
    """
    Rainfall cannot be negative.
    """
    issues: list[str] = []

    negative_rainfall = df.filter(
        F.col("rainfall_mm") < 0
    ).count()

    if negative_rainfall > 0:
        issues.append(
            f"rainfall_mm < 0: {negative_rainfall:,}"
        )

    return issues


def validate_river_discharge(df: DataFrame) -> list[str]:
    """
    Structural validation only.

    Do not introduce an arbitrary physical threshold here.
    """
    return []


def validate_water_level(df: DataFrame) -> list[str]:
    """
    Water Level Silver preserves native observations.

    The dataset is mostly 15-minute data, but previously verified
    source observations include timestamps that are not aligned
    exactly to the 15-minute grid. Therefore off-grid observations
    are reported separately and are NOT treated as Silver failures.
    """
    return []


def validate_tide(df: DataFrame) -> list[str]:
    """
    Tide observations should be aligned to an hourly grid.

    Known source gaps are handled separately in the reporting phase.
    """
    issues: list[str] = []

    non_hourly = df.filter(
        (F.minute("timestamp") != 0)
        | (F.second("timestamp") != 0)
    ).count()

    if non_hourly > 0:
        issues.append(
            f"non-hourly timestamps: {non_hourly:,}"
        )

    unexpected_recovery = df.filter(
        (F.col("quality_flag") == "RECOVERED")
        & (
            (F.year("timestamp") != 2021)
            | (~F.month("timestamp").between(4, 11))
        )
    ).count()

    if unexpected_recovery > 0:
        issues.append(
            "RECOVERED records outside Apr-Nov 2021: "
            f"{unexpected_recovery:,}"
        )

    return issues


# =====================================================================
# Silver table definitions
# =====================================================================

SOURCE_CONFIGS = [
    SourceConfig(
        source="weather",
        required_columns=(
            "timestamp",
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
            "latitude",
            "longitude",
            "quality_flag",
            "source",
            "source_file",
            "silver_processed_at",
        ),
        duplicate_keys=("timestamp",),
        value_columns=(
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
        ),
        validator=validate_weather,
    ),
    SourceConfig(
        source="rainfall",
        required_columns=(
            "timestamp",
            "rainfall_mm",
            "latitude",
            "longitude",
            "quality_flag",
            "source",
            "source_file",
            "silver_processed_at",
        ),
        duplicate_keys=("timestamp",),
        value_columns=("rainfall_mm",),
        validator=validate_rainfall,
    ),
    SourceConfig(
        source="river_discharge",
        required_columns=(
            "timestamp",
            "station",
            "discharge_m3s",
            "quality_flag",
            "source",
            "source_file",
            "silver_processed_at",
        ),
        duplicate_keys=("station", "timestamp"),
        value_columns=("discharge_m3s",),
        validator=validate_river_discharge,
    ),
    SourceConfig(
        source="water_level",
        required_columns=(
            "timestamp",
            "station_code",
            "water_level_m",
            "country_code",
            "parameter",
            "label",
            "unit",
            "observation_type",
            "approval_level",
            "grade",
            "quality_flag",
            "source",
            "source_file",
            "silver_processed_at",
        ),
        duplicate_keys=("station_code", "timestamp"),
        value_columns=("water_level_m",),
        validator=validate_water_level,
    ),
    SourceConfig(
        source="tide",
        required_columns=(
            "timestamp",
            "tide_height_m",
            "quality_flag",
            "recovery_method",
            "source",
            "source_file",
            "silver_processed_at",
        ),
        duplicate_keys=("timestamp",),
        value_columns=("tide_height_m",),
        validator=validate_tide,
    ),
]


# =====================================================================
# Console helpers
# =====================================================================

def print_section(title: str) -> None:
    print()
    print("=" * 110)
    print(title)
    print("=" * 110)


def print_subsection(title: str) -> None:
    print()
    print("-" * 110)
    print(title)
    print("-" * 110)


# =====================================================================
# Generic checks
# =====================================================================

def validate_schema(
    df: DataFrame,
    config: SourceConfig,
) -> list[str]:

    issues: list[str] = []

    actual_columns = set(df.columns)

    missing_columns = [
        column
        for column in config.required_columns
        if column not in actual_columns
    ]

    if missing_columns:
        issues.append(
            "missing required columns: "
            + ", ".join(missing_columns)
        )

    return issues


def count_duplicate_groups(
    df: DataFrame,
    keys: tuple[str, ...],
) -> int:

    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def get_quality_distribution(
    df: DataFrame,
) -> dict[str, int]:

    rows = (
        df.groupBy("quality_flag")
        .count()
        .collect()
    )

    return {
        str(row["quality_flag"]): row["count"]
        for row in rows
    }


def get_temporal_coverage(
    df: DataFrame,
):
    row = (
        df.agg(
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
        )
        .first()
    )

    return (
        row["min_timestamp"],
        row["max_timestamp"],
    )


def count_all_value_columns_null(
    df: DataFrame,
    value_columns: tuple[str, ...],
) -> int:
    """
    Count rows where every core measurement column is NULL.

    This is more appropriate than requiring every individual
    measurement column to be non-null for multi-variable sources
    such as Weather.
    """

    condition = None

    for column in value_columns:
        expression = F.col(column).isNull()

        condition = (
            expression
            if condition is None
            else condition & expression
        )

    return df.filter(condition).count()


# =====================================================================
# Source-specific reporting
# =====================================================================

def report_water_level_grid(df: DataFrame) -> None:
    """
    Informational check only.

    Previous source verification established that Water Level is
    mostly 15-minute data but contains legitimate source timestamps
    outside the 15-minute grid.
    """

    off_grid = df.filter(
        (F.minute("timestamp") % 15 != 0)
        | (F.second("timestamp") != 0)
    ).count()

    print(
        f"15-minute off-grid observations : "
        f"{off_grid:,} [INFO]"
    )


def report_tide_known_gaps(
    df: DataFrame,
    spark: SparkSession,
) -> tuple[int, int]:
    """
    Validate completeness of November 2021.

    The two already-investigated source gaps are accepted only if
    the actual missing timestamp set matches EXPECTED_TIDE_GAPS
    exactly.

    Returns:
        actual_missing_count,
        unexpected_gap_count
    """

    november = (
        df.filter(
            (F.year("timestamp") == 2021)
            & (F.month("timestamp") == 11)
        )
        .select("timestamp")
        .distinct()
    )

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

    missing_rows = (
        expected
        .join(
            november,
            on="timestamp",
            how="left_anti",
        )
        .orderBy("timestamp")
        .collect()
    )

    actual_gaps = {
        row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        for row in missing_rows
    }

    unexpected_gaps = (
        actual_gaps - EXPECTED_TIDE_GAPS
    )

    expected_but_present = (
        EXPECTED_TIDE_GAPS - actual_gaps
    )

    print(
        f"November 2021 missing hours     : "
        f"{len(actual_gaps):,}"
    )

    for timestamp in sorted(actual_gaps):
        label = (
            "KNOWN SOURCE GAP"
            if timestamp in EXPECTED_TIDE_GAPS
            else "UNEXPECTED GAP"
        )

        print(
            f"  {timestamp}  [{label}]"
        )

    if expected_but_present:
        print(
            "Previously known gaps now present:"
        )

        for timestamp in sorted(expected_but_present):
            print(f"  {timestamp}")

    return (
        len(actual_gaps),
        len(unexpected_gaps),
    )


def report_tide_recovery(df: DataFrame) -> None:
    recovered = df.filter(
        F.col("quality_flag") == "RECOVERED"
    )

    recovered_count = recovered.count()

    print(
        f"RECOVERED observations          : "
        f"{recovered_count:,}"
    )

    if recovered_count > 0:
        (
            recovered
            .groupBy(
                F.year("timestamp").alias("year"),
                F.month("timestamp").alias("month"),
            )
            .count()
            .orderBy("year", "month")
            .show(20, truncate=False)
        )


# =====================================================================
# Validate one Silver table
# =====================================================================

def validate_source(
    spark: SparkSession,
    config: SourceConfig,
) -> dict:

    source = config.source
    table_name = get_silver_table(source)

    print_section(
        f"VALIDATE SILVER: {source.upper()}"
    )

    print(f"Table: {table_name}")

    result = {
        "source": source,
        "table": table_name,
        "records": 0,
        "null_timestamp": None,
        "invalid": None,
        "duplicates": None,
        "status": "FAIL",
        "issues": [],
    }

    # -------------------------------------------------------------
    # Read table
    # -------------------------------------------------------------

    try:
        df = spark.table(table_name).cache()
    except Exception as exc:
        result["issues"].append(
            f"cannot read Iceberg table: {exc}"
        )

        print(
            f"ERROR: Cannot read {table_name}"
        )

        return result

    try:
        # ---------------------------------------------------------
        # Schema
        # ---------------------------------------------------------

        schema_issues = validate_schema(
            df,
            config,
        )

        result["issues"].extend(
            schema_issues
        )

        if schema_issues:
            print("Schema: FAIL")

            for issue in schema_issues:
                print(f"  - {issue}")

            # Do not continue column-based checks if schema is broken.
            return result

        print("Schema: PASS")

        # ---------------------------------------------------------
        # Generic quality
        # ---------------------------------------------------------

        total = df.count()

        null_timestamp = df.filter(
            F.col("timestamp").isNull()
        ).count()

        invalid = df.filter(
            F.col("quality_flag") == "INVALID"
        ).count()

        duplicates = count_duplicate_groups(
            df,
            config.duplicate_keys,
        )

        all_values_null = (
            count_all_value_columns_null(
                df,
                config.value_columns,
            )
        )

        min_timestamp, max_timestamp = (
            get_temporal_coverage(df)
        )

        quality_distribution = (
            get_quality_distribution(df)
        )

        result["records"] = total
        result["null_timestamp"] = null_timestamp
        result["invalid"] = invalid
        result["duplicates"] = duplicates

        print(f"Records                    : {total:,}")
        print(
            f"Null timestamp             : "
            f"{null_timestamp:,}"
        )
        print(
            f"INVALID records            : "
            f"{invalid:,}"
        )
        print(
            f"Duplicate key groups       : "
            f"{duplicates:,}"
        )
        print(
            f"All core values NULL       : "
            f"{all_values_null:,}"
        )
        print(
            f"Temporal coverage          : "
            f"{min_timestamp} -> {max_timestamp}"
        )

        print()
        print("Quality distribution:")

        for quality_flag in sorted(
            quality_distribution
        ):
            print(
                f"  {quality_flag:<12} "
                f"{quality_distribution[quality_flag]:,}"
            )

        # ---------------------------------------------------------
        # Generic critical rules
        # ---------------------------------------------------------

        if total == 0:
            result["issues"].append(
                "table contains zero records"
            )

        if null_timestamp > 0:
            result["issues"].append(
                f"null timestamp: {null_timestamp:,}"
            )

        if invalid > 0:
            result["issues"].append(
                f"INVALID records: {invalid:,}"
            )

        if duplicates > 0:
            result["issues"].append(
                f"duplicate key groups: {duplicates:,}"
            )

        # ---------------------------------------------------------
        # Source column integrity
        # ---------------------------------------------------------

        wrong_source = df.filter(
            F.col("source").isNull()
            | (F.col("source") != source)
        ).count()

        null_source_file = df.filter(
            F.col("source_file").isNull()
        ).count()

        null_processed_at = df.filter(
            F.col("silver_processed_at").isNull()
        ).count()

        print(
            f"Wrong/null source          : "
            f"{wrong_source:,}"
        )
        print(
            f"Null source_file           : "
            f"{null_source_file:,}"
        )
        print(
            f"Null silver_processed_at   : "
            f"{null_processed_at:,}"
        )

        if wrong_source > 0:
            result["issues"].append(
                f"wrong/null source: {wrong_source:,}"
            )

        if null_source_file > 0:
            result["issues"].append(
                f"null source_file: {null_source_file:,}"
            )

        if null_processed_at > 0:
            result["issues"].append(
                "null silver_processed_at: "
                f"{null_processed_at:,}"
            )

        # ---------------------------------------------------------
        # Source-specific checks
        # ---------------------------------------------------------

        if config.validator is not None:
            source_issues = config.validator(df)

            result["issues"].extend(
                source_issues
            )

        if source == "water_level":
            print_subsection(
                "WATER LEVEL TEMPORAL PROFILE"
            )

            report_water_level_grid(df)

        if source == "tide":
            print_subsection(
                "TIDE RECOVERY PROFILE"
            )

            report_tide_recovery(df)

            print_subsection(
                "TIDE KNOWN SOURCE GAPS"
            )

            _, unexpected_gap_count = (
                report_tide_known_gaps(
                    df,
                    spark,
                )
            )

            if unexpected_gap_count > 0:
                result["issues"].append(
                    "unexpected Tide gaps in "
                    "November 2021: "
                    f"{unexpected_gap_count:,}"
                )

        # ---------------------------------------------------------
        # Final source status
        # ---------------------------------------------------------

        if not result["issues"]:
            result["status"] = "PASS"

        print()
        print(
            f"{source.upper()} SILVER STATUS: "
            f"{result['status']}"
        )

        if result["issues"]:
            print("Issues:")

            for issue in result["issues"]:
                print(f"  - {issue}")

        return result

    finally:
        df.unpersist()


# =====================================================================
# Final summary
# =====================================================================

def print_summary(results: list[dict]) -> None:

    print_section(
        "FINAL SILVER VALIDATION SUMMARY"
    )

    header = (
        f"{'Source':<20}"
        f"{'Records':>12}"
        f"{'Null TS':>12}"
        f"{'Invalid':>12}"
        f"{'Duplicate':>12}"
        f"{'Status':>12}"
    )

    print(header)
    print("-" * len(header))

    for result in results:
        records = result["records"]

        null_timestamp = (
            "-"
            if result["null_timestamp"] is None
            else f"{result['null_timestamp']:,}"
        )

        invalid = (
            "-"
            if result["invalid"] is None
            else f"{result['invalid']:,}"
        )

        duplicates = (
            "-"
            if result["duplicates"] is None
            else f"{result['duplicates']:,}"
        )

        print(
            f"{result['source']:<20}"
            f"{records:>12,}"
            f"{null_timestamp:>12}"
            f"{invalid:>12}"
            f"{duplicates:>12}"
            f"{result['status']:>12}"
        )

    print("-" * len(header))

    passed = sum(
        result["status"] == "PASS"
        for result in results
    )

    failed = sum(
        result["status"] == "FAIL"
        for result in results
    )

    print(f"PASS : {passed}")
    print(f"FAIL : {failed}")

    if failed == 0:
        print()
        print("OVERALL SILVER STATUS: PASS")
    else:
        print()
        print("OVERALL SILVER STATUS: FAIL")

        print()
        print("Failed checks:")

        for result in results:
            if result["status"] == "FAIL":
                print(
                    f"\n{result['source']}:"
                )

                for issue in result["issues"]:
                    print(
                        f"  - {issue}"
                    )


# =====================================================================
# Main
# =====================================================================

def main() -> None:

    spark = create_spark_session(
        app_name="ValidateSilverLayer"
    )

    try:
        print_section(
            "CAN THO FLOOD LAKEHOUSE - "
            "FINAL SILVER VALIDATION"
        )

        results = []

        for config in SOURCE_CONFIGS:
            result = validate_source(
                spark,
                config,
            )

            results.append(result)

        print_summary(results)

        failed = any(
            result["status"] == "FAIL"
            for result in results
        )

        if failed:
            raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()