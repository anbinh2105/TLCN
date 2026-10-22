"""
Bronze -> Silver transformation for Weather data.

Architecture:
    MinIO Bronze
        -> Apache Spark
        -> Cleaning / Standardization
        -> Apache Iceberg Silver
        -> MinIO Warehouse + Hive Metastore

Silver table:
    lakehouse.silver.weather

Important:
    - Bronze remains immutable.
    - Silver does not interpolate or impute missing values.
    - Exact duplicate rows may be removed.
    - Source-file lineage is preserved.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from transformation.silver.common import (
    QUALITY_OBSERVED,
    QUALITY_MISSING,
    QUALITY_INVALID,
    create_spark_session,
    get_bronze_path,
    get_silver_table,
    ensure_silver_namespace,
    add_silver_metadata,
    remove_exact_duplicates,
    print_dataframe_summary,
    stop_spark,
)


# ============================================================
# Configuration
# ============================================================

SOURCE = "weather"

REQUIRED_COLUMNS = [
    "timestamp",
    "temperature",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_direction",
    "latitude",
    "longitude",
]


# ============================================================
# 1. Read Bronze
# ============================================================

def read_weather_bronze(spark) -> DataFrame:
    """
    Read Weather CSV files from Bronze on MinIO.

    All Bronze columns are initially read as strings.
    Datatype conversion is performed explicitly in Silver.

    _bronze_source_file preserves lineage to the original
    Bronze object.
    """

    bronze_path = get_bronze_path(SOURCE)

    print()
    print("[INFO] Reading Bronze Weather")
    print(f"[INFO] Bronze path: {bronze_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("recursiveFileLookup", "true")
        .csv(bronze_path)
        .withColumn(
            "_bronze_source_file",
            F.input_file_name(),
        )
    )

    return df


# ============================================================
# 2. Validate Bronze schema
# ============================================================

def validate_required_columns(df: DataFrame) -> None:
    """
    Check that all required Weather columns exist.
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Weather Bronze is missing required columns: "
            f"{missing_columns}"
        )

    print("[PASS] Weather required columns exist")


# ============================================================
# 3. Standardize Weather
# ============================================================

def standardize_weather(df: DataFrame) -> DataFrame:
    """
    Convert raw Weather fields into canonical Silver fields.

    Transformations:
        timestamp      -> TimestampType
        temperature    -> temperature_c
        humidity       -> humidity_pct
        pressure       -> pressure_hpa

    Numeric fields are explicitly cast to DoubleType.
    """

    result = (
        df

        # ----------------------------------------------------
        # Timestamp
        # ----------------------------------------------------
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.trim(F.col("timestamp")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

        # ----------------------------------------------------
        # Weather measurements
        # ----------------------------------------------------
        .withColumn(
            "temperature_c",
            F.col("temperature").cast(DoubleType()),
        )
        .withColumn(
            "humidity_pct",
            F.col("humidity").cast(DoubleType()),
        )
        .withColumn(
            "pressure_hpa",
            F.col("pressure").cast(DoubleType()),
        )
        .withColumn(
            "wind_speed",
            F.col("wind_speed").cast(DoubleType()),
        )
        .withColumn(
            "wind_direction",
            F.col("wind_direction").cast(DoubleType()),
        )

        # ----------------------------------------------------
        # Location
        # ----------------------------------------------------
        .withColumn(
            "latitude",
            F.col("latitude").cast(DoubleType()),
        )
        .withColumn(
            "longitude",
            F.col("longitude").cast(DoubleType()),
        )
    )

    # Remove raw fields after canonical fields have been created.
    result = result.drop(
        "temperature",
        "humidity",
        "pressure",
    )

    return result


# ============================================================
# 4. Quality flag
# ============================================================

def add_weather_quality_flag(df: DataFrame) -> DataFrame:
    """
    Add row-level quality status.

    Priority:
        INVALID
            timestamp cannot be parsed

        MISSING
            one or more weather measurements are NULL

        OBSERVED
            valid directly observed record

    Silver does not fill/interpolate missing observations.
    """

    measurement_columns = [
        "temperature_c",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed",
        "wind_direction",
    ]

    has_missing_measurement = F.lit(False)

    for column in measurement_columns:
        has_missing_measurement = (
            has_missing_measurement
            | F.col(column).isNull()
        )

    result = df.withColumn(
        "quality_flag",
        F.when(
            F.col("timestamp").isNull(),
            F.lit(QUALITY_INVALID),
        )
        .when(
            has_missing_measurement,
            F.lit(QUALITY_MISSING),
        )
        .otherwise(
            F.lit(QUALITY_OBSERVED),
        ),
    )

    return result


# ============================================================
# 5. Canonical Silver schema
# ============================================================

def select_weather_columns(df: DataFrame) -> DataFrame:
    """
    Select and order canonical Silver Weather columns.
    """

    return df.select(
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
    )


# ============================================================
# 6. Complete transformation
# ============================================================

def transform_weather(df: DataFrame) -> DataFrame:
    """
    Execute complete Weather Bronze -> Silver transformation.
    """

    # 1. Validate Bronze schema
    validate_required_columns(df)

    # 2. Standardize schema and datatypes
    df = standardize_weather(df)

    # 3. Remove only fully identical rows
    df = remove_exact_duplicates(df)

    # 4. Add quality status
    df = add_weather_quality_flag(df)

    # 5. Add Silver lineage metadata
    df = add_silver_metadata(
        df,
        source=SOURCE,
        source_file_column="_bronze_source_file",
    )

    # 6. Select canonical Silver schema
    df = select_weather_columns(df)

    return df


# ============================================================
# 7. Write Iceberg
# ============================================================

def write_weather_silver(df: DataFrame) -> None:
    """
    Write Weather Silver data as an Apache Iceberg table.

    createOrReplace() is appropriate for the initial historical
    Silver build.

    Incremental ingestion will later use append/MERGE logic.
    """

    silver_table = get_silver_table(SOURCE)

    print()
    print("[INFO] Writing Weather Silver")
    print(f"[INFO] Iceberg table: {silver_table}")

    (
        df.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print("[PASS] Weather Silver Iceberg table written")


# ============================================================
# 8. Silver validation
# ============================================================

def validate_weather_silver(
    spark,
    silver_table: str,
) -> None:
    """
    Run basic post-write validation.
    """

    df = spark.table(silver_table)

    print()
    print("=" * 70)
    print("SILVER WEATHER VALIDATION")
    print("=" * 70)

    # --------------------------------------------------------
    # Row count
    # --------------------------------------------------------

    total_rows = df.count()

    print(f"Total rows: {total_rows}")

    if total_rows == 0:
        raise ValueError(
            "Silver Weather table contains zero rows."
        )

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    print()
    print("Schema:")
    df.printSchema()

    # --------------------------------------------------------
    # Quality distribution
    # --------------------------------------------------------

    print()
    print("Quality flags:")

    (
        df
        .groupBy("quality_flag")
        .count()
        .orderBy("quality_flag")
        .show(truncate=False)
    )

    # --------------------------------------------------------
    # Timestamp range
    # --------------------------------------------------------

    print()
    print("Timestamp range:")

    (
        df
        .select(
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
        )
        .show(truncate=False)
    )

    # --------------------------------------------------------
    # Null counts
    # --------------------------------------------------------

    print()
    print("Null counts:")

    columns_to_check = [
        "timestamp",
        "temperature_c",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed",
        "wind_direction",
        "latitude",
        "longitude",
    ]

    null_expressions = [
        F.sum(
            F.when(
                F.col(column).isNull(),
                1,
            ).otherwise(0)
        ).alias(column)
        for column in columns_to_check
    ]

    df.select(
        *null_expressions
    ).show(
        truncate=False
    )

    # --------------------------------------------------------
    # Duplicate timestamps
    # --------------------------------------------------------

    print()
    print("Duplicate timestamps:")

    duplicate_timestamp_df = (
        df
        .filter(F.col("timestamp").isNotNull())
        .groupBy("timestamp")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_timestamp_count = (
        duplicate_timestamp_df.count()
    )

    print(
        "Duplicate timestamp groups: "
        f"{duplicate_timestamp_count}"
    )

    if duplicate_timestamp_count > 0:
        duplicate_timestamp_df.orderBy(
            "timestamp"
        ).show(
            20,
            truncate=False,
        )

    # --------------------------------------------------------
    # Source lineage
    # --------------------------------------------------------

    print()
    print("Source files:")

    (
        df
        .select("source_file")
        .distinct()
        .show(
            20,
            truncate=False,
        )
    )

    print("=" * 70)


# ============================================================
# 9. Main
# ============================================================

def main():
    spark = None

    try:
        print()
        print("=" * 70)
        print("CAN THO FLOOD LAKEHOUSE")
        print("BRONZE -> SILVER : WEATHER")
        print("=" * 70)

        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------

        spark = create_spark_session(
            app_name="weather-to-silver",
            master="local[*]",
        )

        # ----------------------------------------------------
        # Silver namespace
        # ----------------------------------------------------

        ensure_silver_namespace(spark)

        # ----------------------------------------------------
        # Read Bronze
        # ----------------------------------------------------

        bronze_df = read_weather_bronze(spark)

        print_dataframe_summary(
            bronze_df,
            "WEATHER BRONZE",
        )

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        silver_df = transform_weather(
            bronze_df
        )

        print_dataframe_summary(
            silver_df,
            "WEATHER SILVER BEFORE WRITE",
        )

        # ----------------------------------------------------
        # Write Iceberg
        # ----------------------------------------------------

        write_weather_silver(
            silver_df
        )

        # ----------------------------------------------------
        # Validate persisted Silver table
        # ----------------------------------------------------

        silver_table = get_silver_table(
            SOURCE
        )

        validate_weather_silver(
            spark,
            silver_table,
        )

        print()
        print("=" * 70)
        print("WEATHER BRONZE -> SILVER: PASS")
        print("=" * 70)

    except Exception as exc:
        print()
        print("=" * 70)
        print("WEATHER BRONZE -> SILVER: FAILED")
        print("=" * 70)
        print(
            f"{type(exc).__name__}: {exc}"
        )
        raise

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()