"""
Bronze -> Silver transformation for Rainfall data.

Architecture:
    MinIO Bronze
        -> Apache Spark
        -> Cleaning / Standardization
        -> Apache Iceberg Silver
        -> MinIO Warehouse + Hive Metastore

Silver table:
    lakehouse.silver.rainfall

Rules:
    - Bronze remains immutable.
    - Do not interpolate or impute rainfall in Silver.
    - Preserve missing/invalid records using quality_flag.
    - Remove exact duplicate rows only.
    - Preserve Bronze source-file lineage.
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

SOURCE = "rainfall"

REQUIRED_COLUMNS = [
    "timestamp",
    "rainfall_mm",
    "latitude",
    "longitude",
]


# ============================================================
# 1. Read Bronze
# ============================================================

def read_rainfall_bronze(spark) -> DataFrame:
    """
    Read Rainfall CSV files from Bronze on MinIO.

    All Bronze columns are read as strings.
    Datatype conversion is performed explicitly in Silver.

    _bronze_source_file preserves lineage to the original
    Bronze object.
    """

    bronze_path = get_bronze_path(SOURCE)

    print()
    print("[INFO] Reading Bronze Rainfall")
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
    Check that all required Rainfall columns exist.
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Rainfall Bronze is missing required columns: "
            f"{missing_columns}"
        )

    print("[PASS] Rainfall required columns exist")


# ============================================================
# 3. Standardize Rainfall
# ============================================================

def standardize_rainfall(df: DataFrame) -> DataFrame:
    """
    Convert raw Rainfall fields into canonical Silver fields.

    Canonical schema:
        timestamp
        rainfall_mm
        latitude
        longitude

    date/year/month/day/hour are derived fields from the raw
    dataset and are not persisted in Silver because they can
    be regenerated from timestamp when needed.
    """

    result = (
        df

        # Timestamp
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.trim(F.col("timestamp")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

        # Rainfall
        .withColumn(
            "rainfall_mm",
            F.col("rainfall_mm").cast(DoubleType()),
        )

        # Location
        .withColumn(
            "latitude",
            F.col("latitude").cast(DoubleType()),
        )
        .withColumn(
            "longitude",
            F.col("longitude").cast(DoubleType()),
        )
    )

    return result


# ============================================================
# 4. Quality flag
# ============================================================

def add_rainfall_quality_flag(df: DataFrame) -> DataFrame:
    """
    Add row-level Rainfall quality status.

    Priority:
        INVALID
            timestamp cannot be parsed
            OR rainfall < 0

        MISSING
            rainfall is NULL

        OBSERVED
            valid directly observed rainfall record

    No rainfall value is filled or interpolated in Silver.
    """

    result = df.withColumn(
        "quality_flag",
        F.when(
            F.col("timestamp").isNull(),
            F.lit(QUALITY_INVALID),
        )
        .when(
            F.col("rainfall_mm") < 0,
            F.lit(QUALITY_INVALID),
        )
        .when(
            F.col("rainfall_mm").isNull(),
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

def select_rainfall_columns(df: DataFrame) -> DataFrame:
    """
    Select and order canonical Rainfall Silver columns.
    """

    return df.select(
        "timestamp",
        "rainfall_mm",
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

def transform_rainfall(df: DataFrame) -> DataFrame:
    """
    Execute complete Rainfall Bronze -> Silver transformation.
    """

    # 1. Validate Bronze schema
    validate_required_columns(df)

    # 2. Standardize timestamp and datatypes
    df = standardize_rainfall(df)

    # 3. Remove only fully identical records
    df = remove_exact_duplicates(df)

    # 4. Add row-level quality status
    df = add_rainfall_quality_flag(df)

    # 5. Add Silver lineage metadata
    df = add_silver_metadata(
        df,
        source=SOURCE,
        source_file_column="_bronze_source_file",
    )

    # 6. Select canonical schema
    df = select_rainfall_columns(df)

    return df


# ============================================================
# 7. Write Iceberg
# ============================================================

def write_rainfall_silver(df: DataFrame) -> None:
    """
    Write Rainfall Silver as an Apache Iceberg table.

    createOrReplace() is used for the initial historical load.
    Incremental ingestion will later use append/MERGE logic.
    """

    silver_table = get_silver_table(SOURCE)

    print()
    print("[INFO] Writing Rainfall Silver")
    print(f"[INFO] Iceberg table: {silver_table}")

    (
        df.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print("[PASS] Rainfall Silver Iceberg table written")


# ============================================================
# 8. Silver validation
# ============================================================

def validate_rainfall_silver(
    spark,
    silver_table: str,
) -> None:
    """
    Validate persisted Rainfall Silver data.
    """

    df = spark.table(silver_table)

    print()
    print("=" * 70)
    print("SILVER RAINFALL VALIDATION")
    print("=" * 70)

    # --------------------------------------------------------
    # Row count
    # --------------------------------------------------------

    total_rows = df.count()

    print(f"Total rows: {total_rows}")

    if total_rows == 0:
        raise ValueError(
            "Silver Rainfall table contains zero rows."
        )

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    print()
    print("Schema:")
    df.printSchema()

    # --------------------------------------------------------
    # Quality flags
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
    # Rainfall statistics
    # --------------------------------------------------------

    print()
    print("Rainfall statistics:")

    (
        df
        .select(
            F.min("rainfall_mm").alias("min_rainfall_mm"),
            F.max("rainfall_mm").alias("max_rainfall_mm"),
            F.avg("rainfall_mm").alias("avg_rainfall_mm"),
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
        "rainfall_mm",
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
    # Negative rainfall
    # --------------------------------------------------------

    negative_rainfall_count = (
        df
        .filter(
            F.col("rainfall_mm") < 0
        )
        .count()
    )

    print()
    print(
        "Negative rainfall records: "
        f"{negative_rainfall_count}"
    )

    # --------------------------------------------------------
    # Duplicate timestamps
    # --------------------------------------------------------

    print()
    print("Duplicate timestamps:")

    duplicate_timestamp_df = (
        df
        .filter(
            F.col("timestamp").isNotNull()
        )
        .groupBy("timestamp")
        .count()
        .filter(
            F.col("count") > 1
        )
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
        print("BRONZE -> SILVER : RAINFALL")
        print("=" * 70)

        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------

        spark = create_spark_session(
            app_name="rainfall-to-silver",
            master="local[*]",
        )

        # ----------------------------------------------------
        # Silver namespace
        # ----------------------------------------------------

        ensure_silver_namespace(spark)

        # ----------------------------------------------------
        # Read Bronze
        # ----------------------------------------------------

        bronze_df = read_rainfall_bronze(
            spark
        )

        print_dataframe_summary(
            bronze_df,
            "RAINFALL BRONZE",
        )

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        silver_df = transform_rainfall(
            bronze_df
        )

        print_dataframe_summary(
            silver_df,
            "RAINFALL SILVER BEFORE WRITE",
        )

        # ----------------------------------------------------
        # Write Iceberg
        # ----------------------------------------------------

        write_rainfall_silver(
            silver_df
        )

        # ----------------------------------------------------
        # Validate persisted table
        # ----------------------------------------------------

        silver_table = get_silver_table(
            SOURCE
        )

        validate_rainfall_silver(
            spark,
            silver_table,
        )

        print()
        print("=" * 70)
        print("RAINFALL BRONZE -> SILVER: PASS")
        print("=" * 70)

    except Exception as exc:
        print()
        print("=" * 70)
        print("RAINFALL BRONZE -> SILVER: FAILED")
        print("=" * 70)
        print(
            f"{type(exc).__name__}: {exc}"
        )
        raise

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()