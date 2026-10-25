"""
Bronze -> Silver transformation for River Discharge data.

Architecture:
    MinIO Bronze
        -> Apache Spark
        -> Cleaning / Standardization
        -> Apache Iceberg Silver
        -> MinIO Warehouse + Hive Metastore

Silver table:
    lakehouse.silver.river_discharge

Important:
    River Discharge has DAILY temporal grain.

Silver preserves the native daily grain.
No hourly upsampling, interpolation, forward-fill,
or imputation is performed here.
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

SOURCE = "river_discharge"

REQUIRED_COLUMNS = [
    "timestamp",
    "station",
    "discharge_m3s",
]


# ============================================================
# 1. Read Bronze
# ============================================================

def read_river_discharge_bronze(spark) -> DataFrame:
    """
    Read River Discharge CSV files from Bronze on MinIO.

    Bronze fields are read as strings.
    Datatype conversion is performed explicitly in Silver.

    _bronze_source_file preserves lineage back to
    the original Bronze object.
    """

    bronze_path = get_bronze_path(SOURCE)

    print()
    print("[INFO] Reading Bronze River Discharge")
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
    Check required River Discharge Bronze columns.
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "River Discharge Bronze is missing "
            f"required columns: {missing_columns}"
        )

    print("[PASS] River Discharge required columns exist")


# ============================================================
# 3. Standardize
# ============================================================

def standardize_river_discharge(df: DataFrame) -> DataFrame:
    """
    Standardize River Discharge into canonical Silver types.

    Bronze:
        timestamp       string YYYY-MM-DD
        station         string
        discharge_m3s   string

    Silver:
        timestamp       timestamp
        station         string
        discharge_m3s   double

    Although the canonical field is called timestamp,
    the source has DAILY temporal resolution. Therefore
    values become YYYY-MM-DD 00:00:00.

    date/year/month are derived from timestamp and are
    not persisted in Silver.
    """

    result = (
        df

        # Daily timestamp
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.trim(F.col("timestamp")),
                "yyyy-MM-dd",
            ),
        )

        # Standardize station text
        .withColumn(
            "station",
            F.trim(F.col("station")),
        )

        # Discharge m3/s
        .withColumn(
            "discharge_m3s",
            F.col("discharge_m3s").cast(DoubleType()),
        )
    )

    return result


# ============================================================
# 4. Quality flag
# ============================================================

def add_river_discharge_quality_flag(
    df: DataFrame,
) -> DataFrame:
    """
    Add row-level quality status.

    INVALID:
        - timestamp cannot be parsed
        - discharge < 0

    MISSING:
        - station is null/blank
        - discharge is null

    OBSERVED:
        - valid source observation

    Values are preserved. No filling/interpolation occurs.
    """

    station_missing = (
        F.col("station").isNull()
        | (F.trim(F.col("station")) == "")
    )

    result = df.withColumn(
        "quality_flag",
        F.when(
            F.col("timestamp").isNull(),
            F.lit(QUALITY_INVALID),
        )
        .when(
            F.col("discharge_m3s") < 0,
            F.lit(QUALITY_INVALID),
        )
        .when(
            station_missing
            | F.col("discharge_m3s").isNull(),
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

def select_river_discharge_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Select canonical River Discharge Silver columns.
    """

    return df.select(
        "timestamp",
        "station",
        "discharge_m3s",
        "quality_flag",
        "source",
        "source_file",
        "silver_processed_at",
    )


# ============================================================
# 6. Complete transformation
# ============================================================

def transform_river_discharge(
    df: DataFrame,
) -> DataFrame:
    """
    Execute complete River Discharge Bronze -> Silver flow.
    """

    # 1. Validate raw schema
    validate_required_columns(df)

    # 2. Standardize datatype / timestamp
    df = standardize_river_discharge(df)

    # 3. Remove fully identical rows only
    df = remove_exact_duplicates(df)

    # 4. Add row-level quality status
    df = add_river_discharge_quality_flag(df)

    # 5. Add lineage metadata
    df = add_silver_metadata(
        df,
        source=SOURCE,
        source_file_column="_bronze_source_file",
    )

    # 6. Canonical Silver schema
    df = select_river_discharge_columns(df)

    return df


# ============================================================
# 7. Write Iceberg
# ============================================================

def write_river_discharge_silver(
    df: DataFrame,
) -> None:
    """
    Write River Discharge to Apache Iceberg.

    createOrReplace() is appropriate for the initial
    historical Silver build.

    Incremental ingestion will later use append/MERGE.
    """

    silver_table = get_silver_table(SOURCE)

    print()
    print("[INFO] Writing River Discharge Silver")
    print(f"[INFO] Iceberg table: {silver_table}")

    (
        df.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(
        "[PASS] River Discharge Silver "
        "Iceberg table written"
    )


# ============================================================
# 8. Silver validation
# ============================================================

def validate_river_discharge_silver(
    spark,
    silver_table: str,
) -> None:
    """
    Validate persisted River Discharge Silver table.
    """

    df = spark.table(silver_table)

    print()
    print("=" * 70)
    print("SILVER RIVER DISCHARGE VALIDATION")
    print("=" * 70)

    # --------------------------------------------------------
    # Total rows
    # --------------------------------------------------------

    total_rows = df.count()

    print(f"Total rows: {total_rows}")

    if total_rows == 0:
        raise ValueError(
            "Silver River Discharge contains zero rows."
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
    # Discharge statistics
    # --------------------------------------------------------

    print()
    print("Discharge statistics (m3/s):")

    (
        df
        .select(
            F.min("discharge_m3s").alias(
                "min_discharge_m3s"
            ),
            F.max("discharge_m3s").alias(
                "max_discharge_m3s"
            ),
            F.avg("discharge_m3s").alias(
                "avg_discharge_m3s"
            ),
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
        "station",
        "discharge_m3s",
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

    (
        df
        .select(*null_expressions)
        .show(truncate=False)
    )

    # --------------------------------------------------------
    # Negative discharge
    # --------------------------------------------------------

    negative_count = (
        df
        .filter(
            F.col("discharge_m3s") < 0
        )
        .count()
    )

    print()
    print(
        "Negative discharge records: "
        f"{negative_count}"
    )

    # --------------------------------------------------------
    # Stations
    # --------------------------------------------------------

    print()
    print("Stations:")

    (
        df
        .groupBy("station")
        .count()
        .orderBy(F.desc("count"))
        .show(
            20,
            truncate=False,
        )
    )

    # --------------------------------------------------------
    # Duplicate station + timestamp
    # --------------------------------------------------------

    print()
    print("Duplicate station + timestamp:")

    duplicate_df = (
        df
        .filter(
            F.col("timestamp").isNotNull()
        )
        .groupBy(
            "station",
            "timestamp",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
    )

    duplicate_count = duplicate_df.count()

    print(
        "Duplicate station-timestamp groups: "
        f"{duplicate_count}"
    )

    if duplicate_count > 0:
        (
            duplicate_df
            .orderBy(
                "station",
                "timestamp",
            )
            .show(
                20,
                truncate=False,
            )
        )

    # --------------------------------------------------------
    # Temporal grain check
    # --------------------------------------------------------

    print()
    print("Non-midnight timestamps:")

    non_midnight_count = (
        df
        .filter(
            F.col("timestamp").isNotNull()
        )
        .filter(
            (F.hour("timestamp") != 0)
            | (F.minute("timestamp") != 0)
            | (F.second("timestamp") != 0)
        )
        .count()
    )

    print(
        "Non-midnight timestamp records: "
        f"{non_midnight_count}"
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
        print("BRONZE -> SILVER : RIVER DISCHARGE")
        print("=" * 70)

        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------

        spark = create_spark_session(
            app_name="river-discharge-to-silver",
        )

        # ----------------------------------------------------
        # Namespace
        # ----------------------------------------------------

        ensure_silver_namespace(spark)

        # ----------------------------------------------------
        # Read Bronze
        # ----------------------------------------------------

        bronze_df = read_river_discharge_bronze(
            spark
        )

        print_dataframe_summary(
            bronze_df,
            "RIVER DISCHARGE BRONZE",
        )

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        silver_df = transform_river_discharge(
            bronze_df
        )

        print_dataframe_summary(
            silver_df,
            "RIVER DISCHARGE SILVER BEFORE WRITE",
        )

        # ----------------------------------------------------
        # Write Iceberg
        # ----------------------------------------------------

        write_river_discharge_silver(
            silver_df
        )

        # ----------------------------------------------------
        # Validate persisted table
        # ----------------------------------------------------

        silver_table = get_silver_table(
            SOURCE
        )

        validate_river_discharge_silver(
            spark,
            silver_table,
        )

        print()
        print("=" * 70)
        print(
            "RIVER DISCHARGE BRONZE -> SILVER: PASS"
        )
        print("=" * 70)

    except Exception as exc:
        print()
        print("=" * 70)
        print(
            "RIVER DISCHARGE BRONZE -> SILVER: FAILED"
        )
        print("=" * 70)
        print(
            f"{type(exc).__name__}: {exc}"
        )
        raise

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()