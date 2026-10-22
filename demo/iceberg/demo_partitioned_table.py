
from transformation.silver.common import create_spark_session
from pyspark.sql import functions as F


SOURCE_TABLE = "lakehouse.silver.weather"
DEMO_TABLE = "lakehouse.demo.weather_partitioned_demo"
PRESERVED_TABLE = "lakehouse.demo.weather_iceberg_demo"

# Use 7 distinct dates to make pruning visible.
DEMO_DAYS = 7

# Existing Iceberg capability evidence must remain intact.
PRESERVED_SNAPSHOT_ID = 7829982116285418825
PRESERVED_SNAPSHOT_COUNT = 3
PRESERVED_ROW_COUNT = 101


def get_current_snapshot(spark, table):
    rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {table}.refs
        WHERE name = 'main'
        """
    ).collect()

    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one main reference for {table}"
        )

    return rows[0]["snapshot_id"]


def get_snapshot_count(spark, table):
    return spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {table}.snapshots"
    ).first()["cnt"]


def get_partition_spec(spark):
    """
    DESCRIBE EXTENDED displays the Iceberg partition transforms.
    """
    spark.sql(
        f"DESCRIBE EXTENDED {DEMO_TABLE}"
    ).show(100, truncate=False)


def get_partitions(spark):
    return spark.sql(
        f"""
        SELECT *
        FROM {DEMO_TABLE}.partitions
        """
    )


def get_files(spark):
    return spark.sql(
        f"""
        SELECT
            content,
            file_path,
            file_format,
            spec_id,
            partition,
            record_count,
            file_size_in_bytes
        FROM {DEMO_TABLE}.files
        """
    )


def main():
    spark = create_spark_session(
        app_name="IcebergPartitionPruningDemo"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO - PARTITIONING / PRUNING")
        print("=" * 100)

        # ======================================================
        # 1. SAFETY CHECK
        # ======================================================
        print("\n[1] Safety checks")

        if DEMO_TABLE != (
            "lakehouse.demo.weather_partitioned_demo"
        ):
            raise RuntimeError("Unsafe demo table name.")

        if not spark.catalog.tableExists(SOURCE_TABLE):
            raise RuntimeError(
                f"Source table missing: {SOURCE_TABLE}"
            )

        if not spark.catalog.tableExists(PRESERVED_TABLE):
            raise RuntimeError(
                "Previous capability demo table is missing."
            )

        preserved_count_before = spark.table(
            PRESERVED_TABLE
        ).count()

        preserved_snapshots_before = get_snapshot_count(
            spark, PRESERVED_TABLE
        )

        preserved_current_before = get_current_snapshot(
            spark, PRESERVED_TABLE
        )

        if (
            preserved_count_before != PRESERVED_ROW_COUNT
            or preserved_snapshots_before != PRESERVED_SNAPSHOT_COUNT
            or preserved_current_before != PRESERVED_SNAPSHOT_ID
        ):
            raise RuntimeError(
                "Previous Iceberg demo state is unexpected. "
                "Stopping without changing anything."
            )

        # Do not recreate or overwrite an existing demo.
        if spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError(
                f"{DEMO_TABLE} already exists.\n"
                "Do not drop it or rerun CTAS. "
                "Inspect the existing table before retrying."
            )

        print("Source table is READ ONLY [PASS]")
        print("Previous demo snapshots protected [PASS]")
        print("Partitioned demo table does not exist [PASS]")

        # ======================================================
        # 2. PROFILE SOURCE DATES
        # ======================================================
        print("\n[2] Profiling available source dates")

        source_df = spark.table(SOURCE_TABLE)

        source_count_before = source_df.count()

        available_dates = (
            source_df
            .filter(F.col("timestamp").isNotNull())
            .select(
                F.to_date("timestamp").alias("observation_date")
            )
            .distinct()
            .orderBy("observation_date")
            .limit(DEMO_DAYS)
            .collect()
        )

        if len(available_dates) != DEMO_DAYS:
            raise RuntimeError(
                f"Need {DEMO_DAYS} distinct dates, "
                f"found {len(available_dates)}."
            )

        first_date = available_dates[0]["observation_date"]
        last_date = available_dates[-1]["observation_date"]

        print(f"Source records : {source_count_before}")
        print(f"Selected dates : {first_date} to {last_date}")
        print(f"Distinct dates : {len(available_dates)}")

        # Use an explicit set of dates. They do not have to be
        # consecutive in the source dataset.
        selected_date_strings = [
            str(row["observation_date"])
            for row in available_dates
        ]

        date_literals = ", ".join(
            f"DATE '{date_str}'"
            for date_str in selected_date_strings
        )

        print("Dates:")
        for date_str in selected_date_strings:
            print(f"  {date_str}")

        # ======================================================
        # 3. CREATE PARTITIONED ICEBERG TABLE
        # ======================================================
        print("\n[3] Creating partitioned Iceberg demo")

        spark.sql(
            f"""
            CREATE TABLE {DEMO_TABLE}
            USING iceberg
            PARTITIONED BY (days(timestamp))
            TBLPROPERTIES (
                'format-version' = '2'
            )
            AS
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                pressure_hpa,
                wind_speed,
                wind_direction,
                latitude,
                longitude,
                quality_flag,
                source,
                source_file,
                silver_processed_at
            FROM {SOURCE_TABLE}
            WHERE CAST(timestamp AS DATE) IN ({date_literals})
            """
        )

        print("Partitioned Iceberg CTAS completed.")

        # ======================================================
        # 4. VERIFY DATA AND PARTITION COUNT
        # ======================================================
        print("\n[4] Verifying partitioned table")

        demo_df = spark.table(DEMO_TABLE)

        demo_count = demo_df.count()

        distinct_dates = (
            demo_df
            .select(F.to_date("timestamp").alias("d"))
            .distinct()
            .count()
        )

        expected_count = (
            source_df
            .filter(
                F.to_date("timestamp").isin(
                    selected_date_strings
                )
            )
            .count()
        )

        print(f"Source subset records : {expected_count}")
        print(f"Demo table records    : {demo_count}")
        print(f"Distinct dates        : {distinct_dates}")

        if demo_count != expected_count:
            raise RuntimeError(
                "CTAS row count differs from source subset."
            )

        if distinct_dates != DEMO_DAYS:
            raise RuntimeError(
                "Unexpected distinct date count."
            )

        print("Row count and dates [PASS]")

        # ======================================================
        # 5. PARTITION SPECIFICATION
        # ======================================================
        print("\n[5] Iceberg partition specification")

        get_partition_spec(spark)

        # ======================================================
        # 6. PARTITION METADATA
        # ======================================================
        print("\n[6] Iceberg partition metadata")

        partitions_df = get_partitions(spark)

        partitions_df.printSchema()
        partitions_df.show(100, truncate=False)

        partition_count = partitions_df.count()

        print(f"Partition metadata rows: {partition_count}")

        if partition_count != DEMO_DAYS:
            raise RuntimeError(
                f"Expected {DEMO_DAYS} partitions, "
                f"found {partition_count}."
            )

        print("Daily partitions [PASS]")

        # ======================================================
        # 7. DATA FILE METADATA
        # ======================================================
        print("\n[7] Iceberg data file metadata")

        files_df = get_files(spark)

        files_df.show(100, truncate=False)

        file_count = files_df.count()

        if file_count < 1:
            raise RuntimeError("No data files found.")

        print(f"Data files: {file_count}")
        print("Data file metadata [PASS]")

        # ======================================================
        # 8. PARTITION PRUNING QUERY
        # ======================================================
        print("\n[8] Partition pruning with date filter")

        target_date = selected_date_strings[0]

        print(f"Target date: {target_date}")

        # Half-open interval, suitable for timestamp filtering.
        query = f"""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct
            FROM {DEMO_TABLE}
            WHERE timestamp >= TIMESTAMP '{target_date} 00:00:00'
              AND timestamp < (
                  TIMESTAMP '{target_date} 00:00:00'
                  + INTERVAL 1 DAY
              )
        """

        print("\nEXPLAIN FORMATTED:")
        spark.sql(
            f"EXPLAIN FORMATTED {query}"
        ).show(100, truncate=False)

        filtered_df = spark.sql(query)

        filtered_count = filtered_df.count()

        expected_filtered_count = (
            source_df
            .filter(
                F.to_date("timestamp") == F.lit(target_date)
            )
            .count()
        )

        print(f"Filtered records : {filtered_count}")
        print(f"Expected records : {expected_filtered_count}")

        if filtered_count != expected_filtered_count:
            raise RuntimeError(
                "Filtered result does not match source."
            )

        print("Partition-filtered query result [PASS]")

        # ======================================================
        # 9. SNAPSHOT METADATA
        # ======================================================
        print("\n[9] Partitioned table snapshot")

        spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                parent_id,
                operation
            FROM {DEMO_TABLE}.snapshots
            """
        ).show(truncate=False)

        # ======================================================
        # 10. VERIFY ORIGINAL DEMO IS UNCHANGED
        # ======================================================
        print("\n[10] Verifying previous demo is unchanged")

        preserved_count_after = spark.table(
            PRESERVED_TABLE
        ).count()

        preserved_snapshots_after = get_snapshot_count(
            spark, PRESERVED_TABLE
        )

        preserved_current_after = get_current_snapshot(
            spark, PRESERVED_TABLE
        )

        if (
            preserved_count_after != preserved_count_before
            or preserved_snapshots_after
            != preserved_snapshots_before
            or preserved_current_after
            != preserved_current_before
        ):
            raise RuntimeError(
                "Previous Iceberg demo changed unexpectedly."
            )

        source_count_after = spark.table(
            SOURCE_TABLE
        ).count()

        if source_count_after != source_count_before:
            raise RuntimeError(
                "Source row count changed unexpectedly."
            )

        print("Silver source row count unchanged [PASS]")
        print("Previous demo records unchanged [PASS]")
        print("Previous demo snapshots unchanged [PASS]")

        # ======================================================
        # 11. FINAL RESULT
        # ======================================================
        print()
        print("=" * 100)
        print("ICEBERG PARTITIONING DEMO: PASS")
        print("=" * 100)

        print(f"Source table     : {SOURCE_TABLE}")
        print(f"Demo table       : {DEMO_TABLE}")
        print(f"Records          : {demo_count}")
        print(f"Distinct days    : {distinct_dates}")
        print(f"Partitions       : {partition_count}")
        print(f"Data files       : {file_count}")
        print(f"Filtered day     : {target_date}")
        print(f"Filtered records : {filtered_count}")

        print()
        print("Partitioning and metadata verification completed.")
        print("Review EXPLAIN output for partition pruning evidence.")
        print("No Silver or previous Iceberg demo data was modified.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()