
from transformation.silver.common import create_spark_session
from pyspark.sql import functions as F

SOURCE = "lakehouse.silver.weather"
TABLE = "lakehouse.demo.weather_partitioned_demo"

EXPECTED_RECORDS = 168
EXPECTED_LOCAL_DAYS = 7
EXPECTED_PARTITIONS = 8


def main():
    spark = create_spark_session(
        app_name="VerifyIcebergPartitionedTable"
    )

    try:
        print("=" * 90)
        print("ICEBERG PARTITIONED TABLE - READ ONLY VERIFICATION")
        print("=" * 90)

        print("\n[1] Session timezone")
        timezone = spark.conf.get(
            "spark.sql.session.timeZone"
        )
        print(f"Spark timezone: {timezone}")

        print("\n[2] Table state")

        if not spark.catalog.tableExists(TABLE):
            raise RuntimeError("Partitioned table not found.")

        df = spark.table(TABLE)

        count = df.count()

        print(f"Records: {count}")

        if count != EXPECTED_RECORDS:
            raise RuntimeError("Unexpected record count.")

        print("Record count [PASS]")

        print("\n[3] Local calendar dates")

        local_dates = (
            df.groupBy(
                F.to_date("timestamp").alias("local_date")
            )
            .count()
            .orderBy("local_date")
        )

        local_dates.show(100, truncate=False)

        local_day_count = local_dates.count()

        if local_day_count != EXPECTED_LOCAL_DAYS:
            raise RuntimeError(
                "Unexpected number of local dates."
            )

        print("Local calendar date count [PASS]")

        print("\n[4] Iceberg partition metadata")

        partitions = spark.sql(
            f"SELECT * FROM {TABLE}.partitions"
        )

        partitions.select(
            "partition",
            "record_count",
            "file_count"
        ).show(100, truncate=False)

        partition_count = partitions.count()

        print(f"Iceberg partitions: {partition_count}")

        if partition_count != EXPECTED_PARTITIONS:
            raise RuntimeError(
                "Unexpected Iceberg partition count."
            )

        partition_total = partitions.agg(
            F.sum("record_count").alias("total")
        ).first()["total"]

        print(f"Partition record total: {partition_total}")

        if partition_total != count:
            raise RuntimeError(
                "Partition records do not sum to table count."
            )

        print("Partition metadata consistency [PASS]")

        print("\n[5] Iceberg partition values by row")

        # Iceberg exposes hidden metadata columns.
        # This shows the actual partition assigned to each row.
        spark.sql(
            f"""
            SELECT
                CAST(timestamp AS DATE) AS local_date,
                _partition.timestamp_day AS iceberg_day,
                COUNT(*) AS records
            FROM {TABLE}
            GROUP BY
                CAST(timestamp AS DATE),
                _partition.timestamp_day
            ORDER BY local_date, iceberg_day
            """
        ).show(100, truncate=False)

        print("\n[6] Timestamp examples")

        spark.sql(
            f"""
            SELECT
                timestamp,
                CAST(timestamp AS DATE) AS local_date,
                _partition.timestamp_day AS iceberg_day
            FROM {TABLE}
            ORDER BY timestamp
            LIMIT 30
            """
        ).show(30, truncate=False)

        print("\n[7] Data files")

        files = spark.sql(
            f"""
            SELECT
                partition,
                record_count,
                file_size_in_bytes
            FROM {TABLE}.files
            """
        )

        files.show(100, truncate=False)

        print(f"Data file count: {files.count()}")

        print("\n[8] Snapshot history")

        spark.sql(
            f"""
            SELECT
                snapshot_id,
                parent_id,
                operation
            FROM {TABLE}.snapshots
            """
        ).show(truncate=False)

        print("\n[9] Final result")
        print("=" * 90)
        print("ICEBERG PARTITION VERIFICATION: PASS")
        print("=" * 90)

        print(f"Records          : {count}")
        print(f"Local days       : {local_day_count}")
        print(f"Iceberg partitions: {partition_count}")
        print(f"Partition records: {partition_total}")

        print(
            "Partition counts are internally consistent. "
            "Review the local_date/iceberg_day mapping "
            "to confirm timezone behavior."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()