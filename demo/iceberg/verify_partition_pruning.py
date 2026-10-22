
from transformation.silver.common import create_spark_session

TABLE = "lakehouse.demo.weather_partitioned_demo"
TARGET_DATE = "2018-04-25"

EXPECTED_RECORDS = 168
EXPECTED_SNAPSHOT_ID = 8362217845622271219


def main():
    spark = create_spark_session(
        app_name="IcebergPartitionPruningVerification"
    )

    try:
        print("=" * 90)
        print("ICEBERG PARTITION PRUNING - READ ONLY")
        print("=" * 90)

        if not spark.catalog.tableExists(TABLE):
            raise RuntimeError("Demo table does not exist.")

        # 1. Verify table state
        print("\n[1] Verify table state")

        total_records = spark.table(TABLE).count()

        total_files = spark.sql(
            f"SELECT COUNT(*) AS n FROM {TABLE}.files"
        ).first()["n"]

        snapshot_id = spark.sql(
            f"""
            SELECT snapshot_id
            FROM {TABLE}.refs
            WHERE name = 'main'
            """
        ).first()["snapshot_id"]

        assert total_records == EXPECTED_RECORDS
        assert snapshot_id == EXPECTED_SNAPSHOT_ID

        print(f"Total records : {total_records}")
        print(f"Total files   : {total_files}")
        print(f"Snapshot ID   : {snapshot_id}")

        # 2. Filter by one local calendar day
        print("\n[2] Filter one local day")

        query = f"""
            SELECT
                timestamp,
                temperature_c,
                _file AS data_file,
                _partition.timestamp_day AS partition_day
            FROM {TABLE}
            WHERE timestamp >= TIMESTAMP '{TARGET_DATE} 00:00:00'
              AND timestamp < TIMESTAMP '{TARGET_DATE} 00:00:00'
                  + INTERVAL 1 DAY
        """

        filtered = spark.sql(query)

        filtered_count = filtered.count()

        print(f"Target date      : {TARGET_DATE}")
        print(f"Filtered records : {filtered_count}")

        # 3. Inspect files containing matching records
        print("\n[3] Matching data files")

        matching_files = (
            filtered
            .select("data_file", "partition_day")
            .distinct()
        )

        matching_files.show(100, truncate=False)

        matching_file_count = matching_files.count()

        print(f"All table files : {total_files}")
        print(f"Matching files  : {matching_file_count}")

        if matching_file_count >= total_files:
            raise RuntimeError(
                "Filtered query did not reduce matching files."
            )

        print("Matching file reduction [PASS]")

        # 4. Show Spark/Iceberg physical plan
        print("\n[4] Physical execution plan")

        plan = spark.sql(
            f"EXPLAIN FORMATTED {query}"
        ).first()[0]

        print(plan)

        # 5. Partition distribution
        print("\n[5] Filtered partition distribution")

        filtered.groupBy("partition_day").count().orderBy(
            "partition_day"
        ).show(truncate=False)

        # 6. Read-only snapshot check
        print("\n[6] Verify snapshot unchanged")

        snapshot_after = spark.sql(
            f"""
            SELECT snapshot_id
            FROM {TABLE}.refs
            WHERE name = 'main'
            """
        ).first()["snapshot_id"]

        assert snapshot_after == EXPECTED_SNAPSHOT_ID

        print("Snapshot unchanged [PASS]")

        print("\n" + "=" * 90)
        print("PARTITION FILTER VERIFICATION: PASS")
        print("=" * 90)

        print(f"Total files      : {total_files}")
        print(f"Matching files   : {matching_file_count}")
        print(f"Filtered records : {filtered_count}")

        print(
            "Review the physical plan to confirm "
            "actual Iceberg partition pruning."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
