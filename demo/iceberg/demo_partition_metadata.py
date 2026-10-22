from transformation.silver.common import create_spark_session


DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

EXPECTED_RECORD_COUNT = 101
EXPECTED_COLUMN_COUNT = 13
EXPECTED_SNAPSHOT_COUNT = 3
EXPECTED_CURRENT_SNAPSHOT = 7829982116285418825


def query_metadata(spark, metadata_type):
    return spark.sql(
        f"SELECT * FROM {DEMO_TABLE}.{metadata_type}"
    )


def main():
    spark = create_spark_session(
        app_name="IcebergPartitionMetadataDemo"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — PARTITION / METADATA MANAGEMENT")
        print("=" * 100)

        # ==========================================================
        # 1. SAFETY
        # ==========================================================
        print("\n[1] Safety check")

        if DEMO_TABLE != "lakehouse.demo.weather_iceberg_demo":
            raise RuntimeError("Unexpected demo table.")

        if not spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError("Demo table does not exist.")

        print("READ ONLY verification [PASS]")

        # ==========================================================
        # 2. TABLE STRUCTURE
        # ==========================================================
        print("\n[2] Iceberg table structure")

        df = spark.table(DEMO_TABLE)

        record_count = df.count()
        column_count = len(df.columns)

        df.printSchema()

        print(f"Records : {record_count}")
        print(f"Columns : {column_count}")

        if record_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError("Unexpected record count.")

        if column_count != EXPECTED_COLUMN_COUNT:
            raise RuntimeError("Unexpected column count.")

        print("Table structure [PASS]")

        # ==========================================================
        # 3. TABLE PROPERTIES
        # ==========================================================
        print("\n[3] Iceberg table properties")

        spark.sql(
            f"SHOW TBLPROPERTIES {DEMO_TABLE}"
        ).show(100, truncate=False)

        # ==========================================================
        # 4. PARTITION METADATA
        # ==========================================================
        print("\n[4] Iceberg partition metadata")

        partitions_df = query_metadata(
            spark, "partitions"
        )

        partitions_df.printSchema()
        partitions_df.show(100, truncate=False)

        partition_count = partitions_df.count()

        print(f"Partition metadata rows: {partition_count}")
        print("Partition metadata query [PASS]")

        # ==========================================================
        # 5. DATA FILES
        # ==========================================================
        print("\n[5] Iceberg data files metadata")

        files_df = query_metadata(
            spark, "files"
        )

        files_df.printSchema()

        files_df.select(
            "content",
            "file_path",
            "file_format",
            "record_count",
            "file_size_in_bytes"
        ).show(100, truncate=False)

        data_file_count = files_df.count()

        print(f"Data file metadata rows: {data_file_count}")

        if data_file_count < 1:
            raise RuntimeError("No data file metadata found.")

        print("Data file metadata [PASS]")

        # ==========================================================
        # 6. MANIFEST METADATA
        # ==========================================================
        print("\n[6] Iceberg manifest metadata")

        manifests_df = query_metadata(
            spark, "manifests"
        )

        manifests_df.printSchema()
        manifests_df.show(100, truncate=False)

        manifest_count = manifests_df.count()

        print(f"Manifest count: {manifest_count}")

        if manifest_count < 1:
            raise RuntimeError("No manifest metadata found.")

        print("Manifest metadata [PASS]")

        # ==========================================================
        # 7. SNAPSHOT HISTORY
        # ==========================================================
        print("\n[7] Iceberg snapshot history")

        snapshots_df = spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                parent_id,
                operation
            FROM {DEMO_TABLE}.snapshots
            ORDER BY committed_at, snapshot_id
            """
        )

        snapshots_df.show(truncate=False)

        snapshot_count = snapshots_df.count()

        if snapshot_count != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_SNAPSHOT_COUNT} snapshots, "
                f"found {snapshot_count}."
            )

        print("Snapshot history [PASS]")

        # ==========================================================
        # 8. SNAPSHOT LINEAGE
        # ==========================================================
        print("\n[8] Iceberg snapshot lineage")

        history_df = query_metadata(
            spark, "history"
        )

        history_df.show(100, truncate=False)

        print("Snapshot history metadata [PASS]")

        # ==========================================================
        # 9. CURRENT SNAPSHOT REFERENCE
        # ==========================================================
        print("\n[9] Current Iceberg snapshot reference")

        refs_df = query_metadata(
            spark, "refs"
        )

        refs_df.show(truncate=False)

        main_refs = refs_df.filter("name = 'main'").collect()

        if len(main_refs) != 1:
            raise RuntimeError(
                "Expected exactly one main snapshot reference."
            )

        current_snapshot = main_refs[0]["snapshot_id"]

        print(f"Current snapshot: {current_snapshot}")

        if current_snapshot != EXPECTED_CURRENT_SNAPSHOT:
            raise RuntimeError(
                "Current snapshot changed unexpectedly."
            )

        print("Current snapshot reference [PASS]")

        # ==========================================================
        # 10. METADATA LOG
        # ==========================================================
        print("\n[10] Iceberg metadata log entries")

        metadata_log_df = query_metadata(
            spark, "metadata_log_entries"
        )

        metadata_log_df.printSchema()
        metadata_log_df.show(100, truncate=False)

        print("Metadata log query [PASS]")

        # ==========================================================
        # 11. FINAL VERIFICATION
        # ==========================================================
        print("\n[11] Final READ ONLY verification")

        final_count = spark.table(DEMO_TABLE).count()

        final_snapshots = spark.sql(
            f"SELECT * FROM {DEMO_TABLE}.snapshots"
        ).count()

        final_current_snapshot = (
            query_metadata(spark, "refs")
            .filter("name = 'main'")
            .collect()[0]["snapshot_id"]
        )

        if final_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError("Record count changed.")

        if final_snapshots != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError("Snapshot count changed.")

        if final_current_snapshot != EXPECTED_CURRENT_SNAPSHOT:
            raise RuntimeError("Current snapshot changed.")

        print("Records unchanged [PASS]")
        print("Snapshots unchanged [PASS]")
        print("Current snapshot unchanged [PASS]")

        # ==========================================================
        # RESULT
        # ==========================================================
        print()
        print("=" * 100)
        print("ICEBERG PARTITION / METADATA MANAGEMENT DEMO: PASS")
        print("=" * 100)

        print(f"Table          : {DEMO_TABLE}")
        print(f"Records        : {record_count}")
        print(f"Columns        : {column_count}")
        print(f"Partitions     : {partition_count}")
        print(f"Data files     : {data_file_count}")
        print(f"Manifests      : {manifest_count}")
        print(f"Snapshots      : {snapshot_count}")
        print(f"Current ID     : {current_snapshot}")

        print()
        print("The existing Iceberg table was inspected through")
        print("partition, file, manifest, snapshot, and metadata")
        print("tables without modifying its contents.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()