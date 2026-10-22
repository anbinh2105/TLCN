from transformation.silver.common import create_spark_session


DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

EXPECTED_BEFORE_COUNT = 100
EXPECTED_AFTER_COUNT = 101


def get_snapshots(spark):
    """
    Return Iceberg snapshot history ordered by commit time.
    """
    return spark.sql(
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


def main():
    spark = create_spark_session(
        app_name="IcebergSnapshotDemo"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — SNAPSHOT VERSIONING")
        print("=" * 100)

        # ------------------------------------------------------------------
        # 1. Verify demo table exists
        # ------------------------------------------------------------------
        print(f"\n[1] Checking demo table: {DEMO_TABLE}")

        if not spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError(
                f"Demo table does not exist: {DEMO_TABLE}\n"
                "Run Step 7.1 first:\n"
                "python -m demo.iceberg.setup_iceberg_demo"
            )

        demo_df = spark.table(DEMO_TABLE)

        before_count = demo_df.count()

        print(f"Records before INSERT: {before_count:,}")

        if before_count != EXPECTED_BEFORE_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_BEFORE_COUNT} records before demo, "
                f"but found {before_count}.\n"
                "The demo table may already have been modified.\n"
                "Run Step 7.1 again to reset the demo table."
            )

        # ------------------------------------------------------------------
        # 2. Capture Snapshot 1
        # ------------------------------------------------------------------
        print("\n[2] Snapshot state BEFORE INSERT")

        snapshots_before = get_snapshots(spark)

        snapshots_before.show(
            truncate=False
        )

        snapshot_count_before = snapshots_before.count()

        if snapshot_count_before != 1:
            raise RuntimeError(
                f"Expected exactly 1 snapshot before INSERT, "
                f"but found {snapshot_count_before}.\n"
                "Run Step 7.1 again to reset the demo."
            )

        snapshot_1 = snapshots_before.collect()[0]

        snapshot_1_id = snapshot_1["snapshot_id"]

        print(f"Snapshot 1 ID : {snapshot_1_id}")
        print(
            f"Snapshot 1 operation : "
            f"{snapshot_1['operation']}"
        )

        # ------------------------------------------------------------------
        # 3. Get latest timestamp from demo table
        #
        # We use existing data as the base for the new demo row so the
        # inserted row has the exact same schema as the Iceberg table.
        # ------------------------------------------------------------------
        print("\n[3] Preparing one controlled demo record")

        latest_row_df = spark.sql(
            f"""
            SELECT *
            FROM {DEMO_TABLE}
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )

        latest_row_df.show(
            truncate=False
        )

        # Create a new row based on the latest demo row.
        #
        # timestamp is moved forward by one hour.
        # Other values are preserved because this is only a technical
        # Iceberg snapshot demonstration, not real hydrological data.
        #
        # source is changed to ICEBERG_DEMO so the row is easy to identify.
        demo_record_df = latest_row_df.selectExpr(
            "timestamp + INTERVAL 1 HOUR AS timestamp",
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed",
            "wind_direction",
            "latitude",
            "longitude",
            "'OBSERVED' AS quality_flag",
            "'ICEBERG_DEMO' AS source",
            "'snapshot_demo_generated_record' AS source_file",
            "current_timestamp() AS silver_processed_at",
        )

        print("\nControlled record to INSERT:")

        demo_record_df.show(
            truncate=False
        )

        # ------------------------------------------------------------------
        # 4. Append exactly one row
        #
        # append() performs an Iceberg table commit and should create
        # a new snapshot.
        # ------------------------------------------------------------------
        print("\n[4] Appending 1 record to Iceberg demo table")

        demo_record_df.writeTo(
            DEMO_TABLE
        ).append()

        print("INSERT/APPEND completed.")

        # ------------------------------------------------------------------
        # 5. Verify record count changed 100 -> 101
        # ------------------------------------------------------------------
        print("\n[5] Verifying record count")

        after_count = spark.table(
            DEMO_TABLE
        ).count()

        print(f"Records before : {before_count:,}")
        print(f"Records after  : {after_count:,}")

        if after_count != EXPECTED_AFTER_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_AFTER_COUNT} records after INSERT, "
                f"but found {after_count}"
            )

        if after_count != before_count + 1:
            raise RuntimeError(
                "Record count did not increase by exactly 1."
            )

        print(
            f"Record count transition: "
            f"{before_count} -> {after_count} [PASS]"
        )

        # ------------------------------------------------------------------
        # 6. Read snapshot metadata again
        # ------------------------------------------------------------------
        print("\n[6] Snapshot state AFTER INSERT")

        snapshots_after = get_snapshots(spark)

        snapshots_after.show(
            truncate=False
        )

        snapshot_count_after = snapshots_after.count()

        print(
            f"Snapshots before : {snapshot_count_before}"
        )
        print(
            f"Snapshots after  : {snapshot_count_after}"
        )

        if snapshot_count_after != 2:
            raise RuntimeError(
                f"Expected exactly 2 snapshots after INSERT, "
                f"but found {snapshot_count_after}"
            )

        # ------------------------------------------------------------------
        # 7. Identify Snapshot 2
        # ------------------------------------------------------------------
        snapshot_rows = snapshots_after.collect()

        snapshot_2 = snapshot_rows[-1]

        snapshot_2_id = snapshot_2["snapshot_id"]
        snapshot_2_parent_id = snapshot_2["parent_id"]

        print()
        print("Snapshot relationship:")
        print(f"Snapshot 1 ID        : {snapshot_1_id}")
        print(f"Snapshot 2 ID        : {snapshot_2_id}")
        print(
            f"Snapshot 2 parent ID : "
            f"{snapshot_2_parent_id}"
        )
        print(
            f"Snapshot 2 operation : "
            f"{snapshot_2['operation']}"
        )

        # ------------------------------------------------------------------
        # 8. Verify parent-child relationship
        # ------------------------------------------------------------------
        print("\n[7] Verifying snapshot parent-child relationship")

        if snapshot_2_id == snapshot_1_id:
            raise RuntimeError(
                "Snapshot 2 has the same ID as Snapshot 1."
            )

        if snapshot_2_parent_id != snapshot_1_id:
            raise RuntimeError(
                "Snapshot parent-child relationship is incorrect.\n"
                f"Expected parent_id = {snapshot_1_id}, "
                f"but found {snapshot_2_parent_id}"
            )

        print(
            "Snapshot 2 parent_id matches Snapshot 1 ID [PASS]"
        )

        # ------------------------------------------------------------------
        # 9. Verify demo row exists in current table
        # ------------------------------------------------------------------
        print("\n[8] Verifying inserted demo record")

        inserted_df = spark.sql(
            f"""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                source,
                source_file
            FROM {DEMO_TABLE}
            WHERE source = 'ICEBERG_DEMO'
            """
        )

        inserted_count = inserted_df.count()

        inserted_df.show(
            truncate=False
        )

        if inserted_count != 1:
            raise RuntimeError(
                f"Expected exactly 1 ICEBERG_DEMO record, "
                f"but found {inserted_count}"
            )

        print("Inserted demo record found [PASS]")

        # ------------------------------------------------------------------
        # Final result
        # ------------------------------------------------------------------
        print()
        print("=" * 100)
        print("ICEBERG SNAPSHOT VERSIONING DEMO: PASS")
        print("=" * 100)

        print(f"Table             : {DEMO_TABLE}")
        print(f"Records before    : {before_count}")
        print(f"Records after     : {after_count}")
        print(
            f"Snapshots before  : "
            f"{snapshot_count_before}"
        )
        print(
            f"Snapshots after   : "
            f"{snapshot_count_after}"
        )
        print(f"Snapshot 1        : {snapshot_1_id}")
        print(f"Snapshot 2        : {snapshot_2_id}")
        print(
            f"Snapshot 2 parent : "
            f"{snapshot_2_parent_id}"
        )

        print()
        print(
            "RESULT: One Iceberg append created a new snapshot "
            "with Snapshot 1 as its parent."
        )

        print()
        print(
            "IMPORTANT: Do NOT rerun Step 7.1 before the "
            "Time Travel demo because Snapshot 1 and Snapshot 2 "
            "will be used in Step 7.3."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()