from transformation.silver.common import create_spark_session


DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

EXPECTED_CURRENT_COUNT = 101
EXPECTED_OLD_COUNT = 100
EXPECTED_SNAPSHOT_COUNT = 2


def get_snapshots(spark):
    """
    Read Iceberg snapshot metadata.

    READ ONLY:
    This function does not modify the table.
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
        app_name="IcebergTimeTravelDemo"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — TIME TRAVEL")
        print("=" * 100)

        # ------------------------------------------------------------------
        # 1. Verify demo table exists
        # ------------------------------------------------------------------
        print(f"\n[1] Checking demo table: {DEMO_TABLE}")

        if not spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError(
                f"Demo table does not exist: {DEMO_TABLE}\n"
                "The demo must first be prepared by Steps 7.1 and 7.2."
            )

        # ------------------------------------------------------------------
        # 2. Read snapshot history
        # ------------------------------------------------------------------
        print("\n[2] Reading Iceberg snapshot history")

        snapshots_df = get_snapshots(spark)

        snapshots_df.show(
            truncate=False
        )

        snapshot_rows = snapshots_df.collect()
        snapshot_count_before = len(snapshot_rows)

        print(f"Snapshot count before Time Travel: {snapshot_count_before}")

        if snapshot_count_before != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError(
                f"Expected exactly {EXPECTED_SNAPSHOT_COUNT} snapshots, "
                f"but found {snapshot_count_before}.\n"
                "Do not continue because the demo state is different "
                "from the expected state after Step 7.2."
            )

        # Oldest snapshot = Snapshot 1
        # Latest snapshot = Snapshot 2
        snapshot_1 = snapshot_rows[0]
        snapshot_2 = snapshot_rows[-1]

        snapshot_1_id = snapshot_1["snapshot_id"]
        snapshot_2_id = snapshot_2["snapshot_id"]

        print()
        print(f"Snapshot 1 ID : {snapshot_1_id}")
        print(f"Snapshot 2 ID : {snapshot_2_id}")

        # ------------------------------------------------------------------
        # 3. Verify expected snapshot relationship
        # ------------------------------------------------------------------
        print("\n[3] Verifying snapshot lineage")

        if snapshot_2["parent_id"] != snapshot_1_id:
            raise RuntimeError(
                "Snapshot lineage is incorrect.\n"
                f"Snapshot 1 ID        : {snapshot_1_id}\n"
                f"Snapshot 2 parent ID : {snapshot_2['parent_id']}"
            )

        print(
            "Snapshot 2 parent_id matches Snapshot 1 ID [PASS]"
        )

        # ------------------------------------------------------------------
        # 4. Read CURRENT table
        # ------------------------------------------------------------------
        print("\n[4] Reading CURRENT table")

        current_df = spark.table(DEMO_TABLE)

        current_count = current_df.count()

        print(f"Current record count: {current_count}")

        if current_count != EXPECTED_CURRENT_COUNT:
            raise RuntimeError(
                f"Expected current table to contain "
                f"{EXPECTED_CURRENT_COUNT} records, "
                f"but found {current_count}."
            )

        print(
            f"Current table = Snapshot 2 state "
            f"({current_count} records) [PASS]"
        )

        # ------------------------------------------------------------------
        # 5. TIME TRAVEL to Snapshot 1
        #
        # Spark + Iceberg syntax:
        #
        # SELECT ...
        # FROM table VERSION AS OF <snapshot_id>
        #
        # This is READ ONLY.
        # ------------------------------------------------------------------
        print(
            f"\n[5] Time Travel to Snapshot 1: "
            f"{snapshot_1_id}"
        )

        snapshot_1_df = spark.sql(
            f"""
            SELECT *
            FROM {DEMO_TABLE}
            VERSION AS OF {snapshot_1_id}
            """
        )

        snapshot_1_count = snapshot_1_df.count()

        print(
            f"Snapshot 1 record count: "
            f"{snapshot_1_count}"
        )

        if snapshot_1_count != EXPECTED_OLD_COUNT:
            raise RuntimeError(
                f"Expected Snapshot 1 to contain "
                f"{EXPECTED_OLD_COUNT} records, "
                f"but found {snapshot_1_count}."
            )

        print(
            f"Time Travel Snapshot 1 = "
            f"{snapshot_1_count} records [PASS]"
        )

        # ------------------------------------------------------------------
        # 6. TIME TRAVEL explicitly to Snapshot 2
        # ------------------------------------------------------------------
        print(
            f"\n[6] Time Travel to Snapshot 2: "
            f"{snapshot_2_id}"
        )

        snapshot_2_df = spark.sql(
            f"""
            SELECT *
            FROM {DEMO_TABLE}
            VERSION AS OF {snapshot_2_id}
            """
        )

        snapshot_2_count = snapshot_2_df.count()

        print(
            f"Snapshot 2 record count: "
            f"{snapshot_2_count}"
        )

        if snapshot_2_count != EXPECTED_CURRENT_COUNT:
            raise RuntimeError(
                f"Expected Snapshot 2 to contain "
                f"{EXPECTED_CURRENT_COUNT} records, "
                f"but found {snapshot_2_count}."
            )

        print(
            f"Time Travel Snapshot 2 = "
            f"{snapshot_2_count} records [PASS]"
        )

        # ------------------------------------------------------------------
        # 7. Strong evidence:
        #    ICEBERG_DEMO row must NOT exist in Snapshot 1.
        # ------------------------------------------------------------------
        print("\n[7] Checking demo record in Snapshot 1")

        snapshot_1_demo_count = (
            snapshot_1_df
            .filter("source = 'ICEBERG_DEMO'")
            .count()
        )

        print(
            f"ICEBERG_DEMO records in Snapshot 1: "
            f"{snapshot_1_demo_count}"
        )

        if snapshot_1_demo_count != 0:
            raise RuntimeError(
                "The technical demo record unexpectedly exists "
                "in Snapshot 1."
            )

        print(
            "Demo record does NOT exist in Snapshot 1 [PASS]"
        )

        # ------------------------------------------------------------------
        # 8. The same row MUST exist in Snapshot 2.
        # ------------------------------------------------------------------
        print("\n[8] Checking demo record in Snapshot 2")

        snapshot_2_demo_df = (
            snapshot_2_df
            .filter("source = 'ICEBERG_DEMO'")
        )

        snapshot_2_demo_count = snapshot_2_demo_df.count()

        print(
            f"ICEBERG_DEMO records in Snapshot 2: "
            f"{snapshot_2_demo_count}"
        )

        snapshot_2_demo_df.select(
            "timestamp",
            "temperature_c",
            "humidity_pct",
            "source",
            "source_file",
        ).show(
            truncate=False
        )

        if snapshot_2_demo_count != 1:
            raise RuntimeError(
                f"Expected exactly 1 ICEBERG_DEMO record "
                f"in Snapshot 2, but found "
                f"{snapshot_2_demo_count}."
            )

        print(
            "Demo record exists in Snapshot 2 [PASS]"
        )

        # ------------------------------------------------------------------
        # 9. Compare states
        # ------------------------------------------------------------------
        print("\n[9] Comparing historical and current states")

        print(
            f"Snapshot 1 : "
            f"{snapshot_1_count} records"
        )

        print(
            f"Snapshot 2 : "
            f"{snapshot_2_count} records"
        )

        print(
            f"Current    : "
            f"{current_count} records"
        )

        if snapshot_2_count != current_count:
            raise RuntimeError(
                "Current table does not match Snapshot 2."
            )

        if snapshot_2_count - snapshot_1_count != 1:
            raise RuntimeError(
                "Expected exactly one-record difference "
                "between Snapshot 1 and Snapshot 2."
            )

        print(
            "Historical state comparison [PASS]"
        )

        # ------------------------------------------------------------------
        # 10. Verify Time Travel itself did NOT create another snapshot
        # ------------------------------------------------------------------
        print(
            "\n[10] Verifying Time Travel is READ ONLY"
        )

        snapshots_after_df = get_snapshots(spark)

        snapshot_count_after = snapshots_after_df.count()

        print(
            f"Snapshots before queries : "
            f"{snapshot_count_before}"
        )

        print(
            f"Snapshots after queries  : "
            f"{snapshot_count_after}"
        )

        if snapshot_count_after != snapshot_count_before:
            raise RuntimeError(
                "Snapshot count changed during Time Travel. "
                "The demo was expected to be read-only."
            )

        print(
            "No new snapshot was created [PASS]"
        )

        # ------------------------------------------------------------------
        # Final result
        # ------------------------------------------------------------------
        print()
        print("=" * 100)
        print("ICEBERG TIME TRAVEL DEMO: PASS")
        print("=" * 100)

        print(f"Table              : {DEMO_TABLE}")
        print(f"Snapshot 1 ID      : {snapshot_1_id}")
        print(f"Snapshot 1 records : {snapshot_1_count}")
        print(f"Snapshot 2 ID      : {snapshot_2_id}")
        print(f"Snapshot 2 records : {snapshot_2_count}")
        print(f"Current records    : {current_count}")

        print(
            f"Snapshots before   : "
            f"{snapshot_count_before}"
        )

        print(
            f"Snapshots after    : "
            f"{snapshot_count_after}"
        )

        print()
        print(
            "RESULT: Iceberg successfully queried historical "
            "table state using Snapshot 1 without modifying "
            "the current table."
        )

        print()
        print(
            "Snapshot 1 does not contain the ICEBERG_DEMO row, "
            "while Snapshot 2 does."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()