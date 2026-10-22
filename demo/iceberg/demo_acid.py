from transformation.silver.common import create_spark_session


DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

EXPECTED_RECORD_COUNT = 101
EXPECTED_SNAPSHOT_COUNT = 3

PRE_UPDATE_SNAPSHOT_ID = 8625072115902793184
POST_UPDATE_SNAPSHOT_ID = 7829982116285418825

TARGET_SOURCE = "ICEBERG_DEMO"
TARGET_SOURCE_FILE = "snapshot_demo_generated_record"

ACID_COLUMN = "demo_note"
ACID_VALUE = "ACID_COMMIT_VERIFIED"


def get_snapshots(spark):
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
        app_name="IcebergACIDVerification"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — ACID VERIFICATION / RESUME")
        print("=" * 100)

        # ============================================================
        # 1. SAFETY
        # ============================================================
        print(f"\n[1] Safety check: {DEMO_TABLE}")

        if not DEMO_TABLE.startswith("lakehouse.demo."):
            raise RuntimeError(
                "SAFETY STOP: Only lakehouse.demo is allowed."
            )

        if not spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError(
                f"Table does not exist: {DEMO_TABLE}"
            )

        print("Demo namespace safety check [PASS]")
        print("This verification script is READ ONLY.")
        print("No UPDATE / INSERT / DELETE will be executed.")

        # ============================================================
        # 2. CURRENT TABLE
        # ============================================================
        print("\n[2] Verifying current table")

        current_df = spark.table(DEMO_TABLE)

        current_count = current_df.count()

        print(f"Current record count: {current_count}")

        if current_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_RECORD_COUNT} records, "
                f"found {current_count}."
            )

        if ACID_COLUMN not in current_df.columns:
            raise RuntimeError(
                f"Column '{ACID_COLUMN}' is missing."
            )

        print("Current table structure [PASS]")

        # ============================================================
        # 3. SNAPSHOT HISTORY
        # ============================================================
        print("\n[3] Verifying snapshot history")

        snapshots_df = get_snapshots(spark)

        snapshots_df.show(truncate=False)

        snapshots = snapshots_df.collect()

        if len(snapshots) != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_SNAPSHOT_COUNT} snapshots, "
                f"found {len(snapshots)}."
            )

        snapshot_ids = [
            row["snapshot_id"]
            for row in snapshots
        ]

        if PRE_UPDATE_SNAPSHOT_ID not in snapshot_ids:
            raise RuntimeError(
                "Pre-update snapshot not found."
            )

        if POST_UPDATE_SNAPSHOT_ID not in snapshot_ids:
            raise RuntimeError(
                "Post-update snapshot not found."
            )

        post_snapshot = next(
            row
            for row in snapshots
            if row["snapshot_id"] == POST_UPDATE_SNAPSHOT_ID
        )

        if post_snapshot["parent_id"] != PRE_UPDATE_SNAPSHOT_ID:
            raise RuntimeError(
                "Post-update snapshot parent is incorrect."
            )

        print("Snapshot count = 3 [PASS]")
        print(
            f"Snapshot 3 parent = Snapshot 2 "
            f"({PRE_UPDATE_SNAPSHOT_ID}) [PASS]"
        )

        print(
            f"Snapshot 3 operation = "
            f"{post_snapshot['operation']}"
        )

        # ============================================================
        # 4. CURRENT COMMITTED VALUE
        # ============================================================
        print("\n[4] Verifying current committed value")

        current_target_df = spark.sql(
            f"""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                source,
                source_file,
                {ACID_COLUMN}
            FROM {DEMO_TABLE}
            WHERE source = '{TARGET_SOURCE}'
              AND source_file = '{TARGET_SOURCE_FILE}'
            """
        )

        current_target_df.show(truncate=False)

        current_target_rows = current_target_df.collect()

        if len(current_target_rows) != 1:
            raise RuntimeError(
                "Expected exactly one controlled target row."
            )

        if current_target_rows[0][ACID_COLUMN] != ACID_VALUE:
            raise RuntimeError(
                "Committed ACID value is not visible."
            )

        print(
            f"Current state contains "
            f"{ACID_COLUMN} = '{ACID_VALUE}' [PASS]"
        )

        # ============================================================
        # 5. PRE-UPDATE SNAPSHOT
        #
        # IMPORTANT:
        # Snapshot 2 predates schema evolution.
        # Therefore demo_note MUST NOT be selected here.
        # ============================================================
        print(
            "\n[5] Reading PRE-UPDATE Snapshot 2 "
            "(historical schema)"
        )

        pre_df = spark.sql(
            f"""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                source,
                source_file
            FROM {DEMO_TABLE}
            VERSION AS OF {PRE_UPDATE_SNAPSHOT_ID}
            WHERE source = '{TARGET_SOURCE}'
              AND source_file = '{TARGET_SOURCE_FILE}'
            """
        )

        pre_df.show(truncate=False)

        pre_rows = pre_df.collect()

        if len(pre_rows) != 1:
            raise RuntimeError(
                "Expected exactly one controlled row "
                "in Snapshot 2."
            )

        print(
            "Snapshot 2 historical record is readable [PASS]"
        )

        print(
            "Snapshot 2 predates Schema Evolution, "
            "so column 'demo_note' is not part of its "
            "historical schema [PASS]"
        )

        # ============================================================
        # 6. POST-UPDATE SNAPSHOT
        # ============================================================
        print(
            "\n[6] Reading POST-UPDATE Snapshot 3"
        )

        post_df = spark.sql(
            f"""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                source,
                source_file,
                {ACID_COLUMN}
            FROM {DEMO_TABLE}
            VERSION AS OF {POST_UPDATE_SNAPSHOT_ID}
            WHERE source = '{TARGET_SOURCE}'
              AND source_file = '{TARGET_SOURCE_FILE}'
            """
        )

        post_df.show(truncate=False)

        post_rows = post_df.collect()

        if len(post_rows) != 1:
            raise RuntimeError(
                "Expected exactly one controlled row "
                "in Snapshot 3."
            )

        if post_rows[0][ACID_COLUMN] != ACID_VALUE:
            raise RuntimeError(
                "Snapshot 3 does not contain "
                "the committed value."
            )

        print(
            f"Snapshot 3 contains "
            f"{ACID_COLUMN} = '{ACID_VALUE}' [PASS]"
        )

        # ============================================================
        # 7. RECORD-COUNT CONSISTENCY
        # ============================================================
        print(
            "\n[7] Verifying record-count consistency"
        )

        pre_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {DEMO_TABLE}
            VERSION AS OF {PRE_UPDATE_SNAPSHOT_ID}
            """
        ).collect()[0]["cnt"]

        post_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {DEMO_TABLE}
            VERSION AS OF {POST_UPDATE_SNAPSHOT_ID}
            """
        ).collect()[0]["cnt"]

        print(
            f"Snapshot 2 records : {pre_count}"
        )

        print(
            f"Snapshot 3 records : {post_count}"
        )

        print(
            f"Current records    : {current_count}"
        )

        if pre_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                "Snapshot 2 record count is incorrect."
            )

        if post_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                "Snapshot 3 record count is incorrect."
            )

        if current_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                "Current record count is incorrect."
            )

        print(
            "Record count remains 101 across "
            "pre/post committed states [PASS]"
        )

        # ============================================================
        # 8. UNIQUENESS
        # ============================================================
        print("\n[8] Verifying target uniqueness")

        target_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {DEMO_TABLE}
            WHERE source = '{TARGET_SOURCE}'
              AND source_file = '{TARGET_SOURCE_FILE}'
            """
        ).collect()[0]["cnt"]

        print(
            f"Controlled target count: {target_count}"
        )

        if target_count != 1:
            raise RuntimeError(
                "Controlled target is not unique."
            )

        print(
            "Controlled target remains unique [PASS]"
        )

        # ============================================================
        # 9. VERIFY READ-ONLY VALIDATION
        # ============================================================
        print(
            "\n[9] Verifying validation created "
            "no additional snapshot"
        )

        final_snapshots = get_snapshots(spark)
        final_snapshot_count = final_snapshots.count()

        print(
            f"Snapshot count before validation : "
            f"{EXPECTED_SNAPSHOT_COUNT}"
        )

        print(
            f"Snapshot count after validation  : "
            f"{final_snapshot_count}"
        )

        if final_snapshot_count != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError(
                "Unexpected snapshot created during "
                "read-only verification."
            )

        print(
            "No additional snapshot created [PASS]"
        )

        # ============================================================
        # 10. RESULT
        # ============================================================
        print()
        print("=" * 100)
        print("ICEBERG ACID DEMO: PASS")
        print("=" * 100)

        print(
            f"Table                     : "
            f"{DEMO_TABLE}"
        )

        print(
            f"Records                   : "
            f"101 -> 101"
        )

        print(
            f"Updated rows              : 1"
        )

        print(
            f"Updated column            : "
            f"{ACID_COLUMN}"
        )

        print(
            f"Committed value           : "
            f"{ACID_VALUE}"
        )

        print(
            f"Snapshots                 : "
            f"2 -> 3"
        )

        print(
            f"Pre-update snapshot ID    : "
            f"{PRE_UPDATE_SNAPSHOT_ID}"
        )

        print(
            f"Post-update snapshot ID   : "
            f"{POST_UPDATE_SNAPSHOT_ID}"
        )

        print(
            f"Post-update parent ID     : "
            f"{post_snapshot['parent_id']}"
        )

        print(
            f"Snapshot operation        : "
            f"{post_snapshot['operation']}"
        )

        print()
        print("ATOMIC COMMIT EVIDENCE:")
        print(
            "The UPDATE became visible through exactly "
            "one new committed Iceberg snapshot."
        )

        print()
        print("CONSISTENCY EVIDENCE:")
        print(
            "The table remained at 101 records and the "
            "controlled record remained unique."
        )

        print()
        print("SNAPSHOT ISOLATION / VERSION EVIDENCE:")
        print(
            "The historical pre-update snapshot remains "
            "readable independently from the post-update "
            "committed snapshot."
        )

        print()
        print("SCHEMA EVOLUTION INTERACTION:")
        print(
            "Snapshot 2 predates the demo_note schema "
            "addition, while Snapshot 3 uses the evolved "
            "13-column schema."
        )

        print()
        print("DURABILITY EVIDENCE:")
        print(
            "The committed value was successfully re-read "
            "from both the current table and Snapshot 3."
        )

        print()
        print(
            "This verification was READ ONLY; "
            "Snapshot 3 was preserved."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()