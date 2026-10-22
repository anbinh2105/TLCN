from transformation.silver.common import create_spark_session


DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

EXPECTED_RECORD_COUNT = 101
EXPECTED_SNAPSHOT_COUNT = 2

NEW_COLUMN = "demo_note"
NEW_COLUMN_TYPE = "STRING"
NEW_COLUMN_COMMENT = "Technical column added for Iceberg schema evolution demo"


def get_snapshots(spark):
    """Read Iceberg data snapshot metadata. READ ONLY."""
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
        app_name="IcebergSchemaEvolutionDemo"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — SCHEMA EVOLUTION")
        print("=" * 100)

        # ================================================================
        # 1. SAFETY
        # ================================================================
        print(f"\n[1] Safety check: {DEMO_TABLE}")

        if not DEMO_TABLE.startswith("lakehouse.demo."):
            raise RuntimeError(
                "SAFETY STOP: This demo may modify only "
                "tables inside lakehouse.demo."
            )

        if not spark.catalog.tableExists(DEMO_TABLE):
            raise RuntimeError(
                f"Demo table does not exist: {DEMO_TABLE}"
            )

        print("Demo namespace safety check [PASS]")
        print("Silver tables will NOT be modified.")

        # ================================================================
        # 2. VERIFY CURRENT DATA STATE
        # ================================================================
        print("\n[2] Verifying current table state")

        table_before = spark.table(DEMO_TABLE)
        record_count_before = table_before.count()

        print(f"Current records: {record_count_before}")

        if record_count_before != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_RECORD_COUNT} records, "
                f"but found {record_count_before}."
            )

        print(
            f"Record count = {EXPECTED_RECORD_COUNT} [PASS]"
        )

        # ================================================================
        # 3. SCHEMA BEFORE
        # ================================================================
        print("\n[3] Schema BEFORE evolution")

        table_before.printSchema()

        columns_before = table_before.columns

        print(f"Column count before: {len(columns_before)}")
        print(f"Columns before     : {columns_before}")

        # Important rerun guard
        if NEW_COLUMN in columns_before:
            raise RuntimeError(
                f"Column '{NEW_COLUMN}' already exists.\n"
                "Step 7.4 appears to have already completed.\n"
                "Do NOT run ALTER TABLE again."
            )

        print(
            f"Column '{NEW_COLUMN}' does not exist yet [PASS]"
        )

        # ================================================================
        # 4. DESCRIBE TABLE BEFORE
        # ================================================================
        print("\n[4] DESCRIBE TABLE BEFORE evolution")

        spark.sql(
            f"""
            DESCRIBE TABLE {DEMO_TABLE}
            """
        ).show(
            100,
            truncate=False
        )

        # ================================================================
        # 5. SNAPSHOTS BEFORE
        # ================================================================
        print("\n[5] Snapshot state BEFORE schema evolution")

        snapshots_before_df = get_snapshots(spark)

        snapshots_before_df.show(
            truncate=False
        )

        snapshot_count_before = snapshots_before_df.count()

        print(
            f"Snapshot count before ALTER TABLE: "
            f"{snapshot_count_before}"
        )

        if snapshot_count_before != EXPECTED_SNAPSHOT_COUNT:
            raise RuntimeError(
                f"Expected {EXPECTED_SNAPSHOT_COUNT} snapshots "
                f"before ALTER TABLE, "
                f"but found {snapshot_count_before}."
            )

        print(
            f"Expected snapshot state "
            f"({EXPECTED_SNAPSHOT_COUNT} snapshots) [PASS]"
        )

        # ================================================================
        # 6. ALTER TABLE ADD COLUMN
        # ================================================================
        print("\n[6] Performing controlled Schema Evolution")

        print(
            f"Adding column: {NEW_COLUMN} {NEW_COLUMN_TYPE}"
        )

        spark.sql(
            f"""
            ALTER TABLE {DEMO_TABLE}
            ADD COLUMN {NEW_COLUMN} {NEW_COLUMN_TYPE}
            COMMENT '{NEW_COLUMN_COMMENT}'
            """
        )

        print("ALTER TABLE ADD COLUMN completed.")

        # ================================================================
        # 7. SCHEMA AFTER
        # ================================================================
        print("\n[7] Schema AFTER evolution")

        table_after = spark.table(DEMO_TABLE)

        table_after.printSchema()

        columns_after = table_after.columns

        print(f"Column count after : {len(columns_after)}")
        print(f"Columns after      : {columns_after}")

        if NEW_COLUMN not in columns_after:
            raise RuntimeError(
                f"Schema Evolution failed: "
                f"'{NEW_COLUMN}' was not added."
            )

        if len(columns_after) != len(columns_before) + 1:
            raise RuntimeError(
                "Unexpected column count after ADD COLUMN.\n"
                f"Before: {len(columns_before)}\n"
                f"After : {len(columns_after)}"
            )

        print(
            f"New column '{NEW_COLUMN}' exists [PASS]"
        )

        print(
            f"Column count transition: "
            f"{len(columns_before)} -> "
            f"{len(columns_after)} [PASS]"
        )

        # ================================================================
        # 8. DESCRIBE TABLE AFTER
        # ================================================================
        print("\n[8] DESCRIBE TABLE AFTER evolution")

        spark.sql(
            f"""
            DESCRIBE TABLE {DEMO_TABLE}
            """
        ).show(
            100,
            truncate=False
        )

        # ================================================================
        # 9. VERIFY DATA PRESERVATION
        # ================================================================
        print("\n[9] Verifying existing data")

        record_count_after = table_after.count()

        print(
            f"Records before ALTER TABLE : "
            f"{record_count_before}"
        )

        print(
            f"Records after ALTER TABLE  : "
            f"{record_count_after}"
        )

        if record_count_after != record_count_before:
            raise RuntimeError(
                "Record count changed during Schema Evolution."
            )

        print(
            "Existing record count preserved [PASS]"
        )

        # ================================================================
        # 10. VERIFY OLD ROWS HAVE NULL
        # ================================================================
        print(
            f"\n[10] Verifying existing rows for "
            f"new column '{NEW_COLUMN}'"
        )

        null_count = (
            table_after
            .filter(f"{NEW_COLUMN} IS NULL")
            .count()
        )

        non_null_count = (
            table_after
            .filter(f"{NEW_COLUMN} IS NOT NULL")
            .count()
        )

        print(
            f"NULL {NEW_COLUMN} records     : "
            f"{null_count}"
        )

        print(
            f"Non-NULL {NEW_COLUMN} records : "
            f"{non_null_count}"
        )

        if null_count != record_count_after:
            raise RuntimeError(
                f"Expected all {record_count_after} existing "
                f"records to contain NULL for '{NEW_COLUMN}'."
            )

        if non_null_count != 0:
            raise RuntimeError(
                f"Unexpected non-NULL values in '{NEW_COLUMN}'."
            )

        print(
            "Existing rows remain readable with "
            "NULL in the new column [PASS]"
        )

        # ================================================================
        # 11. SHOW EVIDENCE
        # ================================================================
        print("\n[11] Sample records after Schema Evolution")

        table_after.select(
            "timestamp",
            "temperature_c",
            "source",
            NEW_COLUMN,
        ).orderBy(
            "timestamp"
        ).show(
            5,
            truncate=False
        )

        print(
            "\nICEBERG_DEMO record after Schema Evolution:"
        )

        demo_row_df = (
            table_after
            .filter("source = 'ICEBERG_DEMO'")
            .select(
                "timestamp",
                "temperature_c",
                "source",
                "source_file",
                NEW_COLUMN,
            )
        )

        demo_row_count = demo_row_df.count()

        demo_row_df.show(
            truncate=False
        )

        if demo_row_count != 1:
            raise RuntimeError(
                f"Expected exactly one ICEBERG_DEMO record, "
                f"but found {demo_row_count}."
            )

        print(
            "Original Snapshot Demo record preserved [PASS]"
        )

        # ================================================================
        # 12. SNAPSHOTS AFTER
        # ================================================================
        print(
            "\n[12] Snapshot state AFTER schema evolution"
        )

        snapshots_after_df = get_snapshots(spark)

        snapshots_after_df.show(
            truncate=False
        )

        snapshot_count_after = snapshots_after_df.count()

        print(
            f"Snapshots before ALTER TABLE : "
            f"{snapshot_count_before}"
        )

        print(
            f"Snapshots after ALTER TABLE  : "
            f"{snapshot_count_after}"
        )

        # Do not assume behavior — observe runtime.
        if snapshot_count_after == snapshot_count_before:

            print(
                "ADD COLUMN changed table schema metadata "
                "without creating a new data snapshot [PASS]"
            )

            snapshot_behavior = (
                "Metadata-only schema evolution; "
                "data snapshot count unchanged."
            )

        elif snapshot_count_after > snapshot_count_before:

            print(
                "A new snapshot was observed after "
                "Schema Evolution."
            )

            snapshot_behavior = (
                f"Snapshot count changed from "
                f"{snapshot_count_before} to "
                f"{snapshot_count_after}."
            )

        else:
            raise RuntimeError(
                "Snapshot count unexpectedly decreased."
            )

        # ================================================================
        # 13. FINAL CHECK
        # ================================================================
        print(
            "\n[13] Final Schema Evolution verification"
        )

        final_df = spark.table(DEMO_TABLE)

        final_count = final_df.count()

        final_columns = final_df.columns

        if final_count != EXPECTED_RECORD_COUNT:
            raise RuntimeError(
                "Final record count verification failed."
            )

        if NEW_COLUMN not in final_columns:
            raise RuntimeError(
                "Final schema verification failed."
            )

        print(
            "Final table state verification [PASS]"
        )

        # ================================================================
        # RESULT
        # ================================================================
        print()
        print("=" * 100)
        print("ICEBERG SCHEMA EVOLUTION DEMO: PASS")
        print("=" * 100)

        print(f"Table                   : {DEMO_TABLE}")

        print(
            f"Records                 : "
            f"{record_count_before} -> "
            f"{record_count_after}"
        )

        print(
            f"Columns                 : "
            f"{len(columns_before)} -> "
            f"{len(columns_after)}"
        )

        print(
            f"Added column            : "
            f"{NEW_COLUMN} {NEW_COLUMN_TYPE}"
        )

        print(
            f"Old rows with NULL      : "
            f"{null_count}"
        )

        print(
            f"Data snapshots          : "
            f"{snapshot_count_before} -> "
            f"{snapshot_count_after}"
        )

        print(
            f"Snapshot behavior       : "
            f"{snapshot_behavior}"
        )

        print()
        print(
            "RESULT: Apache Iceberg successfully evolved "
            "the table schema while preserving all existing data."
        )

        print()
        print(
            f"The table changed from "
            f"{len(columns_before)} to "
            f"{len(columns_after)} columns, while the "
            f"record count remained {record_count_after}."
        )

        print()
        print(
            f"All existing records expose the new optional "
            f"column '{NEW_COLUMN}' as NULL."
        )

        print()
        print(
            "IMPORTANT: Do NOT rerun this script after PASS."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()