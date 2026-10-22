from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.weather"
DEMO_NAMESPACE = "lakehouse.demo"
DEMO_TABLE = "lakehouse.demo.weather_iceberg_demo"

# Chỉ cần một tập nhỏ để demo Iceberg capabilities.
DEMO_LIMIT = 100


def main():
    spark = create_spark_session(
        app_name="IcebergCapabilityDemoSetup"
    )

    try:
        print("=" * 100)
        print("ICEBERG CAPABILITY DEMO — SETUP")
        print("=" * 100)

        # ------------------------------------------------------------------
        # 1. Kiểm tra Silver source
        # ------------------------------------------------------------------
        print(f"\n[1] Checking source table: {SOURCE_TABLE}")

        source_df = spark.table(SOURCE_TABLE)

        source_count = source_df.count()

        print(f"Source records: {source_count:,}")
        print("Source schema:")
        source_df.printSchema()

        # ------------------------------------------------------------------
        # 2. Tạo namespace demo
        # ------------------------------------------------------------------
        print(f"\n[2] Creating namespace: {DEMO_NAMESPACE}")

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS {DEMO_NAMESPACE}
            """
        )

        print("Namespace ready.")

        # ------------------------------------------------------------------
        # 3. Xóa demo table cũ nếu có
        #
        # Chỉ DROP table trong namespace demo.
        # TUYỆT ĐỐI không DROP bảng Silver.
        # ------------------------------------------------------------------
        print(f"\n[3] Resetting demo table: {DEMO_TABLE}")

        spark.sql(
            f"""
            DROP TABLE IF EXISTS {DEMO_TABLE}
            """
        )

        # ------------------------------------------------------------------
        # 4. Tạo Iceberg demo table từ 100 records của Silver Weather
        # ------------------------------------------------------------------
        print(
            f"\n[4] Creating demo table from "
            f"{DEMO_LIMIT} Silver Weather records"
        )

        spark.sql(
            f"""
            CREATE TABLE {DEMO_TABLE}
            USING iceberg
            AS
            SELECT *
            FROM {SOURCE_TABLE}
            ORDER BY timestamp
            LIMIT {DEMO_LIMIT}
            """
        )

        print("Demo table created.")

        # ------------------------------------------------------------------
        # 5. Verify record count
        # ------------------------------------------------------------------
        print("\n[5] Verifying demo table")

        demo_df = spark.table(DEMO_TABLE)

        demo_count = demo_df.count()

        print(f"Demo records: {demo_count:,}")

        if demo_count != DEMO_LIMIT:
            raise RuntimeError(
                f"Expected {DEMO_LIMIT} demo records, "
                f"but found {demo_count}"
            )

        # ------------------------------------------------------------------
        # 6. Verify schema
        # ------------------------------------------------------------------
        print("\nDemo schema:")
        demo_df.printSchema()

        # ------------------------------------------------------------------
        # 7. Show sample
        # ------------------------------------------------------------------
        print("\nDemo sample:")

        demo_df.orderBy("timestamp").show(
            10,
            truncate=False
        )

        # ------------------------------------------------------------------
        # 8. Kiểm tra Iceberg metadata — snapshots
        # ------------------------------------------------------------------
        print("\n[6] Iceberg snapshot metadata")

        snapshots_df = spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                parent_id,
                operation
            FROM {DEMO_TABLE}.snapshots
            ORDER BY committed_at
            """
        )

        snapshots_df.show(
            truncate=False
        )

        snapshot_count = snapshots_df.count()

        print(
            f"Snapshot count: {snapshot_count}"
        )

        if snapshot_count < 1:
            raise RuntimeError(
                "Iceberg demo table does not have a snapshot."
            )

        # ------------------------------------------------------------------
        # Final status
        # ------------------------------------------------------------------
        print()
        print("=" * 100)
        print("ICEBERG DEMO SETUP STATUS: PASS")
        print("=" * 100)

        print(f"Source table : {SOURCE_TABLE}")
        print(f"Demo table   : {DEMO_TABLE}")
        print(f"Demo records : {demo_count:,}")
        print(f"Snapshots    : {snapshot_count}")

        print()
        print(
            "Silver table was READ ONLY. "
            "All future capability demonstrations "
            "must use the demo table."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()