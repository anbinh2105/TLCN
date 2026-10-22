from transformation.silver.common import create_spark_session

spark = create_spark_session(
    app_name="CheckGoldWaterLevel",
    master="local[2]",
)

try:
    table = "lakehouse.gold.water_level_hourly"

    exists = spark.catalog.tableExists(table)

    print("Gold table exists:", exists)

    if exists:
        print("Gold record count:", spark.table(table).count())

        spark.sql(
            f"SELECT snapshot_id, operation, committed_at "
            f"FROM {table}.snapshots"
        ).show(truncate=False)

finally:
    spark.stop()