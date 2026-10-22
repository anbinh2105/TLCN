from transformation.silver.common import (
    create_spark_session,
    get_silver_table,
)


def main():
    spark = create_spark_session(
        app_name="InspectSilverSchema"
    )

    try:
        table_name = get_silver_table(
            "water_level"
        )

        df = spark.table(table_name)

        print("=" * 100)
        print("WATER LEVEL SILVER SCHEMA")
        print("=" * 100)

        print(f"Table: {table_name}")
        print(f"Records: {df.count():,}")

        print()
        print("Columns:")
        for column in df.columns:
            print(f"  - {column}")

        print()
        print("Spark schema:")
        df.printSchema()

        print()
        print("Sample:")
        df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()