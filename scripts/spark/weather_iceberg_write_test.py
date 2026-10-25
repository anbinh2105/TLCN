
import sys

sys.path.insert(0, "/opt/project")

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

from transformation.silver.weather_to_silver import (
    read_weather_bronze,
    transform_weather,
)

TABLE = "lakehouse.silver.weather_docker_test"
EXPECTED_ROWS = 73656


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="weather-iceberg-write-test"
        )

        print("Spark master:", spark.sparkContext.master)

        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Not running on Spark Standalone")

        # Check again immediately before writing.
        if spark.catalog.tableExists(TABLE):
            raise RuntimeError(
                f"STOP: Test table already exists: {TABLE}"
            )

        print("\n=== READ BRONZE ===")
        bronze_df = read_weather_bronze(spark)

        print("\n=== TRANSFORM SILVER ===")
        silver_df = transform_weather(bronze_df)

        source_count = silver_df.count()
        print("Source rows:", source_count)

        if source_count != EXPECTED_ROWS:
            raise AssertionError(
                f"Unexpected row count: {source_count}"
            )

        print("\n=== WRITE ICEBERG TEST TABLE ===")

        # create() fails if the table already exists.
        # Never use createOrReplace() here.
        (
            silver_df.writeTo(TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )

        print("\n=== READ BACK ICEBERG ===")

        persisted_df = spark.table(TABLE)

        persisted_count = persisted_df.count()

        print("Persisted rows:", persisted_count)
        persisted_df.printSchema()
        persisted_df.limit(5).show(truncate=False)

        if persisted_count != source_count:
            raise AssertionError(
                "Persisted row count does not match source"
            )

        print("\nWEATHER ICEBERG WRITE TEST: PASS")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
