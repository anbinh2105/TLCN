
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


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="tlcn-weather-silver-dry-run"
        )

        print("Spark master:", spark.sparkContext.master)

        assert spark.sparkContext.master == (
            "spark://spark-master:7077"
        )

        # 1. Read Bronze
        print("\n=== READ WEATHER BRONZE ===")

        bronze_df = read_weather_bronze(spark)

        bronze_df.printSchema()
        bronze_df.limit(5).show(truncate=False)

        # 2. Transform to Silver
        print("\n=== TRANSFORM WEATHER ===")

        silver_df = transform_weather(bronze_df)

        silver_df.printSchema()
        silver_df.limit(5).show(truncate=False)

        # 3. Validate output
        required_columns = {
            "timestamp",
            "temperature_c",
            "humidity_pct",
            "quality_flag",
            "source_file",
        }

        missing = required_columns - set(silver_df.columns)

        if missing:
            raise AssertionError(
                f"Missing Silver columns: {sorted(missing)}"
            )

        
        from pyspark.sql import functions as F

        print("\n=== SILVER DATA QUALITY CHECK ===")

        total_rows = silver_df.count()

        null_timestamps = silver_df.filter(
            F.col("timestamp").isNull()
        ).count()

        duplicate_rows = (
            silver_df
            .groupBy("timestamp", "latitude", "longitude")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(f"Total Silver rows: {total_rows}")
        print(f"Null timestamps: {null_timestamps}")
        print(f"Duplicate timestamp/location groups: {duplicate_rows}")

        print("\nQuality flag distribution:")
        silver_df.groupBy("quality_flag").count().show()

        print("\nTimestamp range:")
        silver_df.agg(
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
        ).show(truncate=False)

        if total_rows == 0:
            raise AssertionError("Silver DataFrame is empty")

        if null_timestamps > 0:
            raise AssertionError("Silver contains null timestamps")

        print("\nSILVER DATA QUALITY CHECK: PASS")

        
        print("\nWEATHER SILVER DRY RUN: PASS")
        print("No Iceberg write operation was executed.")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
