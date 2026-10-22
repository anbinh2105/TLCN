
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


STREAM_TABLE = "lakehouse.gold.water_level_hourly_stream"
BATCH_TABLE = "lakehouse.gold.hourly_hydrology"


def main():
    spark = create_spark_session("S6-Precheck")

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        print("\n=== S6.1 TABLE CHECK ===")

        for table in [STREAM_TABLE, BATCH_TABLE]:
            if not spark.catalog.tableExists(table):
                raise RuntimeError(
                    f"Missing table: {table}"
                )

            print(f"FOUND: {table}")

        stream = spark.table(STREAM_TABLE)
        batch = spark.table(BATCH_TABLE)

        print("\n=== STREAM SCHEMA ===")
        stream.printSchema()

        print("\n=== BATCH SCHEMA ===")
        batch.printSchema()

        print("\n=== STREAM DATA ===")
        stream.orderBy(
            "hour_timestamp"
        ).show(10, truncate=False)

        print("\n=== BATCH SAMPLE ===")
        batch.show(5, truncate=False)

        print("\n=== STREAM TIME RANGE ===")

        stream.agg(
            F.min("hour_timestamp").alias("min_time"),
            F.max("hour_timestamp").alias("max_time"),
            F.count("*").alias("rows")
        ).show(truncate=False)

        print("\n=== BATCH TIME COLUMNS ===")

        time_columns = [
            name for name, dtype in batch.dtypes
            if dtype in ("timestamp", "timestamp_ntz")
        ]

        print("TIMESTAMP COLUMNS:", time_columns)

        print("\n=== BATCH ROWS FOR STREAM HOURS ===")

        # Identify matching hours using the actual
        # timestamp column rather than guessing.
        if "hour_start" in batch.columns:
            batch_time_col = "hour_start"
        elif "timestamp" in batch.columns:
            batch_time_col = "timestamp"
        elif "hour_timestamp" in batch.columns:
            batch_time_col = "hour_timestamp"
        else:
            batch_time_col = None

        if batch_time_col is None:
            print(
                "No recognized batch hour column. "
                "Review schema before joining."
            )
        else:
            stream_hours = (
                stream.select(
                    F.col("hour_timestamp").alias(
                        "matching_hour"
                    )
                )
                .distinct()
            )

            matched_batch = (
                batch.alias("b")
                .join(
                    stream_hours.alias("s"),
                    F.col(f"b.{batch_time_col}")
                    == F.col("s.matching_hour"),
                    "inner"
                )
            )

            print(
                "MATCHED BATCH ROWS:",
                matched_batch.count()
            )

            matched_batch.orderBy(
                F.col(f"b.{batch_time_col}")
            ).show(20, truncate=False)

            print("\n=== BATCH MATCH KEY CHECK ===")

            matched_batch.groupBy(
                F.col(f"b.{batch_time_col}")
            ).count().orderBy(
                F.col(f"b.{batch_time_col}")
            ).show(truncate=False)

        print("\n=== S6.1 RESULT ===")
        print("STATUS: PRECHECK_COMPLETE")
        print("NO ICEBERG WRITE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
