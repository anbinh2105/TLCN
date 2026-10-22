
from transformation.silver.common import create_spark_session
from pyspark.sql import functions as F


def main():
    print("=== SPARK RUNTIME TEST ===", flush=True)

    spark = create_spark_session(
        "S76-Spark-Runtime-Test"
    )

    try:
        print("SPARK VERSION:", spark.version, flush=True)
        print(
            "SPARK MASTER:",
            spark.sparkContext.master,
            flush=True,
        )
        print(
            "TIMEZONE:",
            spark.conf.get(
                "spark.sql.session.timeZone"
            ),
            flush=True,
        )

        print("\n=== TEST 1: BASIC SPARK ===", flush=True)

        df = spark.range(10).withColumn(
            "value", F.col("id") * 2
        )

        print("COUNT:", df.count(), flush=True)

        print("\n=== TEST 2: SILVER ICEBERG ===", flush=True)

        silver = spark.table(
            "lakehouse.silver.water_level_stream"
        )

        print("SILVER COUNT:", silver.count(), flush=True)

        print("\n=== TEST 3: GOLD ICEBERG ===", flush=True)

        gold = spark.table(
            "lakehouse.gold.water_level_hourly_stream"
        )

        print("GOLD COUNT:", gold.count(), flush=True)

        print("\nSPARK RUNTIME: PASS", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
