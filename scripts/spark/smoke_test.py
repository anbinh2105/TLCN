
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("tlcn-spark-smoke-test")
        .getOrCreate()
    )

    try:
        print("Spark version:", spark.version)
        print("Spark master:", spark.sparkContext.master)

        df = spark.range(1, 1001)

        result = df.agg(
            F.count("*").alias("row_count"),
            F.sum("id").alias("id_sum"),
        ).collect()[0]

        print("Row count:", result["row_count"])
        print("ID sum:", result["id_sum"])

        assert result["row_count"] == 1000
        assert result["id_sum"] == 500500

        print("SPARK SMOKE TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
