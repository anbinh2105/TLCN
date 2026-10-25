
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("TLCN-Airflow-Spark-Smoke-Test")
        .getOrCreate()
    )

    try:
        # Thực hiện phép tính phân tán trên Spark Worker
        result = (
            spark.range(0, 1000, 1, 4)
            .selectExpr("sum(id) AS total")
            .collect()[0]["total"]
        )

        expected = 499500

        print(f"SPARK_SMOKE_TEST_RESULT={result}", flush=True)

        if result != expected:
            raise RuntimeError(
                f"Unexpected result: {result}, expected {expected}"
            )

        print("SPARK_SMOKE_TEST_PASS", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
