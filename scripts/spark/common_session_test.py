
import os
import sys

sys.path.insert(0, "/opt/project")

from transformation.silver.common import create_spark_session


def main():
    spark = create_spark_session(
        app_name="tlcn-common-session-test"
    )

    try:
        print("SPARK_MODE:", os.getenv("SPARK_MODE"))
        print("Spark master:", spark.sparkContext.master)
        print("Spark version:", spark.version)

        assert spark.sparkContext.master == (
            "spark://spark-master:7077"
        ), "Spark is not running in Standalone mode"

        print("\n=== READ SILVER WEATHER ===")

        df = spark.table("lakehouse.silver.weather")

        df.select(
            "timestamp",
            "temperature_c",
            "humidity_pct",
        ).limit(5).show(truncate=False)

        print("\nCOMMON SESSION TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
