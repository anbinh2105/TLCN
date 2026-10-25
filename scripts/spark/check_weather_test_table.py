
import sys

sys.path.insert(0, "/opt/project")

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

TABLE = "lakehouse.silver.weather_docker_test"


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="check-weather-test-table"
        )

        exists = spark.catalog.tableExists(TABLE)

        print(f"Table: {TABLE}")
        print(f"Exists: {exists}")

        if exists:
            print("STOP: Test table already exists.")
        else:
            print("PASS: Test table name is available.")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
