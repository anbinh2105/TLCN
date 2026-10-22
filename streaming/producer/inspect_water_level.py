
from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

TABLE = "lakehouse.silver.water_level"

def main():
    spark = None
    try:
        spark = create_spark_session(
            app_name="inspect-streaming-water-level",
            master="local[2]",
        )
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.sparkContext.setLogLevel("ERROR")

        df = spark.table(TABLE)

        print("\n=== SILVER WATER LEVEL SCHEMA ===")
        df.printSchema()

        print("\n=== SAMPLE ROWS ===")
        df.show(10, truncate=False)

        print("\n=== COLUMNS ===")
        print(df.columns)

    finally:
        if spark is not None:
            stop_spark(spark)

if __name__ == "__main__":
    main()
