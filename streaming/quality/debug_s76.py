
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session

from streaming.quality.validate_lineage import (
    read_bronze,
    BRONZE_SCHEMA,
    SILVER,
)


def main():
    rows, _ = read_bronze()

    spark = create_spark_session("S76-Debug")

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        bronze = spark.createDataFrame(
            rows, BRONZE_SCHEMA
        )

        silver = spark.table(SILVER)

        columns = [
            field.name
            for field in BRONZE_SCHEMA.fields
            if field.name != "event_id"
        ]

        joined = bronze.alias("b").join(
            silver.alias("s"),
            F.col("b.event_id") == F.col("s.event_id"),
            "full_outer"
        )

        print("\n=== S7.6 FIELD MISMATCH COUNTS ===")

        for name in columns:
            b = F.col(f"b.{name}")
            s = F.col(f"s.{name}")

            mismatch = joined.filter(
                ~b.eqNullSafe(s)
            ).count()

            print(f"{name}: {mismatch}")

        print("\n=== S7.6 TIMESTAMP SAMPLE ===")

        joined.select(
            F.col("b.event_id").alias("event_id"),
            F.col("b.timestamp").alias(
                "bronze_timestamp"
            ),
            F.col("s.timestamp").alias(
                "silver_timestamp"
            ),
            F.col("b.water_level_m").alias(
                "bronze_water_level"
            ),
            F.col("s.water_level_m").alias(
                "silver_water_level"
            ),
        ).orderBy("event_id").show(
            10, truncate=False
        )

        print("\n=== S7.6 DEBUG COMPLETE ===")
        print("NO DATA WRITTEN")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
