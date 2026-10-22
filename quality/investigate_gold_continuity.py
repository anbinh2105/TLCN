
from pyspark.sql import Window
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


TABLES = {
    "water_level": "lakehouse.gold.water_level_hourly",
    "rainfall": "lakehouse.gold.rainfall_hourly",
    "weather": "lakehouse.gold.weather_hourly",
    "tide": "lakehouse.gold.tide_hourly",
    "discharge": "lakehouse.gold.discharge_hourly",
}


def investigate(spark, name, table):
    print("\n" + "=" * 70)
    print(f"CONTINUITY: {name.upper()}")
    print("=" * 70)

    df = (
        spark.table(table)
        .select("hour_start")
        .where(F.col("hour_start").isNotNull())
        .distinct()
    )

    w = Window.orderBy("hour_start")

    grouped = (
        df
        .withColumn("previous", F.lag("hour_start").over(w))
        .withColumn(
            "new_segment",
            F.when(
                F.col("previous").isNull()
                | (
                    F.col("hour_start").cast("long")
                    - F.col("previous").cast("long")
                    != 3600
                ),
                1
            ).otherwise(0)
        )
        .withColumn(
            "segment_id",
            F.sum("new_segment").over(
                w.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow
                )
            )
        )
    )

    segments = (
        grouped
        .groupBy("segment_id")
        .agg(
            F.min("hour_start").alias("start"),
            F.max("hour_start").alias("end"),
            F.count("*").alias("continuous_hours"),
        )
    )

    summary = segments.agg(
        F.count("*").alias("segment_count"),
        F.max("continuous_hours").alias("longest_hours"),
        F.sum(
            F.when(F.col("continuous_hours") >= 24, 1)
            .otherwise(0)
        ).alias("segments_ge_24h"),
        F.sum(
            F.when(F.col("continuous_hours") >= 168, 1)
            .otherwise(0)
        ).alias("segments_ge_168h"),
        F.sum(
            F.when(F.col("continuous_hours") >= 720, 1)
            .otherwise(0)
        ).alias("segments_ge_720h"),
    ).first()

    print("Number of segments:", summary["segment_count"])
    print("Longest continuous hours:", summary["longest_hours"])
    print("Segments >= 24h:", summary["segments_ge_24h"])
    print("Segments >= 168h:", summary["segments_ge_168h"])
    print("Segments >= 720h:", summary["segments_ge_720h"])

    print("\nTOP 15 CONTINUOUS SEGMENTS")

    for row in (
        segments
        .orderBy(F.desc("continuous_hours"))
        .limit(15)
        .collect()
    ):
        print(
            f"{row['start']} -> {row['end']} "
            f"| {row['continuous_hours']} hours"
        )


def main():
    spark = create_spark_session(
        app_name="InvestigateGoldContinuity",
        master="local[2]"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )

        for name, table in TABLES.items():
            investigate(spark, name, table)

        print("\nGOLD CONTINUITY INVESTIGATION: COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
