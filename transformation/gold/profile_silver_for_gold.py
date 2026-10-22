
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.silver.common import create_spark_session


SOURCES = {
    "weather": "lakehouse.silver.weather",
    "rainfall": "lakehouse.silver.rainfall",
    "water_level": "lakehouse.silver.water_level",
    "tide": "lakehouse.silver.tide",
    "river_discharge": "lakehouse.silver.river_discharge",
}

# Fields whose names may help identify location/station keys.
KEY_CANDIDATES = [
    "station",
    "station_id",
    "station_code",
    "latitude",
    "longitude",
    "location_id",
]


def profile_source(spark, name, table):
    print("\n" + "=" * 90)
    print(f"SOURCE: {name.upper()}")
    print(f"TABLE : {table}")
    print("=" * 90)

    if not spark.catalog.tableExists(table):
        print("[ERROR] Table not found")
        return

    df = spark.table(table)

    print("\n[1] Schema")
    df.printSchema()

    print("\n[2] Columns")
    print(df.columns)

    print("\n[3] Record count")
    total = df.count()
    print(f"Total records: {total}")

    if "timestamp" not in df.columns:
        print("[WARNING] No timestamp column")
        return

    print("\n[4] Temporal coverage")

    df.agg(
        F.min("timestamp").alias("min_timestamp"),
        F.max("timestamp").alias("max_timestamp"),
        F.countDistinct("timestamp").alias(
            "distinct_timestamps"
        ),
        F.sum(
            F.when(F.col("timestamp").isNull(), 1)
             .otherwise(0)
        ).alias("null_timestamps")
    ).show(truncate=False)

    print("\n[5] Timestamp duplicates")

    duplicate_stats = (
        df.groupBy("timestamp")
        .count()
        .filter(F.col("count") > 1)
    )

    print(
        "Repeated timestamp values:",
        duplicate_stats.count()
    )

    print(
        "Note: repeated timestamps may be valid "
        "when multiple stations/locations exist."
    )

    print("\n[6] Temporal interval distribution")

    # Diagnostic only: sorted distinct timestamps.
    # Do not interpret cross-station gaps as sensor gaps.
    distinct_times = (
        df.select("timestamp")
        .filter(F.col("timestamp").isNotNull())
        .distinct()
    )

    w = Window.orderBy("timestamp")

    interval_df = (
        distinct_times
        .withColumn(
            "previous_timestamp",
            F.lag("timestamp").over(w)
        )
        .withColumn(
            "interval_minutes",
            (
                F.unix_timestamp("timestamp")
                - F.unix_timestamp("previous_timestamp")
            ) / 60.0
        )
    )

    (
        interval_df
        .filter(F.col("interval_minutes").isNotNull())
        .groupBy("interval_minutes")
        .count()
        .orderBy(F.desc("count"))
        .show(15, truncate=False)
    )

    print("\n[7] Candidate station/location columns")

    keys = [
        c for c in KEY_CANDIDATES
        if c in df.columns
    ]

    print("Available keys:", keys)

    for key in keys:
        print(f"\nKey: {key}")

        (
            df.groupBy(key)
            .count()
            .orderBy(F.desc("count"))
            .show(10, truncate=False)
        )

    print("\n[8] Quality and provenance columns")

    quality_columns = [
        c for c in df.columns
        if any(
            token in c.lower()
            for token in [
                "quality",
                "flag",
                "method",
                "recovered",
                "source",
            ]
        )
    ]

    print("Available columns:", quality_columns)

    for col_name in quality_columns:
        if (
            "flag" in col_name.lower()
            or "method" in col_name.lower()
        ):
            print(f"\nDistribution: {col_name}")

            (
                df.groupBy(col_name)
                .count()
                .orderBy(F.desc("count"))
                .show(20, truncate=False)
            )

    print("\n[9] Sample records")

    df.orderBy("timestamp").show(
        5, truncate=False
    )

    print(f"\n[{name.upper()} PROFILE COMPLETED]")


def main():
    spark = create_spark_session(
        app_name="GoldReadinessProfiling"
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        print("=" * 90)
        print("STEP 8.0 - GOLD TEMPORAL ALIGNMENT READINESS")
        print("=" * 90)

        print(
            "Spark timezone:",
            spark.conf.get("spark.sql.session.timeZone")
        )

        for name, table in SOURCES.items():
            try:
                profile_source(spark, name, table)
            except Exception as exc:
                print(f"\n[ERROR] {name}: {exc}")
                raise

        print("\n" + "=" * 90)
        print("GOLD READINESS PROFILING: COMPLETED")
        print("NO SILVER OR GOLD TABLES MODIFIED")
        print("=" * 90)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
