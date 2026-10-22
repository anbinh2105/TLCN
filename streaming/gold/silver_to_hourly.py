import tempfile
import argparse

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.silver.common import create_spark_session


SOURCE_TABLE = "lakehouse.silver.water_level_stream"
TARGET_TABLE = "lakehouse.gold.water_level_hourly_stream"

# Water level is expected approximately every 15 minutes.
EXPECTED_OBS_PER_HOUR = 4


def validate_source(df):
    required = {
        "event_id", "timestamp", "station_code",
        "water_level_m", "simulation"
    }

    missing = required - set(df.columns)

    if missing:
        raise RuntimeError(
            f"Missing Silver columns: {sorted(missing)}"
        )

    invalid = df.filter(
        F.col("event_id").isNull()
        | F.col("timestamp").isNull()
        | F.col("station_code").isNull()
        | F.col("water_level_m").isNull()
        | F.isnan("water_level_m")
    ).count()

    if invalid:
        raise RuntimeError(
            f"Invalid Silver records: {invalid}"
        )

    duplicate_ids = (
        df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_ids:
        raise RuntimeError(
            f"Duplicate event IDs: {duplicate_ids}"
        )

    # One observation per station and timestamp.
    duplicate_times = (
        df.groupBy("station_code", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_times:
        raise RuntimeError(
            "Multiple observations for the same "
            "station and timestamp"
        )


def build_hourly(df):
    observations = df.withColumn(
        "hour_timestamp",
        F.date_trunc("hour", F.col("timestamp"))
    )

    hourly = (
        observations
        .groupBy("station_code", "hour_timestamp")
        .agg(
            F.avg("water_level_m").alias(
                "water_level_mean"
            ),
            F.min("water_level_m").alias(
                "water_level_min"
            ),
            F.max("water_level_m").alias(
                "water_level_max"
            ),
            F.count("*").cast("int").alias(
                "water_level_obs_count"
            ),
            F.min("timestamp").alias(
                "first_observation_at"
            ),
            F.max("timestamp").alias(
                "last_observation_at"
            ),
            F.min(
                F.col("simulation").cast("int")
            ).alias("min_simulation"),
            F.max(
                F.col("simulation").cast("int")
            ).alias("max_simulation"),
        )
    )

    # The last observation must be selected by event
    # timestamp, not by Spark's arbitrary row order.
    latest_window = (
        Window.partitionBy(
            "station_code", "hour_timestamp"
        )
        .orderBy(
            F.col("timestamp").desc(),
            F.col("event_id").desc()
        )
    )

    latest = (
        observations
        .withColumn(
            "rn", F.row_number().over(latest_window)
        )
        .filter(F.col("rn") == 1)
        .select(
            "station_code",
            "hour_timestamp",
            F.col("water_level_m").alias(
                "water_level_last"
            )
        )
    )

    result = (
        hourly.join(
            latest,
            on=["station_code", "hour_timestamp"],
            how="inner"
        )
        .withColumn(
            "expected_obs_count",
            F.lit(EXPECTED_OBS_PER_HOUR)
        )
        .withColumn(
            "hour_quality_flag",
            F.when(
                F.col("water_level_obs_count")
                == EXPECTED_OBS_PER_HOUR,
                F.lit("COMPLETE")
            )
            .when(
                F.col("water_level_obs_count")
                < EXPECTED_OBS_PER_HOUR,
                F.lit("PARTIAL")
            )
            .otherwise(F.lit("EXCESS"))
        )
        .withColumn(
            "simulation",
            F.col("max_simulation") == 1
        )
        .withColumn(
            "mixed_simulation",
            F.col("min_simulation")
            != F.col("max_simulation")
        )
        .drop("min_simulation", "max_simulation")
    )

    return result.select(
        "hour_timestamp",
        "station_code",
        "water_level_mean",
        "water_level_min",
        "water_level_max",
        "water_level_last",
        "water_level_obs_count",
        "expected_obs_count",
        "hour_quality_flag",
        "first_observation_at",
        "last_observation_at",
        "simulation",
        "mixed_simulation"
    )


def create_target(spark):
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            hour_timestamp TIMESTAMP,
            station_code STRING,
            water_level_mean DOUBLE,
            water_level_min DOUBLE,
            water_level_max DOUBLE,
            water_level_last DOUBLE,
            water_level_obs_count INT,
            expected_obs_count INT,
            hour_quality_flag STRING,
            first_observation_at TIMESTAMP,
            last_observation_at TIMESTAMP,
            simulation BOOLEAN,
            mixed_simulation BOOLEAN,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true"
    )
    args = parser.parse_args()

    spark = create_spark_session(
        "S5-Silver-Streaming-To-Gold-Hourly"
    )

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        print("\n=== S5 SOURCE CHECK ===")

        source = spark.table(SOURCE_TABLE)
        source_count = source.count()

        print("SOURCE TABLE:", SOURCE_TABLE)
        print("SOURCE RECORDS:", source_count)

        if source_count == 0:
            raise RuntimeError("Silver Streaming is empty")

        validate_source(source)

        checkpoint_dir = tempfile.mkdtemp(
            prefix="tlcn_s5_checkpoint_"
        )

        spark.sparkContext.setCheckpointDir(checkpoint_dir)

        hourly = build_hourly(source).checkpoint(eager=True).cache()

        try:
            hourly_count = hourly.count()

            if hourly_count == 0:
                raise RuntimeError("No hourly groups")

            print("\n=== S5 AGGREGATION ===")
            print("HOURLY GROUPS:", hourly_count)

            hourly.orderBy(
                "hour_timestamp", "station_code"
            ).show(50, truncate=False)

            if args.dry_run:
                print("STATUS: DRY_RUN_PASS")
                print("NO ICEBERG WRITE")
                return

            create_target(spark)

            # Ensure no duplicate keys already exist
            # in the destination before MERGE.
            target = spark.table(TARGET_TABLE)

            duplicate_target_keys = (
                target.groupBy(
                    "station_code", "hour_timestamp"
                )
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            if duplicate_target_keys:
                raise RuntimeError(
                    "Target has duplicate hourly keys"
                )

            hourly.createOrReplaceTempView(
                "s5_hourly_source"
            )

            # Only update if the aggregated values
            # have actually changed.
            change_columns = [
                "water_level_mean",
                "water_level_min",
                "water_level_max",
                "water_level_last",
                "water_level_obs_count",
                "expected_obs_count",
                "hour_quality_flag",
                "first_observation_at",
                "last_observation_at",
                "simulation",
                "mixed_simulation",
            ]

            change_condition = " OR ".join(
                f"NOT (t.{col} <=> s.{col})"
                for col in change_columns
            )

            update_assignments = ",\n".join(
                f"t.{col} = s.{col}"
                for col in change_columns
            )

            insert_columns = [
                "hour_timestamp",
                "station_code",
                *change_columns,
                "gold_processed_at",
            ]

            insert_values = [
                "s.hour_timestamp",
                "s.station_code",
                *[
                    f"s.{col}"
                    for col in change_columns
                ],
                "current_timestamp()",
            ]

            spark.sql(f"""
                MERGE INTO {TARGET_TABLE} t
                USING s5_hourly_source s
                ON (
                    t.station_code = s.station_code
                    AND t.hour_timestamp = s.hour_timestamp
                )
                WHEN MATCHED AND (
                    {change_condition}
                )
                THEN UPDATE SET
                    {update_assignments},
                    t.gold_processed_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (
                    {", ".join(insert_columns)}
                )
                VALUES (
                    {", ".join(insert_values)}
                )
            """)

            result = spark.table(TARGET_TABLE)

            matched = (
                hourly.alias("s")
                .join(
                    result.alias("t"),
                    on=[
                        "station_code",
                        "hour_timestamp"
                    ],
                    how="inner"
                )
            )

            mismatches = matched.filter(
                " OR ".join(
                    f"NOT (s.{col} <=> t.{col})"
                    for col in change_columns
                )
            ).count()

            matched_count = matched.count()

            if (
                matched_count != hourly_count
                or mismatches != 0
            ):
                raise RuntimeError(
                    "S5 verification failed"
                )

            print("\n=== S5 RESULT ===")
            print("STATUS: PASS")
            print("TABLE:", TARGET_TABLE)
            print("VERIFIED HOURLY GROUPS:", matched_count)
            print("VALUE MISMATCHES:", mismatches)
            print("TOTAL TABLE ROWS:", result.count())

            result.orderBy(
                "hour_timestamp", "station_code"
            ).show(50, truncate=False)

        finally:
            hourly.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
