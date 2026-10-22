
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from transformation.silver.common import create_spark_session


STREAM_TABLE = "lakehouse.gold.water_level_hourly_stream"
BATCH_TABLE = "lakehouse.gold.hourly_hydrology"

STATION_LOCATION = {
    "039803": "CAN_THO_URBAN_CENTER"
}


def validate_unique(df, keys, label):
    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates:
        raise RuntimeError(
            f"{label}: {duplicates} duplicate keys"
        )


def build_integrated(stream, batch):
    # Explicit station-to-location mapping.
    mapping_rows = [
        (station, location)
        for station, location in STATION_LOCATION.items()
    ]

    mapping = stream.sparkSession.createDataFrame(
        mapping_rows,
        ["station_code", "location_id"]
    )

    unmapped = (
        stream.select("station_code").distinct()
        .join(mapping, "station_code", "left_anti")
        .count()
    )

    if unmapped:
        raise RuntimeError(
            f"Unmapped streaming stations: {unmapped}"
        )

    s = (
        stream.join(mapping, "station_code", "inner")
        .alias("s")
    )

    b = batch.alias("b")

    joined = s.join(
        b,
        (F.col("s.hour_timestamp") == F.col("b.hour_start"))
        & (F.col("s.location_id") == F.col("b.location_id")),
        "left"
    )

    # Never silently accept missing batch enrichment.
    missing_batch = joined.filter(
        F.col("b.hour_start").isNull()
    ).count()

    if missing_batch:
        raise RuntimeError(
            f"Missing Gold batch enrichment: {missing_batch}"
        )

    # Batch water-level station, when populated,
    # must agree with the streaming station.
    station_conflicts = joined.filter(
        F.col("b.water_level_station_code").isNotNull()
        & (
            F.col("b.water_level_station_code")
            != F.col("s.station_code")
        )
    ).count()

    if station_conflicts:
        raise RuntimeError(
            f"Water-level station conflicts: {station_conflicts}"
        )

    result = joined.select(
        F.col("s.hour_timestamp").alias("hour_start"),
        F.col("s.location_id"),
        F.col("s.station_code"),

        # Water level always comes from streaming.
        F.col("s.water_level_mean"),
        F.col("s.water_level_min"),
        F.col("s.water_level_max"),
        F.col("s.water_level_last"),
        F.col("s.water_level_obs_count"),
        F.col("s.expected_obs_count"),
        F.col("s.hour_quality_flag")
            .alias("water_level_quality_flag"),
        F.col("s.first_observation_at"),
        F.col("s.last_observation_at"),

        # Weather from Gold batch.
        F.col("b.temperature_c"),
        F.col("b.humidity_pct"),
        F.col("b.pressure_hpa"),
        F.col("b.wind_speed"),
        F.col("b.wind_direction"),
        F.col("b.weather_quality_flag"),
        F.col("b.weather_present"),
        F.col("b.weather_usable"),

        # Rainfall from Gold batch.
        F.col("b.rainfall_mm"),
        F.col("b.rainfall_quality_flag"),
        F.col("b.rainfall_present"),
        F.col("b.rainfall_usable"),

        # Discharge from Gold batch.
        F.col("b.discharge_m3s"),
        F.col("b.discharge_quality_flag"),
        F.col("b.discharge_alignment_method"),
        F.col("b.discharge_source_date"),
        F.col("b.discharge_source_temporal_grain"),
        F.col("b.discharge_is_hourly_observation"),
        F.col("b.discharge_station"),
        F.col("b.discharge_present"),
        F.col("b.discharge_usable"),

        # Tide from Gold batch; preserve NULL.
        F.col("b.tide_height_m"),
        F.col("b.tide_quality_flag"),
        F.col("b.tide_is_recovered"),
        F.col("b.tide_recovery_method"),
        F.col("b.tide_present"),
        F.col("b.tide_usable"),
        F.col("b.tide_reference_location"),
        F.col("b.tide_publisher"),

        # Streaming provenance.
        F.col("s.simulation"),
        F.col("s.mixed_simulation"),
        F.lit("STREAM_REPLAY").alias("water_level_origin"),
        F.lit("GOLD_BATCH").alias("enrichment_origin"),
        F.col("s.gold_processed_at")
            .alias("stream_gold_processed_at"),
        F.col("b.gold_processed_at")
            .alias("batch_gold_processed_at")
    )

    # A partial hour is present, but not fully usable
    # under the current completeness rule.
    result = result.withColumn(
        "water_level_present",
        F.lit(True)
    ).withColumn(
        "water_level_usable",
        F.col("water_level_quality_flag") == "COMPLETE"
    )

    source_names = [
        "weather",
        "rainfall",
        "discharge",
        "water_level",
        "tide"
    ]

    present_expr = sum(
        [
            F.when(
                F.coalesce(F.col(f"{name}_present"), F.lit(False)),
                1
            ).otherwise(0)
            for name in source_names
        ]
    )

    usable_expr = sum(
        [
            F.when(
                F.coalesce(F.col(f"{name}_usable"), F.lit(False)),
                1
            ).otherwise(0)
            for name in source_names
        ]
    )

    result = (
        result
        .withColumn(
            "present_source_count",
            present_expr.cast("int")
        )
        .withColumn(
            "usable_source_count",
            usable_expr.cast("int")
        )
        .withColumn(
            "all_five_present",
            F.col("present_source_count") == 5
        )
        .withColumn(
            "all_five_usable",
            F.col("usable_source_count") == 5
        )
    )

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True
    )
    args = parser.parse_args()

    spark = create_spark_session("S6-Integration-DryRun")

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        stream = spark.table(STREAM_TABLE)
        batch = spark.table(BATCH_TABLE)

        print("\n=== S6 SOURCE VALIDATION ===")

        validate_unique(
            stream,
            ["hour_timestamp", "station_code"],
            "STREAM"
        )

        validate_unique(
            batch,
            ["hour_start", "location_id"],
            "BATCH"
        )

        source_count = stream.count()
        print("STREAM ROWS:", source_count)

        if source_count == 0:
            raise RuntimeError("Empty streaming source")

        print("\n=== S6 INTEGRATION ===")

        result = build_integrated(stream, batch).cache()

        try:
            integrated_count = result.count()

            print("INTEGRATED ROWS:", integrated_count)

            if integrated_count != source_count:
                raise RuntimeError(
                    "JOIN changed streaming row count"
                )

            validate_unique(
                result,
                ["hour_start", "location_id", "station_code"],
                "INTEGRATED"
            )

            result.select(
                "hour_start",
                "location_id",
                "station_code",
                "water_level_mean",
                "water_level_obs_count",
                "water_level_quality_flag",
                "rainfall_mm",
                "temperature_c",
                "discharge_m3s",
                "tide_height_m",
                "present_source_count",
                "usable_source_count",
                "simulation"
            ).orderBy("hour_start").show(
                truncate=False
            )

            print("\n=== S6 QUALITY SUMMARY ===")

            result.groupBy(
                "water_level_quality_flag",
                "present_source_count",
                "usable_source_count"
            ).count().show(truncate=False)

            print("\n=== S6 RESULT ===")
            print("STATUS: DRY_RUN_PASS")
            print("INTEGRATED ROWS:", integrated_count)
            print("NO ICEBERG WRITE")

        finally:
            result.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
