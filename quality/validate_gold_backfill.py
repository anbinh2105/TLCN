
from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

BATCH_ID = "20261008_155724_backfill"

START = "2018-04-25 00:00:00"
END = "2026-09-18 23:00:00"

CONFIG = {
    "weather": {
        "silver": "lakehouse.silver.weather",
        "gold": "lakehouse.gold.weather_hourly",
        "keys": ["hour_start"],
        "values": [
            ("temperature_c", "temperature_c"),
            ("humidity_pct", "humidity_pct"),
            ("pressure_hpa", "pressure_hpa"),
            ("wind_speed", "wind_speed"),
            ("wind_direction", "wind_direction"),
            ("quality_flag", "weather_quality_flag"),
            ("latitude", "latitude"),
            ("longitude", "longitude"),
        ],
    },
    "rainfall": {
        "silver": "lakehouse.silver.rainfall",
        "gold": "lakehouse.gold.rainfall_hourly",
        "keys": ["hour_start"],
        "values": [
            ("rainfall_mm", "rainfall_mm"),
            ("quality_flag", "rainfall_quality_flag"),
            ("latitude", "latitude"),
            ("longitude", "longitude"),
        ],
    },
    "river_discharge": {
        "silver": "lakehouse.silver.river_discharge",
        "gold": "lakehouse.gold.discharge_hourly",
        "keys": ["station", "hour_start"],
        "values": [
            ("discharge_m3s", "discharge_m3s"),
            ("quality_flag", "discharge_quality_flag"),
        ],
    },
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def duplicate_count(df, keys):
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def validate_source(spark, source, cfg):
    print("\n" + "=" * 65)
    print(f"VALIDATING GOLD: {source}")
    print("=" * 65)

    silver = spark.table(cfg["silver"])
    gold = spark.table(cfg["gold"])

    total = gold.count()

    require(
        total == 73656,
        f"{source}: total={total}, expected=73656",
    )

    null_ts = gold.filter(
        F.col("hour_start").isNull()
    ).count()

    require(
        null_ts == 0,
        f"{source}: null hour_start={null_ts}",
    )

    duplicates = duplicate_count(gold, cfg["keys"])

    require(
        duplicates == 0,
        f"{source}: duplicate keys={duplicates}",
    )

    bounds = gold.agg(
        F.min("hour_start").alias("start"),
        F.max("hour_start").alias("end"),
        F.countDistinct("hour_start").alias("unique_hours"),
    ).first()

    require(
        str(bounds["start"]) == START,
        f"{source}: unexpected start={bounds['start']}",
    )
    require(
        str(bounds["end"]) == END,
        f"{source}: unexpected end={bounds['end']}",
    )
    require(
        bounds["unique_hours"] == 73656,
        f"{source}: unique hours={bounds['unique_hours']}",
    )

    backfill = gold.filter(
        F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    )

    backfill_count = backfill.count()

    require(
        backfill_count == 41904,
        f"{source}: backfill={backfill_count}, "
        "expected=41904",
    )

    file_count = backfill.select(
        "source_file"
    ).distinct().count()

    require(
        file_count == 16,
        f"{source}: source files={file_count}, expected=16",
    )

    silver_batch = silver.filter(
        F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    )

    if source == "river_discharge":
        silver_keys = silver_batch.select(
            F.col("station"),
            F.col("timestamp").alias("source_timestamp"),
            F.to_date("timestamp").alias("source_date"),
            *[
                F.col(a).alias(f"silver_{a}")
                for a, _ in cfg["values"]
            ],
        )

        joined = backfill.alias("g").join(
            silver_keys.alias("s"),
            (
                F.col("g.station") == F.col("s.station")
            )
            & (
                F.col("g.source_date")
                == F.col("s.source_date")
            ),
            "left",
        )

        missing_source = joined.filter(
            F.col("s.source_timestamp").isNull()
        ).count()

        require(
            missing_source == 0,
            f"Discharge: unmatched Silver rows={missing_source}",
        )

        invalid_alignment = backfill.filter(
            (F.col("alignment_method") != "DAILY_EXPANDED")
            | (F.col("source_temporal_grain") != "DAILY")
            | (~F.col("is_hourly_observation").eqNullSafe(False))
            | F.col("source_date").isNull()
            | (
                F.to_date("hour_start")
                != F.col("source_date")
            )
        ).count()

        require(
            invalid_alignment == 0,
            f"Discharge: invalid alignment={invalid_alignment}",
        )

        incorrect_days = (
            backfill.groupBy("station", "source_date")
            .count()
            .filter(F.col("count") != 24)
            .count()
        )

        require(
            incorrect_days == 0,
            f"Discharge: incorrect days={incorrect_days}",
        )

    else:
        silver_keys = silver_batch.select(
            F.col("timestamp").alias("source_timestamp"),
            *[
                F.col(a).alias(f"silver_{a}")
                for a, _ in cfg["values"]
            ],
        )

        joined = backfill.alias("g").join(
            silver_keys.alias("s"),
            F.col("g.hour_start")
            == F.col("s.source_timestamp"),
            "left",
        )

        missing_source = joined.filter(
            F.col("s.source_timestamp").isNull()
        ).count()

        require(
            missing_source == 0,
            f"{source}: unmatched Silver rows={missing_source}",
        )

    differences = None

    for silver_col, gold_col in cfg["values"]:
        mismatch = ~F.col(
            f"g.{gold_col}"
        ).eqNullSafe(
            F.col(f"s.silver_{silver_col}")
        )

        differences = (
            mismatch if differences is None
            else differences | mismatch
        )

    changed = joined.filter(differences).count()

    require(
        changed == 0,
        f"{source}: changed values={changed}",
    )

    if source == "weather":
        invalid_missing = backfill.filter(
            F.col("weather_is_missing")
        ).count()
    elif source == "rainfall":
        invalid_missing = backfill.filter(
            F.col("rainfall_is_missing")
        ).count()
    else:
        invalid_missing = backfill.filter(
            F.col("discharge_quality_flag") == "INVALID"
        ).count()

    require(
        invalid_missing == 0,
        f"{source}: unexpected missing/invalid="
        f"{invalid_missing}",
    )

    snapshots = spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation
        FROM {cfg['gold']}.snapshots
        ORDER BY committed_at DESC
        """
    )

    snapshot_count = snapshots.count()

    require(
        snapshot_count >= 2,
        f"{source}: fewer than two Iceberg snapshots",
    )

    print("Total rows       :", total)
    print("Unique hours     :", bounds["unique_hours"])
    print("Min timestamp    :", bounds["start"])
    print("Max timestamp    :", bounds["end"])
    print("Duplicate keys   :", duplicates)
    print("Backfill rows    :", backfill_count)
    print("Backfill files   :", file_count)
    print("Changed values   :", changed)
    print("Iceberg snapshots:", snapshot_count)

    snapshots.show(5, truncate=False)

    print(f"[PASS] {source} Gold validation")


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="validate-gold-backfill",
            master="local[2]",
        )

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        for source, cfg in CONFIG.items():
            validate_source(spark, source, cfg)

        print("\nGOLD BACKFILL VALIDATION: PASS")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
