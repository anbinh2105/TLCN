
import argparse

from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)


BATCH_ID = "20261008_155724_backfill"

CONFIG = {
    "weather": {
        "silver": "lakehouse.silver.weather",
        "gold": "lakehouse.gold.weather_hourly",
        "silver_new": 41904,
        "gold_old": 31752,
        "gold_new": 41904,
        "gold_final": 73656,
        "keys": ["hour_start"],
    },
    "rainfall": {
        "silver": "lakehouse.silver.rainfall",
        "gold": "lakehouse.gold.rainfall_hourly",
        "silver_new": 41904,
        "gold_old": 31752,
        "gold_new": 41904,
        "gold_final": 73656,
        "keys": ["hour_start"],
    },
    "river_discharge": {
        "silver": "lakehouse.silver.river_discharge",
        "gold": "lakehouse.gold.discharge_hourly",
        "silver_new": 1746,
        "gold_old": 31752,
        "gold_new": 41904,
        "gold_final": 73656,
        "keys": ["station", "hour_start"],
    },
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def build_weather(silver):
    missing = (
        F.col("temperature_c").isNull()
        | F.col("humidity_pct").isNull()
        | F.col("pressure_hpa").isNull()
        | F.col("wind_speed").isNull()
        | F.col("wind_direction").isNull()
    )

    return silver.select(
        F.date_trunc("hour", "timestamp").alias("hour_start"),
        F.col("temperature_c").cast("double"),
        F.col("humidity_pct").cast("double"),
        F.col("pressure_hpa").cast("double"),
        F.col("wind_speed").cast("double"),
        "wind_direction",
        F.col("quality_flag").alias("weather_quality_flag"),
        (
            F.col("quality_flag").eqNullSafe("MISSING")
            | missing
        ).alias("weather_is_missing"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        "source",
        "source_file",
        F.current_timestamp().alias("gold_processed_at"),
    )


def build_rainfall(silver):
    return silver.select(
        F.date_trunc("hour", "timestamp").alias("hour_start"),
        F.col("rainfall_mm").cast("double"),
        F.col("quality_flag").alias("rainfall_quality_flag"),
        (
            F.col("rainfall_mm").isNull()
            | F.col("quality_flag").eqNullSafe("MISSING")
        ).alias("rainfall_is_missing"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        "source",
        "source_file",
        F.current_timestamp().alias("gold_processed_at"),
    )


def build_discharge(silver):
    daily = silver.withColumn(
        "source_date",
        F.to_date("timestamp"),
    )

    return (
        daily.withColumn(
            "hour_start",
            F.explode(
                F.sequence(
                    F.col("timestamp"),
                    F.expr("timestamp + INTERVAL 23 HOURS"),
                    F.expr("INTERVAL 1 HOUR"),
                )
            ),
        )
        .select(
            "hour_start",
            "station",
            F.col("discharge_m3s").cast("double"),
            F.col("quality_flag").alias(
                "discharge_quality_flag"
            ),
            F.lit("DAILY_EXPANDED").alias(
                "alignment_method"
            ),
            "source_date",
            F.lit("DAILY").alias(
                "source_temporal_grain"
            ),
            F.lit(False).alias(
                "is_hourly_observation"
            ),
            "source",
            "source_file",
            F.current_timestamp().alias("gold_processed_at"),
        )
    )


BUILDERS = {
    "weather": build_weather,
    "rainfall": build_rainfall,
    "river_discharge": build_discharge,
}


def check_duplicates(df, keys):
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def prepare_source(spark, source, cfg):
    print("\n" + "=" * 65)
    print("SOURCE:", source)
    print("SILVER:", cfg["silver"])
    print("GOLD  :", cfg["gold"])
    print("=" * 65)

    require(
        spark.catalog.tableExists(cfg["silver"]),
        f"Missing Silver table: {cfg['silver']}",
    )
    require(
        spark.catalog.tableExists(cfg["gold"]),
        f"Missing Gold table: {cfg['gold']}",
    )

    silver = spark.table(cfg["silver"])
    gold_old = spark.table(cfg["gold"])

    old_count = gold_old.count()
    require(
        old_count == cfg["gold_old"],
        f"{source}: existing Gold count={old_count}, "
        f"expected={cfg['gold_old']}",
    )

    # Read only Silver records originating from this batch.
    batch = silver.filter(
        F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    )

    silver_count = batch.count()
    require(
        silver_count == cfg["silver_new"],
        f"{source}: Silver batch count={silver_count}, "
        f"expected={cfg['silver_new']}",
    )

    source_files = batch.select(
        "source_file"
    ).distinct().count()

    require(
        source_files == 16,
        f"{source}: expected 16 files, got {source_files}",
    )

    null_source_ts = batch.filter(
        F.col("timestamp").isNull()
    ).count()

    require(
        null_source_ts == 0,
        f"{source}: null Silver timestamps",
    )

    invalid = batch.filter(
        F.col("quality_flag") == "INVALID"
    ).count()

    require(
        invalid == 0,
        f"{source}: INVALID Silver records={invalid}",
    )

    if source in ("weather", "rainfall"):
        off_grain = batch.filter(
            F.col("timestamp")
            != F.date_trunc("hour", "timestamp")
        ).count()
    else:
        off_grain = batch.filter(
            F.col("timestamp")
            != F.date_trunc("day", "timestamp")
        ).count()

    require(
        off_grain == 0,
        f"{source}: timestamps not aligned to native grain",
    )

    if source == "river_discharge":
        require(
            batch.filter(
                F.col("station").isNull()
            ).count() == 0,
            "Discharge has null station",
        )

    new = BUILDERS[source](batch)

    # Verify exact field names, types and nullability-compatible
    # ordering. Do not evolve existing Gold schema.
    old_types = {
        field.name: field.dataType
        for field in gold_old.schema.fields
    }
    new_types = {
        field.name: field.dataType
        for field in new.schema.fields
    }

    require(
        old_types == new_types,
        f"{source}: schema mismatch\n"
        f"Gold: {old_types}\n"
        f"New : {new_types}",
    )

    new = new.select(*gold_old.columns)

    new_count = new.count()
    require(
        new_count == cfg["gold_new"],
        f"{source}: transformed rows={new_count}, "
        f"expected={cfg['gold_new']}",
    )

    null_gold_ts = new.filter(
        F.col("hour_start").isNull()
    ).count()

    require(
        null_gold_ts == 0,
        f"{source}: null hour_start",
    )

    duplicate_new = check_duplicates(new, cfg["keys"])
    require(
        duplicate_new == 0,
        f"{source}: duplicate new Gold keys={duplicate_new}",
    )

    duplicate_existing = check_duplicates(
        gold_old, cfg["keys"]
    )
    require(
        duplicate_existing == 0,
        f"{source}: duplicate existing Gold keys="
        f"{duplicate_existing}",
    )

    overlap = (
        new.select(*cfg["keys"])
        .join(
            gold_old.select(*cfg["keys"]),
            cfg["keys"],
            "left_semi",
        )
        .count()
    )

    require(
        overlap == 0,
        f"{source}: {overlap} keys already in Gold",
    )

    lineage_count = new.filter(
        F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    ).count()

    require(
        lineage_count == cfg["gold_new"],
        f"{source}: Gold lineage mismatch",
    )

    if source == "river_discharge":
        incorrect_days = (
            new.groupBy("station", "source_date")
            .count()
            .filter(F.col("count") != 24)
            .count()
        )

        require(
            incorrect_days == 0,
            f"Discharge: {incorrect_days} days "
            "do not contain 24 hourly rows",
        )

        invalid_alignment = new.filter(
            (F.col("alignment_method") != "DAILY_EXPANDED")
            | (F.col("source_temporal_grain") != "DAILY")
            | F.col("is_hourly_observation")
        ).count()

        require(
            invalid_alignment == 0,
            "Discharge alignment metadata invalid",
        )

    print("Existing Gold rows :", old_count)
    print("Silver batch rows  :", silver_count)
    print("New Gold rows      :", new_count)
    print("Duplicate keys     :", duplicate_new)
    print("Existing duplicates:", duplicate_existing)
    print("Overlapping keys   :", overlap)
    print("Source files       :", source_files)
    print("Expected final rows:", cfg["gold_final"])
    print(f"[PASS] {source} Gold preflight")

    return new


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")

    args = parser.parse_args()

    spark = None

    try:
        spark = create_spark_session(
            app_name="gold-incremental-backfill",
            master="local[2]",
        )

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )
        spark.conf.set(
            "spark.sql.adaptive.enabled", "false"
        )

        prepared = {}

        # All three must pass before writing any table.
        for source, cfg in CONFIG.items():
            prepared[source] = prepare_source(
                spark, source, cfg
            )

        print("\nALL GOLD PREFLIGHT CHECKS: PASS")

        if args.dry_run:
            print("GOLD DRY RUN: PASS - no Iceberg writes")
            return

        for source, cfg in CONFIG.items():
            target = cfg["gold"]

            current = spark.table(target).count()

            require(
                current == cfg["gold_old"],
                f"{source}: target changed before append",
            )

            # Recheck keys immediately before writing.
            current_keys = spark.table(
                target
            ).select(*cfg["keys"])

            overlap = (
                prepared[source]
                .select(*cfg["keys"])
                .join(
                    current_keys,
                    cfg["keys"],
                    "left_semi",
                )
                .count()
            )

            require(
                overlap == 0,
                f"{source}: concurrent key overlap={overlap}",
            )

            print(f"\n[WRITE] {target}")

            prepared[source].writeTo(target).append()

            actual = spark.table(target).count()

            require(
                actual == cfg["gold_final"],
                f"{source}: post-write rows={actual}, "
                f"expected={cfg['gold_final']}",
            )

            print(
                f"[PASS] {source}: "
                f"{current} -> {actual}"
            )

        print("\nGOLD INCREMENTAL BACKFILL: PASS")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
