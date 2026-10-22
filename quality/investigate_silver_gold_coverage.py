
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


CONFIGS = {
    "weather": {
        "silver": "lakehouse.silver.weather",
        "gold": "lakehouse.gold.weather_hourly",
        "grain": "hourly",
    },
    "rainfall": {
        "silver": "lakehouse.silver.rainfall",
        "gold": "lakehouse.gold.rainfall_hourly",
        "grain": "hourly",
    },
    "river_discharge": {
        "silver": "lakehouse.silver.river_discharge",
        "gold": "lakehouse.gold.discharge_hourly",
        "grain": "daily",
    },
}


def investigate(spark, source, cfg):
    print("\n" + "=" * 65)
    print("SILVER -> GOLD:", source.upper())
    print("=" * 65)

    silver = spark.table(cfg["silver"])
    gold = spark.table(cfg["gold"])

    if cfg["grain"] == "hourly":
        silver_keys = (
            silver.select(
                F.date_trunc(
                    "hour", "timestamp"
                ).alias("hour_start")
            )
            .distinct()
        )

        gold_keys = (
            gold.select("hour_start")
            .distinct()
        )

        missing_in_gold = (
            silver_keys
            .subtract(gold_keys)
            .count()
        )

        extra_in_gold = (
            gold_keys
            .subtract(silver_keys)
            .count()
        )

        print(
            "Silver distinct hours:",
            silver_keys.count()
        )
        print(
            "Gold distinct hours:",
            gold_keys.count()
        )

    else:
        silver_keys = (
            silver.select(
                F.to_date("timestamp").alias("source_date")
            )
            .distinct()
        )

        gold_keys = (
            gold.select(
                F.to_date("hour_start").alias("source_date")
            )
            .distinct()
        )

        missing_in_gold = (
            silver_keys
            .subtract(gold_keys)
            .count()
        )

        extra_in_gold = (
            gold_keys
            .subtract(silver_keys)
            .count()
        )

        bad_day_count = (
            gold.groupBy(
                F.to_date("hour_start").alias("source_date")
            )
            .agg(
                F.count("*").alias("hour_count")
            )
            .filter(F.col("hour_count") != 24)
            .count()
        )

        print(
            "Silver distinct days:",
            silver_keys.count()
        )
        print(
            "Gold distinct days:",
            gold_keys.count()
        )
        print(
            "Gold days without exactly 24 rows:",
            bad_day_count
        )

    print("Silver keys missing in Gold:", missing_in_gold)
    print("Extra Gold keys:", extra_in_gold)

    passed = (
        missing_in_gold == 0
        and extra_in_gold == 0
    )

    if cfg["grain"] == "daily":
        passed = passed and bad_day_count == 0

    print(
        "RESULT:",
        "PASS" if passed else "FAIL"
    )

    return passed


def main():
    spark = create_spark_session(
        app_name="SilverGoldCoverageInvestigation",
        master="local[2]",
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )

        results = []

        for source, cfg in CONFIGS.items():
            results.append(
                investigate(spark, source, cfg)
            )

        print("\n" + "=" * 65)
        print(
            "SILVER GOLD COVERAGE:",
            "PASS" if all(results) else "FAIL"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
