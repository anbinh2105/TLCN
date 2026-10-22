
from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

EXPECTED = {
    "weather": {
        "rows": 73656,
        "new_rows": 41904,
        "step_seconds": 3600,
    },
    "rainfall": {
        "rows": 73656,
        "new_rows": 41904,
        "step_seconds": 3600,
    },
    "river_discharge": {
        "rows": 3069,
        "new_rows": 1746,
        "step_seconds": 86400,
    },
}

BATCH_ID = "20261008_155724_backfill"


def validate_source(spark, source, config):
    table = f"lakehouse.silver.{source}"
    df = spark.table(table)

    print(f"\n{'=' * 65}")
    print(f"VALIDATING: {table}")
    print(f"{'=' * 65}")

    failures = []

    total = df.count()

    if total != config["rows"]:
        failures.append(
            f"Row count: {total}, expected {config['rows']}"
        )

    null_ts = df.filter(
        F.col("timestamp").isNull()
    ).count()

    if null_ts:
        failures.append(f"Null timestamps: {null_ts}")

    duplicates = (
        df.groupBy("timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates:
        failures.append(
            f"Duplicate timestamps: {duplicates}"
        )

    invalid = df.filter(
        F.col("quality_flag") == "INVALID"
    ).count()

    if invalid:
        failures.append(f"INVALID rows: {invalid}")

    backfill = df.filter(
        F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    )

    backfill_count = backfill.count()

    if backfill_count != config["new_rows"]:
        failures.append(
            f"Backfill rows: {backfill_count}, "
            f"expected {config['new_rows']}"
        )

    backfill_files = backfill.select(
        "source_file"
    ).distinct().count()

    if backfill_files != 16:
        failures.append(
            f"Backfill files: {backfill_files}, expected 16"
        )

    coverage = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts"),
        F.countDistinct("timestamp").alias("unique_ts"),
    ).first()

    if coverage["min_ts"] is None or coverage["max_ts"] is None:
        failures.append("Missing timestamp coverage")
    else:
        expected_slots = (
            int(
                (
                    coverage["max_ts"] - coverage["min_ts"]
                ).total_seconds()
                // config["step_seconds"]
            )
            + 1
        )

        if coverage["unique_ts"] != expected_slots:
            failures.append(
                f"Coverage: {coverage['unique_ts']} unique "
                f"timestamps, expected {expected_slots}"
            )

    print(f"Total rows       : {total}")
    print(f"Null timestamps  : {null_ts}")
    print(f"Duplicate keys   : {duplicates}")
    print(f"INVALID rows     : {invalid}")
    print(f"Backfill rows    : {backfill_count}")
    print(f"Backfill files   : {backfill_files}")
    print(f"Min timestamp    : {coverage['min_ts']}")
    print(f"Max timestamp    : {coverage['max_ts']}")

    if failures:
        for failure in failures:
            print(f"[FAIL] {failure}")
        raise RuntimeError(
            f"{source}: Silver validation failed"
        )

    print(f"[PASS] {source} Silver backfill validation")


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="validate-silver-backfill",
            master="local[2]",
        )

        for source, config in EXPECTED.items():
            validate_source(spark, source, config)

        print("\nSILVER BACKFILL VALIDATION: PASS")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
