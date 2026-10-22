
import argparse
import json
from collections import defaultdict
from pathlib import Path

from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    get_silver_table,
    MINIO_BUCKET,
    stop_spark,
)
from transformation.silver.weather_to_silver import transform_weather
from transformation.silver.rainfall_to_silver import transform_rainfall
from transformation.silver.river_discharge_to_silver import (
    transform_river_discharge,
)


ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = ROOT / "data/metadata/ingestion_log.jsonl"

BATCH_ID = "20261008_155724_backfill"

SOURCES = {
    "weather": {
        "transform": transform_weather,
        "old_count": 31752,
        "new_count": 41904,
        "final_count": 73656,
    },
    "rainfall": {
        "transform": transform_rainfall,
        "old_count": 31752,
        "new_count": 41904,
        "final_count": 73656,
    },
    "river_discharge": {
        "transform": transform_river_discharge,
        "old_count": 1323,
        "new_count": 1746,
        "final_count": 3069,
    },
}


def load_batch_manifest():
    """
    Read only SUCCESS records from the exact Bronze batch.
    Do not scan the entire Bronze directory.
    """
    if not LOG_PATH.is_file():
        raise RuntimeError("Missing ingestion log")

    result = defaultdict(list)

    with LOG_PATH.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue

            record = json.loads(line)

            if record.get("batch_id") != BATCH_ID:
                continue

            if record.get("status") != "SUCCESS":
                continue

            source = record.get("source")

            if source not in SOURCES:
                continue

            key = record.get("object_path")

            expected_prefix = (
                f"bronze/{source}/"
                f"ingestion_date=2026-10-08/"
                f"batch_id={BATCH_ID}/"
            )

            if not key or not key.startswith(expected_prefix):
                raise RuntimeError(
                    f"Unexpected object path: {key}"
                )

            if not key.endswith(".csv"):
                raise RuntimeError(
                    f"Unexpected file extension: {key}"
                )

            result[source].append(record)

    for source in SOURCES:
        records = result[source]
        paths = [r["object_path"] for r in records]

        if len(records) != 16:
            raise RuntimeError(
                f"{source}: expected 16 files, got {len(records)}"
            )

        if len(set(paths)) != 16:
            raise RuntimeError(
                f"{source}: duplicate object paths"
            )

        expected_rows = SOURCES[source]["new_count"]
        actual_rows = sum(
            r["record_count"] for r in records
        )

        if actual_rows != expected_rows:
            raise RuntimeError(
                f"{source}: manifest rows={actual_rows}, "
                f"expected={expected_rows}"
            )

    return result


def read_batch(spark, records):
    paths = [
        f"s3a://{MINIO_BUCKET}/{r['object_path']}"
        for r in records
    ]

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(paths)
        .withColumn(
            "_bronze_source_file",
            F.input_file_name(),
        )
    )


def check_source(spark, source, records):
    config = SOURCES[source]
    table = get_silver_table(source)

    print(f"\n{'=' * 65}")
    print(f"SOURCE: {source}")
    print(f"TABLE : {table}")
    print(f"{'=' * 65}")

    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Silver table missing: {table}")

    old = spark.table(table)
    old_count = old.count()

    if old_count != config["old_count"]:
        raise RuntimeError(
            f"{source}: current Silver count={old_count}; "
            f"expected={config['old_count']}. "
            "Stop to prevent accidental duplicate writes."
        )

    raw = read_batch(spark, records)
    raw_count = raw.count()

    if raw_count != config["new_count"]:
        raise RuntimeError(
            f"{source}: Bronze rows={raw_count}, "
            f"expected={config['new_count']}"
        )

    new = config["transform"](raw)

    # Ensure output schema is compatible with existing Iceberg table.
    old_schema = {
        field.name: field.dataType.simpleString()
        for field in old.schema.fields
    }
    new_schema = {
        field.name: field.dataType.simpleString()
        for field in new.schema.fields
    }

    if old_schema != new_schema:
        raise RuntimeError(
            f"{source}: Silver schema mismatch\n"
            f"OLD: {old_schema}\nNEW: {new_schema}"
        )

    new = new.select(*old.columns)

    new_count = new.count()

    if new_count != config["new_count"]:
        raise RuntimeError(
            f"{source}: transformed rows={new_count}, "
            f"expected={config['new_count']}"
        )

    # Existing datasets have one row per timestamp.
    # A conflicting timestamp must never be silently dropped.
    null_timestamps = new.filter(
        F.col("timestamp").isNull()
    ).count()

    if null_timestamps:
        raise RuntimeError(
            f"{source}: {null_timestamps} null timestamps"
        )

    duplicate_keys = (
        new.groupBy("timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys:
        raise RuntimeError(
            f"{source}: {duplicate_keys} duplicate timestamps"
        )

    overlaps = (
        new.select("timestamp")
        .join(
            old.select("timestamp"),
            on="timestamp",
            how="left_semi",
        )
        .count()
    )

    if overlaps:
        raise RuntimeError(
            f"{source}: {overlaps} timestamps already "
            "exist in Silver. No writes allowed."
        )

    invalid = new.filter(
        F.col("quality_flag") == "INVALID"
    ).count()

    if invalid:
        raise RuntimeError(
            f"{source}: {invalid} INVALID rows"
        )

    missing = new.filter(
        F.col("quality_flag") == "MISSING"
    ).count()

    source_files = new.select(
        "source_file"
    ).distinct().count()

    if source_files != 16:
        raise RuntimeError(
            f"{source}: expected lineage for 16 files, "
            f"got {source_files}"
        )

    # Confirm all lineage paths belong to the batch.
    unexpected_lineage = new.filter(
        ~F.col("source_file").contains(
            f"batch_id={BATCH_ID}/"
        )
    ).count()

    if unexpected_lineage:
        raise RuntimeError(
            f"{source}: unexpected source_file lineage"
        )

    print(f"Existing Silver rows : {old_count}")
    print(f"Bronze backfill rows : {raw_count}")
    print(f"Transformed rows     : {new_count}")
    print(f"Duplicate timestamps : {duplicate_keys}")
    print(f"Overlapping keys     : {overlaps}")
    print(f"INVALID records      : {invalid}")
    print(f"MISSING records      : {missing}")
    print(f"Source files         : {source_files}")
    print(f"Expected final rows  : {config['final_count']}")
    print(f"[PASS] {source} preflight")

    return new


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")

    args = parser.parse_args()
    spark = None

    try:
        manifest = load_batch_manifest()

        spark = create_spark_session(
            app_name="silver-incremental-backfill",
            master="local[2]",
        )

        # Validate all three sources before any writes.
        prepared = {}

        for source, records in manifest.items():
            prepared[source] = check_source(
                spark, source, records
            )

        print("\nALL SOURCES PREFLIGHT: PASS")

        if args.dry_run:
            print("DRY RUN: PASS — no Iceberg writes")
            return

        # No createOrReplace, overwrite, delete or update.
        # Each source is appended only after preflight passes.
        for source in SOURCES:
            table = get_silver_table(source)

            print(f"\n[WRITE] {table}")

            # Recheck target count immediately before writing.
            current_count = spark.table(table).count()

            if current_count != SOURCES[source]["old_count"]:
                raise RuntimeError(
                    f"{source}: target changed since preflight"
                )

            prepared[source].writeTo(table).append()

            actual = spark.table(table).count()
            expected = SOURCES[source]["final_count"]

            if actual != expected:
                raise RuntimeError(
                    f"{source}: post-write rows={actual}, "
                    f"expected={expected}"
                )

            print(
                f"[PASS] {source}: "
                f"{current_count} -> {actual}"
            )

        print("\nSILVER INCREMENTAL BACKFILL: PASS")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
