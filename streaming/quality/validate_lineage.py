
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from minio import Minio
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DoubleType, BooleanType,
    IntegerType, LongType,
)

from transformation.silver.common import create_spark_session
from streaming.quality.rules import check_equal_datasets


BUCKET = "flood-lakehouse"
PREFIX = "bronze/streaming/water_level/"

SILVER = "lakehouse.silver.water_level_stream"
GOLD = "lakehouse.gold.water_level_hourly_stream"

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
REPORT_DIR = Path("reports/streaming_quality")


BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("station_code", StringType()),
    StructField("water_level_m", DoubleType()),
    StructField("quality_flag", StringType()),
    StructField("source", StringType()),
    StructField("simulation", BooleanType()),
    StructField("replay_run_id", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
])


def make_client():
    load_dotenv(dotenv_path=".env")

    endpoint = os.getenv(
        "MINIO_ENDPOINT", "localhost:9000"
    )

    secure = (
        endpoint.startswith("https://")
        or os.getenv("MINIO_SECURE", "false").lower()
        == "true"
    )

    endpoint = endpoint.replace(
        "https://", ""
    ).replace("http://", "")

    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=secure,
    )


def parse_event_time(value):
    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise ValueError(
            f"Event time lacks timezone: {value}"
        )

    # Silver đang lưu timestamp theo giờ địa phương
    # Asia/Ho_Chi_Minh trong pipeline replay.
    # Giữ local wall-clock time để đối soát đúng
    # với quy ước hiện tại của Silver.
    return dt.astimezone(LOCAL_TZ).replace(
        tzinfo=None
    )


def read_bronze():
    client = make_client()
    rows = []
    evidence = []
    seen_ids = set()
    seen_positions = set()

    objects = [
        obj for obj in client.list_objects(
            BUCKET, prefix=PREFIX, recursive=True
        )
        if obj.object_name.endswith(".jsonl")
    ]

    if not objects:
        raise RuntimeError("No Bronze JSONL objects")

    for obj in objects:
        response = client.get_object(
            BUCKET, obj.object_name
        )

        try:
            raw = response.read()
        finally:
            response.close()
            response.release_conn()

        stat = client.stat_object(
            BUCKET, obj.object_name
        )

        metadata = {
            key.lower(): value
            for key, value in stat.metadata.items()
        }

        checksum = hashlib.sha256(raw).hexdigest()
        expected = metadata.get(
            "x-amz-meta-checksum-sha256"
        )

        if not expected or checksum != expected:
            raise RuntimeError(
                f"Checksum verification failed: "
                f"{obj.object_name}"
            )

        lines = [
            line for line in raw.decode("utf-8").splitlines()
            if line.strip()
        ]

        record_count = metadata.get(
            "x-amz-meta-record-count"
        )

        if (
            record_count is None
            or len(lines) != int(record_count)
        ):
            raise RuntimeError(
                f"Record-count mismatch: {obj.object_name}"
            )

        for line in lines:
            envelope = json.loads(line)
            payload = envelope["raw_payload"]

            if isinstance(payload, str):
                payload = json.loads(payload)

            event_id = payload["event_id"]

            position = (
                envelope["kafka_topic"],
                int(envelope["kafka_partition"]),
                int(envelope["kafka_offset"]),
            )

            if event_id in seen_ids:
                raise RuntimeError(
                    f"Duplicate Bronze event_id: {event_id}"
                )

            if position in seen_positions:
                raise RuntimeError(
                    f"Duplicate Kafka position: {position}"
                )

            seen_ids.add(event_id)
            seen_positions.add(position)

            if position[0] != "hydrology.water_level.raw":
                raise RuntimeError("Unexpected Kafka topic")

            if payload.get("simulation") is not True:
                raise RuntimeError(
                    f"Non-simulation event: {event_id}"
                )

            if payload.get("simulation_type") != "historical_replay":
                raise RuntimeError(
                    f"Unexpected simulation type: {event_id}"
                )

            rows.append((
                event_id,
                parse_event_time(payload["event_time"]),
                payload["station_code"],
                float(payload["water_level_m"]),
                payload["source_quality_flag"],
                payload["source_table"],
                payload["simulation"],
                payload["replay_run_id"],
                position[0],
                position[1],
                position[2],
            ))

        evidence.append({
            "object": obj.object_name,
            "bytes": len(raw),
            "sha256": checksum,
            "records": len(lines),
            "checksum_verified": True,
        })

    return rows, evidence


def compare(
    left, right, keys, columns, rule_id
):
    result = check_equal_datasets(
        left, right, keys,
        [(name, name) for name in columns],
        GOLD if "GOLD" in rule_id else SILVER,
        rule_id,
    )

    print(
        f"{rule_id}: {result['status']} "
        f"(mismatches={result['observed']})"
    )

    if result["status"] != "PASS":
        raise RuntimeError(
            f"{rule_id} reconciliation failed"
        )

    return result


def reconcile_bronze_silver(spark, bronze_rows):
    print("\n=== S7.6 BRONZE -> SILVER ===")

    bronze = spark.createDataFrame(
        bronze_rows, BRONZE_SCHEMA
    )

    silver = spark.table(SILVER)

    columns = [
        field.name for field in BRONZE_SCHEMA.fields
    ]

    if bronze.count() != silver.count():
        raise RuntimeError(
            "Bronze/Silver record-count mismatch"
        )

    result = compare(
        bronze,
        silver.select(*columns),
        ["event_id"],
        [name for name in columns if name != "event_id"],
        "S76-BRONZE-SILVER",
    )

    print("BRONZE RECORDS:", bronze.count())
    print("SILVER RECORDS:", silver.count())

    return result


def aggregate_silver(silver):
    """
    Independently reconstruct S5 hourly aggregation
    from event-level Silver data.
    """
    base = silver.withColumn(
        "hour_timestamp",
        F.date_trunc("hour", F.col("timestamp"))
    )

    window = Window.partitionBy(
        "hour_timestamp", "station_code"
    ).orderBy(
        F.col("timestamp").desc(),
        F.col("event_id").desc(),
    )

    last_values = (
        base.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "hour_timestamp",
            "station_code",
            F.col("water_level_m").alias(
                "water_level_last"
            ),
        )
    )

    hourly = base.groupBy(
        "hour_timestamp", "station_code"
    ).agg(
        F.avg("water_level_m").alias("water_level_mean"),
        F.min("water_level_m").alias("water_level_min"),
        F.max("water_level_m").alias("water_level_max"),
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
        ).alias("_min_sim"),
        F.max(
            F.col("simulation").cast("int")
        ).alias("_max_sim"),
    )

    hourly = hourly.join(
        last_values,
        ["hour_timestamp", "station_code"],
        "inner",
    )

    hourly = hourly.withColumn(
        "expected_obs_count", F.lit(4)
    ).withColumn(
        "hour_quality_flag",
        F.when(
            F.col("water_level_obs_count") == 4,
            "COMPLETE"
        ).when(
            F.col("water_level_obs_count") < 4,
            "PARTIAL"
        ).otherwise("EXCESS")
    ).withColumn(
        "simulation",
        F.col("_min_sim") == 1
    ).withColumn(
        "mixed_simulation",
        F.col("_min_sim") != F.col("_max_sim")
    ).drop("_min_sim", "_max_sim")

    return hourly


def reconcile_silver_gold(spark):
    print("\n=== S7.6 SILVER -> GOLD HOURLY ===")

    silver = spark.table(SILVER)
    gold = spark.table(GOLD)

    reconstructed = aggregate_silver(silver)

    columns = [
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

    result = compare(
        reconstructed,
        gold,
        ["hour_timestamp", "station_code"],
        columns,
        "S76-SILVER-GOLD",
    )

    print("RECONSTRUCTED HOURS:", reconstructed.count())
    print("GOLD HOURS:", gold.count())

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--demo-baseline", action="store_true"
    )
    args = parser.parse_args()

    print("\n=== S7.6 BRONZE VALIDATION ===")
    rows, evidence = read_bronze()

    print("BRONZE OBJECTS:", len(evidence))
    print("BRONZE EVENTS:", len(rows))
    print("CHECKSUMS: PASS")

    spark = create_spark_session(
        "S76-Streaming-Lineage"
    )

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        bronze_silver = reconcile_bronze_silver(
            spark, rows
        )

        silver_gold = reconcile_silver_gold(spark)

        counts = {
            "bronze": len(rows),
            "silver": spark.table(SILVER).count(),
            "gold_hourly": spark.table(GOLD).count(),
        }

        if args.demo_baseline:
            expected = {
                "bronze": 10,
                "silver": 10,
                "gold_hourly": 3,
            }

            if counts != expected:
                raise RuntimeError(
                    f"Demo baseline mismatch: {counts}"
                )

        report = {
            "generated_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "stage": "S7.6",
            "pipeline": "historical_replay_streaming",
            "overall_status": "PASS",
            "record_counts": counts,
            "bronze_objects": evidence,
            "checks": [bronze_silver, silver_gold],
        }

        REPORT_DIR.mkdir(
            parents=True, exist_ok=True
        )

        filename = (
            "streaming_lineage_"
            + datetime.now(timezone.utc).strftime(
                "%Y%m%dT%H%M%S%fZ"
            )
            + ".json"
        )

        path = REPORT_DIR / filename

        path.write_text(
            json.dumps(
                report,
                indent=2,
                ensure_ascii=False,
                default=str,
            ),
            encoding="utf-8",
        )

        print("\n=== S7.6 RESULT ===")
        print("STATUS: PASS")
        print("COUNTS:", counts)
        print("REPORT:", path)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
