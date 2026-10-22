
import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transformation.silver.common import create_spark_session


BUCKET = "flood-lakehouse"
TABLE = "lakehouse.silver.water_level_stream"

DEFAULT_OBJECT = (
    "bronze/streaming/water_level/"
    "ingestion_date=2026-10-11/"
    "batch_id=7a21a3565a87a63acafc1d61/"
    "events.jsonl"
)

BRONZE_SCHEMA = T.StructType([
    T.StructField("kafka_key", T.StringType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_topic", T.StringType()),
    T.StructField("raw_payload", T.StringType()),
])

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("schema_version", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("station_code", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("water_level_m", T.DoubleType()),
    T.StructField("source_quality_flag", T.StringType()),
    T.StructField("source_table", T.StringType()),
    T.StructField("simulation", T.BooleanType()),
    T.StructField("simulation_type", T.StringType()),
    T.StructField("replay_run_id", T.StringType()),
    T.StructField("replayed_at", T.StringType()),
])


def create_minio_client():
    load_dotenv(dotenv_path=".env")

    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def download_and_verify(client, object_name, local_path):
    response = client.get_object(BUCKET, object_name)

    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()

    actual_checksum = hashlib.sha256(content).hexdigest()

    metadata = client.stat_object(
        BUCKET, object_name
    ).metadata

    expected_checksum = metadata.get(
        "x-amz-meta-checksum-sha256"
    )

    if not expected_checksum:
        raise RuntimeError(
            "Bronze object has no checksum metadata"
        )

    if actual_checksum != expected_checksum:
        raise RuntimeError(
            "Bronze checksum mismatch"
        )

    local_path.write_bytes(content)

    return actual_checksum


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--object-name",
        default=DEFAULT_OBJECT,
    )

    args = parser.parse_args()

    if not args.object_name.startswith(
        "bronze/streaming/water_level/"
    ) or not args.object_name.endswith("/events.jsonl"):
        raise ValueError("Invalid Bronze object path")

    client = create_minio_client()

    spark = None

    with tempfile.TemporaryDirectory(
        prefix="tlcn_s4_"
    ) as temp_dir:

        local_path = Path(temp_dir) / "events.jsonl"

        checksum = download_and_verify(
            client,
            args.object_name,
            local_path,
        )

        print("BRONZE CHECKSUM:", checksum)

        spark = create_spark_session(
            "S4-Bronze-Streaming-To-Silver"
        )

        try:
            spark.conf.set(
                "spark.sql.session.timeZone",
                "Asia/Ho_Chi_Minh",
            )

            # Read each JSONL line as raw text first.
            # This allows malformed records to be detected.
            raw = spark.read.text(
                local_path.resolve().as_uri()
            )

            parsed = (
                raw
                .withColumn(
                    "bronze",
                    F.from_json(
                        F.col("value"),
                        BRONZE_SCHEMA,
                    ),
                )
                .withColumn(
                    "payload",
                    F.from_json(
                        F.col("bronze.raw_payload"),
                        PAYLOAD_SCHEMA,
                    ),
                )
            )

            # Keep Kafka provenance and the original
            # event timestamp for validation.
            normalized = parsed.select(
                F.col("value").alias("bronze_raw_line"),
                F.col("bronze.kafka_topic").alias(
                    "kafka_topic"
                ),
                F.col("bronze.kafka_partition").alias(
                    "kafka_partition"
                ),
                F.col("bronze.kafka_offset").alias(
                    "kafka_offset"
                ),
                F.col("payload.event_id").alias(
                    "event_id"
                ),
                F.col("payload.station_code").alias(
                    "station_code"
                ),
                F.col("payload.event_time").alias(
                    "event_time_raw"
                ),
                F.col("payload.water_level_m").alias(
                    "water_level_m"
                ),
                F.col(
                    "payload.source_quality_flag"
                ).alias("quality_flag"),
                F.col("payload.source_table").alias(
                    "source"
                ),
                F.col("payload.simulation").alias(
                    "simulation"
                ),
                F.col("payload.replay_run_id").alias(
                    "replay_run_id"
                ),
                F.col("bronze.raw_payload").alias(
                    "raw_payload"
                ),
            )

            normalized = normalized.withColumn(
                "timestamp",
                F.try_to_timestamp(
                    F.col("event_time_raw")
                ),
            )

            invalid_condition = (
                F.col("event_id").isNull()
                | (F.trim(F.col("event_id")) == "")
                | F.col("station_code").isNull()
                | (F.trim(F.col("station_code")) == "")
                | F.col("timestamp").isNull()
                | F.col("water_level_m").isNull()
                | F.isnan("water_level_m")
                | F.col("kafka_topic").isNull()
                | F.col("kafka_partition").isNull()
                | F.col("kafka_offset").isNull()
                | (F.col("kafka_offset") < 0)
                | (F.col("kafka_partition") < 0)
            )

            invalid = normalized.filter(
                invalid_condition
            )

            invalid_count = invalid.count()

            total_count = normalized.count()

            if invalid_count:
                print("INVALID RECORDS:", invalid_count)

                invalid.select(
                    "event_id",
                    "event_time_raw",
                    "station_code",
                    "water_level_m",
                ).show(20, truncate=False)

                raise RuntimeError(
                    "S4 FAILED: invalid Bronze records. "
                    "No Silver write performed."
                )

            valid = normalized.filter(
                ~invalid_condition
            )

            # Duplicate event IDs are allowed only
            # when the underlying payload is identical.
            duplicates = (
                valid.groupBy("event_id")
                .agg(
                    F.count("*").alias("record_count"),
                    F.countDistinct(
                        "raw_payload"
                    ).alias("payload_versions"),
                )
            )

            conflicts = duplicates.filter(
                F.col("payload_versions") > 1
            ).count()

            if conflicts:
                raise RuntimeError(
                    "S4 FAILED: conflicting event_id "
                    "payloads. No Silver write performed."
                )

            # Retain a deterministic Kafka occurrence
            # for identical duplicate payloads.
            from pyspark.sql.window import Window

            window = Window.partitionBy(
                "event_id"
            ).orderBy(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            )

            deduplicated = (
                valid.withColumn(
                    "row_num",
                    F.row_number().over(window),
                )
                .filter(F.col("row_num") == 1)
                .drop("row_num")
            )

            silver = deduplicated.select(
                "event_id",
                "timestamp",
                "station_code",
                "water_level_m",
                "quality_flag",
                "source",
                "simulation",
                "replay_run_id",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                F.current_timestamp().alias(
                    "silver_processed_at"
                ),
            )

            unique_count = silver.count()

            print("\n=== S4 VALIDATION ===")
            print("BRONZE RECORDS:", total_count)
            print("INVALID:", invalid_count)
            print(
                "DUPLICATES:",
                total_count - unique_count,
            )
            print("UNIQUE EVENTS:", unique_count)

            # The streaming table is separate
            # from the existing batch Silver table.
            spark.sql("""
                CREATE TABLE IF NOT EXISTS
                lakehouse.silver.water_level_stream (
                    event_id STRING,
                    timestamp TIMESTAMP,
                    station_code STRING,
                    water_level_m DOUBLE,
                    quality_flag STRING,
                    source STRING,
                    simulation BOOLEAN,
                    replay_run_id STRING,
                    kafka_topic STRING,
                    kafka_partition INT,
                    kafka_offset BIGINT,
                    silver_processed_at TIMESTAMP
                )
                USING iceberg
            """)

            silver.createOrReplaceTempView(
                "s4_water_level_source"
            )

            # Detect conflicts with existing Silver
            # events before executing MERGE.
            existing = spark.table(TABLE).select(
                "event_id",
                F.col("timestamp").alias("old_timestamp"),
                F.col("station_code").alias(
                    "old_station_code"
                ),
                F.col("water_level_m").alias(
                    "old_water_level_m"
                ),
            )

            existing_conflicts = (
                silver.alias("s")
                .join(
                    existing.alias("t"),
                    on="event_id",
                    how="inner",
                )
                .filter(
                    ~F.col("s.timestamp").eqNullSafe(
                        F.col("t.old_timestamp")
                    )
                    | ~F.col("s.station_code").eqNullSafe(
                        F.col("t.old_station_code")
                    )
                    | ~F.col("s.water_level_m").eqNullSafe(
                        F.col("t.old_water_level_m")
                    )
                )
                .count()
            )

            if existing_conflicts:
                raise RuntimeError(
                    "S4 FAILED: existing event_id "
                    "has conflicting values."
                )

            # Insert only new event IDs.
            # Existing rows remain unchanged on rerun.
            spark.sql("""
                MERGE INTO
                    lakehouse.silver.water_level_stream t
                USING s4_water_level_source s
                ON t.event_id = s.event_id
                WHEN NOT MATCHED THEN INSERT (
                    event_id,
                    timestamp,
                    station_code,
                    water_level_m,
                    quality_flag,
                    source,
                    simulation,
                    replay_run_id,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    silver_processed_at
                )
                VALUES (
                    s.event_id,
                    s.timestamp,
                    s.station_code,
                    s.water_level_m,
                    s.quality_flag,
                    s.source,
                    s.simulation,
                    s.replay_run_id,
                    s.kafka_topic,
                    s.kafka_partition,
                    s.kafka_offset,
                    s.silver_processed_at
                )
            """)

            matched_count = (
                spark.table(TABLE)
                .join(
                    silver.select("event_id"),
                    on="event_id",
                    how="inner",
                )
                .count()
            )

            if matched_count != unique_count:
                raise RuntimeError(
                    "S4 FAILED: Silver verification "
                    "count does not match input"
                )

            print("\n=== S4 RESULT ===")
            print("STATUS: PASS")
            print("TABLE:", TABLE)
            print("VERIFIED EVENTS:", matched_count)
            print(
                "TOTAL TABLE ROWS:",
                spark.table(TABLE).count(),
            )

            spark.table(TABLE).orderBy(
                "timestamp"
            ).show(20, truncate=False)

        finally:
            spark.stop()


if __name__ == "__main__":
    main()
