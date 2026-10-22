
import argparse
import hashlib
import io
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from minio import Minio


TOPIC = "hydrology.water_level.raw"
BUCKET = "flood-lakehouse"
PREFIX = "bronze/streaming/water_level"


def create_minio_client():
    load_dotenv(dotenv_path=".env")

    return Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092"
    )
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument(
        "--start-offsets",
        default=None,
        help=(
            "Comma-separated partition:offset pairs, "
            "for example 0:0,1:10,2:0"
        ),
    )
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than zero")

    minio_client = create_minio_client()

    if not minio_client.bucket_exists(BUCKET):
        raise RuntimeError(f"Bucket does not exist: {BUCKET}")

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
    )

    if not args.start_offsets:
        consumer.subscribe([TOPIC])

    if args.start_offsets:
        offsets_by_partition = {}

        for item in args.start_offsets.split(","):
            partition_str, offset_str = item.split(":")
            partition = int(partition_str)
            offset = int(offset_str)

            if partition in offsets_by_partition:
                raise ValueError(
                    f"Duplicate partition: {partition}"
                )

            if partition < 0 or offset < 0:
                raise ValueError(
                    "Partition and offset must be non-negative"
                )

            offsets_by_partition[partition] = offset

        available_partitions = consumer.partitions_for_topic(TOPIC)

        if available_partitions is None:
            raise RuntimeError(
                f"Kafka topic not found: {TOPIC}"
            )

        if set(offsets_by_partition) != set(available_partitions):
            raise ValueError(
                "Offsets must be provided for every partition. "
                f"Available: {sorted(available_partitions)}"
            )

        topic_partitions = [
            TopicPartition(TOPIC, partition)
            for partition in sorted(offsets_by_partition)
        ]

        consumer.assign(topic_partitions)

        for tp in topic_partitions:
            consumer.seek(
                tp,
                offsets_by_partition[tp.partition]
            )

            print(
                f"[SEEK] partition={tp.partition} "
                f"offset={offsets_by_partition[tp.partition]}"
            )

    records = []
    offsets = []

    try:
        for message in consumer:
            # Preserve original Kafka message bytes.
            raw_value = message.value
            payload = json.loads(raw_value.decode("utf-8"))

            # Validate basic fields without modifying raw data.
            required = [
                "event_id",
                "station_code",
                "event_time",
                "water_level_m",
            ]

            missing = [
                field for field in required
                if field not in payload
            ]

            if missing:
                raise ValueError(
                    f"Missing fields: {missing}"
                )

            # Store Kafka metadata alongside original payload.
            record = {
                "kafka_topic": message.topic,
                "kafka_partition": message.partition,
                "kafka_offset": message.offset,
                "kafka_key": (
                    message.key.decode("utf-8")
                    if message.key else None
                ),
                "raw_payload": raw_value.decode("utf-8"),
            }

            records.append(record)
            offsets.append(
                (message.partition, message.offset)
            )

            print(
                f"[READ {len(records)}] "
                f"partition={message.partition} "
                f"offset={message.offset}"
            )

            if len(records) >= args.limit:
                break

    finally:
        consumer.close()

    if not records:
        print("STATUS: NO_MESSAGES")
        return

    # Canonical JSONL encoding.
    lines = [
        json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True
        )
        for record in records
    ]

    content = ("\n".join(lines) + "\n").encode("utf-8")
    checksum = hashlib.sha256(content).hexdigest()

    # Deterministic object name enables safe retries
    # for an identical consumed batch.
    offset_signature = "_".join(
        f"{partition}-{offset}"
        for partition, offset in sorted(offsets)
    )

    batch_id = hashlib.sha256(
        (
            TOPIC + "|" + offset_signature
        ).encode("utf-8")
    ).hexdigest()[:24]

    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")

    object_name = (
        f"{PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"events.jsonl"
    )

    # Avoid silently overwriting an existing raw object.
    try:
        existing = minio_client.stat_object(
            BUCKET, object_name
        )
    except Exception as exc:
        from minio.error import S3Error
        if isinstance(exc, S3Error) and exc.code in (
            "NoSuchKey", "NoSuchObject"
        ):
            existing = None
        else:
            raise

    if existing is not None:
        print("STATUS: ALREADY_EXISTS")
        print("OBJECT:", object_name)
        return

    minio_client.put_object(
        BUCKET,
        object_name,
        io.BytesIO(content),
        length=len(content),
        content_type="application/x-ndjson",
        metadata={
            "checksum-sha256": checksum,
            "record-count": str(len(records)),
            "source": "kafka",
            "batch-id": batch_id,
        },
    )

    print("\n=== S3 RESULT ===")
    print("STATUS: PASS")
    print("RECORDS:", len(records))
    print("BUCKET:", BUCKET)
    print("OBJECT:", object_name)
    print("CHECKSUM:", checksum)


if __name__ == "__main__":
    main()
