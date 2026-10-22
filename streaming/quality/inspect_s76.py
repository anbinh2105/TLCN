
import hashlib
import json
import os

from collections import Counter
from dotenv import load_dotenv
from minio import Minio

from transformation.silver.common import create_spark_session


BUCKET = "flood-lakehouse"

PREFIX = "bronze/streaming/water_level/"

SILVER_TABLE = "lakehouse.silver.water_level_stream"

EXPECTED_TOPIC = "hydrology.water_level.raw"


def inspect_bronze():
    load_dotenv(dotenv_path=".env")

    endpoint = os.getenv(
        "MINIO_ENDPOINT", "localhost:9000"
    )

    endpoint = (
        endpoint.replace("http://", "")
        .replace("https://", "")
    )

    secure = os.getenv(
        "MINIO_SECURE", "false"
    ).lower() == "true"

    client = Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=secure,
    )

    print("\n=== S7.6 BRONZE OBJECTS ===")

    objects = list(
        client.list_objects(
            BUCKET,
            prefix=PREFIX,
            recursive=True,
        )
    )

    jsonl_objects = [
        obj for obj in objects
        if obj.object_name.endswith(".jsonl")
    ]

    print("JSONL OBJECTS:", len(jsonl_objects))

    total_records = 0
    event_ids = []
    kafka_positions = []
    topics = Counter()

    for obj in jsonl_objects:
        response = client.get_object(
            BUCKET, obj.object_name
        )

        try:
            content = response.read()
        finally:
            response.close()
            response.release_conn()

        actual_checksum = hashlib.sha256(
            content
        ).hexdigest()

        metadata = client.stat_object(
            BUCKET, obj.object_name
        )

        print("\nOBJECT:", obj.object_name)
        print("SIZE:", len(content))
        print("SHA256:", actual_checksum)
        print("METADATA:", metadata.metadata)

        expected_checksum = metadata.metadata.get(
            "x-amz-meta-checksum-sha256"
        )

        if not expected_checksum:
            raise RuntimeError(
                f"Missing SHA-256 metadata: {obj.object_name}"
            )

        if actual_checksum != expected_checksum:
            raise RuntimeError(
                f"SHA-256 mismatch: {obj.object_name}"
            )

        print("CHECKSUM: PASS")

        expected_records = metadata.metadata.get(
            "x-amz-meta-record-count"
        )

        if expected_records is None:
            raise RuntimeError("Missing record-count metadata")

        actual_records = sum(
            bool(line.strip())
            for line in content.decode("utf-8").splitlines()
        )

        if actual_records != int(expected_records):
            raise RuntimeError("Bronze record-count mismatch")

        print("RECORD COUNT METADATA: PASS")

        if expected_checksum:
            if actual_checksum != expected_checksum:
                raise RuntimeError(
                    "Bronze checksum mismatch"
                )

            print("CHECKSUM: PASS")
        else:
            print(
                "CHECKSUM: NOT VERIFIED "
                "(metadata key not recognized)"
            )

        for line_number, line in enumerate(
            content.decode("utf-8").splitlines(),
            start=1,
        ):
            if not line.strip():
                continue

            try:
                envelope = json.loads(line)

                payload = envelope["raw_payload"]

                if isinstance(payload, str):
                    payload = json.loads(payload)

                event_id = payload["event_id"]

                partition = int(
                    envelope["kafka_partition"]
                )

                offset = int(
                    envelope["kafka_offset"]
                )

                topic = envelope["kafka_topic"]

            except (KeyError, ValueError, TypeError) as exc:
                raise RuntimeError(
                    f"Invalid JSONL line "
                    f"{line_number} in {obj.object_name}"
                ) from exc

            total_records += 1
            event_ids.append(event_id)
            kafka_positions.append(
                (topic, partition, offset)
            )
            topics[topic] += 1

    print("\n=== S7.6 BRONZE SUMMARY ===")

    print("TOTAL RECORDS:", total_records)

    print(
        "UNIQUE EVENT IDS:",
        len(set(event_ids))
    )

    print(
        "UNIQUE KAFKA POSITIONS:",
        len(set(kafka_positions))
    )

    print("TOPICS:", dict(topics))

    if total_records == 0:
        raise RuntimeError(
            "No Bronze streaming records"
        )

    if len(set(event_ids)) != total_records:
        raise RuntimeError(
            "Duplicate event_id in Bronze"
        )

    if len(set(kafka_positions)) != total_records:
        raise RuntimeError(
            "Duplicate Kafka position in Bronze"
        )

    if set(topics) != {EXPECTED_TOPIC}:
        raise RuntimeError(
            "Unexpected Kafka topic"
        )

    print("BRONZE STRUCTURAL CHECK: PASS")


def inspect_silver():
    spark = create_spark_session(
        "S76-Inspect-Silver"
    )

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        print("\n=== S7.6 SILVER SCHEMA ===")

        silver = spark.table(SILVER_TABLE)

        silver.printSchema()

        print("\n=== S7.6 SILVER SAMPLE ===")

        silver.show(
            3,
            truncate=False,
            vertical=True,
        )

        print("\nSILVER RECORDS:", silver.count())

        print("\n=== S7.6 GOLD HOURLY SCHEMA ===")

        hourly = spark.table(
            "lakehouse.gold.water_level_hourly_stream"
        )

        hourly.printSchema()

        print("\nGOLD HOURLY RECORDS:", hourly.count())

        print("\n=== S7.6 PRECHECK RESULT ===")
        print("STATUS: PRECHECK_COMPLETE")
        print("NO DATA WRITTEN")

    finally:
        spark.stop()


if __name__ == "__main__":
    inspect_bronze()
    inspect_silver()
