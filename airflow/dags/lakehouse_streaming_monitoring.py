
from __future__ import annotations

import json
import socket
import urllib.request
from datetime import datetime, timezone

from airflow.sdk import dag, task


TRINO_URL = "http://cantho-flood-trino:8080"

# Airflow và Kafka hiện chưa dùng chung Docker network.
# Broker connectivity sẽ được kiểm tra sau khi xác minh
# hostname có thể truy cập từ container Airflow.
KAFKA_HOST = "tlcn-kafka"
KAFKA_PORT = 9092

EXPECTED_COUNTS = {
    "iceberg.silver.water_level_stream": 106,
    "iceberg.gold.water_level_hourly_stream": 27,
    "iceberg.gold.hourly_hydrology_stream": 27,
    "iceberg.gold.mart_streaming_hydrology": 27,
}


def trino_scalar(sql: str):
    headers = {
        "X-Trino-User": "airflow",
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "gold",
    }

    request = urllib.request.Request(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers=headers,
        method="POST",
    )

    rows = []

    while True:
        with urllib.request.urlopen(
            request, timeout=30
        ) as response:
            payload = json.load(response)

        if payload.get("error"):
            raise RuntimeError(payload["error"])

        rows.extend(payload.get("data", []))

        next_uri = payload.get("nextUri")

        if not next_uri:
            break

        request = urllib.request.Request(
            next_uri,
            headers=headers,
            method="GET",
        )

    if not rows:
        raise RuntimeError(
            f"Trino returned no rows: {sql}"
        )

    return rows[0][0]


def check_count(table_name: str):
    expected = EXPECTED_COUNTS[table_name]

    actual = int(
        trino_scalar(
            f"SELECT COUNT(*) FROM {table_name}"
        )
    )

    print(
        f"{table_name}: "
        f"actual={actual}, expected={expected}"
    )

    if actual != expected:
        raise ValueError(
            f"Count mismatch for {table_name}: "
            f"{actual} != {expected}"
        )


@dag(
    dag_id="lakehouse_streaming_monitoring",
    description=(
        "Read-only verification of historical "
        "Kafka replay and streaming Iceberg outputs"
    ),
    start_date=datetime(
        2026, 10, 1, tzinfo=timezone.utc
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "streaming", "monitoring"],
)
def lakehouse_streaming_monitoring():

    @task
    def check_kafka_connection():
        with socket.create_connection(
            (KAFKA_HOST, KAFKA_PORT),
            timeout=10,
        ):
            print(
                f"KAFKA CONNECTION: PASS "
                f"({KAFKA_HOST}:{KAFKA_PORT})"
            )

    @task
    def check_streaming_silver():
        check_count(
            "iceberg.silver.water_level_stream"
        )
        print("STREAMING SILVER: PASS")

    @task
    def check_streaming_gold():
        check_count(
            "iceberg.gold.water_level_hourly_stream"
        )
        check_count(
            "iceberg.gold.hourly_hydrology_stream"
        )
        print("STREAMING GOLD: PASS")

    @task
    def check_streaming_mart():
        check_count(
            "iceberg.gold.mart_streaming_hydrology"
        )
        print("STREAMING MART: PASS")

    kafka = check_kafka_connection()
    silver = check_streaming_silver()
    gold = check_streaming_gold()
    mart = check_streaming_mart()

    kafka >> silver >> gold >> mart


lakehouse_streaming_monitoring()
