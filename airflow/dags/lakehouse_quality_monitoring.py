
from datetime import datetime, timedelta
import json
import urllib.request

from airflow.sdk import dag, task


TRINO_URL = "http://cantho-flood-trino:8080/v1/statement"

EXPECTED_BATCH_ROWS = 73656
EXPECTED_STREAMING_ROWS = 27
EXPECTED_ML_ROWS = 73656
EXPECTED_ML_SNAPSHOT = 2528388719354325991


def trino_query(sql):
    """Execute a read-only Trino query through its HTTP API."""
    request = urllib.request.Request(
        TRINO_URL,
        data=sql.encode("utf-8"),
        headers={
            "X-Trino-User": "airflow",
            "Content-Type": "text/plain",
        },
        method="POST",
    )

    rows = []

    while True:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)

        if result.get("error"):
            raise RuntimeError(
                f"Trino query failed: {result['error']}"
            )

        rows.extend(result.get("data", []))

        next_uri = result.get("nextUri")

        if not next_uri:
            break

        # nextUri is returned by the trusted Trino server.
        with urllib.request.urlopen(
            next_uri, timeout=60
        ) as response:
            result = json.load(response)

        if result.get("error"):
            raise RuntimeError(
                f"Trino query failed: {result['error']}"
            )

        rows.extend(result.get("data", []))

        next_uri = result.get("nextUri")

        while next_uri:
            with urllib.request.urlopen(
                next_uri, timeout=60
            ) as response:
                result = json.load(response)

            if result.get("error"):
                raise RuntimeError(
                    f"Trino query failed: {result['error']}"
                )

            rows.extend(result.get("data", []))
            next_uri = result.get("nextUri")

        break

    return rows


def get_scalar(sql):
    rows = trino_query(sql)

    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(
            f"Expected one scalar result, got: {rows}"
        )

    return rows[0][0]


@dag(
    dag_id="lakehouse_quality_monitoring",
    description="Read-only validation of Can Tho Lakehouse",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["lakehouse", "quality", "read-only"],
)
def lakehouse_quality_monitoring():

    @task
    def check_trino_connection():
        value = get_scalar("SELECT 1")

        assert value == 1, (
            f"Unexpected Trino response: {value}"
        )

        print("Trino SQL connection: PASS")

    @task
    def check_batch_gold():
        count = get_scalar(
            "SELECT COUNT(*) "
            "FROM iceberg.gold.hourly_hydrology"
        )

        assert count == EXPECTED_BATCH_ROWS, (
            f"Batch Gold row count changed: {count}"
        )

        print(f"Batch Gold: PASS ({count} rows)")

    @task
    def check_streaming_gold():
        count = get_scalar(
            "SELECT COUNT(*) "
            "FROM iceberg.gold.hourly_hydrology_stream"
        )

        assert count == EXPECTED_STREAMING_ROWS, (
            f"Streaming Gold row count changed: {count}"
        )

        print(f"Streaming Gold: PASS ({count} rows)")

    @task
    def check_ml_snapshot():
        count = get_scalar(
            "SELECT COUNT(*) FROM iceberg.gold.ml_hourly"
        )

        snapshot_id = get_scalar(
            'SELECT snapshot_id '
            'FROM iceberg.gold."ml_hourly$refs" '
            "WHERE name = 'main'"
        )

        assert count == EXPECTED_ML_ROWS, (
            f"ML row count changed: {count}"
        )

        assert snapshot_id == EXPECTED_ML_SNAPSHOT, (
            f"ML snapshot changed: {snapshot_id}"
        )

        print(
            f"ML snapshot: PASS "
            f"({count} rows, snapshot={snapshot_id})"
        )

    (
        check_trino_connection()
        >> check_batch_gold()
        >> check_streaming_gold()
        >> check_ml_snapshot()
    )


lakehouse_quality_monitoring()
