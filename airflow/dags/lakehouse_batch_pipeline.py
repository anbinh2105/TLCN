
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task


TRINO_URL = "http://cantho-flood-trino:8080"
BRONZE_REPORT = Path(
    "/opt/airflow/project-reports/quality/"
    "bronze_quality_summary.json"
)

EXPECTED_SILVER = {
    "weather": 73656,
    "rainfall": 73656,
    "river_discharge": 3069,
    "water_level": 220085,
    "tide": 43822,
}

EXPECTED_WAREHOUSE = {
    "fact_hydrology": 73656,
    "fact_rainfall": 73656,
    "fact_water_level": 73656,
    "fact_data_quality": 73656,
}

EXPECTED_MARTS = {
    "mart_hydrology_overview": 73656,
    "mart_rainfall": 73656,
    "mart_water_level": 73656,
    "mart_flood_risk": 73656,
    "mart_data_quality": 73656,
}

EXPECTED_GOLD_COUNT = 73656
EXPECTED_ML_COUNT = 73656
EXPECTED_ML_SNAPSHOT = 2528388719354325991


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


@dag(
    dag_id="lakehouse_batch_pipeline",
    description=(
        "Read-only verification of completed "
        "Can Tho Flood Lakehouse batch pipeline"
    ),
    start_date=datetime(
        2026, 10, 1, tzinfo=timezone.utc
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "batch", "verification"],
)
def lakehouse_batch_pipeline():

    @task
    def check_bronze_quality():
        if not BRONZE_REPORT.is_file():
            raise FileNotFoundError(BRONZE_REPORT)

        with BRONZE_REPORT.open(
            encoding="utf-8"
        ) as f:
            report = json.load(f)

        total = report["total_objects"]
        passed = report["passed"]
        failed = report["failed"]
        errors = report["errors"]

        if (total, passed, failed, errors) != (
            64, 56, 8, 0
        ):
            raise ValueError(
                "Bronze historical report changed: "
                f"{total}, {passed}, {failed}, {errors}"
            )

        print(
            "BRONZE QUALITY: PASS "
            f"(passed={passed}, "
            f"known_failed={failed}, errors={errors})"
        )

    @task
    def check_silver_tables():
        for source, expected in EXPECTED_SILVER.items():
            sql = (
                "SELECT COUNT(*) "
                f"FROM iceberg.silver.{source}"
            )

            actual = int(trino_scalar(sql))

            print(
                f"SILVER {source}: "
                f"actual={actual}, expected={expected}"
            )

            if actual != expected:
                raise ValueError(
                    f"Silver {source} count mismatch"
                )

        print("SILVER TABLES: PASS")

    @task
    def check_batch_gold():
        actual = int(
            trino_scalar(
                "SELECT COUNT(*) "
                "FROM iceberg.gold.hourly_hydrology"
            )
        )

        if actual != EXPECTED_GOLD_COUNT:
            raise ValueError(
                f"Gold count mismatch: {actual}"
            )

        print(
            f"BATCH GOLD: PASS ({actual} rows)"
        )

    @task
    def check_ml_snapshot():
        count = int(
            trino_scalar(
                "SELECT COUNT(*) "
                "FROM iceberg.gold.ml_hourly"
            )
        )

        snapshot = int(
            trino_scalar(
                'SELECT snapshot_id '
                'FROM iceberg.gold."ml_hourly$refs" '
                "WHERE name = 'main'"
            )
        )

        if count != EXPECTED_ML_COUNT:
            raise ValueError(
                f"ML count mismatch: {count}"
            )

        if snapshot != EXPECTED_ML_SNAPSHOT:
            raise ValueError(
                f"ML snapshot changed: {snapshot}"
            )

        print(
            "ML SNAPSHOT: PASS "
            f"(rows={count}, snapshot={snapshot})"
        )

    @task
    def check_data_warehouse():
        for table, expected in EXPECTED_WAREHOUSE.items():
            actual = int(
                trino_scalar(
                    f"SELECT COUNT(*) FROM iceberg.dw.{table}"
                )
            )

            print(
                f"WAREHOUSE {table}: "
                f"actual={actual}, expected={expected}"
            )

            if actual != expected:
                raise ValueError(
                    f"Warehouse {table} count mismatch: "
                    f"{actual} != {expected}"
                )

        print("DATA WAREHOUSE: PASS")

    @task
    def check_dbt_marts():
        for table, expected in EXPECTED_MARTS.items():
            actual = int(
                trino_scalar(
                    f"SELECT COUNT(*) FROM iceberg.marts.{table}"
                )
            )

            print(
                f"DBT MART {table}: "
                f"actual={actual}, expected={expected}"
            )

            if actual != expected:
                raise ValueError(
                    f"dbt mart {table} count mismatch: "
                    f"{actual} != {expected}"
                )

        print("DBT ANALYTICS MARTS: PASS")

    bronze = check_bronze_quality()
    silver = check_silver_tables()
    gold = check_batch_gold()
    warehouse = check_data_warehouse()
    marts = check_dbt_marts()
    ml = check_ml_snapshot()

    bronze >> silver >> gold >> warehouse >> marts >> ml


lakehouse_batch_pipeline()
