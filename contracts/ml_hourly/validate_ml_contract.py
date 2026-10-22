
import csv
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent
CONTRACT_PATH = BASE_DIR / "data_contract.yaml"
REPORT_PATH = BASE_DIR / "validation_report.json"


def query(sql):
    command = [
        "docker", "exec", "-i",
        "cantho-flood-trino",
        "trino",
        "--output-format", "CSV",
    ]

    result = subprocess.run(
        command,
        input=sql.strip() + ";\n",
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    return list(csv.reader(io.StringIO(result.stdout)))


def scalar(sql):
    rows = query(sql)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError(f"Unexpected query result: {rows}")
    return int(rows[0][0])


def main():
    with CONTRACT_PATH.open(encoding="utf-8-sig") as f:
        contract = yaml.safe_load(f)

    table = contract["data_product"]["table"]
    source = contract["data_product"]["source_table"]

    results = []

    def check(name, actual, expected=0):
        passed = actual == expected
        results.append({
            "test": name,
            "actual": actual,
            "expected": expected,
            "status": "PASS" if passed else "FAIL",
        })
        print(f"{'PASS' if passed else 'FAIL'} | {name}: {actual}")

    # 1. Validate schema names and data types
    actual_schema = {
        row[0]: row[1].lower()
        for row in query(f"DESCRIBE {table}")
    }

    type_mapping = {
        "timestamp_with_time_zone": "timestamp(6) with time zone",
        "varchar": "varchar",
        "double": "double",
        "bigint": "bigint",
        "integer": "integer",
        "boolean": "boolean",
        "date": "date",
    }

    expected_schema = {}
    for group in contract["schema"].values():
        for column, data_type in group.items():
            expected_schema[column] = type_mapping[data_type]

    schema_errors = {
        name: {
            "expected": dtype,
            "actual": actual_schema.get(name),
        }
        for name, dtype in expected_schema.items()
        if actual_schema.get(name) != dtype
    }

    unexpected_columns = sorted(
        set(actual_schema) - set(expected_schema)
    )

    check("schema_mismatches", len(schema_errors))
    check("unexpected_columns", len(unexpected_columns))

    # 2. Primary key and time alignment
    check("null_hour_start", scalar(
        f"SELECT COUNT_IF(hour_start IS NULL) FROM {table}"
    ))
    check("null_location_id", scalar(
        f"SELECT COUNT_IF(location_id IS NULL) FROM {table}"
    ))

    check("duplicate_keys", scalar(f"""
        SELECT COUNT(*)
        FROM (
            SELECT hour_start, location_id
            FROM {table}
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
    """))

    check("invalid_hour_alignment", scalar(f"""
        SELECT COUNT_IF(
            minute(at_timezone(hour_start, 'Asia/Ho_Chi_Minh')) <> 0
            OR second(at_timezone(hour_start, 'Asia/Ho_Chi_Minh')) <> 0
            OR millisecond(at_timezone(hour_start, 'Asia/Ho_Chi_Minh')) <> 0
        )
        FROM {table}
    """))

    # 3. Usable values must not be NULL
    for column in [
        "water_level_mean",
        "rainfall_mm",
        "tide_height_m",
        "discharge_m3s",
    ]:
        flag = {
            "water_level_mean": "water_level_usable",
            "rainfall_mm": "rainfall_usable",
            "tide_height_m": "tide_usable",
            "discharge_m3s": "discharge_usable",
        }[column]

        check(f"{column}_usable_but_null", scalar(f"""
            SELECT COUNT_IF(
                {flag} = TRUE AND {column} IS NULL
            )
            FROM {table}
        """))

    # 4. Overall quality consistency
    check("invalid_all_five_usable", scalar(f"""
        SELECT COUNT_IF(
            all_five_usable = TRUE
            AND NOT (
                COALESCE(water_level_usable, FALSE)
                AND COALESCE(rainfall_usable, FALSE)
                AND COALESCE(tide_usable, FALSE)
                AND COALESCE(discharge_usable, FALSE)
                AND COALESCE(weather_usable, FALSE)
            )
        )
        FROM {table}
    """))

    # 5. Full row comparison with Gold source
    # CTAS selected columns have the same names as Gold.
    comparisons = " OR ".join(
        f"m.{col} IS DISTINCT FROM g.{col}"
        for col in expected_schema
    )

    check("source_mismatched_rows", scalar(f"""
        SELECT COUNT_IF(
            m.hour_start IS NULL OR {comparisons}
        )
        FROM {source} g
        LEFT JOIN {table} m
          ON g.hour_start = m.hour_start
         AND g.location_id = m.location_id
    """))

    source_count = scalar(f"SELECT COUNT(*) FROM {source}")
    ml_count = scalar(f"SELECT COUNT(*) FROM {table}")

    check("row_count_difference", ml_count - source_count)

    # 6. Snapshot for reproducibility
    snapshots = query(f"""
        SELECT CAST(snapshot_id AS VARCHAR)
        FROM iceberg.gold."ml_hourly$snapshots"
        ORDER BY committed_at DESC
        LIMIT 1
    """)

    current_snapshot = snapshots[0][0] if snapshots else None
    expected_snapshot = str(
        contract["versioning"]["iceberg_snapshot_id"]
    )

    check(
        "snapshot_matches_contract",
        current_snapshot,
        expected_snapshot,
    )

    passed = sum(r["status"] == "PASS" for r in results)
    failed = len(results) - passed

    report = {
        "data_product": table,
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "contract_version": contract["contract_version"],
        "snapshot_id": current_snapshot,
        "total_rows": ml_count,
        "summary": {
            "passed": passed,
            "failed": failed,
            "status": "PASS" if failed == 0 else "FAIL",
        },
        "schema_errors": schema_errors,
        "unexpected_columns": unexpected_columns,
        "tests": results,
    }

    REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\nReport: {REPORT_PATH}")
    print(f"PASS={passed} FAIL={failed}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
