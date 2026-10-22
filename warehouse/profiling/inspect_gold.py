"""Step 11.1 - Read-only profiling of the batch Gold hourly hydrology table.

Run from project root:
    python -m warehouse.profiling.inspect_gold
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from transformation.silver.common import ICEBERG_CATALOG, create_spark_session, stop_spark

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TABLE = f"{ICEBERG_CATALOG}.gold.hourly_hydrology"
EXPECTED_COUNT = 73656
BUSINESS_KEYS = ["hour_start", "location_id"]
OUTPUT_DIR = PROJECT_ROOT / "reports" / "warehouse" / "profiling"


def safe_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_value(v) for v in value]
    return value


def run():
    spark = None
    report = {
        "stage": "11.1",
        "mode": "read_only",
        "table": TABLE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_count": EXPECTED_COUNT,
        "business_keys": BUSINESS_KEYS,
        "checks": [],
        "overall_status": "ERROR",
    }

    def check(name, passed, observed=None, expected=None, detail=None):
        entry = {"name": name, "status": "PASS" if passed else "FAIL"}
        if observed is not None:
            entry["observed"] = safe_value(observed)
        if expected is not None:
            entry["expected"] = safe_value(expected)
        if detail:
            entry["detail"] = detail
        report["checks"].append(entry)
        print(f"[{entry['status']}] {name}: {entry.get('observed', '')}")

    try:
        spark = create_spark_session(app_name="cantho-warehouse-profiling", master="local[2]")
        spark.sparkContext.setLogLevel("ERROR")
        exists = spark.catalog.tableExists(TABLE)
        check("gold_table_exists", exists, exists, True)
        if not exists:
            raise RuntimeError(f"Source Iceberg table not found: {TABLE}")

        df = spark.table(TABLE)
        report["schema"] = [
            {"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
            for field in df.schema.fields
        ]
        print("\n=== SCHEMA ===")
        df.printSchema()

        count = df.count()
        report["record_count"] = count
        check("baseline_record_count", count == EXPECTED_COUNT, count, EXPECTED_COUNT)

        missing_keys = [key for key in BUSINESS_KEYS if key not in df.columns]
        check("business_key_columns_exist", not missing_keys, missing_keys, [])
        if not missing_keys:
            null_key_rows = df.filter(
                F.col(BUSINESS_KEYS[0]).isNull() | F.col(BUSINESS_KEYS[1]).isNull()
            ).count()
            check("business_key_not_null", null_key_rows == 0, null_key_rows, 0)
            duplicate_groups = (
                df.groupBy(*BUSINESS_KEYS)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            check("business_key_unique", duplicate_groups == 0, duplicate_groups, 0)

        if "hour_start" in df.columns:
            row = df.agg(
                F.min("hour_start").alias("min_hour_start"),
                F.max("hour_start").alias("max_hour_start"),
            ).first()
            report["time_range"] = safe_value(row.asDict())

        report["dimensions"] = {}
        for name in ("location_id", "station_code", "station_id"):
            if name in df.columns:
                distinct = df.select(name).distinct().count()
                missing = df.filter(F.col(name).isNull()).count()
                report["dimensions"][name] = {"distinct_count": distinct, "null_rows": missing}

        print("\n=== NULL COUNTS ===")
        null_exprs = [
            F.sum(F.when(F.col(f"`{name}`").isNull(), 1).otherwise(0)).alias(name)
            for name in df.columns
        ]
        null_row = df.agg(*null_exprs).first().asDict() if null_exprs else {}
        report["null_counts"] = {key: int(value or 0) for key, value in null_row.items()}
        for name, null_count in report["null_counts"].items():
            print(f"{name}: {null_count}")

        report["quality_columns"] = [
            name for name in df.columns
            if "quality" in name.lower() or "flag" in name.lower()
            or "source_count" in name.lower() or "alignment" in name.lower()
        ]
        report["overall_status"] = (
            "PASS" if all(c["status"] == "PASS" for c in report["checks"]) else "FAIL"
        )
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(f"[ERROR] {report['error']}")
    finally:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output = OUTPUT_DIR / f"gold_profile_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
        output.write_text(json.dumps(safe_value(report), indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nOVERALL: {report['overall_status']}")
        print(f"REPORT: {output.relative_to(PROJECT_ROOT)}")
        stop_spark(spark)
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(run())