
import json
from datetime import datetime
from pathlib import Path

import yaml
from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session


CONFIG_PATH = Path("config/gold_quality_rules.yaml")
REPORT_PATH = Path(
    "reports/gold/quality/gold_alignment_report.json"
)

QUALITY_COLUMNS = {
    "rainfall": "rainfall_quality_flag",
    "weather": "weather_quality_flag",
    "tide": "tide_quality_flag",
}


def validate_table(spark, source, rules):
    table_name = rules["name"]
    checks = []

    def check(name, passed, actual, expected):
        checks.append({
            "name": name,
            "success": bool(passed),
            "actual": actual,
            "expected": expected,
        })

        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}: {actual}")

    print(f"\n{'=' * 65}")
    print(f"VALIDATING: {table_name}")
    print("=" * 65)

    if not spark.catalog.tableExists(table_name):
        check("table_exists", False, False, True)
        return {
            "table": table_name,
            "success": False,
            "checks": checks,
        }

    df = spark.table(table_name)
    columns = set(df.columns)

    required = set(rules["required_columns"])
    keys = rules["keys"]

    missing = sorted(required - columns)

    check(
        "required_columns",
        len(missing) == 0,
        missing,
        [],
    )

    if missing:
        return {
            "table": table_name,
            "success": False,
            "checks": checks,
        }

    missing_keys = sorted(set(keys) - columns)

    if missing_keys:
        check("key_columns", False, missing_keys, [])
        return {
            "table": table_name,
            "success": False,
            "checks": checks,
        }

    actual_rows = df.count()
    expected_rows = rules["expected_rows"]

    check(
        "record_count",
        actual_rows == expected_rows,
        actual_rows,
        expected_rows,
    )

    null_keys = df.filter(
        F.expr(
            " OR ".join(
                f"`{column}` IS NULL"
                for column in keys
            )
        )
    ).count()

    check("null_keys", null_keys == 0, null_keys, 0)

    duplicate_keys = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    check(
        "duplicate_keys",
        duplicate_keys == 0,
        duplicate_keys,
        0,
    )

    off_hour = df.filter(
        F.col("hour_start") !=
        F.date_trunc("hour", F.col("hour_start"))
    ).count()

    check(
        "off_hour_timestamps",
        off_hour == 0,
        off_hour,
        0,
    )

    # Validate quality flag distributions
    if source in QUALITY_COLUMNS:
        quality_column = QUALITY_COLUMNS[source]

        actual_quality = {
            str(row[quality_column]): row["count"]
            for row in (
                df.groupBy(quality_column)
                .count()
                .collect()
            )
        }

        expected_quality = rules.get(
            "expected_quality_counts", {}
        )

        check(
            "quality_distribution",
            actual_quality == expected_quality,
            actual_quality,
            expected_quality,
        )

    # Tide: preserve recovered records
    if source == "tide":
        invalid_recovery = df.filter(
            (
                (F.col("tide_quality_flag") == "RECOVERED")
                & (
                    (F.col("tide_is_recovered") != True)
                    | F.col("tide_is_recovered").isNull()
                    | F.col("recovery_method").isNull()
                )
            )
            | (
                (F.col("tide_quality_flag") == "OBSERVED")
                & (
                    (F.col("tide_is_recovered") != False)
                    | F.col("tide_is_recovered").isNull()
                )
            )
        ).count()

        check(
            "recovery_metadata",
            invalid_recovery == 0,
            invalid_recovery,
            0,
        )

        missing_hours = [
            "2021-11-29 01:00:00",
            "2021-11-29 02:00:00",
        ]

        unexpected_hours = df.filter(
            F.col("hour_start").isin(missing_hours)
        ).count()

        check(
            "known_missing_hours",
            unexpected_hours == 0,
            unexpected_hours,
            0,
        )

    # Discharge: expanded daily records
    if source == "discharge":
        invalid_metadata = df.filter(
            F.col("alignment_method").isNull()
            | (F.col("alignment_method") != "DAILY_EXPANDED")
            | F.col("source_temporal_grain").isNull()
            | (F.col("source_temporal_grain") != "DAILY")
            | F.col("is_hourly_observation").isNull()
            | (F.col("is_hourly_observation") != False)
            | F.col("source_date").isNull()
            | (
                F.to_date("hour_start")
                != F.col("source_date")
            )
        ).count()

        check(
            "alignment_metadata",
            invalid_metadata == 0,
            invalid_metadata,
            0,
        )

        invalid_days = (
            df.groupBy("station", "source_date")
            .agg(
                F.count("*").alias("hour_count"),
                F.countDistinct("discharge_m3s")
                .alias("distinct_discharge"),
            )
            .filter(
                (F.col("hour_count") != 24)
                | (F.col("distinct_discharge") > 1)
            )
            .count()
        )

        check(
            "daily_expansion_24h",
            invalid_days == 0,
            invalid_days,
            0,
        )

    # Collect temporal coverage
    coverage = df.agg(
        F.min("hour_start").alias("min_timestamp"),
        F.max("hour_start").alias("max_timestamp"),
    ).first()

    success = all(
        item["success"] for item in checks
    )

    return {
        "table": table_name,
        "success": success,
        "row_count": actual_rows,
        "min_timestamp": str(coverage["min_timestamp"]),
        "max_timestamp": str(coverage["max_timestamp"]),
        "checks": checks,
    }


def main():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)["gold_alignment"]

    spark = create_spark_session(
        app_name="GoldAlignmentValidation",
        master="local[2]",
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            config["timezone"],
        )
        spark.conf.set("spark.sql.shuffle.partitions", "4")

        results = []

        for source, rules in config["tables"].items():
            result = validate_table(
                spark, source, rules
            )
            results.append(result)

        passed = sum(
            result["success"] for result in results
        )

        report = {
            "validation_name": "gold_alignment",
            "validated_at": datetime.now().isoformat(),
            "passed_tables": passed,
            "failed_tables": len(results) - passed,
            "results": results,
        }

        REPORT_PATH.parent.mkdir(
            parents=True, exist_ok=True
        )

        with REPORT_PATH.open(
            "w", encoding="utf-8"
        ) as f:
            json.dump(
                report, f,
                ensure_ascii=False,
                indent=2,
            )

        print("\n" + "=" * 65)
        print("GOLD ALIGNMENT VALIDATION SUMMARY")
        print("=" * 65)

        print("PASS:", passed)
        print("FAIL:", len(results) - passed)
        print("Report:", REPORT_PATH)

        if passed != len(results):
            raise RuntimeError(
                "Gold Alignment Validation FAILED"
            )

        print("GOLD ALIGNMENT VALIDATION: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
