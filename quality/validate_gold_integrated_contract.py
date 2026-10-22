
from pathlib import Path
from datetime import datetime, timezone
import json

import yaml
from pyspark.sql import functions as F
from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_FILE = ROOT / "config/gold_hourly_hydrology_contract.yaml"
REPORT_FILE = ROOT / "reports/gold/quality/gold_integrated_contract_validation.json"

EXPECTED_QUALITY_COUNTS = {
    "weather": {"OBSERVED": 73639, "MISSING": 17},
    "rainfall": {"OBSERVED": 73639, "MISSING": 17},
    "discharge": {"OBSERVED": 73656},
    "water_level": {
        "COMPLETE": 54485,
        "OFF_GRID_OBSERVATIONS": 372,
        "INCOMPLETE": 125,
    },
    "tide": {"OBSERVED": 38836, "RECOVERED": 4986},
}

checks = []


def check(name, passed, details=None):
    status = "PASS" if passed else "FAIL"
    checks.append({
        "check": name,
        "status": status,
        "details": details or {},
    })
    print(f"[{status}] {name}")


def validate_schema(df, source, cfg):
    actual = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    mismatches = {}
    for column, expected_type in cfg["required_columns"].items():
        actual_type = actual.get(column)
        if actual_type != expected_type:
            mismatches[column] = {
                "expected": expected_type,
                "actual": actual_type,
            }

    check(
        f"{source}.schema",
        not mismatches,
        {"mismatches": mismatches},
    )


def validate_hourly_key(df, source):
    stats = df.agg(
        F.count("*").alias("rows"),
        F.count("hour_start").alias("non_null"),
        F.countDistinct("hour_start").alias("distinct_hours"),
    ).first()

    rows = stats["rows"]
    non_null = stats["non_null"]
    distinct = stats["distinct_hours"]

    check(
        f"{source}.unique_hour",
        rows == non_null == distinct,
        {
            "rows": rows,
            "distinct_hours": distinct,
            "null_hours": rows - non_null,
            "duplicate_rows": non_null - distinct,
        },
    )


def validate_quality(df, source, cfg):
    quality_column = cfg["quality_column"]

    if quality_column not in df.columns:
        check(
            f"{source}.quality",
            False,
            {"missing_column": quality_column},
        )
        return

    actual_counts = {
        row[quality_column]: row["count"]
        for row in df.groupBy(quality_column).count().collect()
    }

    unexpected = [
        flag for flag in actual_counts
        if flag not in cfg["allowed_flags"]
    ]

    check(
        f"{source}.quality_flags",
        not unexpected,
        {
            "counts": actual_counts,
            "unexpected": unexpected,
        },
    )

    expected_counts = EXPECTED_QUALITY_COUNTS[source]

    check(
        f"{source}.quality_baseline",
        actual_counts == expected_counts,
        {
            "actual": actual_counts,
            "expected": expected_counts,
        },
    )


def validate_timeline(spark, cfg):
    start = cfg["start"]
    end = cfg["end"]
    expected = cfg["expected_hours"]

    timeline = spark.sql(f"""
        SELECT explode(
            sequence(
                timestamp('{start}'),
                timestamp('{end}'),
                interval 1 hour
            )
        ) AS hour_start
    """)

    stats = timeline.agg(
        F.count("*").alias("rows"),
        F.countDistinct("hour_start").alias("distinct"),
        F.min("hour_start").alias("min_hour"),
        F.max("hour_start").alias("max_hour"),
    ).first()

    actual = stats["rows"]

    check(
        "canonical_timeline",
        actual == expected
        and stats["distinct"] == expected
        and str(stats["min_hour"]) == start
        and str(stats["max_hour"]) == end,
        {
            "expected": expected,
            "actual": actual,
            "distinct": stats["distinct"],
            "min": str(stats["min_hour"]),
            "max": str(stats["max_hour"]),
        },
    )


def validate_preservation(spark, contract):
    policy = contract["integration"]

    check(
        "preserve_missing_policy",
        policy.get("join_type") == "left"
        and policy.get("preserve_nulls") is True
        and policy.get("drop_incomplete_rows") is False
        and policy.get("fill_missing_with_zero") is False,
        {"policy": policy},
    )

    tide = spark.table("lakehouse.gold.tide_hourly")
    recovered = tide.filter(
        F.col("tide_quality_flag") == "RECOVERED"
    ).count()

    check(
        "tide_recovered_preserved_in_source",
        recovered == 4986,
        {"recovered_rows": recovered},
    )

    discharge = spark.table("lakehouse.gold.discharge_hourly")
    methods = {
        row["alignment_method"]: row["count"]
        for row in discharge.groupBy(
            "alignment_method"
        ).count().collect()
    }

    check(
        "discharge_alignment_preserved_in_source",
        methods == {"DAILY_EXPANDED": 73656},
        {"alignment_methods": methods},
    )


def validate_source_mapping(contract):
    sources = contract["location"]["sources"]
    tide = sources["tide"]
    water = sources["water_level"]

    passed = (
        tide.get("reference_location") == "Dinh An"
        and tide.get("publisher") == "SPTC"
        and tide.get("source_file_count") == 60
        and water.get("station_code") == "039803"
        and tide.get("reference_location") != "Can Tho"
    )

    check(
        "source_location_mapping",
        passed,
        {
            "tide": tide,
            "water_level": water,
        },
    )


def main():
    spark = None

    try:
        with CONTRACT_FILE.open(
            "r", encoding="utf-8"
        ) as file:
            contract = yaml.safe_load(file)

        required_sections = [
            "dataset", "grain", "location",
            "time", "sources", "integration", "ml"
        ]

        check(
            "contract_structure",
            isinstance(contract, dict)
            and all(
                section in contract
                for section in required_sections
            ),
        )

        spark = create_spark_session(
            app_name="gold-integrated-contract-validation",
            master="local[2]",
        )
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        validate_timeline(spark, contract["time"])

        for source, cfg in contract["sources"].items():
            table = cfg["table"]

            exists = spark.catalog.tableExists(table)
            check(
                f"{source}.table_exists",
                exists,
                {"table": table},
            )

            if not exists:
                continue

            df = spark.table(table)

            validate_schema(df, source, cfg)

            if "hour_start" in df.columns:
                validate_hourly_key(df, source)
            else:
                check(
                    f"{source}.unique_hour",
                    False,
                    {"error": "Missing hour_start"},
                )

            validate_quality(df, source, cfg)

        validate_preservation(spark, contract)
        validate_source_mapping(contract)

    except Exception as exc:
        check(
            "execution_error",
            False,
            {"error": str(exc)},
        )
        raise

    finally:
        if spark is not None:
            stop_spark(spark)

        passed = sum(
            c["status"] == "PASS"
            for c in checks
        )
        failed = sum(
            c["status"] == "FAIL"
            for c in checks
        )

        report = {
            "step": "9.1.5",
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "status": "PASS" if failed == 0 else "FAIL",
            "passed": passed,
            "failed": failed,
            "checks": checks,
        }

        REPORT_FILE.parent.mkdir(
            parents=True, exist_ok=True
        )

        REPORT_FILE.write_text(
            json.dumps(
                report, ensure_ascii=False, indent=2
            ),
            encoding="utf-8",
        )

        print("\n" + "=" * 60)
        print("STEP 9.1.5 - CONTRACT VALIDATION")
        print("=" * 60)
        print("STATUS:", report["status"])
        print("PASS:", passed)
        print("FAIL:", failed)
        print("REPORT:", REPORT_FILE)


if __name__ == "__main__":
    main()
