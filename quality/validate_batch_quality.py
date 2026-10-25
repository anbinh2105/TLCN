
"""
Batch Quality Validator for TLCN Lakehouse.

Read-only validation of Integrated Gold.
Designed for Spark Standalone and Airflow.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

from quality.validate_gold_integrated import (
    SOURCES,
    MEASUREMENT,
    validate_schema,
    validate_source_integrity,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

CONTRACT_PATH = (
    ROOT / "config" / "gold_hourly_hydrology_contract.yaml"
)

TARGET = "lakehouse.gold.hourly_hydrology"

REPORT_PATH = Path(
    os.getenv(
        "BATCH_QUALITY_REPORT_PATH",
        "/tmp/tlcn/batch/quality/batch_quality_validation.json",
    )
)


# ============================================================
# 2. VALIDATION RESULT COLLECTOR
# ============================================================

class BatchValidator:
    def __init__(self):
        self.checks = []

    def add(self, name, passed, details=None):
        result = {
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "details": details or {},
        }

        self.checks.append(result)

        print(
            f"[{result['status']}] {name}",
            flush=True,
        )

    def report(self):
        passed = sum(
            check["status"] == "PASS"
            for check in self.checks
        )

        failed = len(self.checks) - passed

        return {
            "validator": "batch_quality",
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "table": TARGET,
            "status": "PASS" if failed == 0 else "FAIL",
            "passed": passed,
            "failed": failed,
            "checks": self.checks,
        }


# ============================================================
# 3. BUSINESS KEY VALIDATION
# ============================================================

def validate_business_key(df, validator):
    """
    Validate the Integrated Gold business key:
        (hour_start, location_id)

    Checks:
    - Table is not empty
    - Business key columns are not NULL
    - location_id is not blank
    - Business key is unique

    Read-only operations.
    """

    required_columns = {
        "hour_start",
        "location_id",
    }

    missing_columns = sorted(
        required_columns - set(df.columns)
    )

    validator.add(
        "business_key.columns_exist",
        len(missing_columns) == 0,
        {"missing_columns": missing_columns},
    )

    if missing_columns:
        return

    stats = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("hour_start").isNull()
                | F.col("location_id").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_key_rows"),
        F.sum(
            F.when(
                F.trim(F.col("location_id")) == "",
                1,
            ).otherwise(0)
        ).alias("blank_location_rows"),
    ).first()

    total_rows = stats["total_rows"]
    null_key_rows = stats["null_key_rows"] or 0
    blank_location_rows = stats["blank_location_rows"] or 0

    validator.add(
        "business_key.not_empty",
        total_rows > 0,
        {"total_rows": total_rows},
    )

    validator.add(
        "business_key.not_null",
        null_key_rows == 0,
        {"null_key_rows": null_key_rows},
    )

    validator.add(
        "business_key.location_not_blank",
        blank_location_rows == 0,
        {"blank_location_rows": blank_location_rows},
    )

    duplicate_groups = (
        df.groupBy("hour_start", "location_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    validator.add(
        "business_key.unique",
        duplicate_groups == 0,
        {"duplicate_groups": duplicate_groups},
    )


# ============================================================
# 4. SOURCE FLAGS AND QUALITY SEMANTICS
# ============================================================

def validate_source_flags(df, contract, validator):
    """
    Validate source presence, usability and row-level counts.

    No historical baseline is enforced.
    All operations are read-only.
    """

    required = {
        "present_source_count",
        "usable_source_count",
        "all_five_present",
        "all_five_usable",
    }

    for source in SOURCES:
        required.update({
            f"{source}_present",
            f"{source}_usable",
            f"{source}_quality_flag",
            MEASUREMENT[source],
        })

    missing = sorted(required - set(df.columns))

    validator.add(
        "source_flags.columns_exist",
        not missing,
        {"missing_columns": missing},
    )

    if missing:
        return

    for source in SOURCES:
        present = F.col(f"{source}_present")
        usable = F.col(f"{source}_usable")
        quality = F.col(f"{source}_quality_flag")
        measurement = F.col(MEASUREMENT[source])

        usable_flags = contract["sources"][source][
            "usable_flags"
        ]

        expected_usable = F.coalesce(
            present
            & quality.isin(usable_flags)
            & measurement.isNotNull(),
            F.lit(False),
        )

        invalid_usable = df.filter(
            ~usable.eqNullSafe(expected_usable)
        ).count()

        validator.add(
            f"{source}.usable_semantics",
            invalid_usable == 0,
            {"invalid_rows": invalid_usable},
        )

        invalid_missing = df.filter(
            (~present.eqNullSafe(F.lit(True)))
            & measurement.isNotNull()
        ).count()

        validator.add(
            f"{source}.missing_measurement_is_null",
            invalid_missing == 0,
            {"invalid_rows": invalid_missing},
        )

        null_flags = df.filter(
            present.isNull() | usable.isNull()
        ).count()

        validator.add(
            f"{source}.flags_not_null",
            null_flags == 0,
            {"invalid_rows": null_flags},
        )

    present_sum = sum(
        (
            F.coalesce(
                F.col(f"{source}_present").cast("int"),
                F.lit(0),
            )
            for source in SOURCES
        ),
        F.lit(0),
    )

    usable_sum = sum(
        (
            F.coalesce(
                F.col(f"{source}_usable").cast("int"),
                F.lit(0),
            )
            for source in SOURCES
        ),
        F.lit(0),
    )

    invalid_counts = df.filter(
        ~F.col("present_source_count").eqNullSafe(
            present_sum
        )
        | ~F.col("usable_source_count").eqNullSafe(
            usable_sum
        )
        | ~F.col("all_five_present").eqNullSafe(
            present_sum == len(SOURCES)
        )
        | ~F.col("all_five_usable").eqNullSafe(
            usable_sum == len(SOURCES)
        )
        | (usable_sum > present_sum)
    ).count()

    validator.add(
        "source_flags.row_level_counts",
        invalid_counts == 0,
        {"invalid_rows": invalid_counts},
    )


# ============================================================
# 5. REPORTING
# ============================================================

def save_report(report):
    """
    Save validation results as JSON.

    The report location is configurable through
    BATCH_QUALITY_REPORT_PATH.
    """

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(
        f"Quality report saved: {REPORT_PATH}",
        flush=True,
    )


def print_summary(report):
    """
    Print validation summary to Airflow task logs.
    """

    print("\n" + "=" * 60)
    print("TLCN BATCH QUALITY VALIDATION")
    print("=" * 60)

    print("TABLE:", report["table"])
    print("STATUS:", report["status"])
    print("PASSED:", report["passed"])
    print("FAILED:", report["failed"])
    print("REPORT:", REPORT_PATH)

    print("=" * 60, flush=True)


# ============================================================
# 6. MAIN EXECUTION
# ============================================================

def main():
    spark = None
    validator = BatchValidator()

    try:
        # Load the existing Gold contract.
        with CONTRACT_PATH.open(
            "r", encoding="utf-8"
        ) as file:
            contract = yaml.safe_load(file)

        # Use Spark configuration from the environment.
        # Do not force master="local[2]".
        spark = create_spark_session(
            app_name="tlcn-batch-quality",
        )

        spark.sparkContext.setLogLevel("WARN")

        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        # Check the target before accessing it.
        table_exists = spark.catalog.tableExists(TARGET)

        validator.add(
            "target.table_exists",
            table_exists,
            {"table": TARGET},
        )

        if table_exists:
            df = spark.table(TARGET)

            # Reuse the existing schema validator.
            validate_schema(
                df, contract, validator
            )

            # Validate business keys without fixed row counts.
            validate_business_key(
                df, validator
            )

            # Validate dynamic source flags.
            validate_source_flags(
                df, contract, validator
            )

            # Compare Integrated Gold with source tables.
            validate_source_integrity(
                spark, df, contract, validator
            )

    except Exception as exc:
        validator.add(
            "execution_error",
            False,
            {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        )

    finally:
        if spark is not None:
            try:
                stop_spark(spark)
            except Exception as exc:
                validator.add(
                    "spark_shutdown_error",
                    False,
                    {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    },
                )

    report = validator.report()

    try:
        save_report(report)
    except Exception as exc:
        print(
            f"Failed to save quality report: {exc}",
            flush=True,
        )
        return 1

    print_summary(report)

    if report["status"] != "PASS":
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
