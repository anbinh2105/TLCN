
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
)

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "config" / "gold_hourly_hydrology_contract.yaml"
REPORT_PATH = ROOT / "reports" / "gold" / "quality" / "gold_integrated_validation.json"

TARGET = "lakehouse.gold.hourly_hydrology"

SOURCES = ["weather", "rainfall", "discharge", "water_level", "tide"]

# Mapping từ cột nguồn sang cột trong bảng Integrated.
SOURCE_MAP = {
    "weather": {
        "temperature_c": "temperature_c",
        "humidity_pct": "humidity_pct",
        "pressure_hpa": "pressure_hpa",
        "wind_speed": "wind_speed",
        "wind_direction": "wind_direction",
        "weather_quality_flag": "weather_quality_flag",
    },
    "rainfall": {
        "rainfall_mm": "rainfall_mm",
        "rainfall_quality_flag": "rainfall_quality_flag",
    },
    "discharge": {
        "station": "discharge_station",
        "discharge_m3s": "discharge_m3s",
        "discharge_quality_flag": "discharge_quality_flag",
        "alignment_method": "discharge_alignment_method",
        "source_date": "discharge_source_date",
        "source_temporal_grain": "discharge_source_temporal_grain",
        "is_hourly_observation": "discharge_is_hourly_observation",
    },
    "water_level": {
        "station_code": "water_level_station_code",
        "water_level_mean": "water_level_mean",
        "water_level_min": "water_level_min",
        "water_level_max": "water_level_max",
        "water_level_last": "water_level_last",
        "water_level_last_timestamp": "water_level_last_timestamp",
        "water_level_obs_count": "water_level_obs_count",
        "water_level_total_rows": "water_level_total_rows",
        "water_level_on_grid_count": "water_level_on_grid_count",
        "water_level_off_grid_count": "water_level_off_grid_count",
        "water_level_non_observed_count": "water_level_non_observed_count",
        "water_level_expected_count": "water_level_expected_count",
        "water_level_completeness": "water_level_completeness",
        "water_level_quality_flag": "water_level_quality_flag",
    },
    "tide": {
        "tide_height_m": "tide_height_m",
        "tide_quality_flag": "tide_quality_flag",
        "tide_is_recovered": "tide_is_recovered",
        "recovery_method": "tide_recovery_method",
    },
}

MEASUREMENT = {
    "weather": "temperature_c",
    "rainfall": "rainfall_mm",
    "discharge": "discharge_m3s",
    "water_level": "water_level_mean",
    "tide": "tide_height_m",
}

BASELINE = {
    "weather_present": 73656,
    "weather_usable": 73639,
    "rainfall_present": 73656,
    "rainfall_usable": 73639,
    "discharge_present": 73656,
    "discharge_usable": 73656,
    "water_level_present": 54982,
    "water_level_usable": 54485,
    "tide_present": 43822,
    "tide_usable": 43822,
    "all_five_present": 39571,
    "all_five_usable": 39150,
    "tide_recovered": 4986,
    "discharge_daily_expanded": 73656,
}


class Validator:
    def __init__(self):
        self.checks = []

    def add(self, name, passed, details=None):
        item = {
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "details": details or {},
        }
        self.checks.append(item)
        print(f"[{item['status']}] {name}")

    def report(self):
        passed = sum(c["status"] == "PASS" for c in self.checks)
        failed = len(self.checks) - passed
        return {
            "step": "9.3",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "table": TARGET,
            "status": "PASS" if failed == 0 else "FAIL",
            "passed": passed,
            "failed": failed,
            "checks": self.checks,
        }


def count_where(df, condition):
    return df.filter(condition).count()


def validate_schema(df, contract, v):
    actual = dict(df.dtypes)

    required = {
        "hour_start": "timestamp",
        "location_id": "string",
        "present_source_count": "int",
        "usable_source_count": "int",
        "all_five_present": "boolean",
        "all_five_usable": "boolean",
        "gold_processed_at": "timestamp",
    }

    # Kế thừa kiểu dữ liệu từ chính bảng nguồn.
    # Đảm bảo bảng Integrated không tự đổi datatype.
    for source in SOURCES:
        required[f"{source}_present"] = "boolean"
        required[f"{source}_usable"] = "boolean"

    for name, dtype in required.items():
        v.add(
            f"schema.{name}",
            actual.get(name) == dtype,
            {"expected": dtype, "actual": actual.get(name)},
        )

    for source in SOURCES:
        table = contract["sources"][source]["table"]
        source_schema = dict(df.sparkSession.table(table).dtypes)

        for old, new in SOURCE_MAP[source].items():
            expected = source_schema.get(old)
            found = actual.get(new)
            v.add(
                f"schema.{source}.{new}",
                expected is not None and expected == found,
                {"expected": expected, "actual": found},
            )


def validate_timeline(df, contract, v):
    cfg = contract["time"]
    expected = int(cfg["expected_hours"])
    location_id = contract["location"]["location_id"]

    stats = df.agg(
        F.count("*").alias("rows"),
        F.count("hour_start").alias("non_null_hours"),
        F.countDistinct(
            "hour_start", "location_id"
        ).alias("unique_keys"),
        F.min("hour_start").cast("string").alias("min_hour"),
        F.max("hour_start").cast("string").alias("max_hour"),
    ).first().asDict()

    v.add(
        "business_key.row_count",
        stats["rows"] == expected,
        stats,
    )

    v.add(
        "business_key.not_null",
        stats["non_null_hours"] == stats["rows"]
        and count_where(df, F.col("location_id").isNull()) == 0,
    )

    v.add(
        "business_key.unique",
        stats["unique_keys"] == stats["rows"],
    )

    v.add(
        "location_id.matches_contract",
        count_where(
            df, F.col("location_id") != location_id
        ) == 0,
    )

    # So sánh trực tiếp với timeline kỳ vọng,
    # không chỉ dựa vào min/max và row count.
    start = str(cfg["start"])
    end = str(cfg["end"])

    expected_df = df.sparkSession.sql(f"""
        SELECT explode(sequence(
            timestamp('{start}'),
            timestamp('{end}'),
            interval 1 hour
        )) AS hour_start
    """)

    actual_hours = df.select("hour_start").distinct()

    missing = expected_df.join(
        actual_hours, "hour_start", "left_anti"
    ).count()

    unexpected = actual_hours.join(
        expected_df, "hour_start", "left_anti"
    ).count()

    v.add(
        "temporal.exact_canonical_timeline",
        missing == 0 and unexpected == 0,
        {
            "missing_hours": missing,
            "unexpected_hours": unexpected,
        },
    )


def validate_flags(df, contract, v):
    expressions = []

    for source in SOURCES:
        for suffix in ("present", "usable"):
            name = f"{source}_{suffix}"
            expressions.append(
                F.sum(F.col(name).cast("long")).alias(name)
            )

    expressions.extend([
        F.sum(F.col("all_five_present").cast("long")).alias(
            "all_five_present"
        ),
        F.sum(F.col("all_five_usable").cast("long")).alias(
            "all_five_usable"
        ),
        F.sum(
            F.when(
                F.col("tide_quality_flag") == "RECOVERED", 1
            ).otherwise(0)
        ).alias("tide_recovered"),
        F.sum(
            F.when(
                F.col("discharge_alignment_method")
                == "DAILY_EXPANDED", 1
            ).otherwise(0)
        ).alias("discharge_daily_expanded"),
    ])

    counts = df.agg(*expressions).first().asDict()

    for name, expected in BASELINE.items():
        v.add(
            f"baseline.{name}",
            counts[name] == expected,
            {"expected": expected, "actual": counts[name]},
        )

    for source in SOURCES:
        present = F.col(f"{source}_present")
        usable = F.col(f"{source}_usable")
        quality = F.col(f"{source}_quality_flag")
        measure = F.col(MEASUREMENT[source])

        allowed = contract["sources"][source]["usable_flags"]

        # usable = present AND quality allowed AND measurement non-NULL.
        expected_usable = (
            present
            & quality.isin(allowed)
            & measure.isNotNull()
        )

        invalid_usable = count_where(
            df,
            ~usable.eqNullSafe(
                F.coalesce(expected_usable, F.lit(False))
            ),
        )

        v.add(
            f"{source}.usable_semantics",
            invalid_usable == 0,
            {"invalid_rows": invalid_usable},
        )

        # Dòng nguồn không tồn tại -> measurement phải NULL.
        invalid_missing = count_where(
            df, (~present) & measure.isNotNull()
        )

        v.add(
            f"{source}.missing_measurement_is_null",
            invalid_missing == 0,
            {"invalid_rows": invalid_missing},
        )

        null_flags = count_where(
            df, present.isNull() | usable.isNull()
        )

        v.add(
            f"{source}.flags_not_null",
            null_flags == 0,
            {"invalid_rows": null_flags},
        )

    # Kiểm tra count và all-five flags theo từng dòng.
    present_sum = sum(
        F.col(f"{s}_present").cast("int") for s in SOURCES
    )
    usable_sum = sum(
        F.col(f"{s}_usable").cast("int") for s in SOURCES
    )

    invalid = count_where(
        df,
        ~F.col("present_source_count").eqNullSafe(present_sum)
        | ~F.col("usable_source_count").eqNullSafe(usable_sum)
        | ~F.col("all_five_present").eqNullSafe(
            present_sum == 5
        )
        | ~F.col("all_five_usable").eqNullSafe(
            usable_sum == 5
        )
        | (usable_sum > present_sum),
    )

    v.add(
        "cross_source.row_level_counts",
        invalid == 0,
        {"invalid_rows": invalid},
    )

    # RECOVERED phải giữ cờ và metadata tương ứng.
    recovered_invalid = count_where(
        df,
        (F.col("tide_quality_flag") == "RECOVERED")
        & ~F.col("tide_is_recovered").eqNullSafe(F.lit(True)),
    )

    v.add(
        "tide.recovery_flag_consistency",
        recovered_invalid == 0,
        {"invalid_rows": recovered_invalid},
    )

    # DAILY_EXPANDED không được diễn giải là phép đo giờ.
    discharge_invalid = count_where(
        df,
        (F.col("discharge_alignment_method") == "DAILY_EXPANDED")
        & ~F.col("discharge_is_hourly_observation").eqNullSafe(
            F.lit(False)
        ),
    )

    v.add(
        "discharge.daily_expansion_semantics",
        discharge_invalid == 0,
        {"invalid_rows": discharge_invalid},
    )


def validate_source_integrity(spark, df, contract, v):
    """
    Đối chiếu từng giá trị nguồn và Integrated.
    eqNullSafe bảo đảm NULL == NULL được coi là khớp.
    """
    for source in SOURCES:
        source_table = contract["sources"][source]["table"]
        src = spark.table(source_table)

        mapping = SOURCE_MAP[source]

        src = src.select(
            F.col("hour_start"),
            *[
                F.col(old).alias(f"src__{new}")
                for old, new in mapping.items()
            ],
            F.lit(True).alias("src__exists"),
        )

        target_cols = [
            "hour_start",
            f"{source}_present",
            *mapping.values(),
        ]

        tgt = df.select(*target_cols)

        # LEFT JOIN từ bảng Integrated: kiểm tra cả giờ có
        # và không có dữ liệu nguồn.
        joined = tgt.join(
            src, "hour_start", "left"
        )

        actual_present = F.coalesce(
            F.col("src__exists"), F.lit(False)
        )

        mismatch = ~F.col(
            f"{source}_present"
        ).eqNullSafe(actual_present)

        for integrated_col in mapping.values():
            mismatch = mismatch | ~F.col(
                integrated_col
            ).eqNullSafe(
                F.col(f"src__{integrated_col}")
            )

        invalid = joined.filter(mismatch).count()

        v.add(
            f"source_integrity.{source}",
            invalid == 0,
            {
                "source_table": source_table,
                "mismatched_rows": invalid,
                "compared_columns": list(mapping.values()),
            },
        )


def save_report(report):
    REPORT_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )
    REPORT_PATH.write_text(
        json.dumps(
            report, indent=2, ensure_ascii=False
        ),
        encoding="utf-8",
    )


def main():
    spark = None
    validator = Validator()

    try:
        with CONTRACT_PATH.open(
            "r", encoding="utf-8"
        ) as f:
            contract = yaml.safe_load(f)

        spark = create_spark_session(
            app_name="validate-gold-integrated",
            master="local[2]",
        )
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        if not spark.catalog.tableExists(TARGET):
            validator.add(
                "target.table_exists", False
            )
            return

        validator.add(
            "target.table_exists", True
        )

        df = spark.table(TARGET)

        validate_schema(df, contract, validator)
        validate_timeline(df, contract, validator)
        validate_flags(df, contract, validator)
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
        raise

    finally:
        report = validator.report()
        save_report(report)

        print("\n" + "=" * 60)
        print("STEP 9.3 - GOLD INTEGRATED DATA QUALITY")
        print("=" * 60)
        print("STATUS:", report["status"])
        print("PASS:", report["passed"])
        print("FAIL:", report["failed"])
        print("REPORT:", REPORT_PATH)

        if spark is not None:
            stop_spark(spark)


if __name__ == "__main__":
    main()
