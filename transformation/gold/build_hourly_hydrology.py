
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

ROOT = Path(__file__).resolve().parents[2]

CONTRACT_PATH = (
    ROOT / "config" / "gold_hourly_hydrology_contract.yaml"
)

REPORT_PATH = Path(
    os.getenv(
        "GOLD_REPORT_PATH",
        str(
            ROOT / "reports" / "gold" / "quality"
            / "hourly_hydrology_dry_run.json"
        ),
    )
)

TARGET_TABLE = "lakehouse.gold.hourly_hydrology"

SOURCE_COLUMNS = {
    "weather": [
        "temperature_c",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed",
        "wind_direction",
        "weather_quality_flag",
    ],
    "rainfall": [
        "rainfall_mm",
        "rainfall_quality_flag",
    ],
    "discharge": [
        "discharge_m3s",
        "discharge_quality_flag",
        "alignment_method",
        "source_date",
        "source_temporal_grain",
        "is_hourly_observation",
    ],
    "water_level": [
        "station_code",
        "water_level_mean",
        "water_level_min",
        "water_level_max",
        "water_level_last",
        "water_level_last_timestamp",
        "water_level_obs_count",
        "water_level_total_rows",
        "water_level_on_grid_count",
        "water_level_off_grid_count",
        "water_level_non_observed_count",
        "water_level_expected_count",
        "water_level_completeness",
        "water_level_quality_flag",
    ],
    "tide": [
        "tide_height_m",
        "tide_quality_flag",
        "tide_is_recovered",
        "recovery_method",
    ],
}

# Đổi tên metadata để tránh nhầm lẫn khi tích hợp.
RENAMES = {
    "discharge": {
        "alignment_method": "discharge_alignment_method",
        "source_date": "discharge_source_date",
        "source_temporal_grain": "discharge_source_temporal_grain",
        "is_hourly_observation": "discharge_is_hourly_observation",
        "station": "discharge_station",
    },
    "tide": {
        "recovery_method": "tide_recovery_method",
    },
    "water_level": {
        "station_code": "water_level_station_code",
    },
}

MEASUREMENT_COLUMNS = {
    "weather": "temperature_c",
    "rainfall": "rainfall_mm",
    "discharge": "discharge_m3s",
    "water_level": "water_level_mean",
    "tide": "tide_height_m",
}


def load_contract():
    with CONTRACT_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_timeline(spark, contract):
    cfg = contract["time"]
    start = cfg["start"]
    end = cfg["end"]

    return spark.sql(f"""
        SELECT explode(
            sequence(
                timestamp('{start}'),
                timestamp('{end}'),
                interval 1 hour
            )
        ) AS hour_start
    """).withColumn(
        "location_id",
        F.lit(contract["location"]["location_id"])
    )


def prepare_source(spark, source, cfg):
    table = cfg["table"]

    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Source table does not exist: {table}")

    df = spark.table(table)

    columns = ["hour_start"] + SOURCE_COLUMNS[source]

    # Giữ metadata station của Discharge nếu có.
    if source == "discharge":
        columns.append("station")

    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise RuntimeError(
            f"{source}: missing required columns: {missing}"
        )

    df = df.select(*columns)

    # Không tự động deduplicate vì có thể che giấu lỗi dữ liệu.
    stats = df.agg(
        F.count("*").alias("rows"),
        F.count("hour_start").alias("non_null_hours"),
        F.countDistinct("hour_start").alias("unique_hours"),
    ).first()

    if not (
        stats["rows"]
        == stats["non_null_hours"]
        == stats["unique_hours"]
    ):
        raise RuntimeError(
            f"{source}: hour_start contains NULL or duplicates"
        )

    for old, new in RENAMES.get(source, {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Marker được tạo từ sự tồn tại của dòng nguồn,
    # không dựa vào giá trị đo có NULL hay không.
    return df.withColumn(
        f"__{source}_joined",
        F.lit(True)
    )


def build_integrated(spark, contract):
    result = create_timeline(spark, contract)

    for source in [
        "weather",
        "rainfall",
        "discharge",
        "water_level",
        "tide",
    ]:
        cfg = contract["sources"][source]
        source_df = prepare_source(spark, source, cfg)

        result = result.join(
            source_df,
            on="hour_start",
            how="left",
        )

        joined = F.col(f"__{source}_joined")

        result = result.withColumn(
            f"{source}_present",
            F.coalesce(joined, F.lit(False))
        ).drop(f"__{source}_joined")

        quality_col = f"{source}_quality_flag"

        usable_flags = cfg["usable_flags"]

        # Một giờ usable phải có quality flag hợp lệ
        # và giá trị đo chính không NULL.
        result = result.withColumn(
            f"{source}_usable",
            F.coalesce(
                F.col(quality_col).isin(usable_flags)
                & F.col(
                    MEASUREMENT_COLUMNS[source]
                ).isNotNull(),
                F.lit(False),
            )
        )

    # Metadata cố định mô tả phạm vi nghiên cứu.
    # Không đồng nhất các trạm đo vật lý.
    result = result.withColumn(
        "tide_reference_location",
        F.lit(
            contract["location"]["sources"]["tide"][
                "reference_location"
            ]
        )
    ).withColumn(
        "tide_publisher",
        F.lit(
            contract["location"]["sources"]["tide"][
                "publisher"
            ]
        )
    )

    present_cols = [
        F.col(f"{s}_present").cast("int")
        for s in SOURCE_COLUMNS
    ]
    usable_cols = [
        F.col(f"{s}_usable").cast("int")
        for s in SOURCE_COLUMNS
    ]

    result = result.withColumn(
        "present_source_count",
        sum(present_cols)
    ).withColumn(
        "usable_source_count",
        sum(usable_cols)
    ).withColumn(
        "all_five_present",
        F.col("present_source_count") == 5
    ).withColumn(
        "all_five_usable",
        F.col("usable_source_count") == 5
    ).withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )

    return result


def validate_integrated(df, contract):
    expected = contract["time"]["expected_hours"]

    source_names = list(SOURCE_COLUMNS)

    agg_exprs = [
        F.count("*").alias("rows"),
        F.count("hour_start").alias("non_null_hours"),
        F.countDistinct(
            "hour_start"
        ).alias("distinct_hours"),
        F.sum(
            F.col("all_five_present").cast("long")
        ).alias("all_five_present"),
        F.sum(
            F.col("all_five_usable").cast("long")
        ).alias("all_five_usable"),
    ]

    for source in source_names:
        agg_exprs.extend([
            F.sum(
                F.col(f"{source}_present").cast("long")
            ).alias(f"{source}_present"),
            F.sum(
                F.col(f"{source}_usable").cast("long")
            ).alias(f"{source}_usable"),
        ])

    stats = df.agg(*agg_exprs).first().asDict()

    checks = {
        "expected_rows": stats["rows"] == expected,
        "unique_hour": (
            stats["rows"] == stats["non_null_hours"]
            == stats["distinct_hours"]
        ),
        "non_null_location_id": (
            df.filter(
                F.col("location_id").isNull()
            ).limit(1).count() == 0
        ),
        "valid_source_counts": (
            df.filter(
                (F.col("present_source_count") < 0)
                | (F.col("present_source_count") > 5)
                | (F.col("usable_source_count") < 0)
                | (F.col("usable_source_count") > 5)
                | (
                    F.col("usable_source_count")
                    > F.col("present_source_count")
                )
            ).limit(1).count() == 0
        ),
    }

    # Những giờ không có dòng Water Level/Tide
    # phải giữ giá trị đo là NULL.
    for source in source_names:
        measurement = MEASUREMENT_COLUMNS[source]
        invalid = df.filter(
            (~F.col(f"{source}_present"))
            & F.col(measurement).isNotNull()
        ).limit(1).count()

        checks[f"{source}_missing_is_null"] = invalid == 0

    # Đối chiếu thống kê đã xác nhận ở bước 8.6.4.
    # Nếu dữ liệu nguồn được cập nhật, cần kiểm tra lại
    # baseline trước khi thay đổi các số này.
    baseline = {
        "all_five_present": 39571,
        "all_five_usable": 39150,
    }

    for name, expected_value in baseline.items():
        checks[f"{name}_baseline"] = (
            stats[name] == expected_value
        )

    # Kiểm tra các dòng có metadata đặc biệt sau JOIN.
    special = df.agg(
        F.sum(
            F.when(
                F.col("tide_quality_flag") == "RECOVERED",
                1,
            ).otherwise(0)
        ).alias("recovered"),
        F.sum(
            F.when(
                F.col("discharge_alignment_method")
                == "DAILY_EXPANDED",
                1,
            ).otherwise(0)
        ).alias("daily_expanded"),
    ).first()

    stats["tide_recovered"] = special["recovered"]
    stats["discharge_daily_expanded"] = special[
        "daily_expanded"
    ]

    checks["tide_recovered_preserved"] = (
        stats["tide_recovered"] == 4986
    )
    checks["discharge_daily_expanded_preserved"] = (
        stats["discharge_daily_expanded"] == 73656
    )

    return {
        "status": (
            "PASS" if all(checks.values()) else "FAIL"
        ),
        "checks": checks,
        "statistics": stats,
    }


def save_report(report):
    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report["generated_at"] = datetime.now(
        timezone.utc
    ).isoformat()

    REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def write_new_table(spark, df):
    if spark.catalog.tableExists(TARGET_TABLE):
        raise RuntimeError(
            f"Target already exists: {TARGET_TABLE}. "
            "Refusing to overwrite existing Iceberg table."
        )

    # Bảng đích là bảng mới. Không tác động
    # đến năm bảng Gold nguồn.
    df.writeTo(TARGET_TABLE).using(
        "iceberg"
    ).create()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["dry-run", "write"],
        default="dry-run",
    )
    args = parser.parse_args()

    spark = None

    try:
        contract = load_contract()

        spark = create_spark_session(
            app_name="gold-hourly-hydrology",
        )

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        print("\nBuilding Gold Integrated DataFrame...")
        df = build_integrated(spark, contract)

        print("Validating integrated data...")
        report = validate_integrated(df, contract)
        report["mode"] = args.mode
        report["target_table"] = TARGET_TABLE

        save_report(report)

        for name, passed in report["checks"].items():
            print(
                f"[{'PASS' if passed else 'FAIL'}] {name}"
            )

        print("\nSTATUS:", report["status"])
        print("REPORT:", REPORT_PATH)

        if report["status"] != "PASS":
            raise RuntimeError(
                "Dry-run validation failed. "
                "Iceberg write is blocked."
            )

        if args.mode == "write":
            print("Creating new Iceberg table...")
            write_new_table(spark, df)
            print("Table created:", TARGET_TABLE)
        else:
            print("DRY-RUN ONLY: No Iceberg writes.")

    finally:
        if spark is not None:
            stop_spark(spark)


if __name__ == "__main__":
    main()
