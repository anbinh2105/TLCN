
import csv
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)


START = "2018-04-25 00:00:00"
END = "2026-09-18 23:00:00"
EXPECTED_HOURS = 73656

OUTPUT_DIR = Path("reports/gold/quality")
JSON_PATH = OUTPUT_DIR / "cross_source_coverage.json"
CSV_PATH = OUTPUT_DIR / "cross_source_coverage_yearly.csv"

# These are analytical policies, not official
# hydrological quality standards.
CONFIG = {
    "weather": {
        "table": "lakehouse.gold.weather_hourly",
        "value": "temperature_c",
        "quality": "weather_quality_flag",
        "accepted": ["OBSERVED"],
    },
    "rainfall": {
        "table": "lakehouse.gold.rainfall_hourly",
        "value": "rainfall_mm",
        "quality": "rainfall_quality_flag",
        "accepted": ["OBSERVED"],
    },
    "discharge": {
        "table": "lakehouse.gold.discharge_hourly",
        "value": "discharge_m3s",
        "quality": "discharge_quality_flag",
        "accepted": ["OBSERVED"],
    },
    "water_level": {
        "table": "lakehouse.gold.water_level_hourly",
        "value": "water_level_mean",
        "quality": "water_level_quality_flag",
        "accepted": ["COMPLETE"],
    },
    "tide": {
        "table": "lakehouse.gold.tide_hourly",
        "value": "tide_height_m",
        "quality": "tide_quality_flag",
        "accepted": ["OBSERVED", "RECOVERED"],
    },
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def timeline_df(spark):
    return spark.sql(
        f"""
        SELECT explode(
            sequence(
                timestamp('{START}'),
                timestamp('{END}'),
                interval 1 hour
            )
        ) AS hour_start
        """
    )


def source_flags(spark, name, cfg):
    table = cfg["table"]

    require(
        spark.catalog.tableExists(table),
        f"Missing table: {table}",
    )

    df = spark.table(table)

    required = {
        "hour_start",
        cfg["value"],
        cfg["quality"],
    }

    missing = required - set(df.columns)

    require(
        not missing,
        f"{table}: missing columns {sorted(missing)}",
    )

    # The existing Gold validation showed one row
    # per hour for each of the five sources.
    # Recheck to avoid a many-to-many join.
    duplicate_hours = (
        df.groupBy("hour_start")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    require(
        duplicate_hours == 0,
        f"{table}: duplicate hourly keys={duplicate_hours}",
    )

    usable = (
        F.col(cfg["value"]).isNotNull()
        & F.col(cfg["quality"]).isin(cfg["accepted"])
    )

    result = df.select(
        "hour_start",
        F.lit(1).alias(f"{name}_present"),
        F.when(usable, 1)
        .otherwise(0)
        .alias(f"{name}_usable"),
    )

    if name == "tide":
        result = result.join(
            df.select(
                "hour_start",
                F.when(
                    F.col("tide_quality_flag") == "RECOVERED",
                    1,
                ).otherwise(0).alias("tide_recovered"),
            ),
            "hour_start",
            "inner",
        )

    if name == "water_level":
        result = result.join(
            df.select(
                "hour_start",
                F.when(
                    F.col("water_level_quality_flag")
                    == "INCOMPLETE",
                    1,
                ).otherwise(0).alias("water_level_incomplete"),
                F.when(
                    F.col("water_level_quality_flag")
                    == "OFF_GRID_OBSERVATIONS",
                    1,
                ).otherwise(0).alias("water_level_off_grid"),
            ),
            "hour_start",
            "inner",
        )

    return result


def build_matrix(spark):
    matrix = timeline_df(spark)

    for name, cfg in CONFIG.items():
        flags = source_flags(spark, name, cfg)

        matrix = matrix.join(
            flags,
            "hour_start",
            "left",
        )

    flag_columns = [
        c for c in matrix.columns
        if c != "hour_start"
    ]

    matrix = matrix.fillna(
        0,
        subset=flag_columns,
    )

    names = list(CONFIG.keys())

    present_cols = [
        F.col(f"{name}_present")
        for name in names
    ]

    usable_cols = [
        F.col(f"{name}_usable")
        for name in names
    ]

    matrix = (
        matrix
        .withColumn(
            "present_source_count",
            sum(present_cols),
        )
        .withColumn(
            "usable_source_count",
            sum(usable_cols),
        )
        .withColumn(
            "all_five_present",
            F.when(
                F.col("present_source_count") == 5,
                1,
            ).otherwise(0),
        )
        .withColumn(
            "all_five_usable",
            F.when(
                F.col("usable_source_count") == 5,
                1,
            ).otherwise(0),
        )
        .withColumn(
            "year",
            F.year("hour_start"),
        )
    )

    return matrix


def summarize(matrix):
    names = list(CONFIG.keys())

    aggregations = [
        F.count("*").alias("total_hours"),
        F.sum("all_five_present").alias(
            "all_five_present_hours"
        ),
        F.sum("all_five_usable").alias(
            "all_five_usable_hours"
        ),
        F.sum("tide_recovered").alias(
            "tide_recovered_hours"
        ),
        F.sum("water_level_incomplete").alias(
            "water_level_incomplete_hours"
        ),
        F.sum("water_level_off_grid").alias(
            "water_level_off_grid_hours"
        ),
    ]

    for name in names:
        aggregations.extend([
            F.sum(f"{name}_present").alias(
                f"{name}_present_hours"
            ),
            F.sum(f"{name}_usable").alias(
                f"{name}_usable_hours"
            ),
        ])

    summary = matrix.agg(*aggregations).first().asDict()

    total = summary["total_hours"]

    summary["all_five_present_pct"] = round(
        100 * summary["all_five_present_hours"] / total,
        4,
    )

    summary["all_five_usable_pct"] = round(
        100 * summary["all_five_usable_hours"] / total,
        4,
    )

    for name in names:
        for suffix in ["present", "usable"]:
            count = summary[f"{name}_{suffix}_hours"]
            summary[f"{name}_{suffix}_pct"] = round(
                100 * count / total,
                4,
            )

    return summary


def yearly_summary(matrix):
    aggs = [
        F.count("*").alias("expected_hours"),
        F.sum("all_five_present").alias(
            "all_five_present_hours"
        ),
        F.sum("all_five_usable").alias(
            "all_five_usable_hours"
        ),
    ]

    for name in CONFIG:
        aggs.extend([
            F.sum(f"{name}_present").alias(
                f"{name}_present_hours"
            ),
            F.sum(f"{name}_usable").alias(
                f"{name}_usable_hours"
            ),
        ])

    return (
        matrix.groupBy("year")
        .agg(*aggs)
        .withColumn(
            "all_five_present_pct",
            F.round(
                F.col("all_five_present_hours")
                * 100 / F.col("expected_hours"),
                2,
            ),
        )
        .withColumn(
            "all_five_usable_pct",
            F.round(
                F.col("all_five_usable_hours")
                * 100 / F.col("expected_hours"),
                2,
            ),
        )
        .orderBy("year")
    )


def contiguous_segments(matrix, flag):
    # Find continuous hourly runs where flag == 1.
    selected = matrix.filter(F.col(flag) == 1)

    w = Window.orderBy("hour_start")

    numbered = (
        selected
        .select("hour_start")
        .withColumn("rn", F.row_number().over(w))
        .withColumn(
            "segment_id",
            F.unix_timestamp("hour_start")
            - F.col("rn") * F.lit(3600),
        )
    )

    return (
        numbered.groupBy("segment_id")
        .agg(
            F.min("hour_start").alias("start"),
            F.max("hour_start").alias("end"),
            F.count("*").alias("hours"),
        )
        .orderBy(F.desc("hours"), "start")
    )


def serialize(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize(v) for v in value]
    return value


def save_csv(path, rows):
    require(bool(rows), "Yearly report is empty")

    with path.open(
        "w", newline="", encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=list(rows[0].keys())
        )
        writer.writeheader()
        writer.writerows(rows)


def main():
    spark = None

    try:
        spark = create_spark_session(
            app_name="cross-source-coverage",
            master="local[2]",
        )

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.conf.set(
            "spark.sql.shuffle.partitions", "4"
        )

        matrix = build_matrix(spark).cache()

        count = matrix.count()

        require(
            count == EXPECTED_HOURS,
            f"Matrix rows={count}, "
            f"expected={EXPECTED_HOURS}",
        )

        summary = summarize(matrix)
        yearly = yearly_summary(matrix)

        present_segments = contiguous_segments(
            matrix, "all_five_present"
        )

        usable_segments = contiguous_segments(
            matrix, "all_five_usable"
        )

        top_present = [
            r.asDict()
            for r in present_segments.limit(10).collect()
        ]

        top_usable = [
            r.asDict()
            for r in usable_segments.limit(10).collect()
        ]

        yearly_rows = [
            r.asDict()
            for r in yearly.collect()
        ]

        print("\n" + "=" * 70)
        print("CROSS-SOURCE COVERAGE SUMMARY")
        print("=" * 70)

        for key, value in summary.items():
            print(f"{key}: {value}")

        print("\nYEARLY COVERAGE")
        yearly.show(20, truncate=False)

        print("\nTOP CONTIGUOUS ALL-FIVE-PRESENT SEGMENTS")
        present_segments.drop("segment_id").show(
            10, truncate=False
        )

        print("\nTOP CONTIGUOUS ALL-FIVE-USABLE SEGMENTS")
        usable_segments.drop("segment_id").show(
            10, truncate=False
        )

        OUTPUT_DIR.mkdir(
            parents=True, exist_ok=True
        )

        report = {
            "step": "8.6.4",
            "name": "Cross-source Coverage Analysis",
            "generated_at": datetime.now().isoformat(),
            "canonical_start": START,
            "canonical_end": END,
            "expected_hours": EXPECTED_HOURS,
            "quality_policy": {
                name: {
                    "measurement": cfg["value"],
                    "accepted_flags": cfg["accepted"],
                }
                for name, cfg in CONFIG.items()
            },
            "summary": summary,
            "yearly": yearly_rows,
            "top_present_segments": top_present,
            "top_usable_segments": top_usable,
        }

        with JSON_PATH.open(
            "w", encoding="utf-8"
        ) as f:
            json.dump(
                serialize(report),
                f,
                ensure_ascii=False,
                indent=2,
            )

        save_csv(CSV_PATH, yearly_rows)

        print("\nCROSS-SOURCE COVERAGE: COMPLETED")
        print("JSON:", JSON_PATH)
        print("CSV :", CSV_PATH)

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
