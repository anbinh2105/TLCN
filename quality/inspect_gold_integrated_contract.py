
from pathlib import Path
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

TABLES = {
    "weather": "lakehouse.gold.weather_hourly",
    "rainfall": "lakehouse.gold.rainfall_hourly",
    "discharge": "lakehouse.gold.discharge_hourly",
    "water_level": "lakehouse.gold.water_level_hourly",
    "tide": "lakehouse.gold.tide_hourly",
}

OUTPUT = Path("reports/gold/quality/gold_schema_inventory.json")

def main():
    spark = None
    try:
        spark = create_spark_session(
            app_name="gold-integrated-contract-inspection",
            master="local[2]",
        )
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables": {},
        }

        for source, table in TABLES.items():
            print("\n" + "=" * 75)
            print(f"SOURCE: {source}")
            print(f"TABLE : {table}")

            if not spark.catalog.tableExists(table):
                raise RuntimeError(f"Missing table: {table}")

            df = spark.table(table)
            df.printSchema()
            df.show(5, truncate=False)

            columns = df.columns
            if "hour_start" not in columns:
                raise RuntimeError(
                    f"{table}: missing hour_start"
                )

            stats = df.agg(
                F.count("*").alias("row_count"),
                F.countDistinct("hour_start").alias(
                    "distinct_hours"
                ),
                F.min("hour_start").alias("min_hour"),
                F.max("hour_start").alias("max_hour"),
            ).first()

            quality_columns = [
                c for c in columns
                if "quality" in c.lower()
                or "alignment" in c.lower()
            ]

            quality_values = {}
            for col_name in quality_columns:
                rows = (
                    df.groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(30)
                    .collect()
                )
                quality_values[col_name] = [
                    {
                        "value": str(r[col_name]),
                        "count": r["count"],
                    }
                    for r in rows
                ]

            station_columns = [
                c for c in columns
                if any(token in c.lower() for token in [
                    "station", "location",
                    "latitude", "longitude",
                    "lat", "lon",
                ])
            ]

            station_samples = {}
            for col_name in station_columns:
                values = (
                    df.select(col_name)
                    .distinct()
                    .limit(20)
                    .collect()
                )
                station_samples[col_name] = [
                    str(r[0]) for r in values
                ]

            report["tables"][source] = {
                "table": table,
                "schema": [
                    {
                        "column": f.name,
                        "datatype": f.dataType.simpleString(),
                        "nullable": f.nullable,
                    }
                    for f in df.schema.fields
                ],
                "row_count": stats["row_count"],
                "distinct_hours": stats["distinct_hours"],
                "min_hour": str(stats["min_hour"]),
                "max_hour": str(stats["max_hour"]),
                "quality_values": quality_values,
                "station_samples": station_samples,
            }

            print("Rows:", stats["row_count"])
            print("Distinct hours:", stats["distinct_hours"])
            print("Quality:", quality_values)
            print("Station/location:", station_samples)

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print("\nREPORT:", OUTPUT)

    finally:
        if spark is not None:
            stop_spark(spark)

if __name__ == "__main__":
    main()
