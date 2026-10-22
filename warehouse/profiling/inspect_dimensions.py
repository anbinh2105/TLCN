from transformation.silver.common import create_spark_session, stop_spark
from pyspark.sql import functions as F

spark = create_spark_session(
    app_name="warehouse-dimension-discovery",
    master="local[2]"
)

try:
    df = spark.table("lakehouse.gold.hourly_hydrology")

    for col_name in [
        "location_id",
        "water_level_station_code",
        "discharge_station",
        "tide_reference_location",
        "tide_publisher",
    ]:
        print(f"\n=== {col_name} ===")
        df.groupBy(col_name).count().orderBy(
            F.desc("count")
        ).show(30, truncate=False)

finally:
    stop_spark(spark)
