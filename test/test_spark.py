from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("CanThoFlood-SparkTest")
        .master("local[*]")
        .config(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )
        .getOrCreate()
    )

    print("=" * 60)
    print("SPARK ENVIRONMENT TEST")
    print("=" * 60)

    print("Spark version :", spark.version)
    print(
        "Timezone      :",
        spark.conf.get(
            "spark.sql.session.timeZone"
        )
    )

    df = spark.createDataFrame(
        [
            ("weather", 1),
            ("rainfall", 2),
            ("water_level", 3),
            ("river_discharge", 4),
            ("tide", 5),
        ],
        ["source", "id"],
    )

    df.show(truncate=False)

    print("Record count  :", df.count())

    spark.stop()


if __name__ == "__main__":
    main()