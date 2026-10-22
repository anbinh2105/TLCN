from transformation.silver.common import (
    create_spark_session,
    ensure_silver_namespace,
    get_bronze_path,
    get_silver_table,
    print_environment_summary,
    stop_spark,
)


def main():
    spark = None

    try:
        print("=" * 70)
        print("SILVER COMMON CONFIGURATION TEST")
        print("=" * 70)

        # --------------------------------------------------
        # 1. Create SparkSession
        # --------------------------------------------------
        spark = create_spark_session(
            app_name="silver-common-test",
            master="local[1]",
        )

        print_environment_summary(spark)

        # --------------------------------------------------
        # 2. Test path helpers
        # --------------------------------------------------
        weather_bronze = get_bronze_path("weather")
        weather_silver = get_silver_table("weather")

        print(f"Bronze path : {weather_bronze}")
        print(f"Silver table: {weather_silver}")

        assert weather_bronze == (
            "s3a://flood-lakehouse/bronze/weather/"
        )

        assert weather_silver == (
            "lakehouse.silver.weather"
        )

        print("Path helpers: PASS")

        # --------------------------------------------------
        # 3. Test Silver namespace
        # --------------------------------------------------
        ensure_silver_namespace(spark)

        namespaces = spark.sql(
            "SHOW NAMESPACES IN lakehouse"
        )

        print("\nNamespaces:")
        namespaces.show(truncate=False)

        namespace_names = [
            row["namespace"]
            for row in namespaces.collect()
        ]

        assert "silver" in namespace_names

        print("Silver namespace: PASS")

        # --------------------------------------------------
        # 4. Test MinIO Bronze access
        # --------------------------------------------------
        jvm = spark.sparkContext._jvm
        hadoop_conf = (
            spark.sparkContext
            ._jsc
            .hadoopConfiguration()
        )

        bronze_path = jvm.org.apache.hadoop.fs.Path(
            weather_bronze
        )

        filesystem = bronze_path.getFileSystem(
            hadoop_conf
        )

        exists = filesystem.exists(bronze_path)

        print(f"Bronze weather exists: {exists}")

        assert exists, (
            f"Bronze path does not exist: {weather_bronze}"
        )

        print("MinIO Bronze access: PASS")

        # --------------------------------------------------
        # FINAL
        # --------------------------------------------------
        print()
        print("=" * 70)
        print("SILVER COMMON TEST: PASS")
        print("=" * 70)

    except Exception as exc:
        print()
        print("=" * 70)
        print("SILVER COMMON TEST: FAIL")
        print("=" * 70)
        print(f"{type(exc).__name__}: {exc}")
        raise

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()