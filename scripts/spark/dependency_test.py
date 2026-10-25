
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("tlcn-standalone-dependency-test")
        .getOrCreate()
    )

    try:
        print("Spark version:", spark.version)
        print("Spark master:", spark.sparkContext.master)

        jvm = spark._jvm

        classes = {
            "Iceberg SparkCatalog":
                "org.apache.iceberg.spark.SparkCatalog",
            "Hadoop S3AFileSystem":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }

        loader = (
            jvm.java.lang.Thread.currentThread()
            .getContextClassLoader()
        )

        for name, class_name in classes.items():
            loader.loadClass(class_name)
            print(f"{name}: AVAILABLE")

        df = spark.range(1, 1001, numPartitions=2)

        result = df.agg(
            F.count("*").alias("row_count"),
            F.sum("id").alias("id_sum"),
        ).collect()[0]

        print("Row count:", result["row_count"])
        print("ID sum:", result["id_sum"])

        assert result["row_count"] == 1000
        assert result["id_sum"] == 500500

        print("STANDALONE DEPENDENCY TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
