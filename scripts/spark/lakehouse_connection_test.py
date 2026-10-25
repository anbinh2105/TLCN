
from pyspark.sql import SparkSession
import os

def main():
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    spark = (
        SparkSession.builder
        .appName("tlcn-lakehouse-connection-test")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .getOrCreate()
    )

    try:
        print("Spark version:", spark.version)
        print("Spark master:", spark.sparkContext.master)

        print("\n=== ICEBERG NAMESPACES ===")
        spark.sql(
            "SHOW NAMESPACES IN lakehouse"
        ).show(truncate=False)

        print("\n=== SILVER TABLES ===")
        spark.sql(
            "SHOW TABLES IN lakehouse.silver"
        ).show(truncate=False)

        
        print("\n=== ICEBERG DATA READ TEST ===")

        table_name = "lakehouse.silver.weather"

        df = spark.table(table_name)

        print("Table:", table_name)

        print("\nSchema:")
        df.printSchema()

        print("\nSample data:")
        df.limit(5).show(truncate=False)

        print("\nICEBERG DATA READ TEST: PASS")

        print("\nLAKEHOUSE CONNECTION TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
