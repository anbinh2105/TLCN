import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# ============================================================
# PYTHON ENVIRONMENT
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# LOAD ENV
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY"
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "flood-lakehouse"
)


if not MINIO_ACCESS_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY is missing")

if not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_SECRET_KEY is missing")


# ============================================================
# CREATE SPARK
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("CanThoFlood-MinIO-Test")
        .master("local[*]")

        # S3A connector
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2"
        )

        # Timezone
        .config(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        # MinIO
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{MINIO_ENDPOINT}"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .getOrCreate()
    )

    return spark


# ============================================================
# TEST
# ============================================================

def main():

    spark = create_spark()

    print("=" * 70)
    print("SPARK -> MINIO S3A TEST")
    print("=" * 70)

    hadoop_version = (
        spark.sparkContext
        ._jvm
        .org.apache.hadoop.util.VersionInfo
        .getVersion()
    )

    print("Spark version :", spark.version)
    print("Hadoop version:", hadoop_version)
    print("Python        :", sys.executable)
    print("MinIO endpoint:", MINIO_ENDPOINT)
    print("Bucket        :", MINIO_BUCKET)

    # --------------------------------------------------------
    # Create test dataframe
    # --------------------------------------------------------

    df = spark.createDataFrame(
        [
            ("weather", "PASS"),
            ("rainfall", "PASS"),
            ("water_level", "PASS"),
            ("river_discharge", "PASS"),
            ("tide", "PASS"),
        ],
        ["source", "status"],
    )

    print("\nTest DataFrame:")

    df.show(
        truncate=False
    )

    # --------------------------------------------------------
    # MinIO test path
    # --------------------------------------------------------

    test_path = (
        f"s3a://{MINIO_BUCKET}/"
        "spark-tests/s3a-connectivity/"
    )

    print("\nWriting to:")
    print(test_path)

    # --------------------------------------------------------
    # WRITE
    # --------------------------------------------------------

    (
        df.write
        .mode("overwrite")
        .parquet(test_path)
    )

    print("\nWRITE: PASS")

    # --------------------------------------------------------
    # READ
    # --------------------------------------------------------

    print("\nReading back from MinIO...")

    result = (
        spark.read
        .parquet(test_path)
    )

    result.show(
        truncate=False
    )

    count = result.count()

    print("READ : PASS")
    print("COUNT:", count)

    if count != 5:
        raise RuntimeError(
            f"Expected 5 rows, found {count}"
        )

    print()
    print("=" * 70)
    print("SPARK -> MINIO S3A: PASS")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()