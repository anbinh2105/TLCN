import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# ============================================================
# WINDOWS / PYTHON ENVIRONMENT
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]


# ============================================================
# LOAD ENV
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "flood-lakehouse"
)

if not MINIO_ACCESS_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY is missing")

if not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_SECRET_KEY is missing")


# ============================================================
# CONSTANTS
# ============================================================

CATALOG_NAME = "lakehouse"

HIVE_URI = "thrift://localhost:9083"

WAREHOUSE = (
    f"s3a://{MINIO_BUCKET}/warehouse"
)

PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.4.2",
        "org.apache.iceberg:"
        "iceberg-spark-runtime-4.1_2.13:1.12.0",
    ]
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("CanThoFlood-Iceberg-Hive-Test")
        .master("local[*]")

        # ----------------------------------------------------
        # Dependencies
        # ----------------------------------------------------
        .config(
            "spark.jars.packages",
            PACKAGES
        )

        # ----------------------------------------------------
        # Timezone
        # ----------------------------------------------------
        .config(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        # ----------------------------------------------------
        # Iceberg extensions
        # ----------------------------------------------------
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions"
        )

        # ----------------------------------------------------
        # Iceberg + Hive Metastore Catalog
        # ----------------------------------------------------
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.type",
            "hive"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.uri",
            HIVE_URI
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            WAREHOUSE
        )

        # ----------------------------------------------------
        # MinIO / S3A
        # ----------------------------------------------------
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

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# MAIN TEST
# ============================================================

def main():

    spark = create_spark()

    try:

        print("=" * 70)
        print("SPARK + ICEBERG + HIVE METASTORE + MINIO TEST")
        print("=" * 70)

        hadoop_version = (
            spark.sparkContext
            ._jvm
            .org.apache.hadoop.util.VersionInfo
            .getVersion()
        )

        scala_version = (
            spark.sparkContext
            ._jvm
            .scala.util.Properties
            .versionNumberString()
        )

        print("Spark version :", spark.version)
        print("Hadoop version:", hadoop_version)
        print("Scala version :", scala_version)
        print("Python        :", sys.executable)

        print("Hive URI      :", HIVE_URI)
        print("MinIO endpoint:", MINIO_ENDPOINT)
        print("Bucket        :", MINIO_BUCKET)
        print("Warehouse     :", WAREHOUSE)
        print("Catalog       :", CATALOG_NAME)

        # ====================================================
        # 1. CREATE NAMESPACE
        # ====================================================

        print("\n[1] Creating namespace...")

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            {CATALOG_NAME}.hive_test_db
            """
        )

        print("Namespace: PASS")

        # ====================================================
        # 2. DROP OLD TEST TABLE
        # ====================================================

        spark.sql(
            f"""
            DROP TABLE IF EXISTS
            {CATALOG_NAME}.hive_test_db.iceberg_hive_test
            """
        )

        # ====================================================
        # 3. CREATE TABLE
        # ====================================================

        print("\n[2] Creating Iceberg table...")

        spark.sql(
            f"""
            CREATE TABLE
            {CATALOG_NAME}.hive_test_db.iceberg_hive_test
            (
                id BIGINT,
                source STRING,
                status STRING
            )
            USING iceberg
            """
        )

        print("CREATE TABLE: PASS")

        # ====================================================
        # 4. INSERT
        # ====================================================

        print("\n[3] Inserting records...")

        spark.sql(
            f"""
            INSERT INTO
            {CATALOG_NAME}.hive_test_db.iceberg_hive_test

            VALUES
                (1, 'weather', 'PASS'),
                (2, 'rainfall', 'PASS'),
                (3, 'water_level', 'PASS'),
                (4, 'river_discharge', 'PASS'),
                (5, 'tide', 'PASS')
            """
        )

        print("INSERT: PASS")

        # ====================================================
        # 5. READ
        # ====================================================

        print("\n[4] Reading Iceberg table through Hive Catalog...")

        df = spark.sql(
            f"""
            SELECT *
            FROM
            {CATALOG_NAME}.hive_test_db.iceberg_hive_test
            ORDER BY id
            """
        )

        df.show(truncate=False)

        count = df.count()

        print("READ : PASS")
        print("COUNT:", count)

        if count != 5:
            raise RuntimeError(
                f"Expected 5 rows, found {count}"
            )

        # ====================================================
        # 6. SNAPSHOTS
        # ====================================================

        print("\n[5] Iceberg snapshots:")

        spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                operation

            FROM
                {CATALOG_NAME}.hive_test_db.
                iceberg_hive_test.snapshots
            """
        ).show(truncate=False)

        # ====================================================
        # 7. SHOW NAMESPACES
        # ====================================================

        print("\n[6] Hive Catalog namespaces:")

        spark.sql(
            f"""
            SHOW NAMESPACES
            IN {CATALOG_NAME}
            """
        ).show(truncate=False)

        # ====================================================
        # 8. SHOW TABLES
        # ====================================================

        print("\n[7] Tables registered in Hive Catalog:")

        spark.sql(
            f"""
            SHOW TABLES
            IN {CATALOG_NAME}.hive_test_db
            """
        ).show(truncate=False)

        print()
        print("=" * 70)
        print(
            "SPARK + ICEBERG + HIVE METASTORE + MINIO: PASS"
        )
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()