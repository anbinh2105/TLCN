"""
Common utilities for Bronze -> Silver transformations.

Project:
    Can Tho Flood Risk Data Lakehouse

Architecture:
    MinIO -> Apache Spark -> Apache Iceberg -> Hive Metastore

Responsibilities:
    - Create a reusable SparkSession.
    - Configure Apache Iceberg with Hive Catalog.
    - Configure MinIO access through Hadoop S3A.
    - Define common Bronze/Silver paths.
    - Define common quality flags.
    - Provide helper functions shared by Silver pipelines.

Important:
    Dataset-specific transformation logic MUST NOT be placed here.
    For example:
        - Tide Excel-date recovery -> tide_to_silver.py
        - Weather cleaning        -> weather_to_silver.py
        - Rainfall rules          -> rainfall_to_silver.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. PROJECT PATHS
# ============================================================

# common.py:
# TLCN/transformation/silver/common.py
#
# parents[0] -> silver
# parents[1] -> transformation
# parents[2] -> TLCN
PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_FILE = PROJECT_ROOT / ".env"


# ============================================================
# 2. LOAD .env
# ============================================================

try:
    from dotenv import load_dotenv

    load_dotenv(ENV_FILE)
except ImportError:
    # python-dotenv is already part of the project environment,
    # but this keeps the error message explicit if environment changes.
    raise RuntimeError(
        "python-dotenv is not installed. "
        "Run: python -m pip install python-dotenv"
    )


# ============================================================
# 3. ENVIRONMENT CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flood-lakehouse")

MINIO_SECURE = (
    os.getenv("MINIO_SECURE", "false")
    .strip()
    .lower()
    in {"1", "true", "yes"}
)

# Hive Metastore
HIVE_METASTORE_URI = os.getenv(
    "HIVE_METASTORE_URI",
    "thrift://localhost:9083",
)

# Iceberg catalog
ICEBERG_CATALOG = os.getenv(
    "ICEBERG_CATALOG",
    "lakehouse",
)

# Warehouse is stored in MinIO.
ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    f"s3a://{MINIO_BUCKET}/warehouse",
)

# Canonical timezone for the project.
TIMEZONE = "Asia/Ho_Chi_Minh"


# ============================================================
# 4. SPARK / ICEBERG DEPENDENCIES
# ============================================================

# IMPORTANT:
# These versions match the stack that has already passed the
# Spark + Iceberg + Hive Metastore + MinIO integration test.
#
# Spark side:
#   Spark       : 4.1.1
#   Hadoop      : 3.4.2
#   Scala       : 2.13
#   Iceberg     : 1.12.0

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-4.1_2.13:1.12.0"
)

SPARK_PACKAGES = ",".join(
    [
        HADOOP_AWS_PACKAGE,
        ICEBERG_PACKAGE,
    ]
)


# ============================================================
# 5. QUALITY FLAGS
# ============================================================

QUALITY_OBSERVED = "OBSERVED"
QUALITY_RECOVERED = "RECOVERED"
QUALITY_IMPUTED = "IMPUTED"
QUALITY_MISSING = "MISSING"
QUALITY_INVALID = "INVALID"

VALID_QUALITY_FLAGS = {
    QUALITY_OBSERVED,
    QUALITY_RECOVERED,
    QUALITY_IMPUTED,
    QUALITY_MISSING,
    QUALITY_INVALID,
}


# ============================================================
# 6. DATA SOURCES
# ============================================================

VALID_SOURCES = {
    "weather",
    "rainfall",
    "water_level",
    "river_discharge",
    "tide",
}


# ============================================================
# 7. LOCAL SPARK ENVIRONMENT
# ============================================================

def configure_local_spark_environment() -> None:
    """
    Configure the local Python + Hadoop environment before
    SparkSession is created.

    Required mainly for local Windows development.

    Configures:
        - PYSPARK_PYTHON
        - PYSPARK_DRIVER_PYTHON
        - HADOOP_HOME
        - hadoop.home.dir
        - PATH containing winutils.exe
    """

    # --------------------------------------------------------
    # Python worker
    # --------------------------------------------------------

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    # --------------------------------------------------------
    # Hadoop / winutils for Windows
    # --------------------------------------------------------

    if os.name == "nt":

        hadoop_home = Path(
            os.getenv("HADOOP_HOME", r"C:\hadoop")
        )

        hadoop_bin = hadoop_home / "bin"
        winutils = hadoop_bin / "winutils.exe"

        if not hadoop_home.exists():
            raise RuntimeError(
                f"HADOOP_HOME does not exist: {hadoop_home}"
            )

        if not winutils.exists():
            raise RuntimeError(
                f"winutils.exe not found: {winutils}"
            )

        # Hadoop checks these values when Spark starts.
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)

        # Add C:\\hadoop\\bin to PATH only once.
        current_path = os.environ.get("PATH", "")

        path_entries = current_path.split(os.pathsep)

        if str(hadoop_bin) not in path_entries:
            os.environ["PATH"] = (
                str(hadoop_bin)
                + os.pathsep
                + current_path
            )


# ============================================================
# 8. CREATE SPARK SESSION
# ============================================================


def create_spark_session(
    app_name: str = "cantho-flood-silver",
    master: str | None = None,
) -> SparkSession:
    """
    Create SparkSession for Windows Local or Docker Standalone.

    Local:
        - master defaults to local[2]
        - use Maven packages
        - configure Windows Hadoop/Python environment

    Docker:
        - master supplied by spark-submit
        - dependencies already baked into Spark image
        - memory/resources supplied by spark-submit
    """

    spark_mode = os.getenv("SPARK_MODE", "local").strip().lower()

    if spark_mode not in {"local", "docker"}:
        raise ValueError(
            "SPARK_MODE must be 'local' or 'docker'"
        )

    if spark_mode == "local":
        configure_local_spark_environment()

    protocol = "https" if MINIO_SECURE else "http"
    minio_url = f"{protocol}://{MINIO_ENDPOINT}"

    builder = SparkSession.builder.appName(app_name)

    # --------------------------------------------------------
    # Local Spark settings
    # --------------------------------------------------------
    if spark_mode == "local":
        builder = (
            builder
            .master(master or "local[2]")
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
        )

    # --------------------------------------------------------
    # Docker Spark settings
    # --------------------------------------------------------
    else:
        # Do not override --master unless explicitly requested.
        if master is not None:
            builder = builder.master(master)

    # --------------------------------------------------------
    # Shared Spark settings
    # --------------------------------------------------------
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", TIMEZONE)

        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )

        # Iceberg Hive Catalog
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.type",
            "hive",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri",
            HIVE_METASTORE_URI,
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE,
        )

        # MinIO / Hadoop S3A
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_url,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
    )

    spark = builder.getOrCreate()

    spark.conf.set(
        "spark.sql.session.timeZone",
        TIMEZONE,
    )

    return spark

# ============================================================
# 9. BRONZE PATH HELPERS
# ============================================================

def get_bronze_path(source: str) -> str:
    """
    Return the Bronze S3A path for a source.

    Example
    -------
    get_bronze_path("weather")

    ->
    s3a://flood-lakehouse/bronze/weather/
    """

    source = source.strip().lower()

    if source not in VALID_SOURCES:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Expected one of: {sorted(VALID_SOURCES)}"
        )

    return (
        f"s3a://{MINIO_BUCKET}/"
        f"bronze/{source}/"
    )


# ============================================================
# 10. SILVER TABLE HELPERS
# ============================================================

def get_silver_table(source: str) -> str:
    """
    Return the fully-qualified Iceberg Silver table name.

    Example
    -------
    get_silver_table("weather")

    ->
    lakehouse.silver.weather
    """

    source = source.strip().lower()

    if source not in VALID_SOURCES:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Expected one of: {sorted(VALID_SOURCES)}"
        )

    return (
        f"{ICEBERG_CATALOG}."
        f"silver."
        f"{source}"
    )


# ============================================================
# 11. CREATE SILVER NAMESPACE
# ============================================================

def ensure_silver_namespace(
    spark: SparkSession,
) -> None:
    """
    Ensure the Iceberg 'silver' namespace exists.

    Physical warehouse metadata/data remains in MinIO.
    Catalog metadata is registered through Hive Metastore.
    """

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG}.silver"
    )


# ============================================================
# 12. COMMON METADATA COLUMNS
# ============================================================

def add_silver_metadata(
    df: DataFrame,
    source: str,
    source_file_column: Optional[str] = None,
) -> DataFrame:
    """
    Add standard metadata columns used by Silver tables.

    Added columns:
        source
        source_file
        silver_processed_at

    Parameters
    ----------
    df:
        Input Spark DataFrame.

    source:
        Logical source name.

    source_file_column:
        Existing column containing the source filename.
        If omitted, Spark input_file_name() is used.

    Notes
    -----
    This function does NOT create quality_flag because quality
    status depends on dataset-specific transformation logic.
    """

    source = source.strip().lower()

    if source not in VALID_SOURCES:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Expected one of: {sorted(VALID_SOURCES)}"
        )

    result = df.withColumn(
        "source",
        F.lit(source),
    )

    if source_file_column and source_file_column in result.columns:
        result = result.withColumn(
            "source_file",
            F.col(source_file_column),
        )
    elif "source_file" not in result.columns:
        result = result.withColumn(
            "source_file",
            F.input_file_name(),
        )

    result = result.withColumn(
        "silver_processed_at",
        F.current_timestamp(),
    )

    return result


# ============================================================
# 13. EXACT DUPLICATE HANDLING
# ============================================================

def remove_exact_duplicates(
    df: DataFrame,
) -> DataFrame:
    """
    Remove rows that are completely identical.

    IMPORTANT:
    This is ONLY for exact row duplicates.

    Conflicting duplicate timestamps/business keys must be
    handled by the individual source pipeline and must NOT be
    silently dropped here.
    """

    return df.dropDuplicates()


# ============================================================
# 14. DATAFRAME SUMMARY
# ============================================================

def print_dataframe_summary(
    df: DataFrame,
    name: str,
) -> None:
    """
    Print basic information useful during Silver development.
    """

    print()
    print("=" * 70)
    print(f"DATAFRAME SUMMARY: {name}")
    print("=" * 70)

    print(f"Columns : {len(df.columns)}")
    print(f"Records : {df.count()}")

    print()
    print("Schema:")
    df.printSchema()

    print("=" * 70)
    print()


# ============================================================
# 15. SPARK ENVIRONMENT SUMMARY
# ============================================================

def print_environment_summary(
    spark: SparkSession,
) -> None:
    """
    Print the important runtime configuration.
    Secrets are intentionally not printed.
    """

    sc = spark.sparkContext

    print()
    print("=" * 70)
    print("CAN THO FLOOD LAKEHOUSE - SILVER ENVIRONMENT")
    print("=" * 70)

    print(f"Spark version : {spark.version}")
    print(
        "Hadoop version: "
        f"{sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}"
    )
    print(
        "Scala version : "
        f"{sc._jvm.scala.util.Properties.versionNumberString()}"
    )

    print(f"Python        : {sys.executable}")
    print(f"Timezone      : {spark.conf.get('spark.sql.session.timeZone')}")
    print(f"Hive URI      : {HIVE_METASTORE_URI}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket        : {MINIO_BUCKET}")
    print(f"Warehouse     : {ICEBERG_WAREHOUSE}")
    print(f"Catalog       : {ICEBERG_CATALOG}")

    print("=" * 70)
    print()


# ============================================================
# 16. SAFE SPARK SHUTDOWN
# ============================================================

def stop_spark(
    spark: Optional[SparkSession],
) -> None:
    """
    Stop Spark safely.

    Windows may occasionally report warnings while deleting
    temporary Spark JAR files. Those cleanup warnings do not
    imply that an Iceberg write failed.
    """

    if spark is not None:
        try:
            spark.stop()
        except Exception as exc:
            print(
                f"[WARN] Spark shutdown encountered "
                f"a non-fatal error: {exc}"
            )