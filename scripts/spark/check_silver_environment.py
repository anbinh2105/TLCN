
import os
import importlib.util

required = [
    "SPARK_MODE",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_BUCKET",
    "HIVE_METASTORE_URI",
]

print("=== SILVER ENVIRONMENT CHECK ===")

for key in required:
    status = "SET" if os.getenv(key) else "MISSING"
    print(f"{key}: {status}")

print(
    "python-dotenv:",
    "AVAILABLE" if importlib.util.find_spec("dotenv") else "MISSING",
)

print(
    "SPARK_MODE_IS_DOCKER:",
    os.getenv("SPARK_MODE") == "docker",
)
