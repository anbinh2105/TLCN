from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "37077",
    "spark.blockManager.port": "37078",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.cores.max": "1",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3",
}


@dag(
    dag_id="lakehouse_batch_orchestration",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tlcn",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["tlcn", "batch", "spark", "orchestration"],
    description="Safe orchestration skeleton for TLCN Batch Lakehouse",
)
def lakehouse_batch_orchestration():

    @task
    def preflight():
        print("Preflight completed.")
        print("Execution mode: SAFE / NO ICEBERG WRITES")

    @task
    def ingestion_stage():
        import subprocess
        import sys

        print("Starting Ingestion Batch Dry-run")

        subprocess.run(
            [
                sys.executable,
                "/opt/project/scripts/spark/ingestion_batch_dry_run.py",
            ],
            check=True,
            env={
                **os.environ,
                "PYTHONPATH": "/opt/project",
            },
        )

        print("Ingestion Dry-run completed successfully.")
        print("No Bronze ingestion executed.")

    silver_stage = SparkSubmitOperator(
        task_id="silver_batch_dry_run",
        conn_id="spark_default",
        application="/opt/project/scripts/spark/silver_batch_dry_run.py",
        name="TLCN-Silver-Batch-Dry-Run",
        deploy_mode="client",
        conf=SPARK_CONF,
        env_vars={
            "PYTHONPATH": (
                "/opt/project:"
                "/opt/spark/python:"
                "/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
            ),
        },
        verbose=False,
    )

    gold_stage = SparkSubmitOperator(
        task_id="gold_stage",
        conn_id="spark_default",
        application="/opt/project/transformation/gold/build_hourly_hydrology.py",
        name="TLCN-Gold-Hourly-Hydrology-Dry-Run",
        deploy_mode="client",
        application_args=["--mode", "dry-run"],
        conf=SPARK_CONF,
        env_vars={
            "PYTHONPATH": (
                "/opt/project:"
                "/opt/spark/python:"
                "/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
            ),
            "GOLD_REPORT_PATH": (
                "/tmp/tlcn/gold/hourly_hydrology_dry_run.json"
            ),
        },
        verbose=False,
    )

    warehouse_stage = SparkSubmitOperator(
        task_id="warehouse_stage",
        conn_id="spark_default",
        application="/opt/project/warehouse/quality/validate_warehouse.py",
        name="TLCN-Warehouse-Quality-Read-Only",
        deploy_mode="client",
        conf=SPARK_CONF,
        env_vars={
            "PYTHONPATH": (
                "/opt/project:"
                "/opt/spark/python:"
                "/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
            ),
            "WAREHOUSE_REPORT_DIR": "/tmp/tlcn/warehouse/quality",
        },
        verbose=False,
    )

    @task
    def dbt_stage():
        import os
        import subprocess
        import tempfile
        from pathlib import Path

        project_dir = Path("/opt/project/dbt/flood_analytics")

        with tempfile.TemporaryDirectory(prefix="tlcn-dbt-") as temp_dir:
            temp_path = Path(temp_dir)
            profiles_dir = temp_path / "profiles"
            logs_dir = temp_path / "logs"
            target_dir = temp_path / "target"

            profiles_dir.mkdir()
            logs_dir.mkdir()
            target_dir.mkdir()

            profiles_file = profiles_dir / "profiles.yml"
            profiles_file.write_bytes(
                (project_dir / "profiles.docker.yml").read_bytes()
            )

            env = os.environ.copy()
            env["DBT_TARGET_PATH"] = str(target_dir)

            command = [
                "dbt",
                "ls",
                "--project-dir", str(project_dir),
                "--profiles-dir", str(profiles_dir),
                "--profile", "flood_analytics",
                "--target", "dev",
                "--log-path", str(logs_dir),
                "--resource-type", "model",
            ]

            print("Running dbt model discovery (read-only).", flush=True)

            subprocess.run(
                command,
                env=env,
                check=True,
                timeout=180,
            )

            print("dbt model discovery completed.", flush=True)
            print("No dbt run or dbt build executed.", flush=True)

    
    quality_stage = SparkSubmitOperator(
        task_id="quality_stage",
        conn_id="spark_default",
        application="/opt/project/quality/validate_batch_quality.py",
        name="TLCN-Batch-Quality-Read-Only",
        deploy_mode="client",
        conf=SPARK_CONF,
        env_vars={
            "PYTHONPATH": (
                "/opt/project:"
                "/opt/spark/python:"
                "/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
            ),
            "SPARK_MODE": "docker",
            "BATCH_QUALITY_REPORT_PATH": (
                "/opt/airflow/batch-quality-reports/"
                "batch_quality_validation.json"
            ),
        },
        verbose=False,
    )

    (
        preflight()
        >> ingestion_stage()
        >> silver_stage
        >> gold_stage
        >> warehouse_stage
        >> dbt_stage()
        >> quality_stage
    )


lakehouse_batch_orchestration()
