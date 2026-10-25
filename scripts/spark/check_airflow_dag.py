
import importlib.util

path = "/opt/project/airflow/dags/spark_standalone_smoke_test.py"

spec = importlib.util.spec_from_file_location("smoke_dag", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

dag = module.dag

assert dag.dag_id == "spark_standalone_smoke_test"
assert "submit_spark_smoke_test" in dag.task_ids

print("DAG_IMPORT_PASS")
print("DAG ID:", dag.dag_id)
print("Tasks:", sorted(dag.task_ids))
