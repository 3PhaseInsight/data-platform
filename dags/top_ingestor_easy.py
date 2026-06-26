from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import threephi_framework.db.db as threephi_db

def load_seed():
    import subprocess
    import os

    result = subprocess.run(
        [
            "psql",
            f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}",
            "-f", "/opt/airflow/data/seed.sql",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed:\n{result.stderr}")

with DAG(
    dag_id="seed_topology",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="load_seed_sql",
        python_callable=load_seed,
    )