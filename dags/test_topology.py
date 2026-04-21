import datetime

from utils import load_dag_config

from threephi_framework import TopologyTester
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def topology_testing():
    config = load_dag_config(__file__)
    with TopologyTester(config) as app:
        app.run()


# Default DAG argsL
default_args = {
    "owner": "inilab",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Define DAG
with DAG(
    dag_id="test_topology",
    description="Test Topology Data",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:
    test_topology_task = PythonOperator(
        task_id="test_topology",
        python_callable=topology_testing,
        doc_md="""
        ## Test Topology
        """,
    )

    test_topology_task
